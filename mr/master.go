package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Phase defines the phase of the master (Map, Reduce)
type Phase int

const (
	// MapPhase indicates Master is doing map tasks
	MapPhase Phase = iota

	// ReducePhase indicates master is doing reduced tasks
	ReducePhase

	// DonePhase indicates master is done with the whole job
	DonePhase
)

// TaskTy defines the task type a worker can do
type TaskTy int

const (
	// Map task
	Map TaskTy = iota

	// Reduce task
	Reduce

	// Exit task
	Exit
)

// State defines the current task state
type State int

const (
	// Idle state
	Idle State = iota

	// Working state
	Working

	// Completed state
	Completed
)

// TaskSt defines the structure for a task (state and assign worker)
type TaskSt struct {
	st  State // Task state (idle, working, completed)
	wid WID   // Worker assigned for the task
}

// Task defines the structure for a task worker can do
type Task struct {
	Ty   TaskTy // Type of the taks
	File string // File path to perform tasks
	M    int    // id for map task
	R    int    // id for reduce task/nReduce for map task
}

// Master defines the structure for the master of mapreduce framework
type Master struct {
	mu           sync.Mutex      // Lock to protect the shared data (tasks)
	cond         *sync.Cond      // Cond var for managing available tasks
	tasks        []Task          // Tasks queue to assign
	taskState    map[Task]TaskSt // Table of the task states
	taskRem      int             // Remaining tasks to complete
	intermediate map[int]string  // Intermediate files from map tasks
	phase        Phase           // Current phase of map reduce
}

// timeout checks if the given task is completed in the timeout interval (10s)
func (m *Master) timeout(task Task) {
	time.Sleep(10 * time.Second)

	// Timeout now
	m.mu.Lock()
	defer m.mu.Unlock()

	tst := m.taskState[task]

	// Task is already completed
	if tst.st == Completed {
		return
	}

	// Not completed, reschedule it
	m.tasks = append(m.tasks, task)
	m.taskState[task] = TaskSt{Idle, ""}
	m.cond.Signal()
	return
}

// AssignTask assign a task to the requesting worker
// RPC handler for the worker to call
func (m *Master) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// While no available jobs or not done yet
	for len(m.tasks) == 0 && m.phase != DonePhase {
		m.cond.Wait()
	}

	// Job is done
	if m.phase == DonePhase {
		reply.T = Task{Exit, "", 0, 0}
		return nil
	}

	// There are still tasks to be done
	task := m.tasks[0]                          // Get tasks
	m.tasks = m.tasks[1:]                       // Truncate tasks queue
	m.taskState[task] = TaskSt{Working, args.W} // Assign worker to this task
	reply.T = task                              // Send task back to worker

	// Set timeout func
	go m.timeout(task)

	return nil
}

// CompleteTask gets notified by a worker for completetion of a task
// RPC handler for the worker to call
func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the task is assignd to this worker
	// (calling worker might be assumed dead)
	if m.taskState[args.T].wid != args.W {
		// Remove intermediate files late worker created
		for _, file := range args.F {
			os.Remove(file)
		}

		reply.Message = "You are dead by master! Try to get another task!\n"
		return errors.New("[ERROR] Dead assignment")
	}

	reply.Message = "Task is completed OK!\n"

	// Complete the task on master
	m.taskState[args.T] = TaskSt{Completed, ""} // Set task completed
	fs, rs := args.F, args.R

	// Rename each tmp file
	for i := 0; i < len(fs) && i < len(rs); i++ {
		var oname string
		if m.phase == MapPhase {
			oname = "mr-" + strconv.Itoa(args.T.M) + "-" + strconv.Itoa(rs[i])
		} else {
			oname = "mr-out-" + strconv.Itoa(rs[i])
		}
		os.Rename(fs[i], oname)
	}

	// Potential phase transition
	m.taskRem--

	if m.phase == MapPhase {
		// Add to intermediate files
		for _, r := range rs {
			oname := "mr-" + strconv.Itoa(args.T.M) + "-" + strconv.Itoa(r)
			if file, ok := m.intermediate[r]; ok {
				m.intermediate[r] = file + " " + oname
			} else {
				m.intermediate[r] = oname
			}
		}

		// Map phase done
		if m.taskRem == 0 {
			m.initReduceTasks()
			m.phase = ReducePhase
			m.cond.Broadcast()
		}
	} else {
		// Tasks all done
		if m.taskRem == 0 {
			m.phase = DonePhase
			m.cond.Broadcast()
		}
	}

	return nil
}

// initMapTasks initializes the map tasks that are needed to be done
func (m *Master) initMapTasks(files []string, nReduce int) {
	m.cond = sync.NewCond(&m.mu)
	m.taskRem = len(files)
	m.tasks = []Task{}
	m.intermediate = make(map[int]string)
	m.taskState = make(map[Task]TaskSt)
	m.phase = MapPhase

	// Initializing map tasks
	c := 0
	for _, file := range files {
		task := Task{Map, file, c, nReduce}
		m.tasks = append(m.tasks, task)
		m.taskState[task] = TaskSt{Idle, ""}
		c++
	}
}

// initReduceTasks initializes the map tasks that are need to be done
func (m *Master) initReduceTasks() {
	for r, file := range m.intermediate {
		m.taskRem++
		task := Task{Reduce, file, 0, r}
		m.tasks = append(m.tasks, task)
		m.taskState[task] = TaskSt{Idle, ""}
	}
}

// server starts a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done periodically to find out
// if the entire job has finished
// main/mrmaster.go calls this function
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := false
	if m.phase == DonePhase {
		ret = true

		// Remove intermediate files
		for _, fn := range m.intermediate {
			ff := func(r rune) bool { return r == ' ' }
			files := strings.FieldsFunc(fn, ff)

			for _, file := range files {
				os.Remove(file)
			}
		}
	}

	return ret
}

// MakeMaster create a Master
// main/mrmaster.go calls this function
// nReduce is the number of reduce tasks to use
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Initialize map tasks
	m.initMapTasks(files, nReduce)

	// Start master server
	m.server()

	return &m
}

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

	// Retry task
	Retry

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
	st  State
	wid WID
}

// Task defines the structure for a task worker can do
type Task struct {
	Ty   TaskTy
	File string
	M    int
	R    int
}

// Master defines the structure for the master of mapreduce framework
type Master struct {
	mu           sync.Mutex
	taskCh       chan Task
	taskState    map[Task]TaskSt
	taskAvail    int
	taskRem      int
	intermediate map[int]string
	phase        Phase
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
	m.taskAvail++
	m.taskCh <- task
	m.taskState[task] = TaskSt{Idle, ""}
	return
}

// AssignTask assign a task to the requesting worker
// RPC handler for the worker to call
func (m *Master) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Job is done
	if m.phase == DonePhase {
		reply.T = Task{Exit, "", 0, 0}
		return nil
	}

	// No available jobs to be assigned
	if m.taskAvail == 0 {
		reply.T = Task{Retry, "", 0, 0}
		return nil
	}

	// There are still tasks to be done
	task := <-m.taskCh                          // Get tasks from channel
	m.taskState[task] = TaskSt{Working, args.W} // Assign worker to this task
	reply.T = task                              // Send task back to worker
	m.taskAvail--

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
		}
	} else {
		// Tasks all done
		if m.taskRem == 0 {
			m.phase = DonePhase
		}
	}

	return nil
}

// initMapTasks initializes the map tasks that are needed to be done
func (m *Master) initMapTasks(files []string, nReduce int) {
	m.taskAvail = len(files)
	m.taskRem = m.taskAvail
	m.intermediate = make(map[int]string)

	var nTasks int
	if m.taskAvail < nReduce {
		nTasks = nReduce
	} else {
		nTasks = m.taskAvail
	}
	m.taskCh = make(chan Task, nTasks)
	m.taskState = make(map[Task]TaskSt)
	m.phase = MapPhase

	// Initializing map tasks
	c := 0
	for _, file := range files {
		task := Task{Map, file, c, nReduce}
		m.taskState[task] = TaskSt{Idle, ""}
		m.taskCh <- task
		c++
	}
}

// initReduceTasks initializes the map tasks that are need to be done
func (m *Master) initReduceTasks() {
	for r, file := range m.intermediate {
		m.taskAvail++
		m.taskRem++
		task := Task{Reduce, file, 0, r}
		m.taskState[task] = TaskSt{Idle, ""}
		m.taskCh <- task
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

	if m.phase == DonePhase {
		// Remove intermediate files
		for _, fn := range m.intermediate {
			ff := func(r rune) bool { return r == ' ' }
			files := strings.FieldsFunc(fn, ff)

			for _, file := range files {
				os.Remove(file)
			}
		}
	}

	return m.phase == DonePhase
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

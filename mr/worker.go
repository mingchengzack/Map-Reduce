package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// KeyValue defines {key, value} pair
// Map functions return a slice of KeyValue
type KeyValue struct {
	Key   string
	Value string
}

// ByKey defines the type for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// WID defines the worker id (string)
type WID string

// ihash returns the hashed value for given key
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker do tasks from master
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Construct worker id
	wid := WID(strconv.Itoa(os.Getuid()) + strconv.Itoa(os.Getpid()))

	// Get task from master
	for task := getTask(wid); task != nil && task.Ty != Exit; task = getTask(wid) {
		if task.Ty == Retry {
			time.Sleep(time.Second) // Wait one second before retrying
		} else {
			fs, rs := doTask(task, mapf, reducef)
			completeTask(wid, task, fs, rs)
		}
	}

	// Worker finishes job and exit
}

// doTask performs the task asked by master
// returns the name of the output file that has the work on it
func doTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) ([]string, []int) {
	switch task.Ty {
	case Map:
		return doMap(task, mapf)
	case Reduce:
		return doReduce(task, reducef)
	default:
		// We should never be here
		return doMap(task, mapf)
	}
}

// doMap performs the map task
func doMap(task *Task, mapf func(string, string) []KeyValue) ([]string, []int) {
	// Read key, values from file
	iname := task.File
	ifile, err := os.Open(iname)
	if err != nil {
		log.Fatalf("cannot open %v", iname)
	}
	content, err := ioutil.ReadAll(ifile)
	if err != nil {
		log.Fatalf("cannot read %v", iname)
	}
	ifile.Close()

	// Applied map function
	intermediate := mapf(iname, string(content))

	// Use map to find out how many Reduce tasks
	nReduce := task.R
	mapR := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		r := ihash(kv.Key) % nReduce
		mapR[r] = append(mapR[r], kv)
	}

	// Store kv pairs in tmp files for each Reduce task
	m := task.M
	fs := []string{}
	rs := []int{}
	for r, kva := range mapR {
		// Creating tmp files to store intermediate data
		cwd, _ := os.Getwd()
		ofile, _ := ioutil.TempFile(cwd, "mr-tmp-"+strconv.Itoa(m)+"-"+strconv.Itoa(r)+"-")
		fname := ofile.Name()
		fs = append(fs, fname)
		rs = append(rs, r)

		// Use json to store kv pair in file
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("[ERROR] Cannot marshall into file! ", err)
			}
		}

		ofile.Close()
	}

	return fs, rs
}

// doReduce performs the reduce task
func doReduce(task *Task, reducef func(string, []string) string) ([]string, []int) {
	// Get files
	ff := func(r rune) bool { return r == ' ' }
	files := strings.FieldsFunc(task.File, ff)

	// Get intermediate kv pairs
	intermediate := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// Decoding all json into kv pairs
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	// Sort intermediate keys for gathering the values with the same key
	sort.Sort(ByKey(intermediate))

	// Create tmp files
	cwd, _ := os.Getwd()
	ofile, _ := ioutil.TempFile(cwd, "mr-tmp-out"+"-"+strconv.Itoa(task.R)+"-")
	f := ofile.Name()

	i := 0
	for i < len(intermediate) {
		// Find out all the values with the same key
		values := []string{}
		j := i
		for ; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {
			values = append(values, intermediate[j].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// Write to tmp output file with kv
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	return []string{f}, []int{task.R}
}

// GetTask gets a task from master
// RPC requests for the worker to call
func getTask(wid WID) *Task {
	args := &GetTaskArgs{wid}
	reply := &GetTaskReply{}

	// send the RPC request, wait for the reply
	if call("Master.AssignTask", args, reply) {
		return &reply.T
	}

	return nil
}

// CompleteTask tells the master it completes the tasks
// RPC requests for the worker to call
func completeTask(wid WID, task *Task, fs []string, rs []int) {
	args := &CompleteTaskArgs{wid, *task, fs, rs}
	reply := &CompleteTaskReply{Message: ""}

	// Tells master the task is completed
	if call("Master.CompleteTask", args, reply) {
		return
	}

	// Worker cannot finish job correctly
	return
}

// call sends an RPC request to the master, wait for the response
// usually returns true, returns false if something goes wrong
// general RPC request for the worker to call
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

//
// RPC definitions
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RPC request and reply definitions

// GetTaskArgs defines the args to send when a worker ask for a task
type GetTaskArgs struct {
	W WID // Worker ID
}

// GetTaskReply defines the reply the worker gets after asking for a task
type GetTaskReply struct {
	T Task // Task assigned for the worker
}

// CompleteTaskArgs defines the args to send when the worker completes a task
type CompleteTaskArgs struct {
	W WID      // Worker ID
	T Task     // Task completed by this worker
	F []string // Created files names
	R []int    // Reduce tasks ID
}

// CompleteTaskReply defines the reply the worker gets after completing a task
type CompleteTaskReply struct {
	Message string // Debug message
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

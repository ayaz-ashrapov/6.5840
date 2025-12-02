package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type RpcArgs struct {
	X int
}

type GetReduceCountReply struct {
	N int
}

type Task struct {
	TaskFile string
	TaskId   int
	TaskType string
	Timeout  time.Duration
}

type TaskResult struct {
	TaskID int
	Err    error
}

type TaskState struct {
	Task   Task
	Result TaskResult
}

type RequestTaskReply struct {
	HasTask bool
	Task    Task
}

type ReportTaskDoneReply struct {
	TaskState TaskState
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	queueCond    sync.Cond
	nReduce      int
	queue        []Task
	taskStateMap map[int]string
}

//The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time
// (for this lab, use ten seconds), and give the same task to a different worker.

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceCount(args *RpcArgs, reply *GetReduceCountReply) error {
	reply.N = c.nReduce
	return nil
}

func (c *Coordinator) RequestTask(args *RpcArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.queue) == 0 {
		c.queueCond.Wait()
	}

	task := c.queue[0]
	c.queue = c.queue[1:]

	reply.HasTask = true
	reply.Task = task
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, ok := c.taskStateMap[args.TaskID]

	if !ok {
		return nil
	}

	if state.Done {
		return nil
	}

	if state.Timer != nil {
		state.Timer.Stop()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	//return true if all tasks are completed
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	tasks := []RequestTaskReply{}
	for idx, filename := range files {
		task := RequestTaskReply{filename, idx, "map"}
		tasks = append(tasks, task)
		c.taskStateMap[idx] = "idle"
	}
	c.queue = tasks
	c.server()
	return &c
}

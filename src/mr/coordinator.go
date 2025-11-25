package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

}

//The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time
// (for this lab, use ten seconds), and give the same task to a different worker.

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceCount(args *RpcArgs, reply *GetReduceCountReply) error {
	reply.N = 1
	return nil
}

func (c *Coordinator) RequestTask(args *RpcArgs, reply *RequestTaskReply) error {
	taskType := "NO_TASK"
	reply.TaskType = taskType
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *RpcArgs, reply *RpcReply) error {
	reply.Y = args.X + 1
	return nil
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

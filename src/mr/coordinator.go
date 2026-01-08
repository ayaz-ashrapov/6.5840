package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskInfo struct {
	State     TaskState
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	files       []string
	nReduce     int
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo

	phase string // map/reduce/done
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	println("Coordinator: requesting task for current phase: ", c.phase)

	if c.phase == "map" {
		println("Coordinator: assigning map task")
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				println("Coordinator: found idle map task: ", i)
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = time.Now()

				reply.Type = TaskMap
				reply.TaskId = i
				reply.Filename = c.files[i]
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)

				return nil
			}
		}

		if anyInProgress(c.mapTasks) {
			reply.Type = TaskWait
		} else {
			c.phase = "reduce"
			reply.Type = TaskWait
		}
		println("Coordinator: no new map task was assigned")
		return nil
	}

	if c.phase == "reduce" {
		println("Coordinator: assigning reduce task")
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == Idle {
				println("Coordinator: found idle reduce task")
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()

				reply.Type = TaskReduce
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)

				return nil
			}
		}

		if anyInProgress(c.reduceTasks) {
			reply.Type = TaskWait
		} else {
			c.phase = "done"
			reply.Type = TaskWait
		}
		println("Coordinator: no new reduce task was assigned")
		return nil
	}
	println("Coordinator: no task assigned")
	reply.Type = TaskNone
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !args.Ok {
		println("Coordinator: Error while reporting task: ", args)
		return nil
	}

	println("Task id to report: ", args.TaskId)
	switch args.Type {
	case TaskMap:
		println("reporting map task status")
		if c.mapTasks[args.TaskId].State == InProgress {
			println("map task completed: ", args.TaskId)
			c.mapTasks[args.TaskId].State = Completed
		}

	case TaskReduce:
		println("reporting reduce task status")
		if c.reduceTasks[args.TaskId].State == InProgress {
			println("reduce task completed: ", args.TaskId)
			c.reduceTasks[args.TaskId].State = Completed
		}

	}

	return nil
}

func anyInProgress(tasks []TaskInfo) bool {
	println("Coordinator: looking for task in progress")
	for i := range len(tasks) {
		if tasks[i].State == InProgress {
			return true
		}
	}

	return false
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
	go c.monitorTimeouts()
}

func (c *Coordinator) monitorTimeouts() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		switch c.phase {
		case "map":
			c.checkTimeouts(c.mapTasks)
		case "reduce":
			c.checkTimeouts(c.reduceTasks)
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) checkTimeouts(tasks []TaskInfo) error {
	for i := range len(tasks) {
		if tasks[i].State == InProgress && time.Since(tasks[i].StartTime) > 10*time.Second {
			tasks[i].State = Idle
			tasks[i].StartTime = time.Now()
		}
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	println("Coordinator: Waiting")
	return c.phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	nMap := len(files)
	c.mapTasks = make([]TaskInfo, 0, nMap)
	c.reduceTasks = make([]TaskInfo, 0, nReduce)
	c.nReduce = nReduce
	c.phase = "map"
	c.files = make([]string, 0)

	for i := range nMap {
		mapTask := TaskInfo{Idle, time.Now()}
		c.mapTasks = append(c.mapTasks, mapTask)
		c.files = append(c.files, files[i])
	}

	for range nReduce {
		reduceTask := TaskInfo{Idle, time.Now()}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	// Your code here.

	c.server()

	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &c
}

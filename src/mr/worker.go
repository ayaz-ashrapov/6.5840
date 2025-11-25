package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"

	"6.5840/mr"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var nReduce = 200

const TaskInterval = 200

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	n, ok := getReduceCount()
	if !ok {
		fmt.Printf("Can't get reduce count. Terminating...")
		return
	}

	nReduce = n
	// Your worker implementation here.
	// 	read the task's input from one or more files,
	// 	execute the task, write the task's output to one or more files
	for {
		reply, ok := requestTask()
		if !ok {
			fmt.Printf("Couldn't get a task. Skipping this iteration")
			continue
		}

		exit, ok := false, true

		switch taskType := reply.TaskType; taskType {
		case "map":
			doMap(mapf, reply.TaskFile, reply.TaskId)
		case "reduce":
			doReduce(reducef, reply.TaskId)
		default:
			fmt.Printf("Task type %s is not recognized", taskType)
		}

		if exit || !ok {
			fmt.Println("Master exited or all tasks done, worker exiting.")
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}

}

func getReduceCount() (int, bool) {
	args := RpcArgs{}
	reply := GetReduceCountReply{}

	ok := call("Coordinator.GetReduceCount", &args, &reply)

	return reply.N, ok
}

func requestTask() (*RequestTaskReply, bool) {
	args := RpcArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)

	return &reply, ok
}

func doMap(mapf func(string, string) []KeyValue, filename string, taskId int) {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
}

func doReduce(reducef func(string, []string) string, taskId int) {
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

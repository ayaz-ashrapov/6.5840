package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var nReduce int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.
	for {
		reply := requestTask()

		switch reply.Type {
		case TaskMap:
			nReduce = reply.NReduce
			println("Worker: doing map task: ", reply.TaskId)
			doMap(mapf, reply.Filename, reply.TaskId)
			reportTaskDone(reply.TaskId, TaskMap, true)
		case TaskReduce:
			nReduce = reply.NReduce
			println("Worker: doing reduce task: ", reply.TaskId)
			doReduce(reducef, reply.TaskId)
			reportTaskDone(reply.TaskId, TaskReduce, true)
		case TaskWait:
			println("Worker: waiting for tasks")
			time.Sleep(200 * time.Millisecond)

		case TaskNone:
			return
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func requestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	call("Coordinator.RequestTask", &args, &reply)
	return reply
}

func reportTaskDone(taskId int, taskType TaskType, success bool) {
	args := ReportTaskArgs{
		TaskId: taskId,
		Type:   taskType,
		Ok:     success,
	}

	reply := ReportTaskReply{}

	call("Coordinator.ReportTaskDone", &args, &reply)
}

func doMap(mapf func(string, string) []KeyValue, filePath string, mapId int) {
	file, err := os.Open(filePath)
	checkError(err, "Cannot open file %v\n", filePath)

	content, err := io.ReadAll(file)
	checkError(err, "Cannot read file %v\n", filePath)
	file.Close()

	kva := mapf(filePath, string(content))
	writeMapOutput(kva, mapId)
}

func writeMapOutput(kva []KeyValue, mapId int) {
	// use io buffers to reduce disk I/O, which greatly improves
	// performance when running in containers with mounted volumes
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// create temp files, use pid to uniquely identify this worker
	for i := range nReduce {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "Cannot create file %v\n", filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Cannot encode %v to file\n", kv)
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		checkError(err, "Cannot flush buffer for file: %v\n", files[i].Name())
	}

	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file %v\n", file.Name())
	}
}

func doReduce(reducef func(string, []string) string, reduceId int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		checkError(err, "Cannot list reduce files")
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		checkError(err, "Cannot open file %v\n", filePath)

		dec := json.NewDecoder(file)

		for {
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				checkError(err, "Cannot decode from file %v\n", filePath)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}

		file.Close()

	}

	writeReduceOutput(reducef, kvMap, reduceId)
}

func writeReduceOutput(reducef func(string, []string) string,
	kvMap map[string][]string, reduceId int) {

	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "Cannot create file %v\n", filePath)

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		checkError(err, "Cannot write mr output (%v, %v) to file", k, v)
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	checkError(err, "Cannot rename file %v\n", filePath)
}

func checkError(err error, format string, v ...any) {
	if err != nil {
		log.Fatalf(format, v)
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

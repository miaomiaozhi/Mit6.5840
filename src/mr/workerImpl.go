package mr

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"6.5840/logger"
)

const (
	mapTmpDir = "./map"
)

func CallFetchTask() (*TaskInfo, error) {
	args := FetchTaskRequest{
		WoderIndex: WorkerIndex,
	}

	reply := FetchTaskReponse{}
	ok := call("Coordinator.PushTask", &args, &reply)
	if ok {
		WorkerIndex = reply.WoderIndex
		logger.Infof("Worker %v fetch task %v",
			WorkerIndex, TaskMap[reply.TaskInfo.TaskType])
		return reply.TaskInfo, nil
	} else {
		logger.Errorf("Worker %v call fetch task handle failed", WorkerIndex)
		return nil, nil
	}
}
func CallFinishTask(taskInfo *TaskInfo, err error) {
	statusCode := Success
	if err != nil {
		statusCode = Failed
	}
	args := MapRequest{
		TaskInfo:   taskInfo,
		StatusCode: statusCode,
	}
	reply := MapResponse{}
	_ = call("Coordinator.FinishTask", &args, &reply)
}

// get kva
func getKvaInMapTask(taskInfo *TaskInfo,
	mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	filename := taskInfo.FileName
	file, err := os.Open(filename)
	if err != nil {
		logger.Errorf("get kva in map task error: cannot open file. %v", filename)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Errorf("get kva in map task error: read content failed. %v", filename)
		return nil, err
	}
	file.Close()

	kva := mapf(filename, string(content))
	return kva, nil
}

func writeIntermediateToFile(index int, kva []KeyValue, result chan error) {
	jsonData, err := json.Marshal(kva)
	if err != nil {
		logger.Errorf("handle %v map task error: marshal data failed %v",
			index, err)
		result <- err
		return
	}

	filePathPrefix := "mr-map-" + strconv.Itoa(WorkerIndex) + "-"
	filePathPrefix = filepath.Join(mapTmpDir, filePathPrefix)
	filePathSuffix := ".txt"

	filePath := filePathPrefix + strconv.Itoa(index) + filePathSuffix
	file, err := os.Create(filePath)
	if err != nil {
		logger.Errorf("handle map task failed, worker (%v-%v): create file error %v",
			WorkerIndex, index, err)
		result <- err
		return
	}
	defer file.Close()

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		logger.Errorf("handle map task failed, worker (%v-%v): write file error %v",
			WorkerIndex, index, err)
		result <- err
		return
	}
	result <- nil
}

func HandleMapTask(taskInfo *TaskInfo,
	mapf func(string, string) []KeyValue) error {
	logger.Infof("Worker %v Handle map task", WorkerIndex)
	if WorkerIndex == -1 {
		logger.Errorf("handle map task error: WorkerIndex is INVALID. taskInfo: %v",
			*taskInfo)
		return errors.New("handle map task error: WorkerIndex is INVALID")
	}

	kva, err := getKvaInMapTask(taskInfo, mapf)
	if err != nil {
		logger.Errorf("handle map task error: %v", err)
		return err
	}

	nReduce := taskInfo.NReduce
	intermediate := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		key := kv.Key
		intermediate[ihash(key)%nReduce] = append(intermediate[ihash(key)%nReduce], kv)
	}

	// write intermediate file to each file
	for index := 0; index < nReduce; index++ {
		result := make(chan error)
		go writeIntermediateToFile(index, intermediate[index], result)
		if err := <-result; err != nil {
			logger.Errorf("handle map task error, worker(%v-%v), %v", WorkerIndex, index, err)
			return err
		}
	}

	logger.Infof("worker %v handle map task success!", WorkerIndex)
	return nil
}
func HandleReduceTask(taskInfo *TaskInfo,
	reducef func(string, []string) string) error {
	logger.Infof("Worker %v Handle reduce task", WorkerIndex)
	return nil
}

func HandleWaitTask(taskInfo *TaskInfo) error {
	logger.Infof("Worker %v Handle wait task", WorkerIndex)
	time.Sleep(5 * time.Second)
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		logger.Info("reply.Y %v", reply.Y)
	} else {
		logger.Error("call RPC handle failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logger.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	logger.Errorf("handling RPC failed: %s", err.Error())
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

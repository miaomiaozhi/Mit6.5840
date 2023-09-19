package mr

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"6.5840/logger"
)

const (
	mapTmpDir = "../mr/map"
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

func HandleMapTask(taskInfo *TaskInfo,
	mapf func(string, string) []KeyValue) error {
	logger.Infof("Worker %v Handle map task", WorkerIndex)
	if WorkerIndex == -1 {
		logger.Errorf("handle map task error: WorkerIndex is INVALID. taskInfo: %v",
			*taskInfo)
		return nil
	}

	// get kva
	filename := taskInfo.FileName
	file, err := os.Open(filename)
	if err != nil {
		logger.Errorf("handle map task error: cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	tmpFileInMapTask := ""
	kva := mapf(tmpFileInMapTask, string(content))

	jsonData, err := json.Marshal(kva)
	if err != nil {
		logger.Errorf("handle map task error: marshal data failed %v", err)
		return err
	}

	tmpFilePathPrefix := "mr-map-" + strconv.Itoa(WorkerIndex) + "-"
	tmpFilePathPrefix = filepath.Join(mapTmpDir, tmpFilePathPrefix)

	logger.Infof("handle map task, Worker %v tmp file path prefix is %v",
		WorkerIndex, tmpFilePathPrefix)

	nReduce := taskInfo.NReduce
	logger.Infof("nReduce is %v", nReduce)
	for i := 0; i < nReduce; i++ {
		pwd, _ := os.Getwd()
		logger.Debugf("pws %v", pwd)
		tmpFilePath := tmpFilePathPrefix + strconv.Itoa(i) + ".txt"
		file, err := os.Create(tmpFilePath)
		if err != nil {
			logger.Errorf("create file error %v", err)
			return err
		}
		defer file.Close()

		if err := os.WriteFile(tmpFilePath, jsonData, 0644); err != nil {
			logger.Errorf("%v", err)
			return err
		}
	}

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

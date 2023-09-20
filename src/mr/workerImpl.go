package mr

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"6.5840/logger"
)

const (
	mapTmpDir = "./map/"
)

func CallFetchTask() (*TaskInfo, error) {
	args := FetchTaskRequest{
		WoderIndex: WorkerIndex,
	}

	reply := FetchTaskReponse{}
	ok := call("Coordinator.PushTask", &args, &reply)
	if ok {
		WorkerIndex = reply.WoderIndex
		// logger.Infof("Worker %v fetch task %v",
		// WorkerIndex, TaskMap[reply.TaskInfo.TaskType])
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

func openFileCreateIfNotExist(dirPath string, filePath string) (*os.File, error) {
	// 检查文件夹是否存在
	_, err := os.Stat(dirPath)
	if err != nil {
		// 文件夹不存在，创建文件夹
		err = os.Mkdir(dirPath, 0755)
		if err != nil {
			logger.Errorf("create dir %v error: %v",
				dirPath, err)
			return nil, err
		}
	}

	return os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

	filePath := filePathPrefix + strconv.Itoa(index)
	file, err := openFileCreateIfNotExist(mapTmpDir, filePath)
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

	// logger.Infof("worker %v handle map task success!", WorkerIndex)
	return nil
}
func HandleReduceTask(taskInfo *TaskInfo,
	reducef func(string, []string) string) error {

	if WorkerIndex == -1 {
		logger.Errorf("handle reduce task error: WorkerIndex is INVALID. taskInfo: %v",
			*taskInfo)
		return errors.New("handle reduce task error: WorkerIndex is INVALID")
	}

	reduceNum := taskInfo.FileName
	filePattern := filepath.Join(mapTmpDir, "mr-map-*-"+reduceNum)
	matchedFiles, err := filepath.Glob(filePattern)
	if err != nil {
		logger.Errorf("Failed to match files: %v", err)
		return err
	}
	// logger.Debugf("worker (%v-%v), match %v",
	// 	WorkerIndex, reduceNum, matchedFiles)

	intermediate := make([]KeyValue, 0)
	for _, file := range matchedFiles {
		contentByte, err := os.ReadFile(file)
		if err != nil {
			logger.Errorf("Failed to open file: %v", err)
			return err
		}

		kva := make([]KeyValue, 0)
		if err := json.Unmarshal(contentByte, &kva); err != nil {
			logger.Errorf("failed to unmarshal kva, worker(%v-%v), err: %v",
				WorkerIndex, reduceNum, err)
			return err
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + reduceNum
	dirPath := "./"
	file, err := openFileCreateIfNotExist(dirPath, oname)
	if err != nil {
		logger.Errorf("Failed to open file: %v", err)
		return err
	}
	defer file.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		key := intermediate[i].Key
		for j < len(intermediate) && intermediate[j].Key == key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		count := reducef(key, values)
		kv := key + " " + count + "\n"

		if _, err := file.WriteString(kv); err != nil {
			logger.Errorf("handle reduce task error, worker(%v-%v), err: %v",
				WorkerIndex, reduceNum, err)
			return err
		}

		i = j
	}

	return nil
}

func HandleWaitTask(taskInfo *TaskInfo) error {
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

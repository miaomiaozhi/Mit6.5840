package mr

import (
	"net/rpc"

	"6.5840/logger"
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

func CallMapTask() error {
	logger.Infof("Worker %v call map task", WorkerIndex)
	return nil
}
func CallReduceTask() error {
	logger.Infof("Worker %v call reduce task", WorkerIndex)
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

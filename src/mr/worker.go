package mr

import (
	"sync"

	"6.5840/logger"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	WorkerIndex = -1
)

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		stop := make(chan bool)
		for {
			select {
			case <-stop:
				logger.Infof("Worker %v received stop signal", WorkerIndex)
				wg.Done()
				return
			default:
				work(stop, mapf, reducef)
			}
		}
	}()

	wg.Wait()
}

func work(stop chan bool,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	taskInfo, err := CallFetchTask()
	if err != nil {
		logger.Errorf("Worker %v call fetch task failed: %v", WorkerIndex, err)
		return
	}
	logger.Infof("task %v", *taskInfo)

	taskType := taskInfo.TaskType

	// handle task now
	switch taskType {
	case FetchTask:
		// do noting
	case MapTask:
		err := HandleMapTask(taskInfo, mapf)
		CallFinishTask(taskInfo, err)
	case ReduceTask:
		err := HandleReduceTask(taskInfo, reducef)
		CallFinishTask(taskInfo, err)
	case WaitTask:
		_ = HandleWaitTask(taskInfo)
	case EndTask:
		logger.Infof("Worker %v finish!", WorkerIndex)
		stop <- true
	default:
		logger.Errorf("Worder %v call error type: %v", WorkerIndex, taskType)
	}
}

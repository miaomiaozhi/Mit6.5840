package mr

import (
	"hash/fnv"
	"sync"
	"time"

	"6.5840/logger"
)

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
				work(stop)
			}
		}
	}()

	wg.Wait()
	logger.Infof("Worder %v finish now!", WorkerIndex)
}

func work(stop chan bool) {
	taskInfo, err := CallFetchTask()
	if err != nil {
		logger.Errorf("Worker %v call fetch task failed: %v", WorkerIndex, err)
		return
	}

	taskType := taskInfo.TaskType

	// handle task now
	switch taskType {
	case FetchTask:
		// do noting
	case MapTask:
		_ = CallMapTask()
	case ReduceTask:
		_ = CallReduceTask()
	case WaitTask:
		time.Sleep(5 * time.Second)
	case EndTask:
		logger.Infof("Worker %v finish!", WorkerIndex)
		stop <- true
	default:
		logger.Errorf("Worder %v call error type: %v", WorkerIndex, taskType)
	}
}

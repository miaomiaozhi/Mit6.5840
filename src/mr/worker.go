package mr

import (
	"context"
	"sync"
	"time"

	"6.5840/logger"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	WorkerIndex = -1
)

type WorkerNode struct {
	workerIndex int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(ctx context.Context) {
		ticker := time.Tick(1000 * time.Millisecond)
		for range ticker {
			select {
			case <-ctx.Done():
				logger.Infof("Worker %v received stop signal", WorkerIndex)
				wg.Done()
				return
			default:
				work(ctx, cancel, mapf, reducef)
			}
		}
	}(ctx)
	wg.Wait()
}

func work(ctx context.Context,
	cancel context.CancelFunc,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
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
		err := HandleMapTask(taskInfo, mapf)
		CallFinishTask(taskInfo, err)
	case ReduceTask:
		err := HandleReduceTask(taskInfo, reducef)
		CallFinishTask(taskInfo, err)
	case WaitTask:
		_ = HandleWaitTask(taskInfo)
	case EndTask:
		logger.Infof("Worker %v finished!", WorkerIndex)
		cancel()
	default:
		logger.Errorf("Worder %v call error type: %v", WorkerIndex, taskType)
	}
}

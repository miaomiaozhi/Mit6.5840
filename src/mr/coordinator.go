package mr

import (
	"sync"
)

type TaskQueue chan *TaskInfo

const (
	maxTasksPoolSize = 1024
)

type Coordinator struct {
	// Your definitions here.
	tasks             TaskQueue
	globalWorkerIndex int
	nReduce           int

	workerIndexLock sync.Mutex
	tasksLock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, inReduce int) *Coordinator {
	// do initialize
	c := Coordinator{
		tasks:             make(TaskQueue, maxTasksPoolSize),
		globalWorkerIndex: -1,
		nReduce:           inReduce,
	}

	// Your code here.
	// TODO: generate tasks here
	for _, file := range files {
		c.tasks <- &TaskInfo{
			TaskType: MapTask,
			FileName: file,
		}
	}

	c.server()
	return &c
}

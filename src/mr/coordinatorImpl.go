package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"6.5840/logger"
)

type TaskQueue chan TaskInfo

var (
	tasks TaskQueue
)

const (
	maxTasksPoolSize = 1024
)

var once sync.Once

func (c *Coordinator) PushTask(args *FetchTaskRequest, reply *FetchTaskReponse) error {
	// preparation
	reply.TaskInfo = &TaskInfo{}
	reply.WoderIndex = args.WoderIndex
	logger.Info("master call push task now, worker %v",
		args.WoderIndex)

	if args.WoderIndex == -1 {
		workerIndexLock.Lock()
		GlobalWorkerIndex += 1
		reply.WoderIndex = GlobalWorkerIndex
		workerIndexLock.Unlock()
		logger.Info("Global index", GlobalWorkerIndex)
	}

	if len(c.GetPool()) == 0 {
		reply.TaskInfo.TaskType = WaitTask
	} else {
		// TODO: push
		reply.TaskInfo.TaskType = MapTask
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// get tasks pool
func (c *Coordinator) GetPool() TaskQueue {
	if tasks == nil {
		once.Do(func() {
			tasks = make(TaskQueue, maxTasksPoolSize)
		})
	}
	return tasks
}

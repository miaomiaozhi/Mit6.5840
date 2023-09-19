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

func (c *Coordinator) PushTask(args *FetchTaskRequest, reply *FetchTaskReponse) error {
	// preparation
	reply.TaskInfo = &TaskInfo{}
	reply.WoderIndex = args.WoderIndex
	reply.TaskInfo.NReduce = c.nReduce

	logger.Infof("master nReduce is %v", c.nReduce)
	logger.Infof("master call push task now, worker %v",
		args.WoderIndex)

	if args.WoderIndex == -1 {
		c.workerIndexLock.Lock()
		c.globalWorkerIndex += 1
		reply.WoderIndex = c.globalWorkerIndex
		c.workerIndexLock.Unlock()
		logger.Info("Global index", c.globalWorkerIndex)
	}

	c.tasksLock.Lock()
	if len(c.GetPool()) == 0 {
		reply.TaskInfo.TaskType = WaitTask
	} else {
		reply.TaskInfo = <-c.GetPool()
	}

	// BUG
	reply.TaskInfo.NReduce = c.nReduce
	c.tasksLock.Unlock()

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
var once sync.Once

func (c *Coordinator) GetPool() TaskQueue {
	if c.tasks == nil {
		once.Do(func() {
			c.tasks = make(TaskQueue, maxTasksPoolSize)
		})
	}
	return c.tasks
}

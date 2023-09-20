package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"6.5840/logger"
)

func (c *Coordinator) pushMapReduceTask(args *FetchTaskRequest, reply *FetchTaskReponse) error {
	c.globalWorkerIndex += 1
	reply.WoderIndex = c.globalWorkerIndex

	if len(c.GetPool()) == 0 {
		reply.TaskInfo.TaskType = WaitTask
	} else {
		reply.TaskInfo = <-c.GetPool()
	}

	return nil
}

func (c *Coordinator) PushTask(args *FetchTaskRequest, reply *FetchTaskReponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// initailize
	reply.TaskInfo = &TaskInfo{}
	reply.WoderIndex = args.WoderIndex

	if c.allDone {
		reply.TaskInfo.TaskType = EndTask
		return nil
	}
	return c.pushMapReduceTask(args, reply)
}

func (c *Coordinator) FinishTask(args *MapRequest, reply *MapResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.StatusCode == Failed {
		c.GetPool() <- args.TaskInfo
		return nil
	}

	taskInfo := args.TaskInfo
	switch taskInfo.TaskType {
	case MapTask:
		c.unImplMapTaskCnt -= 1
		if c.unImplMapTaskCnt == 0 {
			c.mapDone = true

			// logger.Debugf("!!!!!!!!!!!!! generate reduce work now")
			// generate reduce work
			for index := 0; index < c.nReduce; index++ {
				c.tasks <- &TaskInfo{
					TaskType: ReduceTask,
					FileName: strconv.Itoa(index),
					NReduce:  c.nReduce,
				}
			}
		}
	case ReduceTask:
		c.unImplReduceTaskCnt -= 1
		if c.unImplReduceTaskCnt == 0 {
			if !c.mapDone {
				logger.Error("finish ALL task error, map tasks is not finished")
				return nil
			}
			c.allDone = true
		}
	default:
		logger.Errorf("finish task error, invalid task type %v call this rpc",
			taskInfo.TaskType)
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
	if c.allDone {
		ret = true
		logger.Info("coordinator finish all job now")
		time.Sleep(5 * time.Second)
	}

	return ret
}

var (
	once sync.Once
)

// get tasks pool
func (c *Coordinator) GetPool() TaskQueue {
	if c.tasks == nil {
		once.Do(func() {
			c.tasks = make(TaskQueue, maxTasksPoolSize)
		})
	}
	return c.tasks
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ResponseStatus int

const (
	Success = iota
	Failed
)

// Add your RPC definitions here.
type BaseRequest struct {
	TaskType TaskType
}
type BaseResponse struct {
	ResponseStatus ResponseStatus
}

type FetchTaskRequest struct {
	BaseRequest
	WoderIndex int
}
type MapRequest struct {
	BaseRequest
	StatusCode int
	TaskInfo   *TaskInfo
}
type ReduceRequest struct {
	BaseRequest
}

// response
type FetchTaskReponse struct {
	BaseResponse
	WoderIndex int
	TaskInfo   *TaskInfo
}
type MapResponse struct {
	BaseResponse
}
type ReduceResponse struct {
	BaseResponse
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

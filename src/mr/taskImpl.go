package mr

type TaskType int

const (
	FetchTask = iota
	MapTask
	ReduceTask
	WaitTask
	EndTask
	ErrorTask
)

var TaskMap map[TaskType]string

func init() {
	TaskMap = map[TaskType]string{
		FetchTask:  "FetchTask",
		MapTask:    "MapTask",
		ReduceTask: "ReduceTask",
		WaitTask:   "WaitTask",
		EndTask:    "EndTask",
		ErrorTask:  "ErrorTask",
	}
}

type TaskInfo struct {
	TaskType TaskType
	FileName string
}

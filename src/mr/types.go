package mr

import "time"

type TaskType int
type TaskStateType int
type EventType int
type CoordinatorStateType int

const (
	MappingState  CoordinatorStateType = iota
	ReducingState CoordinatorStateType = iota
	DoneState     CoordinatorStateType = iota
)

const (
	MapTask    TaskType = iota
	ReduceTask TaskType = iota
)

const (
	TaskReadyState    TaskStateType = iota
	TaskRunningState  TaskStateType = iota
	TaskFinishedState TaskStateType = iota
)

const (
	FinishedEvent EventType = iota
	ReqTaskEvent  EventType = iota
)

type TaskInfo struct {
	type_       TaskType
	state       TaskStateType
	file_name   string
	assign_time time.Time
}

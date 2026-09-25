package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type Params struct {
	Finished bool
	TaskType TaskType
	TaskID   int
}

type Response struct {
	HasAssign bool
	TaskType  TaskType
	TaskID    int
	FileName  string
	NReduce   int
	NMap      int
	Done      bool
}

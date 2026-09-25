package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	map_tasks    map[int]TaskInfo
	reduce_tasks map[int]TaskInfo
	state        CoordinatorStateType
	mutex        sync.Mutex
}

func (c *Coordinator) Manage(args *Params, reply *Response) error {
	// Add worker into table
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Finished {
		switch args.TaskType {
		case MapTask:
			// Update task from task table
			c.map_tasks[args.TaskID] = TaskInfo{
				type_:       c.map_tasks[args.TaskID].type_,
				state:       TaskFinishedState,
				file_name:   c.map_tasks[args.TaskID].file_name,
				assign_time: c.map_tasks[args.TaskID].assign_time,
			}

			// fmt.Printf("Worker finished map task %d\n", args.TaskID)
		case ReduceTask:
			// Update task from task table
			c.reduce_tasks[args.TaskID] = TaskInfo{
				type_:       c.reduce_tasks[args.TaskID].type_,
				state:       TaskFinishedState,
				file_name:   c.reduce_tasks[args.TaskID].file_name,
				assign_time: c.reduce_tasks[args.TaskID].assign_time,
			}

			// fmt.Printf("Worker finished reduce task %d\n", args.TaskID)
		}
	}
	// Continue to assign task to worker

	reply.HasAssign = false

	all_finished := true
	if c.state == MappingState {
		// Assign map task to worker
		for task_id, task_info := range c.map_tasks {
			if task_info.type_ == MapTask && (task_info.state == TaskReadyState || (task_info.state == TaskRunningState && time.Since(task_info.assign_time) > 10*time.Second)) {
				reply.HasAssign = true
				reply.TaskType = MapTask
				reply.FileName = task_info.file_name
				reply.TaskID = task_id
				reply.NReduce = len(c.reduce_tasks)
				reply.NMap = len(c.map_tasks)

				// Update task from task table
				c.map_tasks[task_id] = TaskInfo{
					type_:       task_info.type_,
					state:       TaskRunningState,
					file_name:   task_info.file_name,
					assign_time: time.Now(),
				}

				// fmt.Printf("Assign map task %d to worker\n", reply.TaskID)
				return nil
			}

			all_finished = all_finished && (task_info.state == TaskFinishedState)
		}

		if all_finished {
			// Set state to reducing
			c.state = ReducingState
			fmt.Print("Coordinator state changed to ReducingState\n")
		} else {
			// fmt.Printf("No map task assigned to worker, all_finished: %v\n", all_finished)
			// for task_id, task_info := range c.map_tasks {
			// 	fmt.Printf("Map task %d: state=%v, file_name=%v, assign_time=%v\n", task_id, task_info.state, task_info.file_name, task_info.assign_time)
			// }
			return nil
		}
	}

	// Assign reduce task to worker, TBC
	if c.state == ReducingState {
		for task_id, task_info := range c.reduce_tasks {
			if task_info.type_ == ReduceTask && (task_info.state == TaskReadyState || (task_info.state == TaskRunningState && time.Since(task_info.assign_time) > 10*time.Second)) {
				reply.HasAssign = true
				reply.TaskType = ReduceTask
				reply.TaskID = task_id
				reply.NReduce = len(c.reduce_tasks)
				reply.NMap = len(c.map_tasks)
				reply.FileName = "" // Reduce tasks do not have a specific file name

				// Update task from task table
				c.reduce_tasks[task_id] = TaskInfo{
					type_:       task_info.type_,
					state:       TaskRunningState,
					file_name:   "",
					assign_time: time.Now(),
				}

				// fmt.Printf("Assign reduce task %d to worker\n", reply.TaskID)
				return nil
			}

			all_finished = all_finished && (task_info.state == TaskFinishedState)
		}

		if all_finished {
			// Done
			c.state = DoneState
			reply.Done = true
			fmt.Print("Coordinator state changed to DoneState\n")
		} else {
			// fmt.Printf("No reduce task assigned to worker, all_finished: %v\n", all_finished)
			return nil
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.state == DoneState
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		map_tasks:    make(map[int]TaskInfo),
		reduce_tasks: make(map[int]TaskInfo),
		state:        MappingState,
		mutex:        sync.Mutex{},
	}

	map_task_id := 0
	// Add map tasks into task table
	for _, file := range files {
		c.map_tasks[map_task_id] = TaskInfo{
			type_:       MapTask,
			state:       TaskReadyState,
			file_name:   file,
			assign_time: time.Time{},
		}
		map_task_id++
	}

	// Add reduce tasks into task table
	for reduce_task_id := 0; reduce_task_id < nReduce; reduce_task_id++ {
		c.reduce_tasks[reduce_task_id] = TaskInfo{
			type_:       ReduceTask,
			state:       TaskReadyState,
			file_name:   "",
			assign_time: time.Time{},
		}
	}

	c.server(sockname)
	return &c
}

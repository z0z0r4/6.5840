package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	args := Params{}
	for {
		reply := Response{}

		// fmt.Printf("Worker %d: call args: %+v\n", os.Getpid(), args)

		ok := call("Coordinator.Manage", &args, &reply)

		// fmt.Printf("Worker %d: call reply ok=%v, reply=%+v\n", os.Getpid(), ok, reply)

		if !ok {
			log.Printf("%d: call failed", os.Getpid())
			return
		} else {
			if reply.Done {
				// fmt.Printf("Worker %d: all tasks done, exiting\n", os.Getpid())
				return
			}

			// Run task
			if reply.HasAssign {
				if reply.TaskType == MapTask {
					// Run map task
					// Read file
					content, err := os.ReadFile(reply.FileName)
					if err != nil {
						log.Fatalf("cannot read %v", reply.FileName)
					}

					// Run map function
					kva := mapf(reply.FileName, string(content))

					// Create intermediate json files
					// intermediate files are named "mr-X-Y", where X is the map task number, and Y is the reduce task number.
					intermediate_file_name_prefix := "mr-tmp-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(reply.TaskID) + "-"
					intermediateFiles := make([]*os.File, reply.NReduce)

					for i := 0; i < reply.NReduce; i++ {
						intermediate_file_name := intermediate_file_name_prefix + strconv.Itoa(i) + ".json"
						intermediateFiles[i], err = os.Create(intermediate_file_name)
						if err != nil {
							log.Fatalf("cannot create %v", intermediate_file_name)
						}
					}

					encoders := make([]*json.Encoder, reply.NReduce)
					for i := 0; i < reply.NReduce; i++ {
						encoders[i] = json.NewEncoder(intermediateFiles[i])
					}

					// Write intermediate files
					for _, kv := range kva {
						reduceTaskNumber := ihash(kv.Key) % reply.NReduce
						enc := encoders[reduceTaskNumber]
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot write to %v", intermediateFiles[reduceTaskNumber].Name())
						}
					}

					// Close intermediate files
					for i := 0; i < reply.NReduce; i++ {
						intermediateFiles[i].Close()
					}

					// Rename tmp files to final intermediate files
					for i := 0; i < reply.NReduce; i++ {
						intermediate_file_name := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i) + ".json"
						err := os.Rename(intermediateFiles[i].Name(), intermediate_file_name)
						if err != nil {
							log.Fatalf("cannot rename %v to %v", intermediateFiles[i].Name(), intermediate_file_name)
						}
					}

					args.Finished = true
					args.TaskID = reply.TaskID
					args.TaskType = reply.TaskType
				} else if reply.TaskType == ReduceTask {
					// Run reduce task
					// Read intermediate files
					intermediate := []KeyValue{}
					for i := 0; i < reply.NMap; i++ {
						intermediate_file_name := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID) + ".json"
						file, err := os.Open(intermediate_file_name)
						if err != nil {
							log.Fatalf("cannot open %v", intermediate_file_name)
						}
						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							intermediate = append(intermediate, kv)
						}
						file.Close()
					}

					// Sort intermediate by key
					sort.Slice(intermediate, func(i, j int) bool {
						return intermediate[i].Key < intermediate[j].Key
					})

					// Generate k -> values and run reduce
					output_kv := make(map[string]string)
					i, j := 0, 0
					for i < len(intermediate) {
						j = i + 1
						for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, intermediate[k].Value)
						}
						output := reducef(intermediate[i].Key, values)
						output_kv[intermediate[i].Key] = output
						i = j
					}

					// Write output to file "mr-out-X", where X is the reduce task number.
					output_file_name := "mr-out-" + strconv.Itoa(reply.TaskID)
					output_file, err := os.Create(output_file_name)
					if err != nil {
						log.Fatalf("cannot create %v", output_file_name)
					}
					for k, v := range output_kv {
						_, err := fmt.Fprintf(output_file, "%v %v\n", k, v)
						if err != nil {
							log.Fatalf("cannot write to %v", output_file_name)
						}
					}
					output_file.Close()

					args.Finished = true
					args.TaskID = reply.TaskID
					args.TaskType = reply.TaskType
				}
			} else {
				time.Sleep(1 * time.Second)
				args.Finished = false
			}
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

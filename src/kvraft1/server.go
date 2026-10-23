package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	kvStore map[string]ValueVersion
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	rep := any(nil)
	if args, ok := req.(rpc.GetArgs); ok {
		if vv, ok := kv.kvStore[args.Key]; ok {
			rep = rpc.GetReply{Value: vv.Value, Version: vv.Version, Err: rpc.OK}
		} else {
			rep = rpc.GetReply{Err: rpc.ErrNoKey}
		}
	} else if args, ok := req.(rpc.PutArgs); ok {
		// println("DoOp: Put", args.Key, "with version", args.Version)
		if vv, ok := kv.kvStore[args.Key]; ok {
			if vv.Version == args.Version {
				kv.kvStore[args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
				rep = rpc.PutReply{Err: rpc.OK}
			} else {
				rep = rpc.PutReply{Err: rpc.ErrVersion}
			}
		} else if args.Version == 0 {
			kv.kvStore[args.Key] = ValueVersion{Value: args.Value, Version: 1}
			rep = rpc.PutReply{Err: rpc.OK}
		} else {
			rep = rpc.PutReply{Err: rpc.ErrNoKey}
		}
	} else {
		// print type
		println("DoOp: unknown request type")
	}

	return rep
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvStore)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, rep := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, rep := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.kvStore = make(map[string]ValueVersion)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

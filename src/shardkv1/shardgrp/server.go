package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu sync.Mutex
	// kvStore map[string]ValueVersion
	kvStore      map[shardcfg.Tshid]map[string]ValueVersion
	ownedShards  map[shardcfg.Tshid]bool // shards owned by this group
	frozenShards map[shardcfg.Tshid]bool // shards that are frozen (i.e., not accepting new Get/Put requests)

	configNum shardcfg.Tnum // current configuration number
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) applyShardStateLocked(shard shardcfg.Tshid, state []byte) {
	// Restore the state of the shard from the provided state
	shard_state := make(map[string]ValueVersion)
	r := bytes.NewReader(state)
	d := labgob.NewDecoder(r)
	d.Decode(&shard_state)

	kv.kvStore[shard] = shard_state
}

func (kv *KVServer) SnapshotShardStateLocked(shard shardcfg.Tshid) []byte {
	// Create a snapshot of the state of the specified shard
	shard_state := make(map[string]ValueVersion)
	if state, ok := kv.kvStore[shard]; ok {
		for k, v := range state {
			shard_state[k] = v
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shard_state)

	return w.Bytes()
}

func (kv *KVServer) DoGetOp(args *rpc.GetArgs) any {
	var rep any
	shard := shardcfg.Key2Shard(args.Key)
	if !kv.ownedShards[shard] {
		rep = rpc.GetReply{Err: rpc.ErrWrongGroup}
		return rep
	}

	if kv.frozenShards[shard] {
		rep = rpc.GetReply{Err: rpc.ErrShardNotReady}
		return rep
	}

	if vv, ok := kv.kvStore[shard][args.Key]; ok {
		rep = rpc.GetReply{Value: vv.Value, Version: vv.Version, Err: rpc.OK}
	} else {
		rep = rpc.GetReply{Err: rpc.ErrNoKey}
	}

	return rep
}

func (kv *KVServer) DoPutOp(args *rpc.PutArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var rep any
	shard := shardcfg.Key2Shard(args.Key)

	if !kv.ownedShards[shard] {
		rep = rpc.PutReply{Err: rpc.ErrWrongGroup}
		return rep
	}

	if kv.frozenShards[shard] {
		rep = rpc.PutReply{Err: rpc.ErrShardNotReady}
		return rep
	}

	if vv, ok := kv.kvStore[shard][args.Key]; ok {
		if vv.Version == args.Version {
			kv.kvStore[shard][args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
			rep = rpc.PutReply{Err: rpc.OK}
		} else {
			rep = rpc.PutReply{Err: rpc.ErrVersion}
		}
	} else if args.Version == 0 {
		kv.kvStore[shard][args.Key] = ValueVersion{Value: args.Value, Version: 1}
		rep = rpc.PutReply{Err: rpc.OK}
	} else {
		rep = rpc.PutReply{Err: rpc.ErrNoKey}
	}
	return rep
}

func (kv *KVServer) DoFrozenShardOp(args *shardrpc.FreezeShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var rep any

	if kv.configNum > args.Num {
		rep = shardrpc.FreezeShardReply{Err: rpc.ErrOldConfig, Num: kv.configNum}
		return rep
	}
	kv.configNum = args.Num

	if !kv.ownedShards[args.Shard] {
		rep = shardrpc.FreezeShardReply{Err: rpc.ErrWrongGroup, Num: kv.configNum}
		return rep
	}

	state := kv.SnapshotShardStateLocked(args.Shard)

	kv.frozenShards[args.Shard] = true
	rep = shardrpc.FreezeShardReply{Err: rpc.OK, Num: kv.configNum, State: state}
	return rep
}

func (kv *KVServer) DoInstallShardOp(args *shardrpc.InstallShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var rep any

	if kv.configNum > args.Num {
		rep = shardrpc.InstallShardReply{Err: rpc.ErrOldConfig, Num: kv.configNum}
		return rep
	}
	kv.configNum = args.Num

	kv.applyShardStateLocked(args.Shard, args.State)

	// Mark the shard as no longer frozen
	kv.frozenShards[args.Shard] = false
	kv.ownedShards[args.Shard] = true

	rep = shardrpc.InstallShardReply{Err: rpc.OK, Num: kv.configNum}
	return rep
}

func (kv *KVServer) DoDeleteShardOp(args *shardrpc.DeleteShardArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var rep any

	if kv.configNum > args.Num {
		rep = shardrpc.DeleteShardReply{Err: rpc.ErrOldConfig, Num: kv.configNum}
		return rep
	}
	kv.configNum = args.Num

	if !kv.ownedShards[args.Shard] {
		rep = shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup, Num: kv.configNum}
		return rep
	}

	// Delete the specified shard
	delete(kv.kvStore, args.Shard)

	// Mark the shard as no longer owned
	kv.ownedShards[args.Shard] = false

	rep = shardrpc.DeleteShardReply{Err: rpc.OK, Num: kv.configNum}
	return rep
}

func (kv *KVServer) DoOp(req any) any {
	rep := any(nil)
	if args, ok := req.(rpc.GetArgs); ok {
		return kv.DoGetOp(&args)
	} else if args, ok := req.(rpc.PutArgs); ok {
		return kv.DoPutOp(&args)
	} else if args, ok := req.(shardrpc.FreezeShardArgs); ok {
		return kv.DoFrozenShardOp(&args)
	} else if args, ok := req.(shardrpc.InstallShardArgs); ok {
		return kv.DoInstallShardOp(&args)
	} else if args, ok := req.(shardrpc.DeleteShardArgs); ok {
		return kv.DoDeleteShardOp(&args)
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
	// e.Encode(kv.shardsFrozen)
	// e.Encode(kv.ownedShards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvStore)
	// d.Decode(kv.shardsFrozen)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.FreezeShardReply)
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.InstallShardReply)
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.DeleteShardReply)
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// kv.kvStore = make(map[string]ValueVersion)
	kv.kvStore = make(map[shardcfg.Tshid]map[string]ValueVersion)
	for i := 0; i < shardcfg.NShards; i++ {
		kv.kvStore[shardcfg.Tshid(i)] = make(map[string]ValueVersion)
	}
	kv.frozenShards = make(map[shardcfg.Tshid]bool)
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	// Your code here

	if kv.gid == 1 {
		// Own all shards initially
		for i := 0; i < shardcfg.NShards; i++ {
			kv.ownedShards[shardcfg.Tshid(i)] = true
		}
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}

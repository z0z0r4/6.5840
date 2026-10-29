package kvraft

import (
	"bytes"
	"sync"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// How long a leader's lease lasts after a majority acknowledged it. A
// leader serves Gets from its own state while it holds a lease, and a
// new leader waits this long before it commits anything.
const LeaseDuration = 500 * time.Millisecond

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	kvStore map[string]ValueVersion

	// The last Put or Txn executed for each clerk, with its result.
	// Resends can end up in the log more than once; only the first copy
	// is executed.
	lastOp map[int64]clientOp
}

type clientOp struct {
	Seq    int64
	PutErr rpc.Err  // if the op was a Put
	Txn    TxnReply // if the op was a Txn
}

// Whether a request (clientId, seq) was already executed.
func (kv *KVServer) seenLocked(clientId, seq int64) (clientOp, bool) {
	op, ok := kv.lastOp[clientId]
	return op, clientId != 0 && ok && seq <= op.Seq
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
		if seen, ok := kv.seenLocked(args.ClientId, args.Seq); ok {
			return rpc.PutReply{Err: seen.PutErr}
		}
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
		if args.ClientId != 0 {
			kv.lastOp[args.ClientId] = clientOp{Seq: args.Seq, PutErr: rep.(rpc.PutReply).Err}
		}
	} else if args, ok := req.(TxnArgs); ok {
		if seen, ok := kv.seenLocked(args.ClientId, args.Seq); ok {
			return seen.Txn
		}
		r := kv.doTxnLocked(&args)
		if args.ClientId != 0 {
			kv.lastOp[args.ClientId] = clientOp{Seq: args.Seq, Txn: r}
		}
		rep = r
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
	e.Encode(kv.lastOp)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	kvStore := make(map[string]ValueVersion)
	lastOp := make(map[int64]clientOp)
	if d.Decode(&kvStore) != nil || d.Decode(&lastOp) != nil {
		panic("Restore: bad snapshot")
	}
	kv.kvStore = kvStore
	kv.lastOp = lastOp
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// A Get doesn't modify the state, so a leader holding a lease can
	// serve it without going through the log.
	err, rep := kv.rsm.Read(*args)
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

// Txn runs a transaction (see txn.go) as a single entry in the log.
func (kv *KVServer) Txn(args *TxnArgs, reply *TxnReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		*reply = rep.(TxnReply)
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
	labgob.Register(TxnArgs{})

	kv := &KVServer{me: me}

	kv.kvStore = make(map[string]ValueVersion)
	kv.lastOp = make(map[int64]clientOp)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	if !tester.NoLeaseReads {
		kv.rsm.EnableLeaseReads(LeaseDuration)
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

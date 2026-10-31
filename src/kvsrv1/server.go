package kvsrv

import (
	"log"
	"sync"

	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	kvStore map[string]ValueVersion

	// The last Put executed for each clerk, to answer resent Puts
	// without executing them a second time.
	lastPut map[uuid.UUID]rpc.Seen
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvStore = make(map[string]ValueVersion)
	kv.lastPut = make(map[uuid.UUID]rpc.Seen)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if vv, ok := kv.kvStore[args.Key]; ok {
		reply.Value = vv.Value
		reply.Version = vv.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// fmt.Printf("Put %s with version %d\n", args.Key, args.Version)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientId != uuid.Nil {
		if seen, ok := kv.lastPut[args.ClientId]; ok && args.Seq <= seen.Seq {
			// A resend of a Put we already executed. A clerk has at most
			// one Put outstanding, so an older Seq is a stale resend
			// whose reply nobody waits for.
			reply.Err = seen.Err
			return
		}
		defer func() {
			kv.lastPut[args.ClientId] = rpc.Seen{Seq: args.Seq, Err: reply.Err}
		}()
	}

	if vv, ok := kv.kvStore[args.Key]; ok {
		if vv.Version == args.Version {
			kv.kvStore[args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
			// fmt.Printf("OK\n")
		} else {
			reply.Err = rpc.ErrVersion
			// fmt.Printf("ErrVersion\n")
		}
	} else {
		if args.Version == 0 {
			kv.kvStore[args.Key] = ValueVersion{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
			// fmt.Printf("OK\n")
		} else {
			reply.Err = rpc.ErrNoKey
			// fmt.Printf("ErrNoKey\n")
		}
	}

	// // Print KVstore
	// for k, v := range kv.kvStore {
	// 	fmt.Printf("Key: %s, Value: %s, Version: %d\n", k, v.Value, v.Version)
	// }
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

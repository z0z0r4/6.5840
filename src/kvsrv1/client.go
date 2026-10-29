package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string

	clientId int64
	seq      int64 // Seq of the last Put
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server, clientId: rpc.NewClientId()}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, reply.Err
		} else if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, reply.Err
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server returns ErrVersion.
//
// Put has exactly-once semantics: every resend carries the same
// (clientId, Seq), so the server executes the Put at most once and
// answers resends with the result of that execution. Put therefore
// never needs to return ErrMaybe.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	ck.seq++
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, Seq: ck.seq}
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			return reply.Err
		}

		time.Sleep(100 * time.Millisecond)
	}
}

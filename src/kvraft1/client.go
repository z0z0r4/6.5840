package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrNoKey:
				return "", 0, reply.Err
			case rpc.ErrWrongLeader:
				// println("Get failed, errWrongLeader, try next server", ck.leader)
			default:
				println("Get failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	sent := false
	for {
		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrVersion:
				if sent {
					// Return ErrMaybe
					// ErrMaybe 说明可能已经被他人修改过了，或者说是之前 Put 成功但是未能收到响应
					return rpc.ErrMaybe
				} else {
					// Directly return ErrVersion
					return rpc.ErrVersion
				}
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrWrongLeader:
				// println("Put failed, errWrongLeader, try next server", ck.leader)
			default:
				println("Put failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		// Even an ErrWrongLeader reply may come from a server that already
		// appended the Put to its log, and that entry can still commit.
		sent = true

		time.Sleep(100 * time.Millisecond)
	}
}

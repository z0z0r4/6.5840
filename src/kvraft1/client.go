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
	misses  int // servers tried without success since the last success

	clientId int64
	seq      int64 // Seq of the last Put
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, clientId: rpc.NewClientId()}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Move on to the next server. Only pause after a whole round of servers
// failed (e.g., during an election), so that finding the leader among
// healthy servers doesn't cost a sleep per server.
func (ck *Clerk) nextServer() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
	ck.misses++
	if ck.misses%len(ck.servers) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
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
	defer func() { ck.misses = 0 }()
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
		ck.nextServer()
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server returns ErrVersion.
//
// Put has exactly-once semantics: every resend, to any server, carries
// the same (clientId, Seq), and the servers execute only the first copy
// that commits and answer later copies with its result. So the reply to
// a resend is the real outcome, and Put never returns ErrMaybe.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	defer func() { ck.misses = 0 }()
	ck.seq++
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, Seq: ck.seq}
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrVersion, rpc.ErrNoKey:
				return reply.Err
			case rpc.ErrWrongLeader:
			default:
				println("Put failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.nextServer()
	}
}

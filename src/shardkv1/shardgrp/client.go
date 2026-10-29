package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	fails := 0
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok {
			fails = 0
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrNoKey:
				return "", 0, reply.Err
			case rpc.ErrWrongLeader:
				// println("Get failed, errWrongLeader, try next server", ck.leader)
			case rpc.ErrWrongGroup:
				return "", 0, reply.Err
			case rpc.ErrShardNotReady:
				// wait for the shard to be ready
				time.Sleep(100 * time.Millisecond)
				continue
			default:
				println("Get failed, try next server", ck.leader, reply.Err)
			}
		}

		// No server in the group answered; it may have left and been shut
		// down, so let the caller re-query the config.
		if !ok {
			fails++
			if fails >= len(ck.servers) {
				return "", 0, rpc.ErrWrongGroup
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	sent := false
	fails := 0
	for {
		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := rpc.PutReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			fails = 0
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
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			case rpc.ErrShardNotReady:
				// wait for the shard to be ready
				time.Sleep(100 * time.Millisecond)
				continue
			default:
				println("Put failed, try next server", ck.leader, reply.Err)
			}
		}

		// No server in the group answered; it may have left and been shut
		// down, so let the caller re-query the config.
		if !ok {
			fails++
			if fails >= len(ck.servers) {
				return rpc.ErrWrongGroup
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		if !ok {
			sent = true
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Retry until the group's leader returns OK, ErrOldConfig or ErrWrongGroup.
func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	for {
		args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.State, rpc.OK
			case rpc.ErrOldConfig, rpc.ErrWrongGroup:
				return nil, reply.Err
			case rpc.ErrWrongLeader:
			default:
				println("FreezeShard failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	for {
		args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
		reply := shardrpc.InstallShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrOldConfig, rpc.ErrWrongGroup:
				return reply.Err
			case rpc.ErrWrongLeader:
			default:
				println("InstallShard failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	for {
		args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrOldConfig:
				return reply.Err
			case rpc.ErrWrongGroup:
				if reply.Num == num {
					// Already deleted by an earlier attempt whose reply was lost
					return rpc.OK
				}
				return reply.Err
			case rpc.ErrWrongLeader:
			default:
				println("DeleteShard failed, try next server", ck.leader, reply.Err)
			}
		}

		// ErrWrongLeader or RPC failure, try the next server
		ck.leader = (ck.leader + 1) % len(ck.servers)

		time.Sleep(100 * time.Millisecond)
	}
}

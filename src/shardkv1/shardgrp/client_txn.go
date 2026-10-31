package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

// Send a transaction request to the group's leader and return its
// reply. If no server answers (the group may have left and shut down),
// return ErrWrongGroup so that the caller re-queries the configuration.
func callTxn[A any, R any](ck *Clerk, method string, args *A, errOf func(*R) rpc.Err) (R, rpc.Err) {
	fails := 0
	for {
		var reply R
		if ck.Call(ck.servers[ck.leader], method, args, &reply) {
			fails = 0
			if err := errOf(&reply); err != rpc.ErrWrongLeader {
				return reply, err
			}
		} else {
			fails++
			if fails >= len(ck.servers) {
				return reply, rpc.ErrWrongGroup
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) TxnGet(args shardrpc.TxnGetArgs) shardrpc.TxnGetReply {
	reply, err := callTxn(ck, "KVServer.TxnGet", &args, func(r *shardrpc.TxnGetReply) rpc.Err { return r.Err })
	reply.Err = err
	return reply
}

func (ck *Clerk) TxnPrepare(args shardrpc.TxnPrepareArgs) rpc.Err {
	_, err := callTxn(ck, "KVServer.TxnPrepare", &args, func(r *shardrpc.TxnPrepareReply) rpc.Err { return r.Err })
	return err
}

func (ck *Clerk) TxnFinish(args shardrpc.TxnFinishArgs) rpc.Err {
	_, err := callTxn(ck, "KVServer.TxnFinish", &args, func(r *shardrpc.TxnFinishReply) rpc.Err { return r.Err })
	return err
}

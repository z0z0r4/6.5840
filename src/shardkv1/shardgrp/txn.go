package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

// Participant side of cross-shard transactions (two-phase commit with
// strict two-phase locking). The client that runs a transaction is its
// coordinator, see ../txn.go.
//
// Each shard has its own lock table and its own record of prepared and
// finished transactions, so that they move together with the shard's
// key/values when the shard moves to another group: a transaction that
// read or prepared a shard can commit it at the shard's new owner.
//
// Locks are acquired with no-wait: a request that conflicts with a lock
// held by another transaction aborts its transaction instead of
// waiting, so there are no deadlocks. Plain Gets and Puts don't take
// locks, but wait (ErrShardNotReady, which the clerk retries) while a
// conflicting lock is held, so they serialize with transactions.

// Lock on one key: a single writer (a prepared transaction) or readers.
type TxnLock struct {
	Writer  shardrpc.TxnId
	Readers map[shardrpc.TxnId]bool
}

type ShardTxns struct {
	Locks    map[string]*TxnLock
	Prepared map[shardrpc.TxnId]map[string]string // writes of prepared transactions
	Finished map[shardrpc.TxnId]bool              // committed or aborted here
}

func makeShardTxns() *ShardTxns {
	return &ShardTxns{
		Locks:    make(map[string]*TxnLock),
		Prepared: make(map[shardrpc.TxnId]map[string]string),
		Finished: make(map[shardrpc.TxnId]bool),
	}
}

func (st *ShardTxns) lock(key string) *TxnLock {
	l, ok := st.Locks[key]
	if !ok {
		l = &TxnLock{Readers: make(map[shardrpc.TxnId]bool)}
		st.Locks[key] = l
	}
	return l
}

// Whether txn may read key: no other transaction is writing it.
func (st *ShardTxns) canRead(txn shardrpc.TxnId, key string) bool {
	l, ok := st.Locks[key]
	return !ok || l.Writer == shardrpc.NoTxn || l.Writer == txn
}

// Whether txn may write key: no other transaction reads or writes it.
func (st *ShardTxns) canWrite(txn shardrpc.TxnId, key string) bool {
	l, ok := st.Locks[key]
	if !ok {
		return true
	}
	if l.Writer != shardrpc.NoTxn && l.Writer != txn {
		return false
	}
	for r := range l.Readers {
		if r != txn {
			return false
		}
	}
	return true
}

// Release txn's locks, forget its writes, and remember it finished.
func (st *ShardTxns) finish(txn shardrpc.TxnId) {
	for key, l := range st.Locks {
		delete(l.Readers, txn)
		if l.Writer == txn {
			l.Writer = shardrpc.NoTxn
		}
		if l.Writer == shardrpc.NoTxn && len(l.Readers) == 0 {
			delete(st.Locks, key)
		}
	}
	delete(st.Prepared, txn)
	st.Finished[txn] = true
}

// Whether a plain Get (write=false) or Put (write=true) of key must wait
// for a transaction.
func (st *ShardTxns) blocks(key string, write bool) bool {
	l, ok := st.Locks[key]
	if !ok {
		return false
	}
	return l.Writer != shardrpc.NoTxn || (write && len(l.Readers) > 0)
}

// The shard's txn state, if this group currently serves it.
func (kv *KVServer) servingLocked(shard shardcfg.Tshid) (*ShardTxns, rpc.Err) {
	if !kv.shards[shard].Owned {
		return nil, rpc.ErrWrongGroup
	}
	if kv.shards[shard].Frozen {
		return nil, shardrpc.ErrShardNotReady
	}
	return kv.txns[shard], rpc.OK
}

// Read key on behalf of a transaction, taking a read lock.
func (kv *KVServer) DoTxnGetOp(args *shardrpc.TxnGetArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := shardcfg.Key2Shard(args.Key)
	st, err := kv.servingLocked(shard)
	if err != rpc.OK {
		return shardrpc.TxnGetReply{Err: err}
	}
	if st.Finished[args.Txn] {
		return shardrpc.TxnGetReply{Err: shardrpc.ErrTxnAborted}
	}
	if !st.canRead(args.Txn, args.Key) {
		st.finish(args.Txn)
		return shardrpc.TxnGetReply{Err: shardrpc.ErrTxnAborted}
	}
	st.lock(args.Key).Readers[args.Txn] = true

	if vv, ok := kv.kvStore[shard][args.Key]; ok {
		return shardrpc.TxnGetReply{Value: vv.Value, Version: vv.Version, Err: rpc.OK}
	}
	return shardrpc.TxnGetReply{Err: rpc.ErrNoKey}
}

// Phase one: vote to commit (OK) if txn still holds its read locks and
// can take write locks on the keys it writes; otherwise abort it here
// and vote no (ErrTxnAborted).
func (kv *KVServer) DoTxnPrepareOp(args *shardrpc.TxnPrepareArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	st, err := kv.servingLocked(args.Shard)
	if err != rpc.OK {
		return shardrpc.TxnPrepareReply{Err: err}
	}
	if _, ok := st.Prepared[args.Txn]; ok {
		return shardrpc.TxnPrepareReply{Err: rpc.OK} // duplicate
	}
	if st.Finished[args.Txn] {
		return shardrpc.TxnPrepareReply{Err: shardrpc.ErrTxnAborted}
	}

	ok := true
	for _, key := range args.Reads {
		if l, held := st.Locks[key]; !held || !l.Readers[args.Txn] {
			ok = false
		}
	}
	for key := range args.Writes {
		if !st.canWrite(args.Txn, key) {
			ok = false
		}
	}
	if !ok {
		st.finish(args.Txn)
		return shardrpc.TxnPrepareReply{Err: shardrpc.ErrTxnAborted}
	}

	for key := range args.Writes {
		st.lock(key).Writer = args.Txn
	}
	writes := make(map[string]string, len(args.Writes))
	for k, v := range args.Writes {
		writes[k] = v
	}
	st.Prepared[args.Txn] = writes
	return shardrpc.TxnPrepareReply{Err: rpc.OK}
}

// Phase two: install txn's writes if it commits, then release its locks.
func (kv *KVServer) DoTxnFinishOp(args *shardrpc.TxnFinishArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	st, err := kv.servingLocked(args.Shard)
	if err != rpc.OK {
		return shardrpc.TxnFinishReply{Err: err}
	}
	if st.Finished[args.Txn] {
		return shardrpc.TxnFinishReply{Err: rpc.OK} // duplicate
	}
	if args.Commit {
		for key, value := range st.Prepared[args.Txn] {
			vv := kv.kvStore[args.Shard][key]
			kv.kvStore[args.Shard][key] = ValueVersion{Value: value, Version: vv.Version + 1}
		}
	}
	st.finish(args.Txn)
	return shardrpc.TxnFinishReply{Err: rpc.OK}
}

func (kv *KVServer) TxnGet(args *shardrpc.TxnGetArgs, reply *shardrpc.TxnGetReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.TxnGetReply)
	}
}

func (kv *KVServer) TxnPrepare(args *shardrpc.TxnPrepareArgs, reply *shardrpc.TxnPrepareReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.TxnPrepareReply)
	}
}

func (kv *KVServer) TxnFinish(args *shardrpc.TxnFinishArgs, reply *shardrpc.TxnFinishReply) {
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(shardrpc.TxnFinishReply)
	}
}

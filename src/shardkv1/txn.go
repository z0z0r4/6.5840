package shardkv

import (
	"math/rand"
	"sort"
	"time"

	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

// Cross-shard transactions with two-phase commit and strict two-phase
// locking. The Clerk running a transaction is its coordinator; every
// shard the transaction touches is a participant (see
// shardgrp/txn.go).
//
//	tx := ck.Begin()
//	v, _, err := tx.Get("a")  // takes a read lock on "a"
//	tx.Put("b", v)            // buffered until Commit
//	err = tx.Commit()         // OK, or ErrTxnAborted
//
// Get reads through a read lock held until the end of the transaction.
// Commit sends each participant shard the transaction's reads and
// writes in that shard (prepare); a participant votes yes if the read
// locks are still held and it can take write locks. If every
// participant votes yes, Commit tells them all to install the writes
// and release the locks; otherwise it aborts the transaction
// everywhere. Lock conflicts abort the transaction instead of waiting
// (no-wait), so transactions never deadlock; RunTxn retries aborted
// transactions.
//
// Limitation: the decision lives only in the coordinator. If a client
// crashes between prepare and finishing, the participants keep the
// transaction's locks.

type Txn struct {
	ck      *Clerk
	id      shardrpc.TxnId
	reads   map[string]bool
	writes  map[string]string
	shards  map[shardcfg.Tshid]bool // participants
	aborted bool
	done    bool
}

func (ck *Clerk) Begin() *Txn {
	return &Txn{
		ck:     ck,
		id:     shardrpc.TxnId(uuid.New()),
		reads:  make(map[string]bool),
		writes: make(map[string]string),
		shards: make(map[shardcfg.Tshid]bool),
	}
}

// Get returns key's value and version, or ErrTxnAborted if the
// transaction had to abort (Commit will then fail). A key the
// transaction wrote earlier reads its buffered value (with version 0).
func (tx *Txn) Get(key string) (string, rpc.Tversion, rpc.Err) {
	if tx.aborted {
		return "", 0, shardrpc.ErrTxnAborted
	}
	if v, ok := tx.writes[key]; ok {
		return v, 0, rpc.OK
	}
	shard := shardcfg.Key2Shard(key)
	tx.shards[shard] = true

	var reply shardrpc.TxnGetReply
	tx.ck.toShard(shard, func(g *shardgrp.Clerk) rpc.Err {
		reply = g.TxnGet(shardrpc.TxnGetArgs{Txn: tx.id, Key: key})
		return reply.Err
	})
	if reply.Err == shardrpc.ErrTxnAborted {
		tx.aborted = true
		return "", 0, shardrpc.ErrTxnAborted
	}
	tx.reads[key] = true
	return reply.Value, reply.Version, reply.Err
}

// Put buffers a write; it takes effect at Commit.
func (tx *Txn) Put(key, value string) {
	tx.writes[key] = value
	tx.shards[shardcfg.Key2Shard(key)] = true
}

// Commit runs two-phase commit. It returns OK if the transaction's
// writes were installed, or ErrTxnAborted if nothing changed.
func (tx *Txn) Commit() rpc.Err {
	if tx.done {
		panic("Commit of a finished transaction")
	}
	tx.done = true
	if tx.aborted {
		tx.finish(false)
		return shardrpc.ErrTxnAborted
	}

	reads := make(map[shardcfg.Tshid][]string)
	writes := make(map[shardcfg.Tshid]map[string]string)
	for key := range tx.reads {
		s := shardcfg.Key2Shard(key)
		reads[s] = append(reads[s], key)
	}
	for key, v := range tx.writes {
		s := shardcfg.Key2Shard(key)
		if writes[s] == nil {
			writes[s] = make(map[string]string)
		}
		writes[s][key] = v
	}

	for _, s := range tx.participants() {
		args := shardrpc.TxnPrepareArgs{Txn: tx.id, Shard: s, Reads: reads[s], Writes: writes[s]}
		err := tx.ck.toShard(s, func(g *shardgrp.Clerk) rpc.Err {
			return g.TxnPrepare(args)
		})
		if err != rpc.OK {
			tx.aborted = true
			tx.finish(false)
			return shardrpc.ErrTxnAborted
		}
	}
	tx.finish(true)
	return rpc.OK
}

// Abort releases the transaction's locks without writing anything.
func (tx *Txn) Abort() {
	if !tx.done {
		tx.done = true
		tx.aborted = true
		tx.finish(false)
	}
}

func (tx *Txn) participants() []shardcfg.Tshid {
	ss := make([]shardcfg.Tshid, 0, len(tx.shards))
	for s := range tx.shards {
		ss = append(ss, s)
	}
	sort.Slice(ss, func(i, j int) bool { return ss[i] < ss[j] })
	return ss
}

// Phase two: tell every participant the outcome, until each confirms.
func (tx *Txn) finish(commit bool) {
	for _, s := range tx.participants() {
		args := shardrpc.TxnFinishArgs{Txn: tx.id, Shard: s, Commit: commit}
		tx.ck.toShard(s, func(g *shardgrp.Clerk) rpc.Err {
			return g.TxnFinish(args)
		})
	}
}

// RunTxn runs fn in a transaction and commits it, retrying with a fresh
// transaction (after a random backoff) whenever it aborts because of a
// conflict. If fn returns an error other than OK or ErrTxnAborted, the
// transaction is aborted and RunTxn returns that error.
func (ck *Clerk) RunTxn(fn func(tx *Txn) rpc.Err) rpc.Err {
	backoff := 10 * time.Millisecond
	for {
		tx := ck.Begin()
		err := fn(tx)
		if err == rpc.OK {
			err = tx.Commit()
		} else {
			tx.Abort()
		}
		if err != shardrpc.ErrTxnAborted {
			return err
		}
		time.Sleep(time.Duration(rand.Int63n(int64(backoff))))
		backoff = min(2*backoff, 500*time.Millisecond)
	}
}

// Send a request for shard to the group that owns it, following the
// shard if it moves: re-query the configuration on ErrWrongGroup and
// wait while the shard is frozen for a move. Returns the first other
// reply.
func (ck *Clerk) toShard(shard shardcfg.Tshid, call func(*shardgrp.Clerk) rpc.Err) rpc.Err {
	for {
		gid := ck.config.Shards[shard]
		g, ok := ck.GetClerk(gid)
		if !ok {
			g = ck.makeGrpClerk(gid, ck.config.Groups[gid])
		}
		switch err := call(g); err {
		case rpc.ErrWrongGroup, shardrpc.ErrShardNotReady:
			time.Sleep(100 * time.Millisecond)
			ck.config = ck.sck.Query()
		default:
			return err
		}
	}
}

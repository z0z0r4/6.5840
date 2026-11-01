package shardrpc

import (
	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

// Errors specific to shardkv, beyond the ones in kvsrv1/rpc shared by
// all the key/value services.
const (
	ErrShardNotReady rpc.Err = "ErrShardNotReady" // shard is frozen for a move, or locked by a transaction
	ErrOldConfig     rpc.Err = "ErrOldConfig"     // request is for an older configuration
	ErrDuplicateOp   rpc.Err = "ErrDuplicateOp"   // reconfiguration step already done
	ErrTxnAborted    rpc.Err = "ErrTxnAborted"    // transaction aborted
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	// State []byte
	Num shardcfg.Tnum
	Err rpc.Err
}

type GetShardArgs struct {
	Shard shardcfg.Tshid
}

type GetShardReply struct {
	Err    rpc.Err
	State  []byte
	Owned  bool
	Frozen bool
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
	Num shardcfg.Tnum
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
	Num shardcfg.Tnum
}

// Cross-shard transactions (see shardgrp/txn.go and ../txn.go).

type TxnId uuid.UUID

// NoTxn (the zero TxnId) names no transaction, e.g. no lock holder.
var NoTxn TxnId

// Read Key for transaction Txn, taking a read lock.
type TxnGetArgs struct {
	Txn TxnId
	Key string
}

type TxnGetReply struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}

// Phase one for Shard: Reads are the keys of Shard that Txn read (and
// must still hold read locks on), Writes the values it writes.
type TxnPrepareArgs struct {
	Txn    TxnId
	Shard  shardcfg.Tshid
	Reads  []string
	Writes map[string]string
}

type TxnPrepareReply struct {
	Err rpc.Err // OK: vote to commit; ErrTxnAborted: vote to abort
}

// Phase two for Shard: commit (install Txn's writes) or abort, and
// release Txn's locks.
type TxnFinishArgs struct {
	Txn    TxnId
	Shard  shardcfg.Tshid
	Commit bool
}

type TxnFinishReply struct {
	Err rpc.Err
}

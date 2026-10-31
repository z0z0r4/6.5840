package rpc

import (
	"github.com/google/uuid"
)

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	// ShardKV specific errors
	ErrShardNotReady = "ErrShardNotReady"
	ErrOldConfig     = "ErrOldConfig"
	ErrDuplicateOp   = "ErrDuplicateOp"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion

	// For exactly-once Puts: a clerk numbers its Puts 1, 2, ... and
	// resends a Put with the same Seq, so the server can recognize a
	// duplicate and return the result of the first execution. A nil
	// ClientId (uuid.Nil) opts out of duplicate detection.
	ClientId uuid.UUID
	Seq      int64
}

// Seen records the last Put a server executed for one clerk.
type Seen struct {
	Seq int64
	Err Err
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

// Range returns every key in [Low, High], in ascending order.
type RangeArgs struct {
	Low  string
	High string
}

type KeyValue struct {
	Key     string
	Value   string
	Version Tversion
}

type RangeReply struct {
	KVs []KeyValue
	Err Err
}

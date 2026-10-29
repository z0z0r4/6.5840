package kvraft

import (
	"6.5840/kvsrv1/rpc"
)

// Transactions in the style of etcd's Txn: a list of comparisons
// guards two lists of operations. The server evaluates all comparisons
// against its current state; if they all hold it executes Then,
// otherwise Else. The whole transaction runs as a single entry in the
// Raft log, so it is atomic and isolated from every other request.

type CmpTarget int

const (
	CmpVersion CmpTarget = iota // compare the key's version (0 if absent)
	CmpValue                    // compare the key's value ("" if absent)
)

type CmpOp int

const (
	EQ CmpOp = iota
	NE
	LT
	GT
)

type Cmp struct {
	Key     string
	Target  CmpTarget
	Op      CmpOp
	Version rpc.Tversion // for CmpVersion
	Value   string       // for CmpValue
}

type OpType int

const (
	OpGet OpType = iota
	OpPut
)

type TxnOp struct {
	Type  OpType
	Key   string
	Value string // for OpPut
}

// Unlike the versioned Put RPC, a Put inside a transaction is
// unconditional: guard it with a CmpVersion comparison instead.
func TxnGet(key string) TxnOp        { return TxnOp{Type: OpGet, Key: key} }
func TxnPut(key, value string) TxnOp { return TxnOp{Type: OpPut, Key: key, Value: value} }

func VersionIs(key string, op CmpOp, v rpc.Tversion) Cmp {
	return Cmp{Key: key, Target: CmpVersion, Op: op, Version: v}
}
func ValueIs(key string, op CmpOp, v string) Cmp {
	return Cmp{Key: key, Target: CmpValue, Op: op, Value: v}
}

type TxnArgs struct {
	If   []Cmp
	Then []TxnOp
	Else []TxnOp

	// for exactly-once execution, shared with Puts
	ClientId int64
	Seq      int64
}

// The result of one TxnOp. For OpGet, Value and Version are the key's
// (Err is ErrNoKey if it is absent); for OpPut, Version is the key's
// new version.
type OpResult struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}

type TxnReply struct {
	Err       rpc.Err
	Succeeded bool // whether Then (rather than Else) ran
	Results   []OpResult
}

func (c Cmp) holds(vv ValueVersion) bool {
	var r int
	if c.Target == CmpVersion {
		r = cmp3(vv.Version < c.Version, vv.Version > c.Version)
	} else {
		r = cmp3(vv.Value < c.Value, vv.Value > c.Value)
	}
	switch c.Op {
	case EQ:
		return r == 0
	case NE:
		return r != 0
	case LT:
		return r < 0
	case GT:
		return r > 0
	}
	return false
}

func cmp3(lt, gt bool) int {
	if lt {
		return -1
	}
	if gt {
		return 1
	}
	return 0
}

// Execute a transaction against kv.kvStore. Caller holds kv.mu.
func (kv *KVServer) doTxnLocked(args *TxnArgs) TxnReply {
	succeeded := true
	for _, c := range args.If {
		if !c.holds(kv.kvStore[c.Key]) {
			succeeded = false
			break
		}
	}
	ops := args.Then
	if !succeeded {
		ops = args.Else
	}

	results := make([]OpResult, len(ops))
	for i, op := range ops {
		vv, ok := kv.kvStore[op.Key]
		switch op.Type {
		case OpGet:
			if ok {
				results[i] = OpResult{Value: vv.Value, Version: vv.Version, Err: rpc.OK}
			} else {
				results[i] = OpResult{Err: rpc.ErrNoKey}
			}
		case OpPut:
			nvv := ValueVersion{Value: op.Value, Version: vv.Version + 1}
			kv.kvStore[op.Key] = nvv
			results[i] = OpResult{Version: nvv.Version, Err: rpc.OK}
		}
	}
	return TxnReply{Err: rpc.OK, Succeeded: succeeded, Results: results}
}

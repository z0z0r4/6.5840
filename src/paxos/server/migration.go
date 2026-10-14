package server

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"6.5840/paxos/consensus"
	"6.5840/paxos/kv"
	"6.5840/paxos/types"
)

// The previous format embedded KV fields in protocol records. Keep that schema
// confined to this application adapter; consensus knows only opaque snapshots.
type legacyRequest struct {
	Op  kv.Operation
	Seq int
}
type legacyCheckpoint struct {
	Seq      int
	KV       map[string]string
	Requests map[string]legacyRequest
}
type legacyInstance struct {
	ProposalAcceptNumber types.ProposalID
	Op                   kv.Operation
	Decided              bool
	ReadValue            string
}
type legacySnapshot struct {
	Checkpoint             legacyCheckpoint
	Version                int
	Peers                  []string
	Insts                  map[int]legacyInstance
	KV                     map[string]string
	Requests               map[string]legacyRequest
	MaxSeq, Applied        int
	RoundNum               uint64
	PromisedProposalNumber types.ProposalID
}

func upgradeKVSnapshot(data []byte) (consensus.Snapshot, error) {
	var old legacySnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&old); err != nil {
		return consensus.Snapshot{}, err
	}
	if old.Version != 4 {
		return consensus.Snapshot{}, fmt.Errorf("unsupported KV snapshot version %d", old.Version)
	}
	if old.KV == nil || old.Checkpoint.KV == nil || old.Requests == nil || old.Checkpoint.Requests == nil || old.Insts == nil {
		return consensus.Snapshot{}, fmt.Errorf("invalid legacy KV snapshot")
	}
	state, _ := json.Marshal(old.KV)
	checkpointState, _ := json.Marshal(old.Checkpoint.KV)
	records := func(old map[string]legacyRequest) map[string]consensus.RequestRecord {
		out := make(map[string]consensus.RequestRecord, len(old))
		for id, r := range old {
			out[id] = consensus.RequestRecord{Op: kv.Encode(r.Op), Seq: r.Seq}
		}
		return out
	}
	insts := make(map[int]consensus.Instance, len(old.Insts))
	for seq, i := range old.Insts {
		insts[seq] = consensus.Instance{ProposalAcceptNumber: i.ProposalAcceptNumber, Op: kv.Encode(i.Op), Decided: i.Decided, Result: i.ReadValue}
	}
	return consensus.Snapshot{Version: consensus.SnapshotVersion, MachineKind: kv.NewStateMachine(kv.New()).Kind(), Peers: old.Peers, State: string(state), Checkpoint: consensus.Checkpoint{Seq: old.Checkpoint.Seq, State: string(checkpointState), Requests: records(old.Checkpoint.Requests)}, Insts: insts, Requests: records(old.Requests), MaxSeq: old.MaxSeq, Applied: old.Applied, RoundNum: old.RoundNum, PromisedProposalNumber: old.PromisedProposalNumber}, nil
}

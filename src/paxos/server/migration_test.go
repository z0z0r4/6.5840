package server

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"6.5840/paxos/consensus"
	"6.5840/paxos/kv"
	"6.5840/paxos/types"
)

func TestLegacyKVSnapshotMigratesAndPreservesDedup(t *testing.T) {
	dir := t.TempDir()
	peers := []string{"local"}
	term := types.ProposalID{Round: 7, NodeID: 0}
	op := kv.Operation{ReqID: "original", Action: "set", Key: "k", Value: "original"}
	records := map[string]legacyRequest{op.ReqID: {Op: op, Seq: 1}}
	old := legacySnapshot{Version: 4, Peers: peers, KV: map[string]string{"k": "newer"}, Requests: records, Checkpoint: legacyCheckpoint{Seq: 1, KV: map[string]string{"k": "original"}, Requests: records}, Insts: map[int]legacyInstance{
		2: {ProposalAcceptNumber: term, Op: kv.Operation{ReqID: "later", Action: "set", Key: "k", Value: "newer"}, Decided: true},
		3: {ProposalAcceptNumber: term, Op: kv.Operation{Action: "get", Key: "k"}, Decided: true, ReadValue: "newer"},
		4: {ProposalAcceptNumber: term, Op: kv.Operation{ReqID: "pending", Action: "set", Key: "next", Value: "pending"}},
	}, MaxSeq: 4, Applied: 3, RoundNum: 8, PromisedProposalNumber: term}
	old.Requests["later"] = legacyRequest{Op: old.Insts[2].Op, Seq: 2}
	old.Checkpoint.Requests = map[string]legacyRequest{op.ReqID: {Op: op, Seq: 1}}
	path := filepath.Join(dir, "peer-0.gob")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gob.NewEncoder(file).Encode(old); err != nil {
		t.Fatal(err)
	}
	file.Close()
	s, err := New(0, peers, dir)
	if err != nil {
		t.Fatal(err)
	}
	state := s.Inspect()
	store := kv.New()
	if err := store.Restore(state.State); err != nil {
		t.Fatal(err)
	}
	if store.Get("k") != "newer" || state.Applied != 3 || state.MaxSeq != 4 || state.ProposalNumber != term || state.Requests[op.ReqID].Op != kv.Encode(op) || state.Insts[3].Result != "newer" || state.Insts[4].Decided || state.Insts[4].Op != kv.Encode(old.Insts[4].Op) {
		t.Fatalf("migration lost application/protocol state: %+v", state)
	}
	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var upgraded consensus.Snapshot
	err = gob.NewDecoder(file).Decode(&upgraded)
	file.Close()
	if err != nil || upgraded.Version != consensus.SnapshotVersion {
		t.Fatalf("migration not durably written: %v", err)
	}
	again, err := New(0, peers, dir)
	if err != nil || !reflect.DeepEqual(state, again.Inspect()) {
		t.Fatalf("upgraded snapshot did not survive restart: %v", err)
	}
}

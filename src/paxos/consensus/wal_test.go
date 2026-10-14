package consensus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"maps"
	"os"
	"testing"

	"6.5840/paxos/kv"
	"6.5840/paxos/types"
	"6.5840/paxos/wal"
)

func TestWALRestoresPromisesAcceptedSuffixAndCommittedState(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	before, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	term := types.ProposalID{Round: 4, NodeID: 0}
	var prepare LeaderPrepareReply
	if err := s.Prepare(&LeaderPrepareArgs{ProposalNumber: term, LeaderID: 0}, &prepare); err != nil || !prepare.Prepared {
		t.Fatal("prepare failed")
	}
	op := command(kv.Operation{ReqID: "once", Action: "set", Key: "k", Value: "v"})
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: term, Value: op}, &accepted)
	decideOperation(t, s, 1, op)
	// Recovery must preserve an accepted but not decided later slot too.
	suffix := command(kv.Operation{ReqID: "pending", Action: "set", Key: "next", Value: "pending"})
	s.Accept(&AcceptArgs{Seq: 2, ProposalNumber: term, Value: suffix}, &accepted)
	after, _ := os.ReadFile(s.statePath)
	if !bytes.Equal(before, after) {
		t.Fatal("ordinary writes rewrote the full snapshot")
	}
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	if restored.applied != 1 || restored.max_seq != 2 || values(restored)["k"] != "v" || restored.requests["once"].Seq != 1 || restored.insts[2].Op != suffix || restored.insts[2].Decided || restored.promisedProposalNumber != term {
		t.Fatal("WAL lost protocol, state or dedup information")
	}
	var lower AcceptReply
	restored.Accept(&AcceptArgs{Seq: 3, ProposalNumber: types.ProposalID{Round: 3, NodeID: 0}, Value: op}, &lower)
	if lower.Accepted {
		t.Fatal("WAL recovery forgot global promise")
	}
	decideOperation(t, restored, 3, command(kv.Operation{Action: "set", Key: "k", Value: "newer"}))
	decideOperation(t, restored, 2, suffix)
	decideOperation(t, restored, 4, op)
	again, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	if again.applied != 4 || values(again)["k"] != "newer" || again.requests["once"].Seq != 1 {
		t.Fatal("replayed duplicate overwrote newer state")
	}
}

func TestWALSequenceProtectsCrashBetweenSnapshotAndReset(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	for seq := 1; seq <= 3; seq++ {
		decideOperation(t, s, seq, command(kv.Operation{ReqID: fmt.Sprint(seq), Action: "set", Key: "k", Value: fmt.Sprint(seq)}))
	}
	oldWAL, err := os.ReadFile(s.walPath())
	if err != nil {
		t.Fatal(err)
	}
	// The next image covers a new sequence without rotating the old WAL, exactly
	// the on-disk state possible after snapshot fsync/rename but before WAL reset.
	s.mu.Lock()
	err = s.writeSnapshotLocked(s.walSequence + 1)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	if restored.applied != 3 || values(restored)["k"] != "3" || len(restored.requests) != 3 {
		t.Fatal("covered WAL records were applied twice")
	}
	decideOperation(t, restored, 4, command(kv.Operation{ReqID: "four", Action: "set", Key: "k", Value: "4"}))
	newWAL, _ := os.ReadFile(s.walPath())
	if !bytes.HasPrefix(newWAL, oldWAL) {
		t.Fatal("new append lost the unrotated journal")
	}
	again, err := newServer(0, s.peers, tDir(s))
	if err != nil || again.applied != 4 || values(again)["k"] != "4" {
		t.Fatalf("append after interrupted rotation: %v", err)
	}
}

func TestWALCheckpointLimitsAndReplayBeforeApplication(t *testing.T) {
	for _, mode := range []string{"records", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			config := Config{ID: 0, Peers: []string{"counter"}, DataDir: t.TempDir(), Machine: &counterMachine{}}
			if mode == "records" {
				config.WALMaxRecords = 1
			} else {
				config.WALMaxBytes = 1
			}
			node, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			// Persist a learned decision but crash before application. With limits of
			// one record/byte the checkpoint contains a decided, not yet applied slot.
			cmd := Command{ReqID: "increment", Data: "5"}
			node.mu.Lock()
			_, err = node.learnDecisionLocked(1, Instance{Op: cmd, Decided: true})
			if err == nil {
				err = node.persistLocked()
			}
			node.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			stat, err := os.Stat(node.walPath())
			if err != nil || stat.Size() != wal.FileHeaderBytes {
				t.Fatalf("journal was not reclaimed: %v", err)
			}
			config.Machine = &counterMachine{}
			restored, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if restored.applied != 1 || restored.machine.Snapshot() != "5" || restored.requests["increment"].Result != "5" {
				t.Fatal("checkpoint lost durable unapplied decision or result")
			}
			for seq := 2; seq <= LogRetention+2; seq++ {
				restored.mu.Lock()
				_, err := restored.decideLocked(seq, Instance{Op: Command{Data: "1"}, Decided: true})
				restored.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			config.Machine = &counterMachine{}
			again, err := New(config)
			if err != nil || again.applied != LogRetention+2 || again.machine.Snapshot() != fmt.Sprint(LogRetention+6) {
				t.Fatalf("checkpoint/compaction restart: %v", err)
			}
		})
	}
}

func TestWALTailRepairAndCorruptionAtNodeStartup(t *testing.T) {
	for _, mode := range []string{"torn", "corrupt", "missing-base", "sequence-gap"} {
		t.Run(mode, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0"})
			decideOperation(t, s, 1, command(kv.Operation{ReqID: "durable", Action: "set", Key: "k", Value: "safe"}))
			before, err := os.ReadFile(s.walPath())
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "torn":
				file, err := os.OpenFile(s.walPath(), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				file.Write([]byte{1, 2, 3})
				file.Close()
			case "corrupt":
				before[len(before)-1] ^= 1
				os.WriteFile(s.walPath(), before, 0600)
			case "missing-base":
				os.Remove(s.statePath)
			case "sequence-gap":
				var payload bytes.Buffer
				gob.NewEncoder(&payload).Encode(journalRecord{Sequence: s.walSequence + 2, RoundNum: s.round_num, MaxSeq: s.max_seq, PromisedProposalNumber: s.promisedProposalNumber})
				wal.Append(s.walPath(), payload.Bytes())
			}
			restored, err := newServer(0, s.peers, tDir(s))
			if mode != "torn" {
				if err == nil {
					t.Fatal("invalid WAL recovery succeeded")
				}
				return
			}
			if err != nil || restored.applied != 1 || values(restored)["k"] != "safe" {
				t.Fatalf("torn recovery: %v", err)
			}
			repaired, _ := os.ReadFile(s.walPath())
			if !bytes.Equal(repaired, before) {
				t.Fatal("incomplete tail was not removed")
			}
			decideOperation(t, restored, 2, command(kv.Operation{Action: "set", Key: "next", Value: "ok"}))
			if _, err := newServer(0, s.peers, tDir(s)); err != nil {
				t.Fatalf("append after repair failed: %v", err)
			}
		})
	}
}

func TestPreWALSnapshotRemainsCompatible(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	decideOperation(t, s, 1, command(kv.Operation{ReqID: "legacy-v5", Action: "set", Key: "k", Value: "v"}))
	// Encoding the old schema omits WALSequence, as in snapshots from the
	// preceding version of this project. A new journal starts at sequence one.
	type preWALSnapshot struct {
		Checkpoint             Checkpoint
		Version                int
		Peers                  []string
		Insts                  map[int]Instance
		State, MachineKind     string
		Requests               map[string]RequestRecord
		MaxSeq, Applied        int
		RoundNum               uint64
		PromisedProposalNumber types.ProposalID
	}
	file, err := os.Create(s.statePath)
	if err != nil {
		t.Fatal(err)
	}
	err = gob.NewEncoder(file).Encode(preWALSnapshot{Checkpoint: s.checkpoint, Version: 5, Peers: s.peers, Insts: maps.Clone(s.insts), State: s.machine.Snapshot(), MachineKind: s.machine.Kind(), Requests: maps.Clone(s.requests), MaxSeq: s.max_seq, Applied: s.applied, RoundNum: s.round_num, PromisedProposalNumber: s.promisedProposalNumber})
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(s.walPath())
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil || restored.applied != 1 {
		t.Fatalf("old snapshot did not load: %v", err)
	}
	decideOperation(t, restored, 2, command(kv.Operation{Action: "set", Key: "next", Value: "ok"}))
	again, err := newServer(0, s.peers, tDir(s))
	if err != nil || values(again)["next"] != "ok" || again.requests["legacy-v5"].Seq != 1 {
		t.Fatalf("old snapshot plus WAL failed: %v", err)
	}
}

type checkedCounter struct {
	counterMachine
	beforeApply func(Command)
}

func (m *checkedCounter) Apply(cmd Command) string {
	m.beforeApply(cmd)
	return m.counterMachine.Apply(cmd)
}
func TestDecisionIsInWALBeforeStateMachineApply(t *testing.T) {
	machine := &checkedCounter{}
	node, err := New(Config{ID: 0, Peers: []string{"counter"}, DataDir: t.TempDir(), Machine: machine})
	if err != nil {
		t.Fatal(err)
	}
	observed := false
	machine.beforeApply = func(cmd Command) {
		found := false
		_, err := wal.Replay(node.walPath(), func(payload []byte) error {
			var record journalRecord
			if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&record); err != nil {
				return err
			}
			if record.Sequence == node.walSequence {
				for _, inst := range record.Entries {
					if inst.Decided && inst.Op == cmd {
						found = true
					}
				}
			}
			return nil
		})
		if err != nil || !found {
			t.Fatalf("application ran before its decision was journaled: %v", err)
		}
		observed = true
	}
	var reply DecideReply
	if err := node.Decide(&DecideArgs{Seq: 1, Instance: Instance{Op: Command{ReqID: "count", Data: "3"}, Decided: true}}, &reply); err != nil || !reply.Result || !observed {
		t.Fatalf("decision apply: %v", err)
	}
}

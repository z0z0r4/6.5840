package consensus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"6.5840/paxos/types"
)

const SnapshotVersion = 5
const snapshotVersion = SnapshotVersion

// Snapshot is the durable consensus state, including an opaque machine image.
type Snapshot struct {
	Checkpoint             Checkpoint
	Version                int
	Peers                  []string
	Insts                  map[int]Instance
	State                  string
	MachineKind            string
	Requests               map[string]RequestRecord
	MaxSeq                 int
	Applied                int
	RoundNum               uint64
	PromisedProposalNumber types.ProposalID
}

type serverSnapshot = Snapshot

// load restores a snapshot before the node begins accepting RPCs. Application
// migrations are injected; the consensus package never decodes application data.
func (s *Node) load(upgrade func([]byte) (Snapshot, error)) error {
	data, err := os.ReadFile(s.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var header struct{ Version int }
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&header); err != nil {
		return fmt.Errorf("decode %s: %w", s.statePath, err)
	}
	var snapshot Snapshot
	migrated := header.Version != snapshotVersion
	if migrated {
		if upgrade == nil {
			return fmt.Errorf("unsupported snapshot version %d", header.Version)
		}
		snapshot, err = upgrade(data)
	} else {
		err = gob.NewDecoder(bytes.NewReader(data)).Decode(&snapshot)
	}
	if err != nil {
		return fmt.Errorf("decode %s: %w", s.statePath, err)
	}
	if snapshot.Version != snapshotVersion || snapshot.MachineKind != s.machine.Kind() || !slices.Equal(snapshot.Peers, s.peers) {
		return fmt.Errorf("snapshot %s does not match this cluster or state machine", s.statePath)
	}
	if snapshot.Insts == nil || snapshot.Requests == nil || snapshot.Applied < 0 || snapshot.Applied > snapshot.MaxSeq || snapshot.Checkpoint.Seq < 0 || snapshot.Checkpoint.Seq > snapshot.Applied || snapshot.Checkpoint.Requests == nil {
		return fmt.Errorf("snapshot %s has invalid state", s.statePath)
	}
	if err := s.machine.Restore(snapshot.Checkpoint.State); err != nil {
		return fmt.Errorf("restore checkpoint: %w", err)
	}
	if err := s.machine.Restore(snapshot.State); err != nil {
		return fmt.Errorf("restore state: %w", err)
	}
	s.checkpoint = snapshot.Checkpoint
	s.insts = snapshot.Insts
	s.requests = snapshot.Requests
	s.max_seq = snapshot.MaxSeq
	s.applied = snapshot.Applied
	s.round_num = snapshot.RoundNum
	s.promisedProposalNumber = snapshot.PromisedProposalNumber
	s.leaseGrantUntil = time.Now().Add(followerLeaseDuration)
	if migrated {
		return s.persistLocked()
	}
	return nil
}

// persistLocked atomically replaces this node's snapshot. The caller holds s.mu.
func (s *Node) persistLocked() error {
	if s.statePath == "" {
		return nil
	}

	dir := filepath.Dir(s.statePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".paxos-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	snapshot := serverSnapshot{
		Checkpoint: s.checkpoint, Version: snapshotVersion, Peers: s.peers, Insts: s.insts, State: s.machine.Snapshot(), MachineKind: s.machine.Kind(), Requests: s.requests,
		MaxSeq: s.max_seq, Applied: s.applied, RoundNum: s.round_num, PromisedProposalNumber: s.promisedProposalNumber,
	}
	if err := gob.NewEncoder(file).Encode(snapshot); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), s.statePath); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func New(config Config) (*Node, error) {
	me, peers, dataDir := config.ID, config.Peers, config.DataDir
	if config.Machine == nil {
		return nil, fmt.Errorf("state machine is required")
	}
	if me < 0 || me >= len(peers) {
		return nil, fmt.Errorf("peer index %d out of range", me)
	}
	s := &Node{
		own_number:             me,
		proposalSlot:           make(chan struct{}, 1),
		leader:                 -1,
		leaderProposalNumber:   types.ProposalID{},
		promisedProposalNumber: types.ProposalID{},
		lastHeartbeatTime:      time.Now(),
		round_num:              1,
		peers:                  slices.Clone(peers),
		majority:               len(peers)/2 + 1,
		insts:                  make(map[int]Instance),
		pinnedResults:          make(map[int]bool),
		machine:                config.Machine,
		requests:               make(map[string]RequestRecord),
		checkpoint:             Checkpoint{State: config.Machine.Snapshot(), Requests: make(map[string]RequestRecord)},
		statePath:              filepath.Join(dataDir, fmt.Sprintf("peer-%d.gob", me)),
	}
	if err := s.load(config.SnapshotUpgrade); err != nil {
		return nil, err
	}
	return s, nil
}

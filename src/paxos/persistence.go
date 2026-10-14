package paxos

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const snapshotVersion = 3

type serverSnapshot struct {
	Checkpoint             Checkpoint
	Version                int
	Peers                  []string
	Insts                  map[int]Instance
	KV                     map[string]string
	Requests               map[string]RequestRecord
	MaxSeq                 int
	Applied                int
	RoundNum               int
	PromisedProposalNumber int
}

// load restores a snapshot before the server begins accepting RPCs.
func (s *Server) load() error {
	file, err := os.Open(s.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	var snapshot serverSnapshot
	if err := gob.NewDecoder(file).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode %s: %w", s.statePath, err)
	}
	if snapshot.Version != snapshotVersion || !slices.Equal(snapshot.Peers, s.peers) {
		return fmt.Errorf("snapshot %s does not match this cluster", s.statePath)
	}
	if snapshot.Insts == nil || snapshot.KV == nil || snapshot.Requests == nil || snapshot.Applied > snapshot.MaxSeq || snapshot.Checkpoint.Seq < 0 || snapshot.Checkpoint.Seq > snapshot.Applied || snapshot.Checkpoint.KV == nil || snapshot.Checkpoint.Requests == nil {
		return fmt.Errorf("snapshot %s has invalid state", s.statePath)
	}

	s.checkpoint = snapshot.Checkpoint
	s.insts = snapshot.Insts
	s.kv = snapshot.KV
	s.requests = snapshot.Requests
	s.max_seq = snapshot.MaxSeq
	s.applied = snapshot.Applied
	s.round_num = snapshot.RoundNum
	s.promisedProposalNumber = snapshot.PromisedProposalNumber
	// A heartbeat grant is volatile. After restart, refuse all proposals for
	// a full replica lease rather than forgetting a still-live commitment.
	s.leaseGrantUntil = time.Now().Add(followerLeaseDuration)
	return nil
}

// persistLocked atomically replaces this node's snapshot. The caller holds s.mu.
func (s *Server) persistLocked() error {
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
		Checkpoint: s.checkpoint, Version: snapshotVersion, Peers: s.peers, Insts: s.insts, KV: s.kv, Requests: s.requests,
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

func newServer(me int, peers []string, dataDir string) (*Server, error) {
	if me < 0 || me >= len(peers) {
		return nil, fmt.Errorf("peer index %d out of range", me)
	}
	s := &Server{
		own_number:             me,
		proposalSlot:           make(chan struct{}, 1),
		leader:                 -1,
		leaderProposalNumber:   -1,
		promisedProposalNumber: -1,
		lastHeartbeatTime:      time.Now(),
		peers:                  peers,
		majority:               len(peers)/2 + 1,
		insts:                  make(map[int]Instance),
		kv:                     make(map[string]string),
		requests:               make(map[string]RequestRecord),
		checkpoint:             Checkpoint{KV: make(map[string]string), Requests: make(map[string]RequestRecord)},
		statePath:              filepath.Join(dataDir, fmt.Sprintf("peer-%d.gob", me)),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

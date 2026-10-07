package paxos

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

const snapshotVersion = 1

type serverSnapshot struct {
	Version  int
	Peers    []string
	Insts    map[int]PaxosInstance
	KV       map[string]string
	MaxSeq   int
	Applied  int
	RoundNum int
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
	if snapshot.Insts == nil || snapshot.KV == nil || snapshot.Applied > snapshot.MaxSeq {
		return fmt.Errorf("snapshot %s has invalid state", s.statePath)
	}

	s.insts = snapshot.Insts
	s.kv = snapshot.KV
	s.max_seq = snapshot.MaxSeq
	s.applied = snapshot.Applied
	s.round_num = snapshot.RoundNum
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
		Version: snapshotVersion, Peers: s.peers, Insts: s.insts, KV: s.kv,
		MaxSeq: s.max_seq, Applied: s.applied, RoundNum: s.round_num,
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
		own_number: me,
		peers:      peers,
		majority:   len(peers)/2 + 1,
		insts:      make(map[int]PaxosInstance),
		kv:         make(map[string]string),
		statePath:  filepath.Join(dataDir, fmt.Sprintf("peer-%d.gob", me)),
	}
	s.cond.L = &s.mu
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

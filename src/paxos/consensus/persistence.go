package consensus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"6.5840/paxos/checksum"
	"6.5840/paxos/types"
	"6.5840/paxos/wal"
)

const SnapshotVersion = 7
const snapshotVersion = SnapshotVersion

// Snapshot is the durable consensus state, including an opaque machine image.
type Snapshot struct {
	WALSequence            uint64
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
func (s *Node) loadSnapshot(upgrade func([]byte) (Snapshot, error)) (bool, error) {
	data, err := os.ReadFile(s.statePath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	checked := bytes.HasPrefix(data, []byte(checksum.Magic))
	if checked {
		data, err = checksum.Decode(data)
		if err != nil {
			return false, fmt.Errorf("verify snapshot %s: %w", s.statePath, err)
		}
	}
	var header struct{ Version int }
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&header); err != nil {
		return false, fmt.Errorf("decode %s: %w", s.statePath, err)
	}
	if !checked && header.Version >= snapshotVersion {
		return false, fmt.Errorf("snapshot %s requires a checksum envelope", s.statePath)
	}
	var snapshot Snapshot
	migrated := !checked || header.Version != snapshotVersion
	if header.Version == 5 || header.Version == 6 {
		// Versions 5 and 6 share the opaque state schema but lack integrity
		// protection. Rewrite them into a checked version 7 image on startup.
		err = gob.NewDecoder(bytes.NewReader(data)).Decode(&snapshot)
		snapshot.Version = snapshotVersion
	} else if header.Version != snapshotVersion {
		if upgrade == nil {
			return false, fmt.Errorf("unsupported snapshot version %d", header.Version)
		}
		snapshot, err = upgrade(data)
	} else {
		err = gob.NewDecoder(bytes.NewReader(data)).Decode(&snapshot)
	}

	if err != nil {
		return false, fmt.Errorf("decode %s: %w", s.statePath, err)
	}
	if snapshot.Version != snapshotVersion || snapshot.MachineKind != s.machine.Kind() || !slices.Equal(snapshot.Peers, s.peers) {
		return false, fmt.Errorf("snapshot %s does not match this cluster or state machine", s.statePath)
	}
	if snapshot.Insts == nil || snapshot.Requests == nil || snapshot.Applied < 0 || snapshot.Applied > snapshot.MaxSeq || snapshot.Checkpoint.Seq < 0 || snapshot.Checkpoint.Seq > snapshot.Applied || snapshot.Checkpoint.Requests == nil {
		return false, fmt.Errorf("snapshot %s has invalid state", s.statePath)
	}
	if err := s.machine.Restore(snapshot.Checkpoint.State); err != nil {
		return false, fmt.Errorf("restore checkpoint: %w", err)
	}
	if err := s.machine.Restore(snapshot.State); err != nil {
		return false, fmt.Errorf("restore state: %w", err)
	}
	s.walSequence = snapshot.WALSequence
	s.checkpoint = snapshot.Checkpoint
	s.insts = snapshot.Insts
	s.requests = snapshot.Requests
	s.max_seq = snapshot.MaxSeq
	s.applied = snapshot.Applied
	s.round_num = snapshot.RoundNum
	s.promisedProposalNumber = snapshot.PromisedProposalNumber
	s.leaseGrantUntil = time.Now().Add(followerLeaseDuration)
	if migrated {
		return true, s.writeSnapshotLocked(snapshot.WALSequence)
	}
	return true, nil
}

// writeSnapshotLocked installs a synced checkpoint before WAL reclamation.
func (s *Node) writeSnapshotLocked(sequence uint64) error {
	if s.statePath == "" {
		return nil
	}

	dir := filepath.Dir(s.statePath)
	if err := wal.EnsureDir(dir); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".paxos-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	snapshot := serverSnapshot{
		WALSequence: sequence, Checkpoint: s.checkpoint, Version: snapshotVersion, Peers: s.peers, Insts: s.insts, State: s.machine.Snapshot(), MachineKind: s.machine.Kind(), Requests: s.requests,
		MaxSeq: s.max_seq, Applied: s.applied, RoundNum: s.round_num, PromisedProposalNumber: s.promisedProposalNumber,
	}
	if err := checksum.Write(file, func(writer io.Writer) error { return gob.NewEncoder(writer).Encode(snapshot) }); err != nil {
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

// journalRecord carries only changed log entries and protocol metadata. Applied
// application state and dedup results are rebuilt deterministically on replay.
type journalRecord struct {
	Sequence               uint64
	RoundNum               uint64
	PromisedProposalNumber types.ProposalID
	MaxSeq                 int
	Entries                map[int]Instance
	Deleted                []int
}

func (s *Node) walPath() string { return s.statePath + ".wal" }
func (s *Node) load(upgrade func([]byte) (Snapshot, error)) error {
	found, err := s.loadSnapshot(upgrade)
	if err != nil {
		return err
	}
	if !found {
		if _, err := os.Stat(s.walPath()); err == nil {
			return fmt.Errorf("WAL exists without base snapshot")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	s.applyLocked()
	base := s.walSequence
	s.walBytes, err = wal.Replay(s.walPath(), func(payload []byte) error {
		if !found {
			return fmt.Errorf("WAL has no base snapshot")
		}
		var record journalRecord
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&record); err != nil {
			return err
		}
		if record.Sequence == 0 {
			return fmt.Errorf("invalid zero WAL sequence")
		}
		if record.Sequence <= base {
			return nil
		}
		if s.walSequence == math.MaxUint64 || record.Sequence != s.walSequence+1 {
			return fmt.Errorf("noncontiguous WAL sequence %d after %d", record.Sequence, s.walSequence)
		}
		if record.RoundNum < s.round_num || record.MaxSeq < s.max_seq || record.PromisedProposalNumber.Compare(s.promisedProposalNumber) < 0 {
			return fmt.Errorf("WAL protocol counters regressed")
		}
		for seq, inst := range record.Entries {
			if seq < 1 || seq > record.MaxSeq {
				return fmt.Errorf("invalid WAL slot %d", seq)
			}
			if err := s.validateCommand(inst.Op); err != nil {
				return err
			}
			if old, ok := s.insts[seq]; ok && old.Decided {
				if !inst.Decided || old.Op != inst.Op {
					return fmt.Errorf("WAL changed decided slot %d", seq)
				}
				if seq <= s.applied {
					inst.Result = old.Result
				}
			}
			s.insts[seq] = inst
		}
		for _, seq := range record.Deleted {
			if seq > s.checkpoint.Seq {
				return fmt.Errorf("WAL deleted uncheckpointed slot %d", seq)
			}
			delete(s.insts, seq)
		}
		s.round_num = record.RoundNum
		s.max_seq = record.MaxSeq
		s.promisedProposalNumber = record.PromisedProposalNumber
		s.applyLocked()
		s.walSequence = record.Sequence
		s.walRecords++
		return nil
	})
	if err != nil {
		return fmt.Errorf("replay %s: %w", s.walPath(), err)
	}
	if !found {
		if err := s.writeSnapshotLocked(0); err != nil {
			return err
		}
	}
	s.durableInsts = maps.Clone(s.insts)
	s.durableCheckpoint = s.checkpoint.Seq
	// Grants remain volatile even when recovery only needed WAL records.
	if found {
		s.leaseGrantUntil = time.Now().Add(followerLeaseDuration)
	}
	return nil
}

// persistLocked syncs the delta before callers acknowledge a promise, accept or
// decision. Disk failures remain fail-stop, as with the original persistence.
func (s *Node) persistLocked() error {
	if s.statePath == "" {
		return nil
	}
	if s.walSequence == math.MaxUint64 {
		return fmt.Errorf("WAL sequence exhausted")
	}
	if s.checkpoint.Seq != s.durableCheckpoint {
		return s.checkpointWALLocked(s.walSequence + 1)
	}
	record := journalRecord{Sequence: s.walSequence + 1, RoundNum: s.round_num, PromisedProposalNumber: s.promisedProposalNumber, MaxSeq: s.max_seq, Entries: make(map[int]Instance)}
	for seq, inst := range s.insts {
		old, ok := s.durableInsts[seq]
		old.Result = inst.Result // Results are derived by application replay.
		if !ok || old != inst {
			inst.Result = ""
			record.Entries[seq] = inst
		}
	}
	for seq := range s.durableInsts {
		if _, ok := s.insts[seq]; !ok {
			record.Deleted = append(record.Deleted, seq)
		}
	}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(record); err != nil {
		return err
	}
	bytesWritten, err := wal.Append(s.walPath(), payload.Bytes())
	if err != nil {
		return err
	}
	s.walSequence = record.Sequence
	s.walRecords++
	s.walBytes += bytesWritten
	s.durableInsts = maps.Clone(s.insts)
	if s.walRecords >= s.walMaxRecords || s.walBytes >= s.walMaxBytes {
		return s.checkpointWALLocked(s.walSequence)
	}
	return nil
}
func (s *Node) checkpointWALLocked(sequence uint64) error {
	// Install and sync the complete image first. A crash before Reset leaves an
	// old WAL whose covered sequence numbers are ignored on the next recovery.
	if err := s.writeSnapshotLocked(sequence); err != nil {
		return err
	}
	if err := wal.Reset(s.walPath()); err != nil {
		return err
	}
	s.walSequence = sequence
	s.walRecords = 0
	s.walBytes = wal.FileHeaderBytes
	s.durableCheckpoint = s.checkpoint.Seq
	s.durableInsts = maps.Clone(s.insts)
	return nil
}

func New(config Config) (*Node, error) {
	me, peers, dataDir := config.ID, config.Peers, config.DataDir
	if config.WALMaxBytes < 0 || config.WALMaxRecords < 0 {
		return nil, fmt.Errorf("WAL limits cannot be negative")
	}
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
	s.walMaxBytes = config.WALMaxBytes
	if s.walMaxBytes == 0 {
		s.walMaxBytes = 16 << 20
	}
	s.walMaxRecords = config.WALMaxRecords
	if s.walMaxRecords == 0 {
		s.walMaxRecords = 512
	}
	if err := s.load(config.SnapshotUpgrade); err != nil {
		return nil, err
	}
	return s, nil
}

package consensus

import (
	"log"
	"maps"
)

const LogRetention = 256

// Checkpoint is state after applying every slot through Seq, including dedup.
type Checkpoint struct {
	Seq      int
	State    string
	Requests map[string]RequestRecord
}

func cloneCheckpoint(c Checkpoint) Checkpoint {
	return Checkpoint{Seq: c.Seq, State: c.State, Requests: maps.Clone(c.Requests)}
}

// Caller holds mu. Never compact undecided slots or drop an in-flight Get result.
func (s *Node) compactLocked() {
	if s.applied-s.checkpoint.Seq < LogRetention {
		return
	}
	s.checkpoint = Checkpoint{Seq: s.applied, State: s.machine.Snapshot(), Requests: maps.Clone(s.requests)}
	s.pruneLocked()
}

func (s *Node) pruneLocked() {
	for seq := range s.insts {
		if seq <= s.checkpoint.Seq && !s.pinnedResults[seq] {
			delete(s.insts, seq)
		}
	}
}

func (s *Node) installCheckpointLocked(c Checkpoint) {
	if c.Seq <= s.applied {
		return
	}
	s.checkpoint = cloneCheckpoint(c)
	if err := s.machine.Restore(c.State); err != nil {
		log.Fatalf("restore checkpoint: %v", err)
	}
	s.requests = maps.Clone(c.Requests)
	s.applied = c.Seq
	s.noteSeq(c.Seq)
	s.pruneLocked()
	s.applyLocked()
}

package paxos

import "maps"

const logRetention = 256

// Checkpoint is state after applying every slot through Seq, including dedup.
type Checkpoint struct {
	Seq      int
	KV       map[string]string
	Requests map[string]RequestRecord
}

func cloneCheckpoint(c Checkpoint) Checkpoint {
	return Checkpoint{Seq: c.Seq, KV: maps.Clone(c.KV), Requests: maps.Clone(c.Requests)}
}

// Caller holds mu. Never compact undecided slots or drop an in-flight Get result.
func (s *Server) compactLocked() {
	if s.applied-s.checkpoint.Seq < logRetention {
		return
	}
	s.checkpoint = Checkpoint{Seq: s.applied, KV: maps.Clone(s.kv), Requests: maps.Clone(s.requests)}
	s.pruneLocked()
}
func (s *Server) pruneLocked() {
	for seq := range s.insts {
		if seq <= s.checkpoint.Seq {
			delete(s.insts, seq)
		}
	}
}
func (s *Server) installCheckpointLocked(c Checkpoint) {
	if c.Seq <= s.applied {
		return
	}
	s.checkpoint = cloneCheckpoint(c)
	s.kv = maps.Clone(c.KV)
	s.requests = maps.Clone(c.Requests)
	s.applied = c.Seq
	s.noteSeq(c.Seq)
	s.pruneLocked()
	s.applyLocked()
}

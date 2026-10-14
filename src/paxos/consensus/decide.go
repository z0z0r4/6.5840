package consensus

import (
	"fmt"
	"log"
)

func (s *Node) decideLocked(seq int, instance Instance) (bool, error) {
	ok, err := s.learnDecisionLocked(seq, instance)
	if err != nil {
		return false, err
	}
	s.applyLearnedLocked("Decide")
	return ok, nil
}

// Record a learned value; the caller applies and persists the complete batch.
func (s *Node) learnDecisionLocked(seq int, instance Instance) (bool, error) {
	if err := s.validateCommand(instance.Op); err != nil {
		return false, err
	}
	if seq <= s.checkpoint.Seq {
		return true, nil
	}
	s.noteSeq(seq)

	if inst, exists := s.insts[seq]; exists && inst.Decided {
		if inst.Op != instance.Op {
			return false, fmt.Errorf("instance %d already decided a different operation", seq)
		}
		return true, nil
	}

	s.insts[seq] = Instance{
		ProposalAcceptNumber: instance.ProposalAcceptNumber,
		Op:                   instance.Op,
		Decided:              true,
	}

	return true, nil
}

// applyLearnedLocked makes chosen entries durable before application writes.
func (s *Node) applyLearnedLocked(operation string) {
	s.mustPersistLocked(operation)
	s.applyLocked()
	if s.statePath != "" && s.checkpoint.Seq != s.durableCheckpoint {
		if err := s.checkpointWALLocked(s.walSequence); err != nil {
			log.Fatalf("checkpoint %s: %v", operation, err)
		}
	}
}

func (s *Node) Decide(args *DecideArgs, reply *DecideReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ok, err := s.decideLocked(args.Seq, args.Instance)
	if err != nil {
		return err
	}
	reply.Result = ok
	return nil
}

func (s *Node) broadcastDecide(seq int, instance Instance) int {
	ch := s.announceDecision(seq, instance)
	return countQuorum(s, ch, func(r DecideReply) bool { return r.Result })
}

// Do not need to wait for a quorum of Decide replies
func (s *Node) announceDecision(seq int, instance Instance) <-chan rpcResult[DecideReply] {
	args := DecideArgs{Seq: seq, Instance: instance}
	return fanout[DecideReply](s, "Server.Decide", &args, func(a, r any) error {
		return s.Decide(a.(*DecideArgs), r.(*DecideReply))
	})
}

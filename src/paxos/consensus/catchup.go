package consensus

import "6.5840/paxos/types"

const CatchUpBatchSize = 64

// FetchLog returns only a contiguous prefix of decided entries, never accepted values.
func (s *Node) FetchLog(args *FetchLogArgs, reply *FetchLogReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inLeaderProposalNumberLocked(args.ProposalNumber) || args.StartSeq < 1 {
		return nil
	}
	if args.StartSeq <= s.checkpoint.Seq {
		reply.Checkpoint = cloneCheckpoint(s.checkpoint)
	}
	start := args.StartSeq
	if start <= s.checkpoint.Seq {
		start = s.checkpoint.Seq + 1
	}
	for seq := start; seq <= s.applied && len(reply.Entries) < CatchUpBatchSize; seq++ {
		inst, exists := s.insts[seq]
		if !exists || !inst.Decided {
			break
		}
		reply.Entries = append(reply.Entries, DecidedEntry{Seq: seq, Instance: inst})
	}
	reply.Result = true
	return nil
}

// catchUp is retriggered by a subsequent heartbeat if a fetch fails or the leader changes.
func (s *Node) catchUp(leader int, proposalNumber types.ProposalID, target int) {
	defer func() {
		s.mu.Lock()
		s.catchingUp = false
		s.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		if s.leader != leader || s.leaderProposalNumber != proposalNumber || s.applied >= target {
			s.mu.Unlock()
			return
		}
		start := s.applied + 1
		peer := s.peers[leader]
		s.mu.Unlock()

		var reply FetchLogReply
		if err := callWithTimeout(peer, "Server.FetchLog", &FetchLogArgs{ProposalNumber: proposalNumber, StartSeq: start}, &reply, snapshotRPCTimeout); err != nil || !reply.Result || (len(reply.Entries) == 0 && reply.Checkpoint.Seq < start) {
			return
		}
		s.mu.Lock()
		if s.leader != leader || s.leaderProposalNumber != proposalNumber {
			s.mu.Unlock()
			return
		}
		if reply.Checkpoint.Seq >= start {
			s.installCheckpointLocked(reply.Checkpoint)
			start = reply.Checkpoint.Seq + 1
		}
		for i, entry := range reply.Entries {
			if entry.Seq != start+i || !entry.Instance.Decided {
				s.applyLocked()
				s.mustPersistLocked("catch-up prefix")
				s.mu.Unlock()
				return
			}
			if _, err := s.learnDecisionLocked(entry.Seq, entry.Instance); err != nil {
				s.applyLocked()
				s.mustPersistLocked("catch-up prefix")
				s.mu.Unlock()
				return
			}
		}
		s.applyLocked()
		s.mustPersistLocked("catch-up batch")
		s.mu.Unlock()
	}
}

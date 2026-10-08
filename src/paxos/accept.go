package paxos

import "context"

func (s *Server) acceptLocked(seq int, proposalNumber int, value OP) bool {
	if seq <= s.checkpoint.Seq {
		return false
	}
	s.noteSeq(seq)

	inst, exists := s.insts[seq]
	if !exists {
		inst = Instance{ProposalAcceptNumber: -1}
	}
	if inst.Decided && inst.Op != value {
		return false
	}
	if proposalNumber < s.promisedProposalNumber || proposalNumber < 0 {
		return false
	}
	// A proposer may retry a proposal number, but cannot change its value in that slot.
	if inst.ProposalAcceptNumber == proposalNumber && inst.Op != value {
		return false
	}
	s.observeProposalNumberLocked(proposalNumber)
	inst.ProposalAcceptNumber, inst.Op = proposalNumber, value
	s.insts[seq] = inst
	s.mustPersistLocked("Accept")
	return true
}

func (s *Server) Accept(args *AcceptArgs, reply *AcceptReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Seq = args.Seq
	reply.Accepted = s.acceptLocked(args.Seq, args.ProposalNumber, args.Value)
	return nil
}

func (s *Server) broadcastAccept(seq int, proposalNumber int, value OP) int {
	return s.broadcastAcceptContext(context.Background(), seq, proposalNumber, value)
}

func (s *Server) broadcastAcceptContext(ctx context.Context, seq, proposalNumber int, value OP) int {
	args := AcceptArgs{Seq: seq, ProposalNumber: proposalNumber, Value: value}
	ch := fanoutContext[AcceptReply](ctx, s, "Server.Accept", &args, func(a, r any) error {
		return s.Accept(a.(*AcceptArgs), r.(*AcceptReply))
	})
	return countQuorumContext(ctx, s, ch, func(r AcceptReply) bool { return r.Accepted })
}

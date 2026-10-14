package consensus

import "6.5840/paxos/types"

func (s *Node) acceptLocked(seq int, proposalNumber types.ProposalID, value Command) bool {
	if seq <= s.checkpoint.Seq {
		return false
	}

	inst := s.insts[seq]
	if inst.Decided && inst.Op != value {
		return false
	}
	if proposalNumber.Compare(s.promisedProposalNumber) < 0 || !s.validProposalNumber(proposalNumber, int(proposalNumber.NodeID)) {
		return false
	}
	// A proposer may retry a proposal number, but cannot change its value in that slot.
	if inst.ProposalAcceptNumber == proposalNumber && inst.Op != value {
		return false
	}
	if !s.promiseProposalNumberLocked(proposalNumber) {
		return false
	}
	s.noteSeq(seq)
	inst.ProposalAcceptNumber, inst.Op = proposalNumber, value
	s.insts[seq] = inst
	s.mustPersistLocked("Accept")
	return true
}

func (s *Node) Accept(args *AcceptArgs, reply *AcceptReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateCommand(args.Value); err != nil {
		return err
	}
	reply.Seq = args.Seq
	reply.Accepted = s.acceptLocked(args.Seq, args.ProposalNumber, args.Value)
	return nil
}

func (s *Node) broadcastAccept(seq int, proposalNumber types.ProposalID, value Command) int {
	args := AcceptArgs{Seq: seq, ProposalNumber: proposalNumber, Value: value}
	ch := fanout[AcceptReply](s, "Server.Accept", &args, func(a, r any) error {
		return s.Accept(a.(*AcceptArgs), r.(*AcceptReply))
	})
	return countQuorum(s, ch, func(r AcceptReply) bool { return r.Accepted })
}

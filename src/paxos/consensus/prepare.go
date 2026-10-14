package consensus

import (
	"time"

	"6.5840/paxos/types"
)

func (s *Node) prepareLocked(n types.ProposalID, leader int, start int) (bool, []UndecidedInstance, int, types.ProposalID) {
	if !s.validProposalNumber(n, leader) || !s.observeProposalNumberLocked(n) {
		return false, nil, s.leader, s.promisedProposalNumber
	}
	s.lastHeartbeatTime = time.Now()
	var values []UndecidedInstance
	for seq, inst := range s.insts {
		if seq > start && seq > s.checkpoint.Seq && inst.ProposalAcceptNumber.Round > 0 {
			values = append(values, UndecidedInstance{Seq: seq, ProposalNumber: inst.ProposalAcceptNumber, Value: inst.Op})
		}
	}
	return true, values, s.leader, s.promisedProposalNumber
}

func (s *Node) Prepare(args *LeaderPrepareArgs, reply *LeaderPrepareReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	prepared, undecided_instances, _, proposalNumber := s.prepareLocked(args.ProposalNumber, args.LeaderID, args.StartSeq)

	reply.Prepared = prepared
	reply.UndecidedInstances = undecided_instances
	reply.ProposalNumber = proposalNumber
	if prepared {
		reply.Checkpoint = cloneCheckpoint(s.checkpoint)
	}

	return nil
}

func (s *Node) broadcastPrepare(n types.ProposalID, applied int) (int, map[int]UndecidedInstance, types.ProposalID, Checkpoint) {
	args := LeaderPrepareArgs{ProposalNumber: n, LeaderID: s.own_number, StartSeq: applied}
	ch := fanout[LeaderPrepareReply](s, "Server.Prepare", &args, func(a, r any) error {
		return s.Prepare(a.(*LeaderPrepareArgs), r.(*LeaderPrepareReply))
	})
	votes := quorumVotes{total: len(s.peers), majority: s.majority}
	highest := n
	var checkpoint Checkpoint
	values := make(map[int]UndecidedInstance)
	for !votes.done() {
		r := <-ch
		votes.record(r.err == nil && r.reply.Prepared)
		if r.err != nil {
			continue
		}
		if r.reply.ProposalNumber.Compare(highest) > 0 {
			highest = r.reply.ProposalNumber
		}
		if !r.reply.Prepared {
			continue
		}
		if r.reply.Checkpoint.Seq > checkpoint.Seq {
			checkpoint = r.reply.Checkpoint
		}
		for _, inst := range r.reply.UndecidedInstances {
			old, exists := values[inst.Seq]
			if !exists || inst.ProposalNumber.Compare(old.ProposalNumber) > 0 {
				values[inst.Seq] = inst
			}
		}
	}
	return votes.yes, values, highest, checkpoint
}

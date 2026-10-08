package paxos

import (
	"context"
	"log"
)

type leaderPhase uint8

const (
	leaderFollower leaderPhase = iota
	leaderRecovering
	leaderReady
)

type proposalStatus uint8

const (
	proposalExpired proposalStatus = iota
	proposalRedirect
	proposalApplied
	proposalConflict
)

type proposalResult struct {
	status proposalStatus
	leader int
	value  string
}

func (s *Server) readyLocked() bool {
	return s.phase == leaderReady && s.inLeaderProposalNumberLocked(s.leaderProposalNumber)
}

// Recovery observes the leader state directly. A failed heartbeat transitions
// the leader to follower, so no separate failure signal is needed.
func (s *Server) recoveryInterruptedLocked(proposalNumber int) bool {
	return s.phase != leaderRecovering || !s.inLeaderProposalNumberLocked(proposalNumber)
}

func (s *Server) inLeaderProposalNumberLocked(proposalNumber int) bool {
	return s.leader == s.own_number && s.leaderProposalNumber == proposalNumber && s.promisedProposalNumber == proposalNumber
}

func (s *Server) validProposalNumber(proposalNumber, leader int) bool {
	return leader >= 0 && leader < len(s.peers) && proposalNumber >= 0 && proposalNumber%len(s.peers) == leader
}

// observeProposalNumberLocked is the single transition for learning a higher promise.
func (s *Server) observeProposalNumberLocked(proposalNumber int) bool {
	if proposalNumber < s.promisedProposalNumber {
		return false
	}
	if proposalNumber > s.promisedProposalNumber {
		s.promisedProposalNumber = proposalNumber
		s.phase = leaderFollower
		s.leader = proposalNumber % len(s.peers)
		s.leaderProposalNumber = proposalNumber
		s.mustPersistLocked("promise")
	} else if s.leader == -1 {
		// Leadership identity is volatile, while the promise survives restart.
		s.leader = proposalNumber % len(s.peers)
		s.leaderProposalNumber = proposalNumber
	} else if s.leaderProposalNumber != proposalNumber || s.leader != proposalNumber%len(s.peers) {
		return false
	}
	return true
}

func (s *Server) invalidateLeaderLocked(proposalNumber int, clearLeader bool) {
	if !s.inLeaderProposalNumberLocked(proposalNumber) {
		return
	}
	s.phase = leaderFollower
	if clearLeader {
		s.leader = -1
	}
}

func (s *Server) proposedResultLocked(seq int, op OP) proposalResult {
	if seq < 1 || seq > s.applied {
		return proposalResult{status: proposalExpired}
	}
	if op.ReqID != "" {
		record, exists := s.requests[op.ReqID]
		if !exists || record.Op != op {
			return proposalResult{status: proposalConflict}
		}
	}
	if op.Action == "get" {
		if inst, ok := s.insts[seq]; ok {
			return proposalResult{status: proposalApplied, value: inst.ReadValue}
		}
		// The slot may already be in the checkpoint; the read value is not
		// recoverable from the current KV map, so its pin must keep it present.
		return proposalResult{status: proposalExpired}
	}
	return proposalResult{status: proposalApplied}
}

func (s *Server) redirectAddress(result proposalResult) string {
	if result.status != proposalRedirect || result.leader < 0 || result.leader >= len(s.peers) {
		return ""
	}
	return s.peers[result.leader]
}

func (s *Server) mustPersistLocked(operation string) {
	if err := s.persistLocked(); err != nil {
		log.Fatalf("persist %s: %v", operation, err)
	}
}

type rpcResult[T any] struct {
	reply T
	err   error
}

// 发出 RPC 调用到所有节点，分别本地调用和远程调用，并收集响应到 channel 中并返回
func fanout[T any](s *Server, method string, args any, local func(any, any) error) <-chan rpcResult[T] {
	return fanoutContext[T](context.Background(), s, method, args, local)
}

func fanoutContext[T any](ctx context.Context, s *Server, method string, args any, local func(any, any) error) <-chan rpcResult[T] {
	ch := make(chan rpcResult[T], len(s.peers))
	var reply T
	err := local(args, &reply)
	ch <- rpcResult[T]{reply: reply, err: err}
	for i, peer := range s.peers {
		if i == s.own_number {
			continue
		}
		go func(peer string) {
			var reply T
			rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
			defer cancel()
			err := callWithContext(rpcCtx, peer, method, args, &reply)
			ch <- rpcResult[T]{reply: reply, err: err}
		}(peer)
	}
	return ch
}

type quorumVotes struct {
	total, majority int
	yes, seen       int
}

func (q *quorumVotes) record(vote bool) {
	q.seen++
	if vote {
		q.yes++
	}
}

func (q quorumVotes) done() bool {
	return q.yes >= q.majority || q.yes+q.total-q.seen < q.majority
}

func countQuorum[T any](s *Server, ch <-chan rpcResult[T], accepted func(T) bool) int {
	return countQuorumContext(context.Background(), s, ch, accepted)
}

func countQuorumContext[T any](ctx context.Context, s *Server, ch <-chan rpcResult[T], accepted func(T) bool) int {
	q := quorumVotes{total: len(s.peers), majority: s.majority}
	for !q.done() {
		select {
		case <-ctx.Done():
			return q.yes
		case r := <-ch:
			q.record(r.err == nil && accepted(r.reply))
		}
	}
	return q.yes
}

func (s *Server) catchupRoundNumLocked(proposalNumber int) {
	if s.round_num*len(s.peers)+s.own_number < proposalNumber {
		s.round_num = proposalNumber/len(s.peers) + 1
	}
}

// noteSeq records the largest instance number this server has heard about.
// The caller must hold s.mu.
func (s *Server) noteSeq(seq int) {
	if seq > s.max_seq {
		s.max_seq = seq
	}
}

// The caller must hold s.mu.
func (s *Server) makeProposalNumberLocked() int {
	round := s.round_num
	s.round_num++
	return round*len(s.peers) + s.own_number
}

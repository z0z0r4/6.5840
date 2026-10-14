package paxos

import (
	"log"
	"time"

	"6.5840/paxos/types"
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
	proposalBusy
)

type proposalResult struct {
	status proposalStatus
	leader int
	value  string
}

func (s *Server) readyLocked() bool {
	return s.phase == leaderReady && s.inLeaderProposalNumberLocked(s.leaderProposalNumber)
}

// Recovery stops when leadership changes, independently of read lease renewal.
func (s *Server) recoveryInterruptedLocked(proposalNumber types.ProposalID) bool {
	return s.phase != leaderRecovering || !s.inLeaderProposalNumberLocked(proposalNumber)
}

func (s *Server) inLeaderProposalNumberLocked(proposalNumber types.ProposalID) bool {
	return s.leader == s.own_number && s.leaderProposalNumber == proposalNumber && s.promisedProposalNumber == proposalNumber
}

func (s *Server) validProposalNumber(proposalNumber types.ProposalID, leader int) bool {
	return leader >= 0 && leader < len(s.peers) && proposalNumber.Round > 0 && proposalNumber.NodeID == uint64(leader)
}

// promiseProposalNumberLocked enforces leases and advances the global promise.
// Accept uses this without learning the proposer's leadership identity.
func (s *Server) promiseProposalNumberLocked(proposalNumber types.ProposalID) bool {
	// On restart the lease owner is unknown, so every proposal waits out the
	// protection period. A live grant permits only its existing proposal number.
	if proposalNumber != s.leaderProposalNumber && time.Now().Before(s.leaseGrantUntil) {
		return false
	}
	if proposalNumber.Compare(s.promisedProposalNumber) < 0 {
		return false
	}
	if proposalNumber.Compare(s.promisedProposalNumber) > 0 {
		s.promisedProposalNumber = proposalNumber
		s.phase = leaderFollower
		s.readLeaseUntil = time.Time{}
		s.leader = -1
		s.leaderProposalNumber = types.ProposalID{}
		s.mustPersistLocked("promise")
	}
	return true
}

// Prepare and heartbeat identify the leader; Accept only establishes a promise.
func (s *Server) observeProposalNumberLocked(proposalNumber types.ProposalID) bool {
	if !s.promiseProposalNumberLocked(proposalNumber) {
		return false
	}
	s.leader = int(proposalNumber.NodeID)
	s.leaderProposalNumber = proposalNumber
	return true
}

func (s *Server) invalidateLeaderLocked(proposalNumber types.ProposalID, clearLeader bool) {
	if !s.inLeaderProposalNumberLocked(proposalNumber) {
		return
	}
	s.phase = leaderFollower
	s.readLeaseUntil = time.Time{}
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
		// Historical reads may have been compacted; their result is unavailable.
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
			err := call(peer, method, args, &reply)
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
	q := quorumVotes{total: len(s.peers), majority: s.majority}
	for !q.done() {
		r := <-ch
		q.record(r.err == nil && accepted(r.reply))
	}
	return q.yes
}

func (s *Server) catchupRoundNumLocked(proposalNumber types.ProposalID) {
	if (types.ProposalID{Round: s.round_num, NodeID: uint64(s.own_number)}).Compare(proposalNumber) <= 0 {
		if proposalNumber.Round == ^uint64(0) {
			panic("paxos: proposal round exhausted")
		}
		s.round_num = proposalNumber.Round + 1
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
func (s *Server) makeProposalNumberLocked() types.ProposalID {
	if s.round_num == 0 || s.round_num == ^uint64(0) {
		panic("paxos: proposal round exhausted")
	}
	round := s.round_num
	s.round_num++
	return types.ProposalID{Round: round, NodeID: uint64(s.own_number)}
}

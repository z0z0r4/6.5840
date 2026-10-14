package consensus

import (
	"sync"
	"time"

	"6.5840/paxos/statemachine"
	"6.5840/paxos/types"
)

type Instance struct {
	ProposalAcceptNumber types.ProposalID
	Op                   Command
	Decided              bool
	Result               string
}

type Node struct {
	own_number int
	peers      []string
	majority   int
	statePath  string

	insts         map[int]Instance
	machine       statemachine.Machine
	requests      map[string]RequestRecord
	checkpoint    Checkpoint
	pinnedResults map[int]bool
	pendingWrites map[string]*pendingProposal
	pendingCount  int

	phase                  leaderPhase
	catchingUp             bool
	proposalSlot           chan struct{}
	leader                 int
	leaderProposalNumber   types.ProposalID
	promisedProposalNumber types.ProposalID

	heartbeatTicker   *time.Ticker
	lastHeartbeatTime time.Time

	leaseGrantUntil time.Time
	readLeaseUntil  time.Time

	max_seq   int
	applied   int
	round_num uint64

	mu sync.Mutex
}

// Try to applyLocked all decided Command in order from applied+1 until the first undecided Command
func (s *Node) applyLocked() {
	defer s.compactLocked()
	for {
		next_seq_apply := s.applied + 1

		inst, exists := s.insts[next_seq_apply]
		if !exists || !inst.Decided {
			break
		}

		if inst.Op.ReqID != "" {
			if _, exists := s.requests[inst.Op.ReqID]; exists {
				s.applied++
				continue
			}
		}
		if !inst.Op.Noop {
			inst.Result = s.machine.Apply(inst.Op)
			s.insts[next_seq_apply] = inst
		}
		if inst.Op.ReqID != "" {
			s.requests[inst.Op.ReqID] = RequestRecord{Op: inst.Op, Seq: next_seq_apply, Result: inst.Result}
		}

		s.applied++
	}
}

func (s *Node) driveProposal(op Command) Result {
	s.mu.Lock()
	if !s.readyLocked() {
		result := Result{Status: Redirect, Leader: s.leader}
		s.mu.Unlock()
		return result
	}

	// 检查重复请求
	if old, exists := s.requests[op.ReqID]; op.ReqID != "" && exists {
		status := Applied
		if old.Op != op {
			status = Conflict
		}
		s.mu.Unlock()
		return Result{Status: status, Value: old.Result}
	}

	// The lease only permits local reads; otherwise use the normal proposal.
	if op.ReadOnly && time.Now().Before(s.readLeaseUntil) {
		result := Result{Status: Applied, Value: s.machine.Read(op)}
		s.mu.Unlock()
		return result
	}

	proposalNumber, seq := s.leaderProposalNumber, s.max_seq+1
	if !op.Noop {
		s.pinnedResults[seq] = true
	}

	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if !op.Noop {
			delete(s.pinnedResults, seq)
			if seq <= s.checkpoint.Seq {
				delete(s.insts, seq)
			}
		}
	}()

	for {
		s.mu.Lock()
		// 释放锁后重新检查
		active := s.readyLocked() && s.inLeaderProposalNumberLocked(proposalNumber)
		leader := s.leader
		s.mu.Unlock()

		if !active {
			return Result{Status: Redirect, Leader: leader}
		}

		if s.broadcastAccept(seq, proposalNumber, op) >= s.majority {
			s.announceDecision(seq, Instance{ProposalAcceptNumber: proposalNumber, Op: op, Decided: true})

			s.mu.Lock()
			result := s.proposedResultLocked(seq, op)
			s.mu.Unlock()

			return result
		}

		// Retry the same slot and value until chosen or leadership changes.
		time.Sleep(retryInterval)
	}
}

// Recheck heartbeat expiration after acquiring the proposal slot.
func (s *Node) electLeader() {
	s.proposalSlot <- struct{}{}
	defer func() { <-s.proposalSlot }()

	s.mu.Lock()
	if s.readyLocked() || time.Since(s.lastHeartbeatTime) < heartbeatTimeout {
		s.mu.Unlock()
		return
	}
	s.catchupRoundNumLocked(s.promisedProposalNumber)
	n := s.makeProposalNumberLocked()
	// Failed Prepare/recovery must not keep receiving leader heartbeats forever.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.readyLocked() {
			s.invalidateLeaderLocked(n, true)
		}
	}()
	applied := s.applied
	s.mustPersistLocked("round")
	s.mu.Unlock()

	// Prepare phase
	count, values, highest, checkpoint := s.broadcastPrepare(n, applied)
	if count < s.majority {
		s.mu.Lock()
		s.catchupRoundNumLocked(highest)
		s.mu.Unlock()
		return
	}

	// Recover phase
	s.mu.Lock()
	if !s.inLeaderProposalNumberLocked(n) {
		s.mu.Unlock()
		return
	}
	s.phase = leaderRecovering
	s.installCheckpointLocked(checkpoint)
	s.mustPersistLocked("election checkpoint")
	applied = s.applied
	end := s.max_seq
	s.mu.Unlock()

	for seq := range values {
		if seq > end {
			end = seq
		}
	}
	// Recover only the suffix after this candidate's applied prefix.
	for seq := applied + 1; seq <= end; seq++ {
		s.mu.Lock()
		if s.recoveryInterruptedLocked(n) {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		op := Command{Noop: true}
		if old, exists := values[seq]; exists {
			op = old.Value
		}
		accepted := s.broadcastAccept(seq, n, op)
		if accepted < s.majority {
			return
		}
		s.broadcastDecide(seq, Instance{ProposalAcceptNumber: n, Op: op, Decided: true})
	}

	s.mu.Lock()
	if s.inLeaderProposalNumberLocked(n) {
		s.phase = leaderReady
		s.lastHeartbeatTime = time.Now()
	}
	s.mu.Unlock()
}

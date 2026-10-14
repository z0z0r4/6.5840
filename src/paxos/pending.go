package paxos

import "context"

// Pending state is ephemeral. Only applied request records are persisted.
type pendingProposal struct {
	op        OP
	done      chan struct{}
	abandoned chan struct{}
	result    proposalResult
	waiters   int
	started   bool
}

// waitForProposal waits for a server-owned worker. Cancellation can discard queued
// work, but after the worker owns the proposal slot it cannot stop consensus.
func (s *Server) waitForProposal(ctx context.Context, op OP) proposalResult {
	if ctx.Err() != nil {
		return proposalResult{status: proposalExpired}
	}
	s.mu.Lock()
	if !s.readyLocked() {
		result := proposalResult{status: proposalRedirect, leader: s.leader}
		s.mu.Unlock()
		return result
	}
	if record, ok := s.requests[op.ReqID]; op.ReqID != "" && ok {
		status := proposalApplied
		if record.Op != op {
			status = proposalConflict
		}
		s.mu.Unlock()
		return proposalResult{status: status}
	}
	p := s.pendingWrites[op.ReqID]
	if op.ReqID == "" {
		// Reads cannot reuse a result from before their invocation.
		p = nil
	}
	if p != nil && p.op != op {
		s.mu.Unlock()
		return proposalResult{status: proposalConflict}
	}
	if p == nil {
		if s.pendingCount >= maxPendingProposals {
			s.mu.Unlock()
			return proposalResult{status: proposalBusy}
		}
		p = &pendingProposal{op: op, done: make(chan struct{}), abandoned: make(chan struct{})}
		if op.ReqID != "" {
			if s.pendingWrites == nil {
				s.pendingWrites = make(map[string]*pendingProposal)
			}
			s.pendingWrites[op.ReqID] = p
		}
		s.pendingCount++
		go s.runPending(p)
	}
	p.waiters++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		p.waiters--
		if p.waiters == 0 && !p.started {
			close(p.abandoned)
			if s.pendingWrites[op.ReqID] == p {
				delete(s.pendingWrites, op.ReqID)
			}
		}
	}()
	select {
	case <-ctx.Done():
		return proposalResult{status: proposalExpired}
	case <-p.done:
		return p.result
	}
}

func (s *Server) runPending(p *pendingProposal) {
	result := proposalResult{status: proposalExpired}
	select {
	case s.proposalSlot <- struct{}{}:
		s.mu.Lock()
		p.started = p.waiters > 0
		started := p.started
		s.mu.Unlock()
		if started {
			result = s.driveProposal(p.op)
		}
		<-s.proposalSlot
	case <-p.abandoned:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingWrites[p.op.ReqID] == p {
		delete(s.pendingWrites, p.op.ReqID)
	}
	s.pendingCount--
	p.result = result
	close(p.done)
}

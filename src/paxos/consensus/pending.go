package consensus

import "sync"

// Pending state is ephemeral. Only applied request records are persisted.
type pendingProposal struct {
	op        Command
	done      chan struct{}
	abandoned chan struct{}
	result    Result
	waiters   int
	started   bool
}

// Submission is a waiter on server-owned work. Releasing the last waiter may
// discard queued work, but cannot stop an active proposal. Each handle must be
// released, including after successful completion. Result is valid after Done.
type Submission struct {
	node    *Node
	pending *pendingProposal
	once    sync.Once
}

func (p *Submission) Done() <-chan struct{} { return p.pending.done }
func (p *Submission) Result() Result        { <-p.pending.done; return p.pending.result }
func (p *Submission) Release() {
	p.once.Do(func() {
		if p.node == nil {
			return
		}
		s := p.node
		s.mu.Lock()
		defer s.mu.Unlock()
		item := p.pending
		item.waiters--
		if item.waiters == 0 && !item.started {
			close(item.abandoned)
			if s.pendingWrites[item.op.ReqID] == item {
				delete(s.pendingWrites, item.op.ReqID)
			}
		}
	})
}

func completed(result Result) *Submission {
	p := &pendingProposal{done: make(chan struct{}), result: result}
	close(p.done)
	return &Submission{pending: p}
}

// Submit admits a command without receiving a caller's context. Requests with
// the same ID share work; reads always receive their own execution point.
func (s *Node) Submit(op Command) *Submission {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateCommand(op); err != nil {
		return completed(Result{Status: Rejected, Error: err.Error()})
	}
	if !s.readyLocked() {
		return completed(Result{Status: Redirect, Leader: s.leader})
	}
	if record, ok := s.requests[op.ReqID]; op.ReqID != "" && ok {
		status := Applied
		if record.Op != op {
			status = Conflict
		}
		return completed(Result{Status: status, Value: record.Result})
	}
	p := s.pendingWrites[op.ReqID]
	if op.ReqID == "" {
		p = nil
	}
	if p != nil && p.op != op {
		return completed(Result{Status: Conflict})
	}
	if p == nil {
		if s.pendingCount >= maxPendingProposals {
			return completed(Result{Status: Busy})
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
	return &Submission{node: s, pending: p}
}

func (s *Node) runPending(p *pendingProposal) {
	result := Result{Status: Expired}
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

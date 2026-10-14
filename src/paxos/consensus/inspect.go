package consensus

import (
	"6.5840/paxos/types"
	"maps"
)

// RedirectAddress maps a protocol result to the configured peer address.
func (s *Node) RedirectAddress(result Result) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.redirectAddress(result)
}

// State is an immutable diagnostic snapshot of the node, including opaque application state.
type State struct {
	Checkpoint                  Checkpoint
	ID, Applied, MaxSeq, Leader int
	ProposalNumber              types.ProposalID
	Ready                       bool
	State                       string
	Requests                    map[string]RequestRecord
	Insts                       map[int]Instance
}

func (s *Node) Inspect() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return State{Checkpoint: cloneCheckpoint(s.checkpoint), ID: s.own_number, Applied: s.applied, MaxSeq: s.max_seq, Leader: s.leader, ProposalNumber: s.promisedProposalNumber, Ready: s.readyLocked(), State: s.machine.Snapshot(), Requests: maps.Clone(s.requests), Insts: maps.Clone(s.insts)}
}

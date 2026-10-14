package types

import "cmp"

// ProposalID orders proposals by round, then by the proposer node ID.
// Round zero is reserved for an unset proposal; valid rounds start at one.
type ProposalID struct {
	Round  uint64
	NodeID uint64
}

// Compare returns -1, 0, or 1 according to the lexicographic proposal order.
func (p ProposalID) Compare(other ProposalID) int {
	if order := cmp.Compare(p.Round, other.Round); order != 0 {
		return order
	}
	return cmp.Compare(p.NodeID, other.NodeID)
}

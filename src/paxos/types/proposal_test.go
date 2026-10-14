package types

import "testing"

func TestProposalIDOrder(t *testing.T) {
	ordered := []ProposalID{
		{},
		{Round: 1, NodeID: 0},
		{Round: 1, NodeID: 1},
		{Round: 1, NodeID: ^uint64(0)},
		{Round: 2, NodeID: 0},
		{Round: 1 << 63, NodeID: 0},
		{Round: ^uint64(0), NodeID: ^uint64(0)},
	}
	for i, a := range ordered {
		for j, b := range ordered {
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got := a.Compare(b); got != want {
				t.Fatalf("%+v.Compare(%+v) = %d, want %d", a, b, got, want)
			}
		}
	}
}

package consensus

import "6.5840/paxos/types"

type LeaderPrepareArgs struct {
	Seq            int
	ProposalNumber types.ProposalID
	LeaderID       int
	StartSeq       int
}

type UndecidedInstance struct {
	Seq            int
	ProposalNumber types.ProposalID
	Value          Command
}

type LeaderPrepareReply struct {
	Checkpoint Checkpoint
	Seq        int
	Prepared   bool

	// The following fields are only valid if Prepared is false
	ProposalNumber types.ProposalID

	// The following field is only valid if Prepared is true
	UndecidedInstances []UndecidedInstance
}

type AcceptArgs struct {
	Seq            int
	ProposalNumber types.ProposalID
	Value          Command
}

type AcceptReply struct {
	Seq      int
	Accepted bool
}

type DecideArgs struct {
	Seq      int
	Instance Instance
}

type DecideReply struct {
	Result bool
}

type HeartbeatArgs struct {
	LeaderID       int
	ProposalNumber types.ProposalID
	Applied        int
}

type FetchLogArgs struct {
	ProposalNumber types.ProposalID
	StartSeq       int
}

type DecidedEntry struct {
	Seq      int
	Instance Instance
}

type FetchLogReply struct {
	Checkpoint Checkpoint
	Result     bool
	Entries    []DecidedEntry
}

type HeartbeatReply struct {
	Result         bool
	ProposalNumber types.ProposalID
}

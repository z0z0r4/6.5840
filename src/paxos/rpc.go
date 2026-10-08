package paxos

import (
	"context"
	"net"
	"net/rpc"
	"time"
)

type OP struct {
	ReqID  string
	Action string
	Key    string
	Value  string
}

type ClientSetRequest struct {
	ReqID            string
	Key              string
	Value            string
	DeadlineUnixNano int64
}

type ClientSetReply struct {
	Result        bool
	IsLeader      bool
	LeaderID      int
	LeaderAddress string
}

type ClientGetRequest struct {
	Key              string
	DeadlineUnixNano int64
}

type ClientGetResponse struct {
	Result bool
	Value  string

	LeaderAddress string
}

type LeaderPrepareArgs struct {
	Seq            int
	ProposalNumber int
	LeaderID       int
	StartSeq       int
}

type UndecidedInstance struct {
	Seq            int
	ProposalNumber int
	Value          OP
}

type LeaderPrepareReply struct {
	Checkpoint Checkpoint
	Seq        int
	Prepared   bool

	// The following fields are only valid if Prepared is false
	ProposalNumber int

	// The following field is only valid if Prepared is true
	UndecidedInstances []UndecidedInstance
}

type AcceptArgs struct {
	Seq            int
	ProposalNumber int
	Value          OP
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
	ProposalNumber int
	Applied        int
}

type FetchLogArgs struct {
	ProposalNumber int
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
	Result bool
}

func call(peer, method string, args any, reply any) error {
	return callWithTimeout(peer, method, args, reply, rpcTimeout)
}

func callWithTimeout(peer, method string, args any, reply any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return callWithContext(ctx, peer, method, args, reply)
}

func callWithContext(ctx context.Context, peer, method string, args, reply any) error {
	deadline, _ := ctx.Deadline()
	conn, err := (&net.Dialer{Deadline: deadline}).DialContext(ctx, "tcp", peer)
	if err != nil {
		return err
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return err
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}

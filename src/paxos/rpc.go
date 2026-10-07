package paxos

import (
	"net"
	"net/rpc"
	"time"
)

const rpcTimeout = 500 * time.Millisecond
const clientTimeout = 10 * time.Second

type OP struct {
	Action string
	Key    string
	Value  string
}

type ClientSetRequest struct {
	Key              string
	Value            string
	DeadlineUnixNano int64
}

type ClientSetReply struct {
	Result bool
}

type ClientGetRequest struct {
	Key              string
	DeadlineUnixNano int64
}

type ClientGetResponse struct {
	Result bool
	Value  string
}

type PrepareArgs struct {
	Seq            int
	ProposalNumber int
}

type PrepareReply struct {
	Seq                    int
	Prepared               bool
	AcceptedProposalNumber int
	Value                  OP
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
	Instance PaxosInstance
}

type DecideReply struct {
	Result bool
}

func call(peer, method string, args any, reply any) bool {
	return callWithTimeout(peer, method, args, reply, rpcTimeout)
}

func callWithTimeout(peer, method string, args any, reply any, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	conn, err := (&net.Dialer{Deadline: deadline}).Dial("tcp", peer)
	if err != nil {
		return false
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return false
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply) == nil
}

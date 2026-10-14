package api

import "6.5840/paxos/statemachine"

type ClientSetRequest struct {
	ReqID     string
	Key       string
	Value     string
	TimeoutMs int64
}

type ClientSetReply struct {
	Error         string
	Status        statemachine.Status
	Result        bool
	IsLeader      bool
	LeaderID      int
	LeaderAddress string
}

type ClientGetRequest struct {
	Key       string
	TimeoutMs int64
}

type ClientGetResponse struct {
	Error  string
	Status statemachine.Status
	Result bool
	Value  string

	LeaderAddress string
}

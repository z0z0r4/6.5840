package paxos

import "time"

const (
	rpcTimeout           = 500 * time.Millisecond
	serverWaitTimeout    = 10 * time.Second
	maxPendingProposals  = 128
	clientTimeout        = 10 * time.Second
	clientAttemptTimeout = 2 * time.Second
	// Leases assume bounded clock-rate drift; the master expires before replicas.
	followerLeaseDuration = time.Second
	leaderLeaseDuration   = 800 * time.Millisecond
	heartbeatInterval     = 200 * time.Millisecond
	heartbeatTimeout      = 1500 * time.Millisecond
	healthCheckInterval   = 100 * time.Millisecond
	electionJitter        = 500 * time.Millisecond
	retryInterval         = 30 * time.Millisecond
	snapshotRPCTimeout    = 5 * time.Second
)

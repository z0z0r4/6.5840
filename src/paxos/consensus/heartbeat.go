package consensus

import (
	"time"

	"6.5840/paxos/types"
)

func (s *Node) heartbeatLocked(n types.ProposalID, leader int) bool {
	if !s.validProposalNumber(n, leader) || !s.observeProposalNumberLocked(n) {
		return false
	}
	now := time.Now()
	s.lastHeartbeatTime = now
	s.leaseGrantUntil = now.Add(followerLeaseDuration)
	return true
}

func (s *Node) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := s.heartbeatLocked(args.ProposalNumber, args.LeaderID)

	reply.Result = result
	reply.ProposalNumber = s.promisedProposalNumber
	if result && args.LeaderID != s.own_number && args.Applied > s.applied && !s.catchingUp {
		s.catchingUp = true
		go s.catchUp(args.LeaderID, args.ProposalNumber, args.Applied)
	}
	return nil
}

// Use the caller's proposal number so a delayed heartbeat cannot adopt a newer term.
func (s *Node) broadcastHeartbeat(proposalNumber types.ProposalID) int {
	// Start before acquiring locks or doing I/O: delayed replies never extend
	// the master deadline beyond the replicas' protection.
	start := time.Now()
	s.mu.Lock()
	if !s.inLeaderProposalNumberLocked(proposalNumber) {
		s.mu.Unlock()
		return 0
	}
	applied := s.applied
	s.mu.Unlock()
	args := HeartbeatArgs{LeaderID: s.own_number, ProposalNumber: proposalNumber, Applied: applied}
	ch := fanout[HeartbeatReply](s, "Server.Heartbeat", &args, func(a, r any) error {
		return s.Heartbeat(a.(*HeartbeatArgs), r.(*HeartbeatReply))
	})
	// Inspect even replies arriving after the quorum. A failed minority election
	// can leave a higher promise that otherwise prevents that replica from ever
	// following this master. Stop renewing and let electLeader recover a new term.
	votes := make(chan rpcResult[HeartbeatReply], len(s.peers))
	go func() {
		for range s.peers {
			r := <-ch
			if r.err == nil && r.reply.ProposalNumber.Compare(proposalNumber) > 0 {
				s.mu.Lock()
				if s.inLeaderProposalNumberLocked(proposalNumber) {
					s.catchupRoundNumLocked(r.reply.ProposalNumber)
					s.invalidateLeaderLocked(proposalNumber, true)
				}
				s.mu.Unlock()
			}
			votes <- r
		}
	}()
	count := countQuorum(s, votes, func(r HeartbeatReply) bool { return r.Result })
	s.mu.Lock()
	defer s.mu.Unlock()
	if count >= s.majority && s.inLeaderProposalNumberLocked(proposalNumber) &&
		(s.phase == leaderReady || s.phase == leaderRecovering) {
		until := start.Add(leaderLeaseDuration)
		if time.Now().Before(until) && until.After(s.readLeaseUntil) {
			s.readLeaseUntil = until
		}
	}
	return count
}

func (s *Node) StartHeartbeat() {
	s.heartbeatTicker = time.NewTicker(heartbeatInterval)
	go func() {
		for range s.heartbeatTicker.C {
			s.mu.Lock()
			active := s.phase == leaderReady || s.phase == leaderRecovering
			n := s.leaderProposalNumber
			s.mu.Unlock()
			if active {
				// A missed quorum leaves the read lease to expire. Leadership is
				// independent of the lease, and proposals can still seek a quorum.
				s.broadcastHeartbeat(n)
			}
		}
	}()
}

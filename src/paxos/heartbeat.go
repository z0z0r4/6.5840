package paxos

import "time"

func (s *Server) heartbeatLocked(n int, leader int) bool {
	if !s.validProposalNumber(n, leader) || !s.observeProposalNumberLocked(n) {
		return false
	}
	s.lastHeartbeatTime = time.Now()
	return true
}

func (s *Server) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := s.heartbeatLocked(args.ProposalNumber, args.LeaderID)

	reply.Result = result
	if result && args.LeaderID != s.own_number && args.Applied > s.applied && !s.catchingUp {
		s.catchingUp = true
		go s.catchUp(args.LeaderID, args.ProposalNumber, args.Applied)
	}
	return nil
}

// Use the caller's proposal number so a delayed heartbeat cannot adopt a newer term.
func (s *Server) broadcastHeartbeat(proposalNumber int) int {
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
	return countQuorum(s, ch, func(r HeartbeatReply) bool { return r.Result })
}

func (s *Server) StartHeartbeat() {
	s.heartbeatTicker = time.NewTicker(heartbeatInterval)
	go func() {
		for range s.heartbeatTicker.C {
			s.mu.Lock()
			active := s.phase == leaderReady || s.phase == leaderRecovering
			n := s.leaderProposalNumber
			s.mu.Unlock()
			if active && s.broadcastHeartbeat(n) < s.majority {
				s.mu.Lock()
				s.invalidateLeaderLocked(n, true)
				s.mu.Unlock()
			}
		}
	}()
}

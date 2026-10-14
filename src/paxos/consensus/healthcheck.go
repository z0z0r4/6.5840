package consensus

import (
	"math/rand"
	"time"
)

func (s *Node) StartHealthCheck() {
	go func() {
		for {
			time.Sleep(healthCheckInterval)
			s.mu.Lock()
			expired := !s.readyLocked() && time.Since(s.lastHeartbeatTime) >= heartbeatTimeout
			s.mu.Unlock()
			if expired {
				time.Sleep(time.Duration(rand.Int63n(int64(electionJitter))))
				s.electLeader()
			}
		}
	}()
}

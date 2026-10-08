package paxos

import "time"

type Client struct {
	target_server string
	peers         []string
}

// InitPeers enables failover when the currently contacted server is unavailable.
func (c *Client) InitPeers(peers []string) {
	c.peers = append([]string(nil), peers...)
	if len(peers) > 0 {
		c.target_server = peers[0]
	}
}

func (c *Client) Send_set(key string, value string) bool {
	reqID := newRequestID()
	deadline := time.Now().Add(clientTimeout)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		attemptDeadline := time.Now().Add(clientAttemptTimeout)
		if deadline.Before(attemptDeadline) {
			attemptDeadline = deadline
		}
		var reply ClientSetReply
		err := callWithTimeout(c.target_server, "Server.Set", &ClientSetRequest{ReqID: reqID, Key: key, Value: value, DeadlineUnixNano: attemptDeadline.UnixNano()}, &reply, time.Until(attemptDeadline))
		if err == nil && reply.Result {
			return true
		}
		c.nextTarget(reply.LeaderAddress, attempt)
		waitForRetry(deadline)
	}
	return false
}

func (c *Client) Send_get(key string) (bool, string) {
	deadline := time.Now().Add(clientTimeout)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		attemptDeadline := time.Now().Add(clientAttemptTimeout)
		if deadline.Before(attemptDeadline) {
			attemptDeadline = deadline
		}
		var reply ClientGetResponse
		err := callWithTimeout(c.target_server, "Server.Get", &ClientGetRequest{Key: key, DeadlineUnixNano: attemptDeadline.UnixNano()}, &reply, time.Until(attemptDeadline))
		if err == nil && reply.Result {
			return true, reply.Value
		}
		c.nextTarget(reply.LeaderAddress, attempt)
		waitForRetry(deadline)
	}
	return false, ""
}

func (c *Client) nextTarget(leader string, attempt int) {
	if leader != "" && leader != c.target_server && attempt%2 == 0 {
		c.target_server = leader
	} else if len(c.peers) > 0 {
		c.target_server = c.peers[attempt%len(c.peers)]
	}
}

func (c *Client) Init(target string) { c.target_server = target }

func (c *Client) Set(key, value string) bool { return c.Send_set(key, value) }

func waitForRetry(deadline time.Time) {
	delay := min(retryInterval, time.Until(deadline))
	if delay > 0 {
		time.Sleep(delay)
	}
}

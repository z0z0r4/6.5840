package client

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"6.5840/paxos/api"
	"6.5840/paxos/statemachine"
	"6.5840/paxos/transport"
)

var ErrRequestConflict = errors.New("paxos: request ID reused with different operation")

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

// SetContext bounds waiting and retries. A timeout leaves the write outcome
// unknown; retries within this call use the same request ID.
func (c *Client) SetContext(ctx context.Context, key, value string) error {
	reqID := uuid.NewString()
	return c.retry(ctx, func(attemptCtx context.Context, budget int64) (bool, string, statemachine.Status, error) {
		var reply api.ClientSetReply
		err := transport.CallWithContext(attemptCtx, c.target_server, "Server.Set", &api.ClientSetRequest{ReqID: reqID, Key: key, Value: value, TimeoutMs: budget}, &reply)
		return reply.Result, reply.LeaderAddress, reply.Status, err
	})
}

func (c *Client) GetContext(ctx context.Context, key string) (string, error) {
	var value string
	err := c.retry(ctx, func(attemptCtx context.Context, budget int64) (bool, string, statemachine.Status, error) {
		var reply api.ClientGetResponse
		err := transport.CallWithContext(attemptCtx, c.target_server, "Server.Get", &api.ClientGetRequest{Key: key, TimeoutMs: budget}, &reply)
		if err == nil && reply.Result {
			value = reply.Value
		}
		return reply.Result, reply.LeaderAddress, reply.Status, err
	})
	return value, err
}

func (c *Client) retry(ctx context.Context, invoke func(context.Context, int64) (bool, string, statemachine.Status, error)) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, clientAttemptTimeout)
		deadline, _ := attemptCtx.Deadline()
		// Round up so a positive sub-millisecond budget never means "use default".
		budget := max(int64(1), int64((time.Until(deadline)+time.Millisecond-1)/time.Millisecond))
		ok, leader, status, err := invoke(attemptCtx, budget)
		cancel()
		if err == nil && ok {
			return nil
		}
		if err == nil && status == statemachine.Conflict {
			return ErrRequestConflict
		}
		if err == nil && status == statemachine.Rejected {
			return errors.New("paxos: request rejected")
		}
		c.nextTarget(leader, attempt)
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) Send_set(key, value string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	return c.SetContext(ctx, key, value) == nil
}

func (c *Client) Send_get(key string) (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	value, err := c.GetContext(ctx, key)
	return err == nil, value
}

func (c *Client) nextTarget(leader string, attempt int) {
	if leader != "" && leader != c.target_server && attempt%2 == 0 {
		c.target_server = leader
	} else if len(c.peers) > 0 {
		c.target_server = c.peers[attempt%len(c.peers)]
	}
}

func (c *Client) Init(target string)         { c.target_server = target }
func (c *Client) Set(key, value string) bool { return c.Send_set(key, value) }

package paxos

import "time"

type Client struct {
	target_server string
}

func (c *Client) Send_set(key string, value string) bool {
	var reply ClientSetReply
	deadline := time.Now().Add(clientTimeout).UnixNano()
	callWithTimeout(c.target_server, "Server.Set", &ClientSetRequest{Key: key, Value: value, DeadlineUnixNano: deadline}, &reply, clientTimeout)
	return reply.Result
}

func (c *Client) Send_get(key string) (bool, string) {
	var reply ClientGetResponse
	deadline := time.Now().Add(clientTimeout).UnixNano()
	callWithTimeout(c.target_server, "Server.Get", &ClientGetRequest{Key: key, DeadlineUnixNano: deadline}, &reply, clientTimeout)
	return reply.Result, reply.Value
}

func (c *Client) Init(target_server string) {
	c.target_server = target_server
}

package consensus

import (
	"6.5840/paxos/transport"
	"time"
)

func call(peer, method string, args, reply any) error {
	return transport.Call(peer, method, args, reply)
}
func callWithTimeout(peer, method string, args, reply any, timeout time.Duration) error {
	return transport.CallWithTimeout(peer, method, args, reply, timeout)
}

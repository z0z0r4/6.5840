package transport

import (
	"context"
	"net"
	"net/rpc"
	"time"
)

const PeerTimeout = 500 * time.Millisecond

func Call(peer, method string, args any, reply any) error {
	return CallWithTimeout(peer, method, args, reply, PeerTimeout)
}

func CallWithTimeout(peer, method string, args any, reply any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return CallWithContext(ctx, peer, method, args, reply)
}

func CallWithContext(ctx context.Context, peer, method string, args, reply any) error {
	deadline, _ := ctx.Deadline()
	conn, err := (&net.Dialer{Deadline: deadline}).DialContext(ctx, "tcp", peer)
	if err != nil {
		return err
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return err
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(method, args, reply)
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

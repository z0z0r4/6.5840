package paxos

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// newRequestID generates a UUID.
func newRequestID() string {
	return uuid.NewString()
}

type RequestRecord struct {
	Op  OP
	Seq int
}

func requestContext(deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(clientTimeout)
	if deadlineUnixNano != 0 {
		requested := time.Unix(0, deadlineUnixNano)
		if requested.Before(deadline) {
			deadline = requested
		}
	}
	return context.WithDeadline(context.Background(), deadline)
}

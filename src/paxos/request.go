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

// The wire budget limits only the handler's wait, never the consensus worker.
func handlerWaitContext(timeoutMs int64) (context.Context, context.CancelFunc) {
	timeout := serverWaitTimeout
	if timeoutMs > 0 && timeoutMs < serverWaitTimeout.Milliseconds() {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	return context.WithTimeout(context.Background(), timeout)
}

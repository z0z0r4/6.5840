package client

import "time"

const (
	clientTimeout        = 10 * time.Second
	clientAttemptTimeout = 2 * time.Second
	retryInterval        = 30 * time.Millisecond
)

package paxos

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"testing"
	"time"
)

func waitPendingDrained(t *testing.T, s *Server) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		s.mu.Lock()
		count := s.pendingCount
		s.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("pending workers did not drain")
}

// Delay a follower until the original handler has returned its timeout.
type delayedAcceptPeer struct {
	*Server
	started chan struct{}
	release chan struct{}
}

func (p *delayedAcceptPeer) Accept(args *AcceptArgs, reply *AcceptReply) error {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.release
	return p.Server.Accept(args, reply)
}

func TestHandlerTimeoutKeepsConsensusAndDeduplicatesPending(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	peers := []string{"local", l.Addr().String()}
	follower, err := newServer(1, peers, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer := &delayedAcceptPeer{Server: follower, started: make(chan struct{}, 8), release: make(chan struct{})}
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", peer); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go registry.ServeConn(conn)
		}
	}()
	s := newProtocolServer(t, peers)
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	term := s.leaderProposalNumber
	id := newRequestID()
	var reply ClientSetReply
	if err := s.Set(&ClientSetRequest{ReqID: id, Key: "k", Value: "v", TimeoutMs: 30}, &reply); err != nil || reply.Result || reply.Status != proposalExpired {
		t.Fatalf("short handler: %+v %v", reply, err)
	}
	select {
	case <-peer.started:
	case <-time.After(time.Second):
		t.Fatal("Accept never started")
	}
	s.mu.Lock()
	if !s.readyLocked() || s.leaderProposalNumber != term || s.pendingCount != 1 || s.max_seq != 1 || s.applied != 0 {
		s.mu.Unlock()
		t.Fatal("timeout altered leadership or stopped worker")
	}
	s.mu.Unlock()
	// Retry attaches to the same worker; conflicting retries fail immediately.
	var retry ClientSetReply
	if err := s.Set(&ClientSetRequest{ReqID: id, Key: "k", Value: "v", TimeoutMs: 20}, &retry); err != nil || retry.Result {
		t.Fatal("retry unexpectedly committed")
	}
	var conflict ClientSetReply
	s.Set(&ClientSetRequest{ReqID: id, Key: "k", Value: "wrong"}, &conflict)
	if conflict.Status != proposalConflict {
		t.Fatal("pending conflict not rejected")
	}
	close(peer.release)
	waitPendingDrained(t, s)
	s.mu.Lock()
	if !s.readyLocked() || s.leaderProposalNumber != term || s.applied != 1 || s.max_seq != 1 || s.kv["k"] != "v" || len(s.requests) != 1 || len(s.pendingWrites) != 0 {
		s.mu.Unlock()
		t.Fatal("abandoned write failed to commit once in the same term")
	}
	s.mu.Unlock()
	s.Set(&ClientSetRequest{ReqID: id, Key: "k", Value: "v"}, &retry)
	if !retry.Result {
		t.Fatal("committed retry failed")
	}
	// Subsequent writes continue without election/recovery.
	if err := s.Set(&ClientSetRequest{ReqID: newRequestID(), Key: "next", Value: "ok"}, &reply); err != nil || !reply.Result {
		t.Fatal("next slot failed")
	}
}

func TestClientContextCancelsSilentRPC(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := l.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	c := &Client{}
	c.Init(l.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.SetContext(ctx, "k", "v") }()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("client never connected")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("client ignored cancellation")
	}
}

func TestPendingWaitersCancelIndependently(t *testing.T) {
	s := newProtocolServer(t, []string{"local"})
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	s.proposalSlot <- struct{}{}
	op := OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "v"}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan proposalResult, 1)
	second := make(chan proposalResult, 1)
	go func() { first <- s.waitForProposal(ctx, op) }()
	survivorCtx, survivorCancel := context.WithTimeout(context.Background(), time.Second)
	defer survivorCancel()
	go func() { second <- s.waitForProposal(survivorCtx, op) }()
	until := time.Now().Add(time.Second)
	attached := false
	for time.Now().Before(until) {
		s.mu.Lock()
		p := s.pendingWrites[op.ReqID]
		attached = p != nil && p.waiters == 2
		s.mu.Unlock()
		if attached {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !attached {
		cancel()
		<-s.proposalSlot
		t.Fatal("retry waiters did not share a worker")
	}
	cancel()
	if result := <-first; result.status != proposalExpired {
		t.Fatal("cancelled waiter succeeded")
	}
	<-s.proposalSlot
	if result := <-second; result.status != proposalApplied {
		t.Fatalf("surviving waiter: %+v", result)
	}
	waitPendingDrained(t, s)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.max_seq != 1 || s.applied != 1 || len(s.requests) != 1 {
		t.Fatal("shared request was not committed once")
	}
}

func TestPendingQueueIsBoundedAndAbandonedWorkDrains(t *testing.T) {
	s := newProtocolServer(t, []string{"local"})
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	s.proposalSlot <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan proposalResult, maxPendingProposals)
	for i := 0; i < maxPendingProposals; i++ {
		op := OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "v"}
		go func() { results <- s.waitForProposal(ctx, op) }()
	}
	until := time.Now().Add(time.Second)
	full := false
	for time.Now().Before(until) {
		s.mu.Lock()
		full = s.pendingCount == maxPendingProposals
		s.mu.Unlock()
		if full {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !full {
		cancel()
		<-s.proposalSlot
		t.Fatal("queue did not fill")
	}
	if result := s.waitForProposal(ctx, OP{Action: "get", Key: "k"}); result.status != proposalBusy {
		cancel()
		<-s.proposalSlot
		t.Fatal("queue exceeded admission limit")
	}
	cancel()
	for i := 0; i < maxPendingProposals; i++ {
		select {
		case result := <-results:
			if result.status != proposalExpired {
				t.Fatal("queued write committed before admission")
			}
		case <-time.After(time.Second):
			t.Fatal("waiter ignored cancellation")
		}
	}
	waitPendingDrained(t, s)
	<-s.proposalSlot
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.max_seq != 0 || s.applied != 0 || len(s.pendingWrites) != 0 || !s.readyLocked() {
		t.Fatal("abandoned queue mutated log or leadership")
	}
}

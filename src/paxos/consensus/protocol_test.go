package consensus

import (
	"context"
	"encoding/gob"
	"fmt"
	"maps"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"

	"6.5840/paxos/kv"
	"6.5840/paxos/types"
)

func tDir(s *Server) string { return filepath.Dir(s.statePath) }

func newProtocolServer(t *testing.T, peers []string) *Server {
	t.Helper()
	s, err := newServer(0, peers, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGlobalPromiseProtectsEveryInstance(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	var first AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &first)
	if !first.Accepted {
		t.Fatal("initial Accept rejected")
	}
	var prepare LeaderPrepareReply
	s.Prepare(&LeaderPrepareArgs{ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}, LeaderID: 2}, &prepare)
	if !prepare.Prepared {
		t.Fatal("higher global promise rejected")
	}
	restarted, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []*Server{s, restarted} {
		for _, seq := range []int{1, 2} {
			var reply AcceptReply
			node.Accept(&AcceptArgs{Seq: seq, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &reply)
			if reply.Accepted {
				t.Fatalf("old proposal accepted in slot %d despite global promise", seq)
			}
		}
		if node.insts[1].ProposalAcceptNumber != (types.ProposalID{Round: 2, NodeID: 1}) {
			t.Fatal("global Prepare overwrote the instance's accepted number")
		}
	}
}

func TestLearnedDecisionDoesNotEstablishPromise(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	op := command(kv.Operation{Action: "set", Key: "k", Value: "chosen"})
	var decided DecideReply
	if err := s.Decide(&DecideArgs{Seq: 1, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 3, NodeID: 2}, Op: op, Decided: true}}, &decided); err != nil || !decided.Result {
		t.Fatalf("learn decision: %+v %v", decided, err)
	}
	if s.promisedProposalNumber != (types.ProposalID{}) || s.insts[1].ProposalAcceptNumber != (types.ProposalID{Round: 3, NodeID: 2}) || values(s)["k"] != "chosen" {
		t.Fatal("learning a decision changed the promise or lost the value")
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 4, NodeID: 1}, Value: command(kv.Operation{Action: "set", Key: "k", Value: "conflict"})}, &accepted)
	if accepted.Accepted || s.insts[1].Op != op {
		t.Fatal("removing the instance promise allowed a conflicting decided value")
	}
}

func decideOperation(t *testing.T, s *Server, seq int, op OP) {
	t.Helper()
	var reply DecideReply
	if err := s.Decide(&DecideArgs{Seq: seq, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: op, Decided: true}}, &reply); err != nil || !reply.Result {
		t.Fatalf("decide slot %d: reply=%+v err=%v", seq, reply, err)
	}
}

func TestBroadcastCompletesLocalTransition(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	// Block the local handler. fanout must not return until it completes.
	s.mu.Lock()
	done := make(chan (<-chan rpcResult[AcceptReply]), 1)
	started := make(chan struct{})
	go func() {
		args := AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 1, NodeID: 0}, Value: command(kv.Operation{Action: "nop"})}
		ch := fanout[AcceptReply](s.Node, "Server.Accept", &args, func(a, r any) error {
			close(started)
			return s.Accept(a.(*AcceptArgs), r.(*AcceptReply))
		})
		done <- ch
	}()
	<-started
	select {
	case <-done:
		s.mu.Unlock()
		t.Fatal("broadcast returned before the local Accept completed")
	case <-time.After(10 * time.Millisecond):
	}
	s.mu.Unlock()
	select {
	case ch := <-done:
		result := <-ch
		if result.err != nil || !result.reply.Accepted {
			t.Fatalf("local Accept failed: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast failed to return after the local handler completed")
	}
}

func TestQueuedProposalCancellation(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	s.proposalSlot <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() { done <- s.waitForProposal(ctx, command(kv.Operation{Action: "get", Key: "k"})) }()
	cancel()
	select {
	case result := <-done:
		if result.Status != Expired {
			t.Fatalf("cancelled queued proposal: %+v", result)
		}
	case <-time.After(time.Second):
		<-s.proposalSlot
		t.Fatal("queued proposal ignored cancellation")
	}
	<-s.proposalSlot
	waitPendingDrained(t, s)
	if s.max_seq != 0 || !s.readyLocked() {
		t.Fatal("queued cancellation mutated log or leadership")
	}
	result := s.waitForProposal(context.Background(), command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "ok"}))
	if result.Status != Applied {
		t.Fatalf("cancellation prevented the next proposal: %+v", result)
	}
}

func TestGetAtCompactionBoundary(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	s.statePath = ""
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	if s.broadcastHeartbeat(s.leaderProposalNumber) != s.majority {
		t.Fatal("failed to obtain read lease")
	}
	for seq := 1; seq < LogRetention; seq++ {
		decideOperation(t, s, seq, command(kv.Operation{Action: "set", Key: "k", Value: fmt.Sprint(seq)}))
	}
	var reply ClientGetResponse
	if err := s.Get(&ClientGetRequest{Key: "k"}, &reply); err != nil || !reply.Result || reply.Value != fmt.Sprint(LogRetention-1) {
		t.Fatalf("read at compaction boundary: %+v %v", reply, err)
	}
	if s.max_seq != LogRetention-1 || s.checkpoint.Seq != 0 || len(s.insts) != LogRetention-1 {
		t.Fatal("local read changed the log or checkpoint")
	}
	decideOperation(t, s, LogRetention, command(kv.Operation{Action: "set", Key: "k", Value: reply.Value}))
	if s.checkpoint.Seq != LogRetention || len(s.insts) != 0 {
		t.Fatalf("write did not compact state: checkpoint=%d entries=%d", s.checkpoint.Seq, len(s.insts))
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: LogRetention, ProposalNumber: types.ProposalID{Round: 1, NodeID: 0}, Value: command(kv.Operation{Action: "set", Key: "k", Value: "bad"})}, &accepted)
	if accepted.Accepted {
		t.Fatal("compacted read slot was accepted again")
	}
	decideOperation(t, s, LogRetention, command(kv.Operation{Action: "set", Key: "k", Value: "bad"}))
	if values(s)["k"] != reply.Value || len(s.insts) != 0 {
		t.Fatal("late Decide recreated a compacted slot")
	}
}

func TestRejectedProposalNumbersDoNotChangePromise(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	var prepared LeaderPrepareReply
	s.Prepare(&LeaderPrepareArgs{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 1}, &prepared)
	for _, args := range []LeaderPrepareArgs{
		{ProposalNumber: types.ProposalID{Round: 1, NodeID: 1}, LeaderID: 1},
		{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 2},
		{ProposalNumber: types.ProposalID{Round: 3, NodeID: 1}, LeaderID: 2},
		{ProposalNumber: types.ProposalID{}, LeaderID: 0},
		{ProposalNumber: types.ProposalID{Round: 3, NodeID: 0}, LeaderID: 3},
	} {
		var prepare LeaderPrepareReply
		var heartbeat HeartbeatReply
		s.Prepare(&args, &prepare)
		s.Heartbeat(&HeartbeatArgs{ProposalNumber: args.ProposalNumber, LeaderID: args.LeaderID}, &heartbeat)
		if prepare.Prepared || heartbeat.Result || s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.leaderProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.leader != 1 {
			t.Fatalf("invalid proposal number changed leadership: args=%+v prepare=%+v heartbeat=%+v", args, prepare, heartbeat)
		}
	}
	var accept AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 1, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &accept)
	if accept.Accepted || len(s.insts) != 0 {
		t.Fatal("stale Accept created an instance")
	}
	for _, args := range []FetchLogArgs{{ProposalNumber: types.ProposalID{Round: 1, NodeID: 1}, StartSeq: 1}, {ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, StartSeq: 0}} {
		var reply FetchLogReply
		s.FetchLog(&args, &reply)
		if reply.Result || len(reply.Entries) != 0 {
			t.Fatal("nonleader or invalid fetch returned a log")
		}
	}
}

func TestCheckpointIsolationAndAppliedSuffix(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	s.statePath = ""
	decideOperation(t, s, 3, command(kv.Operation{Action: "set", Key: "suffix", Value: "applied"}))
	checkpoint := Checkpoint{Seq: 2, State: `{"k":"prefix"}`, Requests: map[string]RequestRecord{}}
	s.mu.Lock()
	s.installCheckpointLocked(checkpoint)
	checkpoint.State = `{"k":"external mutation"}`
	copy := cloneCheckpoint(s.checkpoint)
	copy.State = `{"k":"reply mutation"}`
	s.installCheckpointLocked(Checkpoint{Seq: 1, State: `{"k":"older"}`, Requests: map[string]RequestRecord{}})
	s.mu.Unlock()
	if s.applied != 3 || values(s)["k"] != "prefix" || checkpointValues(s.checkpoint)["k"] != "prefix" || values(s)["suffix"] != "applied" {
		t.Fatal("checkpoint aliases caller data, regresses state, or loses the decided suffix")
	}
}

func TestDecidedConflictDoesNotMutateState(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	op := command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "chosen"})
	decideOperation(t, s, 1, op)
	before := s.insts[1]
	var reply DecideReply
	err := s.Decide(&DecideArgs{Seq: 1, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 11, NodeID: 0}, Op: command(kv.Operation{Action: "set", Key: "k", Value: "conflict"}), Decided: true}}, &reply)
	if err == nil || reply.Result || s.insts[1] != before || values(s)["k"] != "chosen" {
		t.Fatal("conflicting Decide changed a chosen operation")
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: command(kv.Operation{Action: "nop"})}, &accepted)
	if accepted.Accepted || s.insts[1] != before {
		t.Fatal("conflicting Accept changed a decided slot")
	}
	decideOperation(t, s, 1, op)
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil || values(restored)["k"] != "chosen" || restored.applied != 1 {
		t.Fatalf("decided value did not survive restart: %v", err)
	}
}

func TestSnapshotValidation(t *testing.T) {
	for _, mode := range []string{"corrupt", "cluster", "version", "invalid-prefix", "missing-kv"} {
		t.Run(mode, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0"})
			decideOperation(t, s, 1, command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "stored"}))
			if mode == "cluster" {
				if _, err := newServer(0, []string{"different-peer"}, tDir(s)); err == nil {
					t.Fatal("snapshot for a different cluster loaded")
				}
				return
			}
			if mode == "corrupt" {
				if err := os.WriteFile(s.statePath, []byte("truncated gob"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				snapshot := serverSnapshot{Version: snapshotVersion, Peers: s.peers, Checkpoint: cloneCheckpoint(s.checkpoint), Insts: maps.Clone(s.insts), State: s.machine.Snapshot(), MachineKind: s.machine.Kind(), Requests: maps.Clone(s.requests), MaxSeq: s.max_seq, Applied: s.applied, PromisedProposalNumber: s.promisedProposalNumber}
				switch mode {
				case "version":
					snapshot.Version++
				case "invalid-prefix":
					snapshot.Applied = snapshot.MaxSeq + 1
				case "missing-kv":
					snapshot.State = ""
				}
				file, err := os.Create(s.statePath)
				if err != nil {
					t.Fatal(err)
				}
				err = gob.NewEncoder(file).Encode(snapshot)
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := newServer(0, s.peers, tDir(s)); err == nil {
				t.Fatal("invalid snapshot loaded")
			}
		})
	}
}

func TestFetchLogBatchAndCheckpoint(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	s.statePath = ""
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	for seq := 1; seq <= LogRetention+CatchUpBatchSize+3; seq++ {
		decideOperation(t, s, seq, command(kv.Operation{Action: "set", Key: "k", Value: fmt.Sprint(seq)}))
	}
	var reply FetchLogReply
	s.FetchLog(&FetchLogArgs{ProposalNumber: s.leaderProposalNumber, StartSeq: 1}, &reply)
	if !reply.Result || reply.Checkpoint.Seq != LogRetention || len(reply.Entries) != CatchUpBatchSize {
		t.Fatalf("wrong checkpoint/batch response: checkpoint=%d entries=%d result=%v", reply.Checkpoint.Seq, len(reply.Entries), reply.Result)
	}
	for i, entry := range reply.Entries {
		if entry.Seq != LogRetention+i+1 || !entry.Instance.Decided {
			t.Fatal("fetch returned a noncontiguous or undecided entry")
		}
	}
	reply.Checkpoint.State = `{"k":"caller mutation"}`
	if reply.Checkpoint.State == s.checkpoint.State {
		t.Fatal("fetch checkpoint aliases server state")
	}
	var suffix FetchLogReply
	s.FetchLog(&FetchLogArgs{ProposalNumber: s.leaderProposalNumber, StartSeq: reply.Entries[len(reply.Entries)-1].Seq + 1}, &suffix)
	if len(suffix.Entries) != 3 || suffix.Checkpoint.Seq != 0 {
		t.Fatalf("wrong remaining suffix: %+v", suffix)
	}
}

func TestHigherProposalNumberInvalidatesLeader(t *testing.T) {
	for _, source := range []string{"prepare", "accept", "heartbeat"} {
		t.Run(source, func(t *testing.T) {
			dir := t.TempDir()
			peers := []string{"p0", "p1", "p2"}
			s, err := newServer(0, peers, dir)
			if err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.observeProposalNumberLocked(types.ProposalID{Round: 1, NodeID: 0})
			s.phase = leaderReady
			if !s.readyLocked() {
				t.Fatal("initial leader is not ready")
			}
			s.mu.Unlock()

			switch source {
			case "prepare":
				var reply LeaderPrepareReply
				s.Prepare(&LeaderPrepareArgs{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 1}, &reply)
				if !reply.Prepared {
					t.Fatal("higher prepare rejected")
				}
			case "accept":
				var reply AcceptReply
				s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &reply)
				if !reply.Accepted {
					t.Fatal("higher accept rejected")
				}
			case "heartbeat":
				var reply HeartbeatReply
				s.Heartbeat(&HeartbeatArgs{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 1}, &reply)
				if !reply.Result {
					t.Fatal("higher heartbeat rejected")
				}
			}
			leader, leaderProposalNumber := 1, types.ProposalID{Round: 2, NodeID: 1}
			if source == "accept" {
				leader, leaderProposalNumber = -1, types.ProposalID{}
			}
			s.mu.Lock()
			if s.readyLocked() || s.phase != leaderFollower || s.leader != leader || s.leaderProposalNumber != leaderProposalNumber || s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) {
				t.Fatal("higher proposal number did not invalidate the old leadership correctly")
			}
			// A delayed failure from the old term must not invalidate the new one.
			s.invalidateLeaderLocked(types.ProposalID{Round: 1, NodeID: 0}, true)
			if s.leader != leader {
				t.Fatal("old-term failure changed the leadership")
			}
			s.mu.Unlock()
			result := s.waitForProposal(context.Background(), command(kv.Operation{Action: "get", Key: "k"}))
			if result.Status != Redirect || result.Leader != leader {
				t.Fatalf("old leader served a new proposal: %+v", result)
			}
			s, err = newServer(0, peers, dir)
			if err != nil {
				t.Fatal(err)
			}
			if s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.readyLocked() {
				t.Fatal("restart lost promise or restored ready leadership")
			}
		})
	}
}

func TestLeaderPromiseSurvivesRestart(t *testing.T) {
	for _, source := range []string{"prepare", "heartbeat"} {
		t.Run(source, func(t *testing.T) {
			dir := t.TempDir()
			peers := []string{"p0", "p1", "p2", "p3", "p4"}
			s, err := newServer(0, peers, dir)
			if err != nil {
				t.Fatal(err)
			}
			if source == "prepare" {
				var reply LeaderPrepareReply
				if err := s.Prepare(&LeaderPrepareArgs{ProposalNumber: types.ProposalID{Round: 5, NodeID: 1}, LeaderID: 1}, &reply); err != nil || !reply.Prepared {
					t.Fatalf("prepare: %+v %v", reply, err)
				}
			} else {
				var reply HeartbeatReply
				if err := s.Heartbeat(&HeartbeatArgs{ProposalNumber: types.ProposalID{Round: 5, NodeID: 1}, LeaderID: 1}, &reply); err != nil || !reply.Result {
					t.Fatalf("heartbeat: %+v %v", reply, err)
				}
			}
			s, err = newServer(0, peers, dir)
			if err != nil {
				t.Fatal(err)
			}
			var accept AcceptReply
			s.Accept(&AcceptArgs{Seq: 100, ProposalNumber: types.ProposalID{Round: 4, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &accept)
			if accept.Accepted {
				t.Fatal("old leader accepted in a fresh slot after restart")
			}
			var heartbeat HeartbeatReply
			s.Heartbeat(&HeartbeatArgs{LeaderID: 1, ProposalNumber: types.ProposalID{Round: 4, NodeID: 1}}, &heartbeat)
			if heartbeat.Result {
				t.Fatal("stale heartbeat accepted")
			}
		})
	}
}

func TestSameProposalNumberCannotAcceptDifferentValues(t *testing.T) {
	s, err := newServer(0, []string{"p0"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var first, second AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: command(kv.Operation{Action: "set", Key: "k", Value: "first"})}, &first)
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: command(kv.Operation{Action: "set", Key: "k", Value: "second"})}, &second)
	if !first.Accepted || second.Accepted {
		t.Fatalf("same proposal number conflicting values: first=%v second=%v", first.Accepted, second.Accepted)
	}
}

func TestCompactedPrepareRecovery(t *testing.T) {
	peers := make([]string, 5)
	listeners := make([]net.Listener, 5)
	for i := range peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		peers[i] = l.Addr().String()
		t.Cleanup(func() { l.Close() })
	}
	nodes := make([]*Server, 5)
	for i := range nodes {
		s, err := newServer(i, peers, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		s.statePath = "" // isolate election logic from disk cost; restart covered above.
		nodes[i] = s
		if i != 0 {
			for seq := 1; seq <= LogRetention+3; seq++ {
				var reply DecideReply
				err := s.Decide(&DecideArgs{Seq: seq, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: command(kv.Operation{Action: "set", Key: "k", Value: fmt.Sprint(seq)}), Decided: true}}, &reply)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		registry := rpc.NewServer()
		if err := registry.Register(s); err != nil {
			t.Fatal(err)
		}
		l := listeners[i]
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go registry.ServeConn(conn)
			}
		}()
	}
	// No background elections: node 0, which has no log, is the candidate.
	nodes[0].lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	nodes[0].electLeader()
	s := nodes[0]
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.readyLocked() || s.applied != LogRetention+3 || s.checkpoint.Seq < LogRetention || values(s)["k"] != fmt.Sprint(LogRetention+3) {
		t.Fatalf("stale candidate lost compacted prefix: ready=%v applied=%d checkpoint=%d kv=%v", s.readyLocked(), s.applied, s.checkpoint.Seq, values(s))
	}
}

func TestDecidedDuplicateRequestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []string{"p0"}
	s, err := newServer(0, peers, dir)
	if err != nil {
		t.Fatal(err)
	}
	op := command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "x"})
	// Duplicates can occur in different slots after retry or leader recovery.
	for _, seq := range []int{2, 1} {
		var reply DecideReply
		if err := s.Decide(&DecideArgs{Seq: seq, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: op, Decided: true}}, &reply); err != nil {
			t.Fatal(err)
		}
	}
	s, err = newServer(0, peers, dir)
	if err != nil {
		t.Fatal(err)
	}
	var newer DecideReply
	s.Decide(&DecideArgs{Seq: 3, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "newer"}), Decided: true}}, &newer)
	var reply DecideReply
	s.Decide(&DecideArgs{Seq: 4, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: op, Decided: true}}, &reply)
	if values(s)["k"] != "newer" || s.applied != 4 || s.requests[op.ReqID].Seq != 1 {
		t.Fatalf("duplicate application: kv=%v applied=%d requests=%v", values(s), s.applied, s.requests)
	}
}

func TestAcceptQuorumUsesPeerTimeout(t *testing.T) {
	// A silent peer is bounded by the transport timeout, independently of callers.
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
	s := newProtocolServer(t, []string{"p0", l.Addr().String()})
	start := time.Now()
	if votes := s.broadcastAccept(1, types.ProposalID{Round: 1, NodeID: 0}, command(kv.Operation{Action: "nop"})); votes >= s.majority {
		t.Fatal("silent peer formed a majority")
	}
	if time.Since(start) > rpcTimeout+300*time.Millisecond {
		t.Fatal("peer RPC exceeded transport timeout")
	}
	select {
	case conn := <-accepted:
		conn.Close()
	case <-time.After(time.Second):
		t.Fatal("peer never received connection")
	}
}

func TestQueuedElectionRechecksHeartbeat(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.proposalSlot <- struct{}{}
	done := make(chan struct{})
	go func() { s.electLeader(); close(done) }()
	var reply HeartbeatReply
	s.Heartbeat(&HeartbeatArgs{LeaderID: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}}, &reply)
	<-s.proposalSlot
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queued election did not finish")
	}
	if s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.leader != 1 || s.round_num != 1 {
		t.Fatal("fresh heartbeat was ignored after queueing")
	}
}

func TestFailedRecoveryLeavesFollower(t *testing.T) {
	peers := make([]string, 3)
	listeners := make([]net.Listener, 3)
	for i := range peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i], peers[i] = l, l.Addr().String()
		t.Cleanup(func() { l.Close() })
	}
	nodes := make([]*Server, 3)
	for i := range peers {
		s, err := newServer(i, peers, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		s.statePath = ""
		s.max_seq = 1 // force recovery of a missing slot
		nodes[i] = s
		wrapper := &TestRPCServer{Server: s}
		wrapper.rejectAccept.Store(true)
		registry := rpc.NewServer()
		if err := registry.RegisterName("Server", wrapper); err != nil {
			t.Fatal(err)
		}
		l := listeners[i]
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go registry.ServeConn(c)
			}
		}()
	}
	nodes[0].lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	nodes[0].electLeader()
	s := nodes[0]
	if s.phase != leaderFollower || s.leader != -1 {
		t.Fatal("failed recovery left a heartbeat-producing leader")
	}
}

func TestHeartbeatLeaseProtectsProposalNumber(t *testing.T) {
	for _, source := range []string{"prepare", "accept", "heartbeat"} {
		t.Run(source, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0", "p1", "p2"})
			var granted HeartbeatReply
			s.Heartbeat(&HeartbeatArgs{LeaderID: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}}, &granted)
			if !granted.Result {
				t.Fatal("initial lease rejected")
			}
			attempt := func() bool {
				switch source {
				case "prepare":
					var r LeaderPrepareReply
					s.Prepare(&LeaderPrepareArgs{LeaderID: 2, ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}}, &r)
					return r.Prepared
				case "accept":
					var r AcceptReply
					s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}, Value: command(kv.Operation{Action: "nop"})}, &r)
					return r.Accepted
				default:
					var r HeartbeatReply
					s.Heartbeat(&HeartbeatArgs{LeaderID: 2, ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}}, &r)
					return r.Result
				}
			}
			until := s.leaseGrantUntil
			if attempt() || s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.leaseGrantUntil != until || s.max_seq != 0 {
				t.Fatal("another proposal bypassed or mutated the live lease")
			}
			// Same-term traffic is allowed while other proposals are excluded.
			var accepted AcceptReply
			s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &accepted)
			if !accepted.Accepted {
				t.Fatal("lease blocked its owner")
			}
			s.leaseGrantUntil = time.Now().Add(-time.Second)
			if !attempt() || s.promisedProposalNumber != (types.ProposalID{Round: 3, NodeID: 2}) {
				t.Fatal("expired lease prevented takeover")
			}
		})
	}
}

func TestPeriodicHeartbeatRenewsLocalReads(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	s.electLeader()
	decideOperation(t, s, 1, command(kv.Operation{Action: "set", Key: "k", Value: "v"}))
	if r := s.waitForProposal(context.Background(), command(kv.Operation{Action: "get", Key: "k"})); r.Status != Applied || r.Value != "v" || s.max_seq != 2 || s.applied != 2 || !s.readyLocked() || !s.readLeaseUntil.IsZero() {
		t.Fatalf("read without a lease did not fall back to a proposal: %+v", r)
	}
	s.StartHeartbeat()
	t.Cleanup(func() { s.heartbeatTicker.Stop() })
	deadline := time.Now().Add(2 * time.Second)
	var first time.Time
	for time.Now().Before(deadline) {
		s.mu.Lock()
		first = s.readLeaseUntil
		s.mu.Unlock()
		if !first.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if first.IsZero() {
		t.Fatal("periodic heartbeat did not grant a read lease")
	}
	for time.Now().Before(deadline) {
		s.mu.Lock()
		renewed := s.readLeaseUntil.After(first)
		s.mu.Unlock()
		if renewed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.heartbeatTicker.Stop()
	s.mu.Lock()
	if !s.readLeaseUntil.After(first) {
		s.mu.Unlock()
		t.Fatal("periodic heartbeat did not renew the lease")
	}
	// Stop renewal. Reads must use the existing lease and touch no log.
	until, seq, applied := s.readLeaseUntil, s.max_seq, s.applied
	s.mu.Unlock()
	for i := 0; i < 10; i++ {
		if r := s.waitForProposal(context.Background(), command(kv.Operation{Action: "get", Key: "k"})); r.Status != Applied || r.Value != "v" {
			t.Fatalf("leased local read: %+v", r)
		}
	}
	s.mu.Lock()
	if s.readLeaseUntil != until || s.max_seq != seq || s.applied != applied {
		s.mu.Unlock()
		t.Fatal("read renewed its lease or changed the log")
	}
	s.readLeaseUntil = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if r := s.waitForProposal(context.Background(), command(kv.Operation{Action: "get", Key: "k"})); r.Status != Applied || r.Value != "v" || s.max_seq != seq+1 || s.applied != applied+1 || !s.readyLocked() {
		t.Fatalf("expired read lease did not fall back to a proposal: %+v", r)
	}
}

func TestLeaseRestartProtection(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	var granted HeartbeatReply
	s.Heartbeat(&HeartbeatArgs{LeaderID: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}}, &granted)
	restarted, err := newServer(0, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []types.ProposalID{{Round: 2, NodeID: 1}, {Round: 3, NodeID: 2}} {
		var r HeartbeatReply
		restarted.Heartbeat(&HeartbeatArgs{LeaderID: int(n.NodeID), ProposalNumber: n}, &r)
		if r.Result || restarted.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || !restarted.readLeaseUntil.IsZero() {
			t.Fatal("restart forgot lease protection or restored a read lease")
		}
	}
	restarted.leaseGrantUntil = time.Now().Add(-time.Second)
	var prepared LeaderPrepareReply
	restarted.Prepare(&LeaderPrepareArgs{LeaderID: 2, ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}}, &prepared)
	if !prepared.Prepared {
		t.Fatal("restart protection never allowed a new leader")
	}
}

func TestHeartbeatWithoutQuorumDoesNotGrantReadLease(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "127.0.0.1:0", "127.0.0.1:0"})
	s.observeProposalNumberLocked(types.ProposalID{Round: 1, NodeID: 0})
	s.phase = leaderReady
	if s.broadcastHeartbeat(types.ProposalID{Round: 1, NodeID: 0}) >= s.majority || !s.readLeaseUntil.IsZero() {
		t.Fatal("local heartbeat alone established a master lease")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if r := s.waitForProposal(ctx, command(kv.Operation{Action: "get", Key: "k"})); r.Status != Expired {
		t.Fatal("isolated leader returned read without quorum")
	}
	s.mu.Lock()
	if s.applied != 0 || s.max_seq != 1 || decodeCommand(s.insts[1].Op).Action != "get" || s.insts[1].Decided || !s.readyLocked() || len(s.pinnedResults) != 1 {
		s.mu.Unlock()
		t.Fatal("handler timeout stopped consensus or served a stale read")
	}
	s.invalidateLeaderLocked(s.leaderProposalNumber, true)
	s.mu.Unlock()
	waitPendingDrained(t, s)
}

func TestFallbackGetAtCompactionBoundary(t *testing.T) {
	for _, lease := range []string{"unready", "expired"} {
		t.Run(lease, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0"})
			s.statePath = ""
			s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
			s.electLeader()
			for seq := 1; seq < LogRetention; seq++ {
				decideOperation(t, s, seq, command(kv.Operation{Action: "set", Key: "k", Value: fmt.Sprint(seq)}))
			}
			if lease == "expired" {
				s.readLeaseUntil = time.Now().Add(-time.Second)
			}
			var reply ClientGetResponse
			if err := s.Get(&ClientGetRequest{Key: "k"}, &reply); err != nil || !reply.Result || reply.Value != fmt.Sprint(LogRetention-1) {
				t.Fatalf("fallback read at compaction boundary: %+v %v", reply, err)
			}
			if s.applied != LogRetention || s.max_seq != LogRetention || s.checkpoint.Seq != LogRetention || len(s.insts) != 0 || len(s.pinnedResults) != 0 || !s.readyLocked() {
				t.Fatal("fallback read did not compact and release its pinned result")
			}
		})
	}
}

type noReadLeasePeer struct {
	*Server
	heartbeats chan struct{}
}

func (p *noReadLeasePeer) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	reply.ProposalNumber = args.ProposalNumber
	p.heartbeats <- struct{}{}
	return nil
}

func TestFailedLeaseRenewalKeepsLeaderAndAllowsProposals(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	peers := []string{"local", l.Addr().String()}
	follower, err := newServer(1, peers, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &noReadLeasePeer{Server: follower, heartbeats: make(chan struct{}, 16)}
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", p); err != nil {
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
	if !s.readyLocked() {
		t.Fatal("leader election failed")
	}
	n := s.leaderProposalNumber
	s.readLeaseUntil = time.Now().Add(50 * time.Millisecond)
	s.StartHeartbeat()
	t.Cleanup(func() { s.heartbeatTicker.Stop() })
	// Two failed renewals ensure the first has completed and the lease expired.
	for i := 0; i < 2; i++ {
		select {
		case <-p.heartbeats:
		case <-time.After(2 * time.Second):
			t.Fatal("leader stopped sending heartbeats after failed renewal")
		}
	}
	s.mu.Lock()
	ready := s.readyLocked() && s.leaderProposalNumber == n && !time.Now().Before(s.readLeaseUntil)
	s.mu.Unlock()
	if !ready {
		t.Fatal("failed read lease renewal revoked leadership")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if r := s.waitForProposal(ctx, command(kv.Operation{ReqID: newRequestID(), Action: "set", Key: "k", Value: "v"})); r.Status != Applied {
		t.Fatalf("write after failed lease renewal: %+v", r)
	}
	if r := s.waitForProposal(ctx, command(kv.Operation{Action: "get", Key: "k"})); r.Status != Applied || r.Value != "v" {
		t.Fatalf("fallback read after failed lease renewal: %+v", r)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.readyLocked() || s.leaderProposalNumber != n || s.max_seq != 2 || s.applied != 2 {
		t.Fatal("write/read proposals changed leadership or failed to advance the log")
	}
}

func TestHeartbeatLateHigherPromiseStopsRenewal(t *testing.T) {
	peers := []string{"local", "", ""}
	for i := 1; i < len(peers); i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peers[i] = l.Addr().String()
		t.Cleanup(func() { l.Close() })
		registry := rpc.NewServer()
		if err := registry.RegisterName("Server", &leaseHeartbeatPeer{higher: i == 2}); err != nil {
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
	}
	s := newProtocolServer(t, peers)
	s.observeProposalNumberLocked(types.ProposalID{Round: 1, NodeID: 0})
	s.phase = leaderReady
	if s.broadcastHeartbeat(types.ProposalID{Round: 1, NodeID: 0}) != s.majority {
		t.Fatal("initial majority heartbeat failed")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		stopped := s.phase == leaderFollower && s.readLeaseUntil.IsZero() && s.round_num >= 4
		s.mu.Unlock()
		if stopped {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("late higher promise did not stop the old master or advance its next round")
}

type leaseHeartbeatPeer struct{ higher bool }

func (p *leaseHeartbeatPeer) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	if p.higher {
		time.Sleep(50 * time.Millisecond)
		reply.ProposalNumber = types.ProposalID{Round: 3, NodeID: 2}
	} else {
		reply.Result = true
		reply.ProposalNumber = args.ProposalNumber
	}
	return nil
}

func TestAcceptDoesNotLearnLeader(t *testing.T) {
	for _, source := range []string{"prepare", "heartbeat"} {
		t.Run(source, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0", "p1", "p2"})
			for seq := 1; seq <= 2; seq++ {
				var reply AcceptReply
				s.Accept(&AcceptArgs{Seq: seq, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &reply)
				if !reply.Accepted || s.promisedProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.leader != -1 || s.leaderProposalNumber != (types.ProposalID{}) || s.readyLocked() {
					t.Fatal("Accept learned leadership instead of only updating the promise")
				}
			}
			if source == "prepare" {
				var reply LeaderPrepareReply
				s.Prepare(&LeaderPrepareArgs{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 1}, &reply)
				if !reply.Prepared {
					t.Fatal("same-promise Prepare was rejected")
				}
			} else {
				var reply HeartbeatReply
				s.Heartbeat(&HeartbeatArgs{ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, LeaderID: 1}, &reply)
				if !reply.Result {
					t.Fatal("same-promise heartbeat was rejected")
				}
			}
			if s.leader != 1 || s.leaderProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) || s.readyLocked() {
				t.Fatal("Prepare/heartbeat failed to learn leader or incorrectly became ready")
			}
			var reply AcceptReply
			s.Accept(&AcceptArgs{Seq: 3, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: command(kv.Operation{Action: "nop"})}, &reply)
			if !reply.Accepted || s.leader != 1 || s.leaderProposalNumber != (types.ProposalID{Round: 2, NodeID: 1}) {
				t.Fatal("same-term Accept changed known leadership")
			}
		})
	}
}

func TestProposalIDGenerationAndRestart(t *testing.T) {
	s, err := newServer(1, []string{"p0", "p1", "p2"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if n := s.makeProposalNumberLocked(); n != (types.ProposalID{Round: 1, NodeID: 1}) {
		t.Fatalf("first proposal: %+v", n)
	}
	for _, observed := range []types.ProposalID{
		{Round: 2, NodeID: 0}, // The local node already wins the same-round tie.
		{Round: 2, NodeID: 1}, // A retry must not reuse an observed proposal.
		{Round: 3, NodeID: 2}, // A higher node ID wins the same-round tie.
		{Round: 1 << 63, NodeID: 2},
	} {
		s.catchupRoundNumLocked(observed)
		if n := s.makeProposalNumberLocked(); n.Compare(observed) <= 0 || n.NodeID != 1 {
			t.Fatalf("generated %+v after observing %+v", n, observed)
		}
	}
	promise := types.ProposalID{Round: s.round_num, NodeID: 1}
	var reply LeaderPrepareReply
	s.Prepare(&LeaderPrepareArgs{ProposalNumber: promise, LeaderID: 1}, &reply)
	if !reply.Prepared {
		t.Fatal("large proposal rejected")
	}
	restarted, err := newServer(1, s.peers, tDir(s))
	if err != nil {
		t.Fatal(err)
	}
	if restarted.promisedProposalNumber != promise || restarted.round_num != s.round_num {
		t.Fatal("restart lost the uint64 proposal or round")
	}
	restarted.catchupRoundNumLocked(restarted.promisedProposalNumber)
	if n := restarted.makeProposalNumberLocked(); n.Compare(promise) <= 0 {
		t.Fatalf("restart reused proposal: %+v <= %+v", n, promise)
	}
}

func TestAcceptRejectsInvalidProposalID(t *testing.T) {
	s := newProtocolServer(t, []string{"p0", "p1", "p2"})
	for _, n := range []types.ProposalID{{}, {NodeID: 1}, {Round: 1, NodeID: 3}, {Round: 1, NodeID: ^uint64(0)}} {
		var reply AcceptReply
		s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: n, Value: command(kv.Operation{Action: "nop"})}, &reply)
		if reply.Accepted || len(s.insts) != 0 || s.promisedProposalNumber != (types.ProposalID{}) {
			t.Fatalf("invalid proposal mutated state: %+v", n)
		}
	}
}

package paxos

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"6.5840/paxos/types"
)

// Probe is registered only by the test subprocess, never by Serve.
type NodeState struct {
	Checkpoint                  Checkpoint
	ID, Applied, MaxSeq, Leader int
	ProposalNumber              types.ProposalID
	Ready                       bool
	KV                          map[string]string
	Requests                    map[string]RequestRecord
	Insts                       map[int]Instance
}

type StateProbe struct{ s *Server }

func (p *StateProbe) State(_ *struct{}, out *NodeState) error {
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	*out = NodeState{ID: s.own_number, Applied: s.applied, MaxSeq: s.max_seq,
		Leader: s.leader, ProposalNumber: s.promisedProposalNumber, Ready: s.readyLocked(),
		Checkpoint: cloneCheckpoint(s.checkpoint), Requests: maps.Clone(s.requests), KV: maps.Clone(s.kv), Insts: maps.Clone(s.insts)}
	return nil
}

type testCluster struct {
	t            *testing.T
	peers        []string
	procs        []*exec.Cmd
	parentPipes  []*os.File
	stoppedPorts []net.Listener
	root         string
	// Remember all decisions ever observed, including on subsequently killed nodes.
	observed  map[int]NodeState
	decisions map[int]OP
	links     [][]*faultLink
	routes    [][]string
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	c := &testCluster{t: t, peers: make([]string, 5), procs: make([]*exec.Cmd, 5), parentPipes: make([]*os.File, 5), stoppedPorts: make([]net.Listener, 5), root: t.TempDir(), decisions: make(map[int]OP)}
	if artifacts := os.Getenv("PAXOS_TEST_ARTIFACTS"); artifacts != "" {
		if err := os.MkdirAll(artifacts, 0700); err != nil {
			t.Fatal(err)
		}
		root, err := os.MkdirTemp(artifacts, t.Name()+"-")
		if err != nil {
			t.Fatal(err)
		}
		c.root = root
		t.Logf("retained cluster artifacts: %s", root)
	}
	var reservations []net.Listener
	for i := range c.peers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, l)
		c.peers[i] = l.Addr().String()
	}
	c.links = make([][]*faultLink, 5)
	c.routes = make([][]string, 5)
	for from := 0; from < 5; from++ {
		c.links[from] = make([]*faultLink, 5)
		c.routes[from] = append([]string(nil), c.peers...)
		for to := 0; to < 5; to++ {
			if from == to {
				continue
			}
			link, err := newFaultLink(c.peers[to])
			if err != nil {
				t.Fatal(err)
			}
			c.links[from][to] = link
			c.routes[from][to] = link.listener.Addr().String()
		}
	}
	for _, l := range reservations {
		l.Close()
	}
	t.Cleanup(func() {
		for _, row := range c.links {
			for _, link := range row {
				if link != nil {
					link.close()
				}
			}
		}
	})
	t.Cleanup(func() {
		for i := range c.procs {
			c.stop(i)
			if c.stoppedPorts[i] != nil {
				c.stoppedPorts[i].Close()
			}
		}
		for i := range c.procs {
			b, _ := os.ReadFile(c.logPath(i))
			// A child killed by the harness cannot report its race-detector exit status.
			if strings.Contains(string(b), "WARNING: DATA RACE") {
				t.Errorf("node %d reported a data race", i)
			}
			if t.Failed() {
				lines := strings.Split(string(b), "\n")
				if len(lines) > 40 {
					lines = lines[len(lines)-40:]
				}
				t.Logf("node %d log: %s", i, strings.Join(lines, "\n"))
			}
		}
	})
	for i := range c.procs {
		c.start(i)
	}
	return c
}

func (c *testCluster) logPath(i int) string {
	return filepath.Join(c.root, fmt.Sprintf("node-%d.log", i))
}

func (c *testCluster) stop(i int) {
	if c.procs[i] != nil {
		c.procs[i].Process.Kill()
		c.procs[i].Wait()
		c.procs[i] = nil
		c.parentPipes[i].Close()
		c.parentPipes[i] = nil
		// Keep this server port out of the ephemeral client pool while stopped.
		l, err := net.Listen("tcp", c.peers[i])
		if err != nil {
			c.t.Errorf("reserve stopped node %d port: %v", i, err)
			return
		}
		c.stoppedPorts[i] = l
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
	}
}

func (c *testCluster) start(i int) {
	c.t.Helper()
	if c.procs[i] != nil {
		c.t.Fatalf("node %d already running", i)
	}
	if c.stoppedPorts[i] != nil {
		c.stoppedPorts[i].Close()
		c.stoppedPorts[i] = nil
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLeaderProcess$")
	// Only the parent owns the write end. EOF also handles hard test timeouts,
	// where testing.Cleanup does not run and children would otherwise survive.
	parentRead, parentWrite, err := os.Pipe()
	if err != nil {
		c.t.Fatal(err)
	}
	defer parentRead.Close()
	cmd.ExtraFiles = []*os.File{parentRead}
	cmd.Env = append(os.Environ(), "PAXOS_TEST_CHILD=1", "PAXOS_TEST_PUBLIC_PEERS="+strings.Join(c.peers, ","), "PAXOS_TEST_PEERS="+strings.Join(c.routes[i], ","), "PAXOS_TEST_ME="+strconv.Itoa(i), "PAXOS_TEST_DATA="+c.root)
	f, err := os.OpenFile(c.logPath(i), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		parentWrite.Close()
		c.t.Fatal(err)
	}
	cmd.Stdout = f
	cmd.Stderr = f
	err = cmd.Start()
	f.Close()
	if err != nil {
		parentWrite.Close()
		c.t.Fatal(err)
	}
	c.procs[i] = cmd
	c.parentPipes[i] = parentWrite
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.state(i); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %d failed to start", i)
}

func (c *testCluster) state(i int) (NodeState, error) {
	var s NodeState
	err := callWithTimeout(c.peers[i], "Probe.State", &struct{}{}, &s, time.Second)
	if err == nil && s.ID != i {
		err = fmt.Errorf("queried node %d but received identity %d", i, s.ID)
	}
	return s, err
}

func (c *testCluster) client(i int) *Client {
	x := &Client{}
	x.InitPeers(c.peers)
	x.target_server = c.peers[i]
	return x
}

func (c *testCluster) set(k, v string) {
	c.t.Helper()
	if !c.client(0).Send_set(k, v) {
		c.t.Fatalf("set %q=%q failed", k, v)
	}
}

func (c *testCluster) leader() NodeState {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, p := range c.procs {
			if p == nil {
				continue
			}
			s, err := c.state(i)
			if err == nil && s.Ready && s.Leader == i {
				return s
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no ready leader")
	return NodeState{}
}

// Every snapshot must agree with a replay of its applied prefix. Check decisions
// above applied as well: a hole must never hide conflicting chosen operations.
func (c *testCluster) validate(s NodeState) error {
	if c.observed == nil {
		c.observed = make(map[int]NodeState)
	}
	if old, ok := c.observed[s.ID]; ok && (s.Applied < old.Applied || s.MaxSeq < old.MaxSeq || s.ProposalNumber.Compare(old.ProposalNumber) < 0) {
		return fmt.Errorf("node %d durable counters regressed: applied %d->%d max %d->%d proposal number %v->%v", s.ID, old.Applied, s.Applied, old.MaxSeq, s.MaxSeq, old.ProposalNumber, s.ProposalNumber)
	}
	c.observed[s.ID] = s
	if s.Ready && s.Leader != s.ID {
		return fmt.Errorf("node %d ready while following %d", s.ID, s.Leader)
	}
	if s.ID < 0 || s.ID >= len(c.peers) || s.Applied < 0 || s.Applied > s.MaxSeq {
		return fmt.Errorf("invalid counters: %+v", s)
	}
	kv := maps.Clone(s.Checkpoint.KV)
	requests := maps.Clone(s.Checkpoint.Requests)
	if s.Checkpoint.Seq < 0 || s.Checkpoint.Seq > s.Applied || kv == nil || requests == nil {
		return fmt.Errorf("node %d invalid checkpoint", s.ID)
	}
	for seq := s.Checkpoint.Seq + 1; seq <= s.Applied; seq++ {
		inst, ok := s.Insts[seq]
		if !ok || !inst.Decided {
			return fmt.Errorf("node %d applied past missing/undecided slot %d", s.ID, seq)
		}
		if inst.Op.ReqID != "" {
			if _, exists := requests[inst.Op.ReqID]; exists {
				continue
			}
			requests[inst.Op.ReqID] = RequestRecord{Op: inst.Op, Seq: seq}
		}
		switch inst.Op.Action {
		case "set":
			kv[inst.Op.Key] = inst.Op.Value
		case "get":
			if inst.ReadValue != kv[inst.Op.Key] {
				return fmt.Errorf("node %d slot %d read=%q replay=%q", s.ID, seq, inst.ReadValue, kv[inst.Op.Key])
			}
		case "nop":
		default:
			return fmt.Errorf("node %d unknown operation at %d", s.ID, seq)
		}
	}
	if !reflect.DeepEqual(requests, s.Requests) {
		return fmt.Errorf("node %d dedup table differs from log replay", s.ID)
	}
	if !reflect.DeepEqual(kv, s.KV) {
		return fmt.Errorf("node %d KV differs from log replay: got=%v replay=%v", s.ID, s.KV, kv)
	}
	for seq, inst := range s.Insts {
		if seq < 1 || seq > s.MaxSeq || (!inst.Decided && inst.ProposalAcceptNumber.Compare(s.ProposalNumber) > 0) {
			return fmt.Errorf("node %d invalid slot %d: %+v", s.ID, seq, inst)
		}
		if inst.Decided {
			if old, ok := c.decisions[seq]; ok && old != inst.Op {
				return fmt.Errorf("conflicting decision node=%d slot=%d old=%+v new=%+v", s.ID, seq, old, inst.Op)
			}
			c.decisions[seq] = inst.Op
		}
	}
	return nil
}

// No client Get here: reads must not append logs or redirect to the leader.
func (c *testCluster) converge(want map[string]string) []NodeState {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		states := make([]NodeState, 0, 5)
		all := true
		applied := -1
		for i, p := range c.procs {
			if p == nil {
				continue
			}
			s, err := c.state(i)
			if err != nil {
				all = false
				last = fmt.Sprintf("node %d: %v", i, err)
				continue
			}
			if err := c.validate(s); err != nil {
				c.t.Fatal(err)
			}
			states = append(states, s)
			if want != nil && !reflect.DeepEqual(s.KV, want) {
				all = false
				last = fmt.Sprintf("node %d KV=%v want=%v applied=%d", i, s.KV, want, s.Applied)
			}
			if applied < 0 {
				applied = s.Applied
			}
			if s.Applied != applied || s.MaxSeq != s.Applied {
				all = false
				last = fmt.Sprintf("node %d applied=%d max=%d reference=%d", i, s.Applied, s.MaxSeq, applied)
			}
		}
		if all && len(states) > 0 {
			readyLeaders := 0
			for _, s := range states {
				if s.Ready && s.Leader == s.ID {
					readyLeaders++
				}
				if s.Leader != states[0].Leader || s.ProposalNumber != states[0].ProposalNumber {
					all = false
					last = "leader/proposal number mismatch across nodes"
				}
			}
			if readyLeaders != 1 {
				all = false
				last = fmt.Sprintf("ready leaders=%d", readyLeaders)
			}
			for _, s := range states {
				if !reflect.DeepEqual(s.KV, states[0].KV) {
					all = false
					last = "KV mismatch across nodes"
				}
			}
			if all {
				return states
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("cluster failed to converge: %s", last)
	return nil
}

// Each directed server-to-server link has its own TCP proxy. Faults therefore
// affect Paxos traffic without disconnecting the client's diagnostic channel.
type faultLink struct {
	listener   net.Listener
	target     string
	mu         sync.Mutex
	blocked    bool
	unreliable bool
	count      int
	conns      map[net.Conn]bool
	wg         sync.WaitGroup
}

func newFaultLink(target string) (*faultLink, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &faultLink{listener: l, target: target, conns: make(map[net.Conn]bool)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.count++
			blocked, unreliable, count := p.blocked, p.unreliable, p.count
			// Deterministic request loss, response loss and delayed delivery. Response
			// loss permits the server to mutate state before the sender sees failure.
			if blocked || (unreliable && count%7 == 0) {
				p.mu.Unlock()
				conn.Close()
				continue
			}
			p.conns[conn] = true
			p.wg.Add(1)
			p.mu.Unlock()
			go p.forward(conn, unreliable, count)
		}
	}()
	return p, nil
}

func (p *faultLink) forward(conn net.Conn, unreliable bool, count int) {
	defer p.wg.Done()
	defer func() { conn.Close(); p.mu.Lock(); delete(p.conns, conn); p.mu.Unlock() }()
	if unreliable && count%3 == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	remote, err := net.DialTimeout("tcp", p.target, 100*time.Millisecond)
	if err != nil {
		return
	}
	defer remote.Close()
	p.mu.Lock()
	if p.blocked {
		p.mu.Unlock()
		return
	}
	p.conns[remote] = true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, remote); p.mu.Unlock() }()
	// Both copy goroutines must terminate; closing either end releases the other.
	done := make(chan struct{})
	go func() { io.Copy(remote, conn); remote.Close(); close(done) }()
	if unreliable && count%11 == 0 {
		io.Copy(io.Discard, remote)
	} else {
		io.Copy(conn, remote)
	}
	conn.Close()
	remote.Close()
	<-done
}

func (p *faultLink) configure(blocked, unreliable bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	p.unreliable = unreliable
	for conn := range p.conns {
		conn.Close()
	}
}

func (p *faultLink) close() {
	p.listener.Close()
	p.configure(true, false)
	p.wg.Wait()
}

func (c *testCluster) partition(groups ...[]int) {
	membership := make(map[int]int)
	for group, nodes := range groups {
		for _, node := range nodes {
			membership[node] = group
		}
	}
	if len(membership) != 5 {
		c.t.Fatal("partition must include all five nodes")
	}
	for from, row := range c.links {
		for to, link := range row {
			if link != nil {
				link.configure(membership[from] != membership[to], false)
			}
		}
	}
}

func (c *testCluster) heal(unreliable bool) {
	for _, row := range c.links {
		for _, link := range row {
			if link != nil {
				link.configure(false, unreliable)
			}
		}
	}
}

// The child runs the same server code with its own RPC registry and process.
func TestLeaderProcess(t *testing.T) {
	if os.Getenv("PAXOS_TEST_CHILD") != "1" {
		return
	}
	parent := os.NewFile(3, "test-parent")
	go func() {
		io.Copy(io.Discard, parent)
		parent.Close()
		os.Exit(0)
	}()
	peers := strings.Split(os.Getenv("PAXOS_TEST_PEERS"), ",")
	me, _ := strconv.Atoi(os.Getenv("PAXOS_TEST_ME"))
	s, err := newServer(me, peers, os.Getenv("PAXOS_TEST_DATA"))
	if err != nil {
		t.Fatal(err)
	}
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", &TestRPCServer{Server: s, publicPeers: strings.Split(os.Getenv("PAXOS_TEST_PUBLIC_PEERS"), ",")}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterName("Probe", &StateProbe{s: s}); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", peers[me])
	if err != nil {
		t.Fatal(err)
	}
	s.StartHeartbeat()
	s.StartHealthCheck()
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go registry.ServeConn(conn)
	}
}

// The test transport uses per-source proxy addresses for peer RPCs. Client
// redirects must advertise the real listening addresses, not those fault links.
type TestRPCServer struct {
	*Server
	publicPeers  []string
	rejectAccept atomic.Bool
}

func (s *TestRPCServer) publicAddress(address string) string {
	for i, peer := range s.peers {
		if address == peer {
			return s.publicPeers[i]
		}
	}
	return address
}

func (s *TestRPCServer) Set(args *ClientSetRequest, reply *ClientSetReply) error {
	err := s.Server.Set(args, reply)
	reply.LeaderAddress = s.publicAddress(reply.LeaderAddress)
	return err
}

func (s *TestRPCServer) Get(args *ClientGetRequest, reply *ClientGetResponse) error {
	err := s.Server.Get(args, reply)
	reply.LeaderAddress = s.publicAddress(reply.LeaderAddress)
	return err
}

// Selective RPC rejection keeps heartbeats healthy while Accept fails, allowing
// tests to distinguish an abandoned proposal from a complete network outage.
func (s *TestRPCServer) TestRejectAccept(args *bool, reply *bool) error {
	s.rejectAccept.Store(*args)
	*reply = true
	return nil
}

func (s *TestRPCServer) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if s.rejectAccept.Load() {
		reply.Seq = args.Seq
		reply.Accepted = false
		return nil
	}
	return s.Server.Accept(args, reply)
}

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
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &first)
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
			node.Accept(&AcceptArgs{Seq: seq, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &reply)
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
	op := OP{Action: "set", Key: "k", Value: "chosen"}
	var decided DecideReply
	if err := s.Decide(&DecideArgs{Seq: 1, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 3, NodeID: 2}, Op: op, Decided: true}}, &decided); err != nil || !decided.Result {
		t.Fatalf("learn decision: %+v %v", decided, err)
	}
	if s.promisedProposalNumber != (types.ProposalID{}) || s.insts[1].ProposalAcceptNumber != (types.ProposalID{Round: 3, NodeID: 2}) || s.kv["k"] != "chosen" {
		t.Fatal("learning a decision changed the promise or lost the value")
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 4, NodeID: 1}, Value: OP{Action: "set", Key: "k", Value: "conflict"}}, &accepted)
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
		args := AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 1, NodeID: 0}, Value: OP{Action: "nop"}}
		ch := fanout[AcceptReply](s, "Server.Accept", &args, func(a, r any) error {
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
	done := make(chan proposalResult, 1)
	go func() { done <- s.propose(ctx, OP{Action: "get", Key: "k"}) }()
	cancel()
	select {
	case result := <-done:
		if result.status != proposalExpired {
			t.Fatalf("cancelled queued proposal: %+v", result)
		}
	case <-time.After(time.Second):
		<-s.proposalSlot
		t.Fatal("queued proposal ignored cancellation")
	}
	<-s.proposalSlot
	if s.max_seq != 0 || !s.readyLocked() {
		t.Fatal("queued cancellation mutated log or leadership")
	}
	result := s.propose(context.Background(), OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "ok"})
	if result.status != proposalApplied {
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
	for seq := 1; seq < logRetention; seq++ {
		decideOperation(t, s, seq, OP{Action: "set", Key: "k", Value: fmt.Sprint(seq)})
	}
	var reply ClientGetResponse
	if err := s.Get(&ClientGetRequest{Key: "k"}, &reply); err != nil || !reply.Result || reply.Value != fmt.Sprint(logRetention-1) {
		t.Fatalf("read at compaction boundary: %+v %v", reply, err)
	}
	if s.max_seq != logRetention-1 || s.checkpoint.Seq != 0 || len(s.insts) != logRetention-1 {
		t.Fatal("local read changed the log or checkpoint")
	}
	decideOperation(t, s, logRetention, OP{Action: "set", Key: "k", Value: reply.Value})
	if s.checkpoint.Seq != logRetention || len(s.insts) != 0 {
		t.Fatalf("write did not compact state: checkpoint=%d entries=%d", s.checkpoint.Seq, len(s.insts))
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: logRetention, ProposalNumber: types.ProposalID{Round: 1, NodeID: 0}, Value: OP{Action: "set", Key: "k", Value: "bad"}}, &accepted)
	if accepted.Accepted {
		t.Fatal("compacted read slot was accepted again")
	}
	decideOperation(t, s, logRetention, OP{Action: "set", Key: "k", Value: "bad"})
	if s.kv["k"] != reply.Value || len(s.insts) != 0 {
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
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 1, NodeID: 1}, Value: OP{Action: "nop"}}, &accept)
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
	decideOperation(t, s, 3, OP{Action: "set", Key: "suffix", Value: "applied"})
	checkpoint := Checkpoint{Seq: 2, KV: map[string]string{"k": "prefix"}, Requests: map[string]RequestRecord{}}
	s.mu.Lock()
	s.installCheckpointLocked(checkpoint)
	checkpoint.KV["k"] = "external mutation"
	copy := cloneCheckpoint(s.checkpoint)
	copy.KV["k"] = "reply mutation"
	s.installCheckpointLocked(Checkpoint{Seq: 1, KV: map[string]string{"k": "older"}, Requests: map[string]RequestRecord{}})
	s.mu.Unlock()
	if s.applied != 3 || s.kv["k"] != "prefix" || s.checkpoint.KV["k"] != "prefix" || s.kv["suffix"] != "applied" {
		t.Fatal("checkpoint aliases caller data, regresses state, or loses the decided suffix")
	}
}

func TestDecidedConflictDoesNotMutateState(t *testing.T) {
	s := newProtocolServer(t, []string{"p0"})
	op := OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "chosen"}
	decideOperation(t, s, 1, op)
	before := s.insts[1]
	var reply DecideReply
	err := s.Decide(&DecideArgs{Seq: 1, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 11, NodeID: 0}, Op: OP{Action: "set", Key: "k", Value: "conflict"}, Decided: true}}, &reply)
	if err == nil || reply.Result || s.insts[1] != before || s.kv["k"] != "chosen" {
		t.Fatal("conflicting Decide changed a chosen operation")
	}
	var accepted AcceptReply
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: OP{Action: "nop"}}, &accepted)
	if accepted.Accepted || s.insts[1] != before {
		t.Fatal("conflicting Accept changed a decided slot")
	}
	decideOperation(t, s, 1, op)
	restored, err := newServer(0, s.peers, tDir(s))
	if err != nil || restored.kv["k"] != "chosen" || restored.applied != 1 {
		t.Fatalf("decided value did not survive restart: %v", err)
	}
}

func TestSnapshotValidation(t *testing.T) {
	for _, mode := range []string{"corrupt", "cluster", "version", "invalid-prefix", "missing-kv"} {
		t.Run(mode, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0"})
			decideOperation(t, s, 1, OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "stored"})
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
				snapshot := serverSnapshot{Version: snapshotVersion, Peers: s.peers, Checkpoint: cloneCheckpoint(s.checkpoint), Insts: maps.Clone(s.insts), KV: maps.Clone(s.kv), Requests: maps.Clone(s.requests), MaxSeq: s.max_seq, Applied: s.applied, PromisedProposalNumber: s.promisedProposalNumber}
				switch mode {
				case "version":
					snapshot.Version++
				case "invalid-prefix":
					snapshot.Applied = snapshot.MaxSeq + 1
				case "missing-kv":
					snapshot.KV = nil
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
	for seq := 1; seq <= logRetention+catchUpBatchSize+3; seq++ {
		decideOperation(t, s, seq, OP{Action: "set", Key: "k", Value: fmt.Sprint(seq)})
	}
	var reply FetchLogReply
	s.FetchLog(&FetchLogArgs{ProposalNumber: s.leaderProposalNumber, StartSeq: 1}, &reply)
	if !reply.Result || reply.Checkpoint.Seq != logRetention || len(reply.Entries) != catchUpBatchSize {
		t.Fatalf("wrong checkpoint/batch response: checkpoint=%d entries=%d result=%v", reply.Checkpoint.Seq, len(reply.Entries), reply.Result)
	}
	for i, entry := range reply.Entries {
		if entry.Seq != logRetention+i+1 || !entry.Instance.Decided {
			t.Fatal("fetch returned a noncontiguous or undecided entry")
		}
	}
	reply.Checkpoint.KV["k"] = "caller mutation"
	if reflect.DeepEqual(reply.Checkpoint.KV, s.checkpoint.KV) {
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
				s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &reply)
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
			result := s.propose(context.Background(), OP{Action: "get", Key: "k"})
			if result.status != proposalRedirect || result.leader != leader {
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
			s.Accept(&AcceptArgs{Seq: 100, ProposalNumber: types.ProposalID{Round: 4, NodeID: 1}, Value: OP{Action: "nop"}}, &accept)
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
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: OP{Action: "set", Key: "k", Value: "first"}}, &first)
	s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 11, NodeID: 0}, Value: OP{Action: "set", Key: "k", Value: "second"}}, &second)
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
			for seq := 1; seq <= logRetention+3; seq++ {
				var reply DecideReply
				err := s.Decide(&DecideArgs{Seq: seq, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: OP{Action: "set", Key: "k", Value: fmt.Sprint(seq)}, Decided: true}}, &reply)
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
	if !s.readyLocked() || s.applied != logRetention+3 || s.checkpoint.Seq < logRetention || s.kv["k"] != fmt.Sprint(logRetention+3) {
		t.Fatalf("stale candidate lost compacted prefix: ready=%v applied=%d checkpoint=%d kv=%v", s.readyLocked(), s.applied, s.checkpoint.Seq, s.kv)
	}
}

func TestDecidedDuplicateRequestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	peers := []string{"p0"}
	s, err := newServer(0, peers, dir)
	if err != nil {
		t.Fatal(err)
	}
	op := OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "x"}
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
	s.Decide(&DecideArgs{Seq: 3, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "newer"}, Decided: true}}, &newer)
	var reply DecideReply
	s.Decide(&DecideArgs{Seq: 4, Instance: Instance{ProposalAcceptNumber: types.ProposalID{Round: 1, NodeID: 0}, Op: op, Decided: true}}, &reply)
	if s.kv["k"] != "newer" || s.applied != 4 || s.requests[op.ReqID].Seq != 1 {
		t.Fatalf("duplicate application: kv=%v applied=%d requests=%v", s.kv, s.applied, s.requests)
	}
}

func TestFiveNodeRecovery(t *testing.T) {
	c := newTestCluster(t)
	c.set("shared", "first")
	c.set("shared", "second")
	want := map[string]string{"shared": "second"}
	c.converge(want)
	leader := c.leader()
	follower := (leader.ID + 1) % 5
	c.stop(follower)
	// More than two FetchLog batches, with overwrites and no subsequent client
	// request after restart: catch-up must be driven by heartbeat alone.
	for i := 0; i < 2*catchUpBatchSize+7; i++ {
		k := fmt.Sprintf("k%d", i%9)
		v := strconv.Itoa(i)
		c.set(k, v)
		want[k] = v
	}
	c.converge(want)
	before := c.leader()
	c.start(follower)
	c.converge(want)
	after := c.leader()
	if before.ID != after.ID || before.ProposalNumber != after.ProposalNumber {
		t.Fatalf("follower restart triggered election: before leader=%d proposalNumber=%v, after leader=%d proposal number=%v", before.ID, before.ProposalNumber, after.ID, after.ProposalNumber)
	}
	c.stop(after.ID)
	c.set("failover", "ok")
	want["failover"] = "ok"
	c.converge(want)
	replacement := c.leader()
	c.start(after.ID)
	c.converge(want)
	restored := c.leader()
	if replacement.ID != restored.ID || replacement.ProposalNumber != restored.ProposalNumber {
		t.Fatalf("node that missed Prepare triggered election: before leader=%d proposalNumber=%v, after leader=%d proposal number=%v", replacement.ID, replacement.ProposalNumber, restored.ID, restored.ProposalNumber)
	}
	for i := range c.procs {
		c.stop(i)
	}
	for i := range c.procs {
		c.start(i)
	}
	c.converge(want)
	// Reads through all entry points verify the public API separately.
	for i := range c.peers {
		ok, v := c.client(i).Send_get("shared")
		if !ok || v != "second" {
			t.Fatalf("get through node %d: %q ok=%v", i, v, ok)
		}
	}
	c.converge(want)
}

func TestFiveNodeQuorumLoss(t *testing.T) {
	c := newTestCluster(t)
	c.set("baseline", "ok")
	want := map[string]string{"baseline": "ok"}
	c.converge(want)
	l := c.leader().ID
	a, b, d := (l+1)%5, (l+2)%5, (l+3)%5
	c.stop(a)
	c.stop(b)
	c.set("majority", "ok")
	want["majority"] = "ok"
	c.converge(want)
	c.stop(d)
	// Contact both remaining nodes directly with bounded requests. A failed
	// request can be accepted by a minority; it must never be reported successful.
	for i, p := range c.procs {
		if p == nil {
			continue
		}
		var reply ClientSetReply
		deadline := time.Now().Add(250 * time.Millisecond)
		err := callWithTimeout(c.peers[i], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "uncertain", Value: "maybe", DeadlineUnixNano: deadline.UnixNano()}, &reply, 2*time.Second)
		if err != nil {
			t.Fatalf("bounded Set on node %d did not return: %v", i, err)
		}
		if reply.Result {
			t.Fatal("write succeeded without quorum")
		}
	}
	for i, p := range c.procs {
		if p == nil {
			continue
		}
		s, err := c.state(i)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.validate(s); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.KV, want) {
			t.Fatalf("minority applied unchosen write: node %d KV=%v", i, s.KV)
		}
	}
	c.start(a)
	c.start(b)
	c.start(d)
	// The timed-out write is ambiguous and may be recovered. Overwrite it after
	// recovery so the final expected state does not assume failure means rollback.
	c.set("uncertain", "resolved")
	want["uncertain"] = "resolved"
	c.converge(want)
}

type historyInput struct {
	Key, Value string
	Set        bool
}

var paxosKVModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		m := map[string][]porcupine.Operation{}
		for _, op := range h {
			k := op.Input.(historyInput).Key
			m[k] = append(m[k], op)
		}
		var out [][]porcupine.Operation
		for _, v := range m {
			out = append(out, v)
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(historyInput)
		if in.Set {
			return output.(bool), in.Value
		}
		return output.(string) == state.(string), state
	},
}

func TestFiveNodeConcurrentLinearizability(t *testing.T) {
	c := newTestCluster(t)
	c.leader()
	workers, rounds := 12, 20
	if os.Getenv("PAXOS_STRESS") == "1" {
		workers, rounds = 24, 50
	}
	var mu sync.Mutex
	var history []porcupine.Operation
	origin := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := c.client(id % 5)
			for n := 0; n < rounds; n++ {
				in := historyInput{Key: fmt.Sprintf("hot%d", (id+n)%3), Set: n%2 == 0, Value: fmt.Sprintf("%d/%d", id, n)}
				call := time.Since(origin).Nanoseconds()
				var out interface{}
				if in.Set {
					ok := client.Set(in.Key, in.Value)
					if !ok {
						t.Errorf("worker %d set failed", id)
						return
					}
					out = true
				} else {
					ok, v := client.Send_get(in.Key)
					if !ok {
						t.Errorf("worker %d get failed", id)
						return
					}
					out = v
				}
				ret := time.Since(origin).Nanoseconds()
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: id, Input: in, Output: out, Call: call, Return: ret})
				mu.Unlock()
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	monitoring := true
workload:
	for {
		select {
		case <-done:
			break workload
		case <-ticker.C:
			if !monitoring {
				continue
			}
			for i := range c.peers {
				state, err := c.state(i)
				if err == nil {
					err = c.validate(state)
				}
				if err != nil {
					t.Errorf("during concurrent workload: node %d: %v", i, err)
					monitoring = false
					break
				}
			}
		}
	}
	ticker.Stop()
	if t.Failed() {
		return
	}
	encoded, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.root, "history.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := porcupine.CheckOperationsVerbose(paxosKVModel, history, 10*time.Second)
	if result != porcupine.Ok {
		t.Logf("history: %+v", history)
		t.Fatalf("history is not verified linearizable: %s (%d operations)", result, len(history))
	}
	// Pin an unambiguous final value for every key, then inspect all replicas.
	want := map[string]string{}
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("hot%d", i)
		c.set(k, "final")
		want[k] = "final"
	}
	states := c.converge(want)
	t.Logf("verified %d operations, %d replicas, applied=%d", len(history), len(states), states[0].Applied)
}

func TestFiveNodePartitions(t *testing.T) {
	c := newTestCluster(t)
	c.set("base", "ok")
	want := map[string]string{"base": "ok"}
	c.converge(want)
	l := c.leader().ID
	minority := []int{l, (l + 1) % 5}
	majority := []int{(l + 2) % 5, (l + 3) % 5, (l + 4) % 5}
	c.partition(minority, majority)
	var reply ClientSetReply
	deadline := time.Now().Add(400 * time.Millisecond)
	err := callWithTimeout(c.peers[l], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "pending", Value: "minority", DeadlineUnixNano: deadline.UnixNano()}, &reply, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Result {
		t.Fatal("isolated old leader committed with two nodes")
	}
	// Start in the majority so this request cannot wait on the isolated leader's
	// stale redirect. The normal client still follows leader hints and retries.
	client := c.client(majority[0])
	if !client.Send_set("pending", "majority") {
		t.Fatal("majority partition unavailable")
	}
	want["pending"] = "majority"
	for _, i := range minority {
		s, err := c.state(i)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.validate(s); err != nil {
			t.Fatal(err)
		}
		if s.KV["pending"] != "" {
			t.Fatalf("minority node %d applied unchosen operation", i)
		}
	}
	c.heal(false)
	c.converge(want)
	// Move a peer across the boundary, changing the majority while retaining its
	// intersection. The old leader belongs to the new minority again.
	l = c.leader().ID
	c.partition([]int{l, (l + 2) % 5}, []int{(l + 1) % 5, (l + 3) % 5, (l + 4) % 5})
	client = c.client((l + 1) % 5)
	if !client.Send_set("moved", "ok") {
		t.Fatal("new majority unavailable")
	}
	want["moved"] = "ok"
	c.heal(false)
	c.converge(want)
}

func TestFiveNodeUnreliableRPC(t *testing.T) {
	c := newTestCluster(t)
	c.leader()
	c.heal(true)
	want := map[string]string{}
	for i := 0; i < 35; i++ {
		k := fmt.Sprintf("loss%d", i%5)
		v := strconv.Itoa(i)
		c.set(k, v)
		want[k] = v
		ok, got := c.client(i % 5).Send_get(k)
		if !ok || got != v {
			t.Fatalf("read after lossy write: key=%s got=%q want=%q ok=%v", k, got, v, ok)
		}
	}
	c.heal(false)
	c.converge(want)
}

func TestFiveNodeRecoverAcceptedSuffix(t *testing.T) {
	c := newTestCluster(t)
	c.set("base", "ok")
	c.converge(map[string]string{"base": "ok"})
	leader := c.leader()
	seq := leader.Applied + 3
	op := OP{Action: "set", Key: "chosen", Value: "survives"}
	// A quorum accepts out of order, then the proposer crashes before Decide.
	// Election must preserve the chosen value and insert NOP in both holes.
	for j := 0; j < 3; j++ {
		node := (leader.ID + j) % 5
		var reply AcceptReply
		err := call(c.peers[node], "Server.Accept", &AcceptArgs{Seq: seq, ProposalNumber: leader.ProposalNumber, Value: op}, &reply)
		if err != nil || !reply.Accepted {
			t.Fatalf("inject accepted quorum node=%d reply=%+v err=%v", node, reply, err)
		}
	}
	c.stop(leader.ID)
	// No client write causes recovery: only the leader election can fill holes.
	states := c.converge(map[string]string{"base": "ok", "chosen": "survives"})
	for _, s := range states {
		for hole := seq - 2; hole < seq; hole++ {
			if s.Insts[hole].Op.Action != "nop" {
				t.Fatalf("node %d hole %d not filled with NOP", s.ID, hole)
			}
		}
		if s.Insts[seq].Op != op {
			t.Fatalf("node %d lost quorum-accepted value", s.ID)
		}
	}
	c.start(leader.ID)
	c.converge(map[string]string{"base": "ok", "chosen": "survives"})
}

func TestFiveNodeAbandonedProposal(t *testing.T) {
	c := newTestCluster(t)
	c.set("base", "ok")
	c.converge(map[string]string{"base": "ok"})
	leader := c.leader()
	reject := true
	for i := range c.peers {
		var reply bool
		if err := call(c.peers[i], "Server.TestRejectAccept", &reject, &reply); err != nil || !reply {
			t.Fatalf("set Accept rejection node %d: %v", i, err)
		}
	}
	var reply ClientSetReply
	deadline := time.Now().Add(60 * time.Millisecond)
	err := callWithTimeout(c.peers[leader.ID], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "abandoned", Value: "uncertain", DeadlineUnixNano: deadline.UnixNano()}, &reply, time.Second)
	if err != nil || reply.Result {
		t.Fatalf("expected failed proposal: err=%v reply=%+v", err, reply)
	}
	reject = false
	for i := range c.peers {
		var reply bool
		if err := call(c.peers[i], "Server.TestRejectAccept", &reject, &reply); err != nil {
			t.Fatal(err)
		}
	}
	// Healthy heartbeats alone cannot heal an accepted-but-undecided slot. A
	// timeout must force recovery before new requests are assigned later slots.
	c.set("abandoned", "resolved")
	c.set("next", "ok")
	c.converge(map[string]string{"base": "ok", "abandoned": "resolved", "next": "ok"})
}

func TestFiveNodeCompactionRecovery(t *testing.T) {
	c := newTestCluster(t)
	id := newRequestID()
	leader := c.leader().ID
	var reply ClientSetReply
	if err := call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "original", DeadlineUnixNano: time.Now().Add(time.Second).UnixNano()}, &reply); err != nil || !reply.Result {
		t.Fatalf("initial write: %v %+v", err, reply)
	}
	follower := (leader + 1) % 5
	c.stop(follower)
	want := map[string]string{}
	for i := 0; i < logRetention+15; i++ {
		v := fmt.Sprint(i)
		c.set("k", v)
		want["k"] = v
	}
	states := c.converge(want)
	for _, s := range states {
		if s.Checkpoint.Seq < logRetention || len(s.Insts) >= logRetention {
			t.Fatalf("node %d not compacted: checkpoint=%d entries=%d", s.ID, s.Checkpoint.Seq, len(s.Insts))
		}
	}
	c.start(follower)
	states = c.converge(want)
	for _, s := range states {
		if s.Checkpoint.Seq < logRetention {
			t.Fatalf("node %d did not install checkpoint", s.ID)
		}
	}
	c.stop(c.leader().ID)
	c.set("after", "failover")
	want["after"] = "failover"
	for i, p := range c.procs {
		if p == nil {
			c.start(i)
		}
	}
	c.converge(want)
	for i := range c.procs {
		c.stop(i)
	}
	for i := range c.procs {
		c.start(i)
	}
	c.converge(want)
	leader = c.leader().ID
	reply = ClientSetReply{}
	if err := call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "original", DeadlineUnixNano: time.Now().Add(time.Second).UnixNano()}, &reply); err != nil || !reply.Result {
		t.Fatalf("compacted duplicate: %v %+v", err, reply)
	}
	c.converge(want)
	// Stale Accept must not recreate an already compacted slot.
	var accepted AcceptReply
	s, _ := c.state(leader)
	if err := call(c.peers[leader], "Server.Accept", &AcceptArgs{Seq: 1, ProposalNumber: s.ProposalNumber, Value: OP{Action: "set", Key: "k", Value: "bad"}}, &accepted); err != nil || accepted.Accepted {
		t.Fatalf("compacted slot accepted: %v %+v", err, accepted)
	}
	c.converge(want)
}

func TestFiveNodeRequestDeduplication(t *testing.T) {
	c := newTestCluster(t)
	if !c.client(0).Set("k", "base") || !c.client(1).Set("k", "client") {
		t.Fatal("client writes failed")
	}
	c.converge(map[string]string{"k": "client"})
	id := newRequestID()
	retry := func() {
		t.Helper()
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) {
			leader := c.leader().ID
			var reply ClientSetReply
			err := callWithTimeout(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "once", DeadlineUnixNano: until.UnixNano()}, &reply, time.Second)
			if err == nil && reply.Result {
				return
			}
		}
		t.Fatal("duplicate request did not complete")
	}
	// Lose the response after the leader handles the RPC. The proxy forwards the
	// request and discards the reply, so the caller sees a timeout after commit.
	leader := c.leader().ID
	proxy, err := newFaultLink(c.peers[leader])
	if err != nil {
		t.Fatal(err)
	}
	proxy.mu.Lock()
	proxy.unreliable = true
	proxy.count = 10
	proxy.mu.Unlock()
	var reply ClientSetReply
	err = callWithTimeout(proxy.listener.Addr().String(), "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "once", DeadlineUnixNano: time.Now().Add(time.Second).UnixNano()}, &reply, time.Second)
	proxy.close()
	if err == nil {
		t.Fatal("response-loss RPC unexpectedly succeeded")
	}
	want := map[string]string{"k": "once"}
	c.converge(want)
	// A late duplicate must not overwrite a newer write.
	c.set("k", "newer")
	want["k"] = "newer"
	c.converge(want)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); retry() }()
	}
	wg.Wait()
	c.converge(want)
	// Reusing the ID with a different payload is rejected.
	leader = c.leader().ID
	reply = ClientSetReply{}
	err = call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "bad", DeadlineUnixNano: time.Now().Add(time.Second).UnixNano()}, &reply)
	if err != nil || reply.Result {
		t.Fatalf("conflicting ID: reply=%+v err=%v", reply, err)
	}
	c.stop(leader)
	retry()
	c.start(leader)
	c.converge(want)
	for i := range c.procs {
		c.stop(i)
	}
	for i := range c.procs {
		c.start(i)
	}
	retry()
	c.converge(want)
	// A new ID with identical arguments is a distinct high-level invocation.
	id = newRequestID()
	retry()
	want["k"] = "once"
	c.converge(want)
}

func TestFiveNodeClientRetriesSameUUID(t *testing.T) {
	c := newTestCluster(t)
	leader := c.leader().ID
	proxy, err := newFaultLink(c.peers[leader])
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	proxy.mu.Lock()
	proxy.unreliable = true
	proxy.count = 10
	proxy.mu.Unlock()
	client := &Client{}
	client.Init(proxy.listener.Addr().String())
	// First reply is lost; the actual Client.Put loop must reuse the UUID.
	if !client.Set("retry", "x") {
		t.Fatal("client failed to retry lost response")
	}
	states := c.converge(map[string]string{"retry": "x"})
	for _, s := range states {
		if len(s.Requests) != 1 {
			t.Fatalf("node %d client retry used multiple request IDs: %v", s.ID, s.Requests)
		}
	}
}

func TestRequestDeadlineBounds(t *testing.T) {
	for _, requested := range []int64{0, time.Now().Add(time.Hour).UnixNano(), time.Now().Add(-time.Second).UnixNano()} {
		before := time.Now()
		ctx, cancel := requestContext(requested)
		deadline, ok := ctx.Deadline()
		if !ok || deadline.After(before.Add(clientTimeout+time.Millisecond)) {
			t.Fatal("request has no bounded server deadline")
		}
		if requested < 0 || (requested != 0 && requested < before.UnixNano()) {
			if ctx.Err() == nil {
				t.Fatal("expired request was extended")
			}
		}
		cancel()
	}
}

func TestAcceptQuorumHonorsRequestDeadline(t *testing.T) {
	// A peer accepts TCP but never replies. Cancellation must beat rpcTimeout.
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
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if votes := s.broadcastAcceptContext(ctx, 1, types.ProposalID{Round: 1, NodeID: 0}, OP{Action: "nop"}); votes >= s.majority {
		t.Fatal("silent peer formed a majority")
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("request waited for full node RPC timeout")
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
					s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 3, NodeID: 2}, Value: OP{Action: "nop"}}, &r)
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
			s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &accepted)
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
	decideOperation(t, s, 1, OP{Action: "set", Key: "k", Value: "v"})
	if r := s.propose(context.Background(), OP{Action: "get", Key: "k"}); r.status != proposalApplied || r.value != "v" || s.max_seq != 2 || s.applied != 2 || !s.readyLocked() || !s.readLeaseUntil.IsZero() {
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
		if r := s.propose(context.Background(), OP{Action: "get", Key: "k"}); r.status != proposalApplied || r.value != "v" {
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
	if r := s.propose(context.Background(), OP{Action: "get", Key: "k"}); r.status != proposalApplied || r.value != "v" || s.max_seq != seq+1 || s.applied != applied+1 || !s.readyLocked() {
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
	if r := s.propose(ctx, OP{Action: "get", Key: "k"}); r.status != proposalExpired || s.applied != 0 {
		t.Fatal("isolated ready leader served a read without quorum lease")
	}
	if s.max_seq != 1 || s.insts[1].Op.Action != "get" || s.insts[1].Decided || len(s.pinnedReads) != 0 {
		t.Fatal("isolated read did not attempt a proposal or release its pin")
	}
}

func TestFallbackGetAtCompactionBoundary(t *testing.T) {
	for _, lease := range []string{"unready", "expired"} {
		t.Run(lease, func(t *testing.T) {
			s := newProtocolServer(t, []string{"p0"})
			s.statePath = ""
			s.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
			s.electLeader()
			for seq := 1; seq < logRetention; seq++ {
				decideOperation(t, s, seq, OP{Action: "set", Key: "k", Value: fmt.Sprint(seq)})
			}
			if lease == "expired" {
				s.readLeaseUntil = time.Now().Add(-time.Second)
			}
			var reply ClientGetResponse
			if err := s.Get(&ClientGetRequest{Key: "k"}, &reply); err != nil || !reply.Result || reply.Value != fmt.Sprint(logRetention-1) {
				t.Fatalf("fallback read at compaction boundary: %+v %v", reply, err)
			}
			if s.applied != logRetention || s.max_seq != logRetention || s.checkpoint.Seq != logRetention || len(s.insts) != 0 || len(s.pinnedReads) != 0 || !s.readyLocked() {
				t.Fatal("fallback read did not compact and release its pinned result")
			}
		})
	}
}

// The peer accepts proposals but declines lease renewal in the same term.
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
	if r := s.propose(ctx, OP{ReqID: newRequestID(), Action: "set", Key: "k", Value: "v"}); r.status != proposalApplied {
		t.Fatalf("write after failed lease renewal: %+v", r)
	}
	if r := s.propose(ctx, OP{Action: "get", Key: "k"}); r.status != proposalApplied || r.value != "v" {
		t.Fatalf("fallback read after failed lease renewal: %+v", r)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.readyLocked() || s.leaderProposalNumber != n || s.max_seq != 2 || s.applied != 2 {
		t.Fatal("write/read proposals changed leadership or failed to advance the log")
	}
}

// A paused master keeps its ready phase while its lease expires. A new master
// may then commit, but resuming the old process must never expose stale data.
func TestFiveNodePausedMasterLease(t *testing.T) {
	c := newTestCluster(t)
	c.set("lease", "before")
	c.converge(map[string]string{"lease": "before"})
	old := c.leader().ID
	if err := c.procs[old].Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	majority := []int{(old + 1) % 5, (old + 2) % 5, (old + 3) % 5, (old + 4) % 5}
	c.partition([]int{old}, majority)
	if !c.client(majority[0]).Send_set("lease", "after") {
		t.Fatal("new master could not write after the old lease expired")
	}
	if err := c.procs[old].Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	var reply ClientGetResponse
	if err := callWithTimeout(c.peers[old], "Server.Get", &ClientGetRequest{Key: "lease", DeadlineUnixNano: time.Now().Add(250 * time.Millisecond).UnixNano()}, &reply, clientAttemptTimeout); err != nil {
		t.Fatal(err)
	}
	if reply.Result {
		t.Fatalf("resumed isolated master served stale data: %+v", reply)
	}
	ok, value := c.client(majority[0]).Send_get("lease")
	if !ok || value != "after" {
		t.Fatalf("new master read: value=%q ok=%v", value, ok)
	}
	c.heal(false)
	c.converge(map[string]string{"lease": "after"})
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
				s.Accept(&AcceptArgs{Seq: seq, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &reply)
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
			s.Accept(&AcceptArgs{Seq: 3, ProposalNumber: types.ProposalID{Round: 2, NodeID: 1}, Value: OP{Action: "nop"}}, &reply)
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
		s.Accept(&AcceptArgs{Seq: 1, ProposalNumber: n, Value: OP{Action: "nop"}}, &reply)
		if reply.Accepted || len(s.insts) != 0 || s.promisedProposalNumber != (types.ProposalID{}) {
			t.Fatalf("invalid proposal mutated state: %+v", n)
		}
	}
}

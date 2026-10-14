package paxos

import (
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
	"github.com/google/uuid"

	"6.5840/paxos/consensus"
	"6.5840/paxos/kv"
	"6.5840/paxos/server"
	"6.5840/paxos/statemachine"
	"6.5840/paxos/transport"
	"6.5840/paxos/types"
)

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
	state := p.s.Node.Inspect()
	*out = NodeState{Checkpoint: state.Checkpoint, ID: state.ID, Applied: state.Applied, MaxSeq: state.MaxSeq, Leader: state.Leader, ProposalNumber: state.ProposalNumber, Ready: state.Ready, KV: stateValues(state.State), Requests: state.Requests, Insts: state.Insts}
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
	x.Init(c.peers[i])
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
	kv := maps.Clone(checkpointValues(s.Checkpoint))
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
		switch decodeCommand(inst.Op).Action {
		case "set":
			kv[decodeCommand(inst.Op).Key] = decodeCommand(inst.Op).Value
		case "get":
			if inst.Result != kv[decodeCommand(inst.Op).Key] {
				return fmt.Errorf("node %d slot %d read=%q replay=%q", s.ID, seq, inst.Result, kv[decodeCommand(inst.Op).Key])
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
	s, err := server.New(me, peers, os.Getenv("PAXOS_TEST_DATA"))
	if err != nil {
		t.Fatal(err)
	}
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", &TestRPCServer{Server: s, routes: peers, publicPeers: strings.Split(os.Getenv("PAXOS_TEST_PUBLIC_PEERS"), ",")}); err != nil {
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

type TestRPCServer struct {
	*Server
	publicPeers  []string
	routes       []string
	rejectAccept atomic.Bool
}

func (s *TestRPCServer) publicAddress(address string) string {
	for i, peer := range s.routes {
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
		err := callWithTimeout(c.peers[i], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "uncertain", Value: "maybe", TimeoutMs: 250}, &reply, 2*time.Second)
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
	err := callWithTimeout(c.peers[l], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "pending", Value: "minority", TimeoutMs: 400}, &reply, 2*time.Second)
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
	op := command(kv.Operation{Action: "set", Key: "chosen", Value: "survives"})
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
			if decodeCommand(s.Insts[hole].Op).Action != "nop" {
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
	err := callWithTimeout(c.peers[leader.ID], "Server.Set", &ClientSetRequest{ReqID: newRequestID(), Key: "abandoned", Value: "uncertain", TimeoutMs: 60}, &reply, time.Second)
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
	// The original worker must finish its slot after the handler stops waiting,
	// before later operations are assigned slots.
	c.set("abandoned", "resolved")
	c.set("next", "ok")
	c.converge(map[string]string{"base": "ok", "abandoned": "resolved", "next": "ok"})
	state, err := c.state(leader.ID)
	if err != nil || !state.Ready || state.ProposalNumber != leader.ProposalNumber {
		t.Fatalf("handler timeout changed leader term: %+v %v", state, err)
	}
}

func TestFiveNodeCompactionRecovery(t *testing.T) {
	c := newTestCluster(t)
	id := newRequestID()
	leader := c.leader().ID
	var reply ClientSetReply
	if err := call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "original", TimeoutMs: 1000}, &reply); err != nil || !reply.Result {
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
	if err := call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "original", TimeoutMs: 1000}, &reply); err != nil || !reply.Result {
		t.Fatalf("compacted duplicate: %v %+v", err, reply)
	}
	c.converge(want)
	// Stale Accept must not recreate an already compacted slot.
	var accepted AcceptReply
	s, _ := c.state(leader)
	if err := call(c.peers[leader], "Server.Accept", &AcceptArgs{Seq: 1, ProposalNumber: s.ProposalNumber, Value: command(kv.Operation{Action: "set", Key: "k", Value: "bad"})}, &accepted); err != nil || accepted.Accepted {
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
			err := callWithTimeout(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "once", TimeoutMs: time.Until(until).Milliseconds()}, &reply, time.Second)
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
	err = callWithTimeout(proxy.listener.Addr().String(), "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "once", TimeoutMs: 1000}, &reply, time.Second)
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
	err = call(c.peers[leader], "Server.Set", &ClientSetRequest{ReqID: id, Key: "k", Value: "bad", TimeoutMs: 1000}, &reply)
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
	if err := callWithTimeout(c.peers[old], "Server.Get", &ClientGetRequest{Key: "lease", TimeoutMs: 250}, &reply, clientAttemptTimeout); err != nil {
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

type OP = statemachine.Command
type Checkpoint = consensus.Checkpoint
type Instance = consensus.Instance
type RequestRecord = consensus.RequestRecord
type AcceptArgs = consensus.AcceptArgs
type AcceptReply = consensus.AcceptReply
type DecideArgs = consensus.DecideArgs
type DecideReply = consensus.DecideReply

const clientAttemptTimeout = 2 * time.Second

func newRequestID() string       { return uuid.NewString() }
func command(op kv.Operation) OP { return kv.Encode(op) }
func decodeCommand(cmd OP) kv.Operation {
	op, err := kv.Decode(cmd)
	if err != nil {
		panic(err)
	}
	return op
}

func stateValues(state string) map[string]string {
	store := kv.New()
	if err := store.Restore(state); err != nil {
		panic(err)
	}
	return store.Values()
}

func checkpointValues(c Checkpoint) map[string]string { return stateValues(c.State) }

var call = transport.Call
var callWithTimeout = transport.CallWithTimeout

const logRetention = consensus.LogRetention
const catchUpBatchSize = consensus.CatchUpBatchSize

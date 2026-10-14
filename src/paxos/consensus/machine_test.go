package consensus

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"6.5840/paxos/kv"
)

// An unrelated application demonstrates that the protocol has no KV dependency.
type counterMachine struct{ value int }

func (*counterMachine) Kind() string { return "counter/v1" }
func (*counterMachine) Validate(cmd Command) error {
	if cmd.ReadOnly {
		if cmd.Data != "read" {
			return fmt.Errorf("invalid counter read")
		}
		return nil
	}
	n, err := strconv.Atoi(cmd.Data)
	if err != nil || n < 0 {
		return fmt.Errorf("invalid counter increment")
	}
	return nil
}

func (m *counterMachine) Apply(cmd Command) string {
	if !cmd.ReadOnly {
		n, _ := strconv.Atoi(cmd.Data)
		m.value += n
	}
	return strconv.Itoa(m.value)
}

func (m *counterMachine) Read(Command) string { return strconv.Itoa(m.value) }
func (m *counterMachine) Snapshot() string    { return strconv.Itoa(m.value) }
func (m *counterMachine) Restore(data string) error {
	n, err := strconv.Atoi(data)
	if err != nil || n < 0 {
		return fmt.Errorf("invalid counter snapshot")
	}
	m.value = n
	return nil
}

func submitCounter(t *testing.T, node *Node, cmd Command) Result {
	t.Helper()
	ticket := node.Submit(cmd)
	defer ticket.Release()
	select {
	case <-ticket.Done():
		return ticket.Result()
	case <-time.After(time.Second):
		t.Fatal("counter proposal did not finish")
		return Result{}
	}
}

func TestGenericMachineResultsDedupCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	config := Config{ID: 0, Peers: []string{"counter"}, DataDir: dir, Machine: &counterMachine{}}
	node, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	node.lastHeartbeatTime = time.Now().Add(-2 * heartbeatTimeout)
	node.electLeader()
	write := Command{ReqID: "first", Data: "2"}
	for i := 0; i < 2; i++ {
		if result := submitCounter(t, node, write); result.Status != Applied || result.Value != "2" {
			t.Fatalf("increment/dedup result %+v", result)
		}
	}
	if node.applied != 1 {
		t.Fatal("duplicate command appended another slot")
	}
	for i := 0; i < LogRetention; i++ {
		result := submitCounter(t, node, Command{ReqID: fmt.Sprint(i), Data: "1"})
		if result.Status != Applied {
			t.Fatalf("increment failed %+v", result)
		}
	}
	if node.checkpoint.Seq < LogRetention {
		t.Fatal("counter state did not compact")
	}
	read := Command{Data: "read", ReadOnly: true}
	want := strconv.Itoa(LogRetention + 2)
	if result := submitCounter(t, node, read); result.Status != Applied || result.Value != want {
		t.Fatalf("counter slow read %+v", result)
	}
	before := node.applied
	node.broadcastHeartbeat(node.leaderProposalNumber)
	if result := submitCounter(t, node, read); result.Value != want || node.applied != before {
		t.Fatal("lease read appended a slot or returned wrong application result")
	}
	config.Machine = &counterMachine{}
	restored, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if restored.machine.Snapshot() != want || restored.requests["first"].Result != "2" {
		t.Fatal("generic state or original write result lost on restart")
	}
	if _, err := New(Config{ID: 0, Peers: config.Peers, DataDir: dir, Machine: kv.NewStateMachine(kv.New())}); err == nil {
		t.Fatal("snapshot loaded into a different application")
	}
}

func TestInvalidCommandDoesNotMutateConsensus(t *testing.T) {
	node, err := New(Config{ID: 0, Peers: []string{"counter"}, DataDir: t.TempDir(), Machine: &counterMachine{}})
	if err != nil {
		t.Fatal(err)
	}
	ticket := node.Submit(Command{ReqID: "bad", Data: "invalid"})
	defer ticket.Release()
	if ticket.Result().Status != Rejected || node.max_seq != 0 || node.pendingCount != 0 {
		t.Fatal("invalid application command mutated consensus")
	}
	var reply AcceptReply
	if err := node.Accept(&AcceptArgs{Seq: 1, Value: Command{Noop: true, Data: "payload"}}, &reply); err == nil || node.max_seq != 0 {
		t.Fatal("invalid no-op envelope was accepted")
	}
}

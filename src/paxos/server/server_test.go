package server

import (
	"6.5840/paxos/consensus"
	"6.5840/paxos/kv"
	"testing"
	"time"
)

func TestHandlerWaitBudgetBounds(t *testing.T) {
	for _, requested := range []int64{0, -1, 50, 1<<63 - 1} {
		before := time.Now()
		ctx, cancel := handlerWaitContext(requested)
		after := time.Now()
		deadline, ok := ctx.Deadline()
		limit := serverWaitTimeout
		if requested == 50 {
			limit = 50 * time.Millisecond
		}
		if !ok || deadline.After(after.Add(limit)) || deadline.Before(before.Add(limit)) {
			t.Fatal("handler budget not bounded using local time")
		}
		cancel()
	}
}

type countedBackend struct {
	*kv.Store
	writes int
}

func (s *countedBackend) Set(key, value string) { s.writes++; s.Store.Set(key, value) }

func TestWithStorageAppliesThroughSuppliedBackend(t *testing.T) {
	backend := &countedBackend{Store: kv.New()}
	s, err := NewWithStorage(0, []string{"local"}, t.TempDir(), backend)
	if err != nil {
		t.Fatal(err)
	}
	var reply consensus.DecideReply
	cmd := kv.Encode(kv.Operation{ReqID: "write", Action: "set", Key: "k", Value: "v"})
	if err := s.Decide(&consensus.DecideArgs{Seq: 1, Instance: consensus.Instance{Op: cmd, Decided: true}}, &reply); err != nil || !reply.Result {
		t.Fatalf("apply decision: %v", err)
	}
	if backend.writes != 1 || backend.Get("k") != "v" {
		t.Fatal("server bypassed supplied KV backend")
	}
	if _, err := NewWithStorage(0, []string{"local"}, t.TempDir(), nil); err == nil {
		t.Fatal("accepted nil storage")
	}
}

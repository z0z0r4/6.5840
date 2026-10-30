package kvsrv

import (
	//"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// Test Put with a single client and a reliable network
func TestReliablePut(t *testing.T) {
	const Val = "6.5840"
	const Ver = 0

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("One client and reliable Put")

	ck := ts.MakeClerk()
	if err := ck.Put("k", Val, Ver); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v; expected OK", err)
	} else if val != Val {
		t.Fatalf("Get value err %v; expected %v", val, Val)
	} else if ver != Ver+1 {
		t.Fatalf("Get wrong version %v; expected %v", ver, Ver+1)
	}

	if err := ck.Put("k", Val, 0); err != rpc.ErrVersion {
		t.Fatalf("expected Put to fail with ErrVersion; got err=%v", err)
	}

	if err := ck.Put("y", Val, rpc.Tversion(1)); err != rpc.ErrNoKey {
		t.Fatalf("expected Put to fail with ErrNoKey; got err=%v", err)
	}

	if _, _, err := ck.Get("y"); err != rpc.ErrNoKey {
		t.Fatalf("expected Get to fail with ErrNoKey; got err=%v", err)
	}
}

// Many clients putting on same key.
func TestPutConcurrentReliable(t *testing.T) {
	const (
		PORCUPINETIME = 10 * time.Second
		NCLNT         = 10
		NSEC          = 1
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: many clients racing to put values to the same key")

	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientPut(me, ck, []string{"k"}, done)
	})
	ck := ts.MakeClerk()
	ts.CheckPutConcurrent(ck, "k", rs, &kvtest.ClntRes{}, ts.IsReliable())
	ts.CheckPorcupineT(PORCUPINETIME)
}

// Check if memory used on server is reasonable. Exactly-once Puts
// require the server to remember the last Put of every clerk, so allow
// a small, constant amount of memory per clerk.
func TestMemPutManyClientsReliable(t *testing.T) {
	const (
		NCLIENT = 20_000
		MEM     = 1000
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	v := kvtest.RandValue(MEM)

	cks := make([]kvtest.IKVClerk, NCLIENT)
	for i, _ := range cks {
		cks[i] = ts.MakeClerk()
	}

	// force allocation of ends for server in each client
	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", "", 1); err != rpc.ErrNoKey {
			t.Fatalf("Put failed %v", err)
		}
	}

	ts.Begin("Test: memory use many put clients")

	// allow threads started by labrpc to start
	time.Sleep(1 * time.Second)

	m0 := ts.Config.Group(0).MemSize()

	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", v, rpc.Tversion(i)); err != rpc.OK {
			t.Fatalf("Put failed %v", err)
		}
	}

	m1 := ts.Config.Group(0).MemSize()
	f := (float64(m1) - float64(m0)) / NCLIENT
	if m1 > m0+(NCLIENT*100) {
		t.Fatalf("error: server using too much memory %d %d (%.2f byte per client)\n", m0, m1, f)
	}
}

// Test with one client and unreliable network. Puts are exactly-once,
// so even when requests or replies are dropped, every Put must return
// OK (never ErrMaybe) and must have been executed exactly once.
func TestUnreliableNet(t *testing.T) {
	const NTRY = 100

	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("One client")

	ck := ts.MakeClerk()

	for try := 0; try < NTRY; try++ {
		if err := ts.PutJson(ck, "k", try, rpc.Tversion(try), 0); err != rpc.OK {
			t.Fatalf("Put %d returned %v; expected OK", try, err)
		}
		v := 0
		if ver := ts.GetJson(ck, "k", 0, &v); ver != rpc.Tversion(try+1) {
			t.Fatalf("Wrong version %d expect %d", ver, try+1)
		}
		if v != try {
			t.Fatalf("Wrong value %d expect %d", v, try)
		}
	}

	ts.CheckPorcupine()
}

// Many clients append to the same key over an unreliable network. With
// exactly-once Puts no append is lost or duplicated: no Put returns
// ErrMaybe, and every client's entries appear without gaps.
func TestAppendsExactlyOnceUnreliable(t *testing.T) {
	const (
		NCLNT = 5
		NSEC  = 5
	)

	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("Test: many clients appending to one key exactly once")

	ck := ts.MakeClerk()
	if err := ts.PutJson(ck, "k", []kvtest.EntryN{}, 0, -1); err != rpc.OK {
		t.Fatalf("Put initial value: %v", err)
	}

	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientAppend(me, ck, done)
	})

	for c, r := range rs {
		if r.Nmaybe != 0 {
			t.Fatalf("client %d got %d ErrMaybe; Puts should be exactly-once", c, r.Nmaybe)
		}
	}

	es := []kvtest.EntryN{}
	ver := ts.GetJson(ck, "k", -1, &es)
	ts.CheckAppends(es, NCLNT, rs, ver)
}

// The server should keep only the last Put of a clerk, not one entry per
// Put: the memory used for duplicate detection must not grow with the
// number of Puts a clerk performs.
func TestMemManyPutsOneClient(t *testing.T) {
	const NPUT = 50_000

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: memory use one client many puts")

	ck := ts.MakeClerk()
	if err := ck.Put("k", "x", 0); err != rpc.OK {
		t.Fatalf("Put failed %v", err)
	}

	m0 := ts.Config.Group(0).MemSize()

	for i := 1; i <= NPUT; i++ {
		if err := ck.Put("k", "x", rpc.Tversion(i)); err != rpc.OK {
			t.Fatalf("Put failed %v", err)
		}
	}

	m1 := ts.Config.Group(0).MemSize()
	if m1 > m0+100_000 {
		t.Fatalf("error: server memory grew from %d to %d after %d puts by one clerk", m0, m1, NPUT)
	}
}

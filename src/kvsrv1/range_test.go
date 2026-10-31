package kvsrv

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

func rangeKey(i int) string {
	return fmt.Sprintf("key%07d", i)
}

func checkRange(t *testing.T, kvs []rpc.KeyValue, want []string) {
	if len(kvs) != len(want) {
		t.Fatalf("Range returned %d keys; expected %d (%v)", len(kvs), len(want), kvs)
	}
	for i, kv := range kvs {
		if kv.Key != want[i] {
			t.Fatalf("Range key %d is %q; expected %q", i, kv.Key, want[i])
		}
	}
}

// Range over RPC returns exactly the keys in [low, high], in order,
// with their current values and versions.
func TestRangeBasic(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: Range")

	ck := ts.MakeClerk().(*kvtest.TestClerk).IKVClerk.(*Clerk)

	const N = 200
	keys := make([]string, 0, N)
	for _, i := range rand.Perm(N) {
		if err := ck.Put(rangeKey(i), fmt.Sprint(i), 0); err != rpc.OK {
			t.Fatalf("Put err %v", err)
		}
	}
	for i := 0; i < N; i++ {
		keys = append(keys, rangeKey(i))
	}
	if err := ck.Put(rangeKey(7), "seven", 1); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	kvs, err := ck.Range(rangeKey(5), rangeKey(9))
	if err != rpc.OK {
		t.Fatalf("Range err %v", err)
	}
	checkRange(t, kvs, keys[5:10])
	if kvs[2].Value != "seven" || kvs[2].Version != 2 {
		t.Fatalf("Range returned %v for %q; expected (seven, 2)", kvs[2], rangeKey(7))
	}
	if kvs[0].Value != "5" || kvs[0].Version != 1 {
		t.Fatalf("Range returned %v for %q; expected (5, 1)", kvs[0], rangeKey(5))
	}

	// bounds that aren't keys themselves
	kvs, _ = ck.Range("key0000010x", "key0000013x")
	checkRange(t, kvs, keys[11:14])

	// everything
	kvs, _ = ck.Range("", "~")
	checkRange(t, kvs, keys)

	// nothing
	kvs, _ = ck.Range("a", "b")
	checkRange(t, kvs, nil)
	kvs, _ = ck.Range(rangeKey(9), rangeKey(5))
	checkRange(t, kvs, nil)
}

// A short Range on a large store must cost time proportional to the
// size of the range, not to the size of the store. Scanning the whole
// key/value map for every Range (the lazy solution) fails this test.
func TestRangeFast(t *testing.T) {
	const (
		NKEY   = 200_000
		NRANGE = 2_000
		WIDTH  = 10
	)

	kv := MakeKVServer()
	for i := 0; i < NKEY; i++ {
		reply := rpc.PutReply{}
		kv.Put(&rpc.PutArgs{Key: rangeKey(i), Value: "v", Version: 0}, &reply)
		if reply.Err != rpc.OK {
			t.Fatalf("Put err %v", reply.Err)
		}
	}

	start := time.Now()
	for r := 0; r < NRANGE; r++ {
		lo := rand.Intn(NKEY - WIDTH)
		reply := rpc.RangeReply{}
		kv.Range(&rpc.RangeArgs{Low: rangeKey(lo), High: rangeKey(lo + WIDTH - 1)}, &reply)
		if reply.Err != rpc.OK || len(reply.KVs) != WIDTH || reply.KVs[0].Key != rangeKey(lo) {
			t.Fatalf("Range(%d) returned %v %d keys", lo, reply.Err, len(reply.KVs))
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("%d Ranges of %d keys over %d keys took %v; Range should not scan all keys", NRANGE, WIDTH, NKEY, d)
	}
}

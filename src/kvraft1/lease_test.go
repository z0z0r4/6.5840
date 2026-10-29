package kvraft

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	tester "6.5840/tester1"
)

// A 3-server kvraft with lease-based reads on or off.
func makeLeaseTest(t *testing.T, part string, lease bool) *Test {
	var extra []string
	if !lease {
		extra = []string{"--no-lease-reads"}
		part += ", no leases"
	}
	ts := makeTestArgs(t, part, 1, 3, true, false, false, -1, false, extra)
	tester.AnnotateTest(part, 3)
	return ts
}

func leaderOf(ck kvtest.IKVClerk) int {
	return ck.(*kvtest.TestClerk).IKVClerk.(*Clerk).Leader()
}

// Perform NGET Gets and return the number of RPCs and the time they took.
func leaseGets(t *testing.T, lease bool) (int, time.Duration) {
	const NGET = 300

	ts := makeLeaseTest(t, "Ext lease reads", lease)
	defer ts.Cleanup()

	ck := ts.MakeClerk()
	if err := ck.Put("k", "v", 0); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	n0 := ts.RpcTotal()
	t0 := time.Now()
	for i := 0; i < NGET; i++ {
		if v, ver, err := ck.Get("k"); err != rpc.OK || v != "v" || ver != 1 {
			t.Fatalf("Get = (%v, %v, %v); expected (v, 1, OK)", v, ver, err)
		}
	}
	return ts.RpcTotal() - n0, time.Since(t0)
}

// A leader holding a lease serves Gets from its own state, without
// sending AppendEntries for them, so Gets take fewer RPCs and less time.
func TestLeaseReadsFasterExt(t *testing.T) {
	nl, dl := leaseGets(t, true)
	n, d := leaseGets(t, false)
	t.Logf("300 Gets: with leases %d RPCs in %v; without %d RPCs in %v", nl, dl, n, d)
	if 2*nl > n {
		t.Fatalf("Gets with leases took %d RPCs; expected less than half of the %d without", nl, n)
	}
	if dl > d {
		t.Fatalf("Gets with leases took %v; expected less than the %v without", dl, d)
	}
}

// Kill the leader NROUND times and measure how long the next Put takes
// to complete under a new leader.
func termSwitches(t *testing.T, lease bool) []time.Duration {
	const NROUND = 4

	ts := makeLeaseTest(t, "Ext lease term switch", lease)
	defer ts.Cleanup()

	ck := ts.MakeClerk()
	ver := rpc.Tversion(0)
	if err := ck.Put("k", "v", ver); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}
	ver++

	ds := make([]time.Duration, 0, NROUND)
	for r := 0; r < NROUND; r++ {
		l := leaderOf(ck)
		ts.Group(Gid).ShutdownServer(l)

		t0 := time.Now()
		if err := ck.Put("k", "v", ver); err != rpc.OK {
			t.Fatalf("Put err %v", err)
		}
		ds = append(ds, time.Since(t0))
		ver++

		ts.Group(Gid).StartServer(l)
		ts.Group(Gid).ConnectOne(l)
		time.Sleep(kvtest.ElectionTimeout)
	}
	return ds
}

func mean(ds []time.Duration) time.Duration {
	sum := time.Duration(0)
	for _, d := range ds {
		sum += d
	}
	return sum / time.Duration(len(ds))
}

// A new leader must wait until the old leader's lease has expired
// before it commits anything, so a term switch makes the service
// unavailable for at least LeaseDuration longer than without leases.
func TestLeaseTermSwitchSlowerExt(t *testing.T) {
	dl := termSwitches(t, true)
	d := termSwitches(t, false)
	t.Logf("first Put after killing the leader: with leases %v (mean %v); without %v (mean %v)", dl, mean(dl), d, mean(d))
	for _, x := range dl {
		if x < LeaseDuration {
			t.Fatalf("a Put completed %v after the leader died; a new leader must wait at least the lease duration %v", x, LeaseDuration)
		}
	}
	if mean(dl) < mean(d)+LeaseDuration/4 {
		t.Fatalf("term switches with leases (mean %v) should be slower than without (mean %v)", mean(dl), mean(d))
	}
}

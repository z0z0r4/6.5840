package shardkv

import (
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

func txnClerk(ck kvtest.IKVClerk) *Clerk {
	return ck.(*kvtest.TestClerk).IKVClerk.(*Clerk)
}

// Keys "t0", "t1", ... are spread over all shards.
func txnKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "t" + strconv.Itoa(i)
	}
	return ks
}

// Start Gid1 plus n more groups, so that keys are spread over groups.
func (ts *Test) setupGroups(n int) []tester.Tgid {
	gids := []tester.Tgid{ts.setupKVService()}
	for i := 0; i < n; i++ {
		gid := ts.newGid()
		if ok := ts.joinGroups(ts.sck, []tester.Tgid{gid}); !ok {
			ts.Fatalf("join %d failed", gid)
		}
		gids = append(gids, gid)
	}
	return gids
}

func TestTxnBasicExt(t *testing.T) {
	ts := MakeTest(t, "Test (Ext): cross-shard transactions basic ...", true)
	defer ts.Cleanup()
	ts.setupGroups(2)

	ck := ts.MakeClerk()
	tck := txnClerk(ck)
	ks := txnKeys(shardcfg.NShards) // one key per shard, so several groups

	// write every key in one transaction
	tx := tck.Begin()
	for i, k := range ks {
		tx.Put(k, strconv.Itoa(i))
	}
	if err := tx.Commit(); err != rpc.OK {
		ts.Fatalf("Commit err %v", err)
	}
	for i, k := range ks {
		ts.CheckGet(ck, k, strconv.Itoa(i), 1)
	}

	// read every key, write them back incremented
	tx = tck.Begin()
	for i, k := range ks {
		v, ver, err := tx.Get(k)
		if err != rpc.OK || v != strconv.Itoa(i) || ver != 1 {
			ts.Fatalf("tx.Get(%v) = (%v, %v, %v)", k, v, ver, err)
		}
		tx.Put(k, strconv.Itoa(i+1))
	}
	if v, _, _ := tx.Get(ks[0]); v != "1" {
		ts.Fatalf("tx.Get doesn't see its own write: %v", v)
	}
	if err := tx.Commit(); err != rpc.OK {
		ts.Fatalf("Commit err %v", err)
	}
	for i, k := range ks {
		ts.CheckGet(ck, k, strconv.Itoa(i+1), 2)
	}

	// an aborted transaction changes nothing
	tx = tck.Begin()
	tx.Put(ks[0], "x")
	tx.Put(ks[1], "y")
	tx.Abort()
	ts.CheckGet(ck, ks[0], "1", 2)
	ts.CheckGet(ck, ks[1], "2", 2)

	// tx1 holds a read lock on k, so tx2 can't write k
	k := ks[3]
	tx1 := tck.Begin()
	if _, _, err := tx1.Get(k); err != rpc.OK {
		ts.Fatalf("tx1.Get err %v", err)
	}
	tx2 := txnClerk(ts.MakeClerk()).Begin()
	tx2.Put(k, "tx2")
	if err := tx2.Commit(); err != rpc.ErrTxnAborted {
		ts.Fatalf("tx2 wrote a key read-locked by tx1: %v", err)
	}

	// a plain Put of k waits until tx1 finishes
	ck2 := ts.MakeClerk()
	putDone := make(chan rpc.Err)
	go func() { putDone <- ck2.Put(k, "plain", 2) }()
	select {
	case err := <-putDone:
		ts.Fatalf("plain Put finished (%v) while tx1 holds a read lock", err)
	case <-time.After(500 * time.Millisecond):
	}
	tx1.Put(ks[4], "tx1")
	if err := tx1.Commit(); err != rpc.OK {
		ts.Fatalf("tx1 Commit err %v", err)
	}
	if err := <-putDone; err != rpc.OK {
		ts.Fatalf("plain Put err %v", err)
	}
	ts.CheckGet(ck, k, "plain", 3)
	ts.CheckGet(ck, ks[4], "tx1", 3)

	// tx1 and tx2 each read a key the other one writes: they can't both
	// commit (no-wait aborts at least one).
	tx1 = tck.Begin()
	tx2 = txnClerk(ts.MakeClerk()).Begin()
	tx1.Get(ks[5])
	tx2.Get(ks[6])
	tx1.Put(ks[6], "1")
	tx2.Put(ks[5], "2")
	e1, e2 := tx1.Commit(), tx2.Commit()
	if e1 == rpc.OK && e2 == rpc.OK {
		ts.Fatalf("both conflicting transactions committed")
	}
}

// Transfers between accounts spread over shards in several groups, with
// RunTxn; an auditor reads all accounts in one transaction and the total
// must never change. If reconfig, a controller keeps moving shards by
// joining and removing a group at the same time, so locks and prepared
// transactions must move with their shards.
func txnBank(t *testing.T, part string, reliable, reconfig bool) {
	const (
		NACCT   = 2 * shardcfg.NShards
		INITIAL = 100
		NCLNT   = 4
		NSEC    = 5
	)
	ts := MakeTest(t, part, reliable)
	defer ts.Cleanup()
	ts.setupGroups(2)

	accts := txnKeys(NACCT)
	ck := txnClerk(ts.MakeClerk())
	if err := ck.RunTxn(func(tx *Txn) rpc.Err {
		for _, a := range accts {
			tx.Put(a, strconv.Itoa(INITIAL))
		}
		return rpc.OK
	}); err != rpc.OK {
		ts.Fatalf("init err %v", err)
	}

	total := func(tx *Txn) (int, rpc.Err) {
		sum := 0
		for _, a := range accts {
			v, _, err := tx.Get(a)
			if err != rpc.OK {
				return 0, err
			}
			n, _ := strconv.Atoi(v)
			sum += n
		}
		return sum, rpc.OK
	}

	var transfers, audits atomic.Int32
	done := make(chan struct{})
	finished := make(chan bool)

	for c := 0; c < NCLNT; c++ {
		go func(c int) {
			ck := txnClerk(ts.MakeClerk())
			for {
				select {
				case <-done:
					finished <- true
					return
				default:
				}
				if c == 0 {
					var sum int
					ck.RunTxn(func(tx *Txn) rpc.Err {
						var err rpc.Err
						sum, err = total(tx)
						return err
					})
					if sum != NACCT*INITIAL {
						ts.Fatalf("audit: total %d; expected %d", sum, NACCT*INITIAL)
					}
					audits.Add(1)
					continue
				}
				a, b := rand.Intn(NACCT), rand.Intn(NACCT-1)
				if b >= a {
					b++
				}
				x := rand.Intn(10)
				ck.RunTxn(func(tx *Txn) rpc.Err {
					va, _, err := tx.Get(accts[a])
					if err != rpc.OK {
						return err
					}
					vb, _, err := tx.Get(accts[b])
					if err != rpc.OK {
						return err
					}
					na, _ := strconv.Atoi(va)
					nb, _ := strconv.Atoi(vb)
					tx.Put(accts[a], strconv.Itoa(na-x))
					tx.Put(accts[b], strconv.Itoa(nb+x))
					return rpc.OK
				})
				transfers.Add(1)
			}
		}(c)
	}

	if reconfig {
		sck := ts.makeShardCtrler()
		end := time.Now().Add(NSEC * time.Second)
		for time.Now().Before(end) {
			gid := ts.newGid()
			ts.joinGroups(sck, []tester.Tgid{gid})
			ts.leaveGroups(sck, []tester.Tgid{gid})
		}
	} else {
		time.Sleep(NSEC * time.Second)
	}
	close(done)
	for c := 0; c < NCLNT; c++ {
		<-finished
	}

	if transfers.Load() == 0 || audits.Load() == 0 {
		ts.Fatalf("no progress: %d transfers, %d audits", transfers.Load(), audits.Load())
	}
	sum := 0
	for _, a := range accts {
		v, _, err := ck.Get(a)
		if err != rpc.OK {
			ts.Fatalf("Get(%v) err %v", a, err)
		}
		n, _ := strconv.Atoi(v)
		sum += n
	}
	if sum != NACCT*INITIAL {
		ts.Fatalf("final total %d; expected %d", sum, NACCT*INITIAL)
	}
	t.Logf("%d transfers, %d audits", transfers.Load(), audits.Load())
}

func TestTxnBankExt(t *testing.T) {
	txnBank(t, "Test (Ext): cross-shard transactions bank ...", true, false)
}

func TestTxnBankUnreliableExt(t *testing.T) {
	txnBank(t, "Test (Ext): cross-shard transactions bank ...", false, false)
}

func TestTxnBankReconfigExt(t *testing.T) {
	txnBank(t, "Test (Ext): cross-shard transactions bank with reconfiguration ...", true, true)
}

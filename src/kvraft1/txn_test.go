package kvraft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	tester "6.5840/tester1"
)

func clerkOf(ck kvtest.IKVClerk) *Clerk {
	return ck.(*kvtest.TestClerk).IKVClerk.(*Clerk)
}

// Comparisons pick the branch; operations see each other's effects;
// results report values and versions.
func TestTxnBasicExt(t *testing.T) {
	ts := MakeTest(t, "Ext transactions basic", 1, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnBasicExt", 3)
	defer ts.Cleanup()

	ck := clerkOf(ts.MakeClerk())

	// a missing key has version 0 and value ""
	ok, res := ck.Txn([]Cmp{VersionIs("a", EQ, 0), ValueIs("b", EQ, "")},
		[]TxnOp{TxnPut("a", "1"), TxnPut("b", "2"), TxnGet("a")},
		[]TxnOp{TxnGet("a")})
	if !ok || len(res) != 3 {
		t.Fatalf("Txn = %v %v; expected Then branch with 3 results", ok, res)
	}
	if res[0].Version != 1 || res[1].Version != 1 || res[2] != (OpResult{Value: "1", Version: 1, Err: rpc.OK}) {
		t.Fatalf("Then results %v", res)
	}

	// a failed comparison runs Else and changes nothing
	ok, res = ck.Txn([]Cmp{VersionIs("a", EQ, 1), ValueIs("b", NE, "2")},
		[]TxnOp{TxnPut("a", "x")},
		[]TxnOp{TxnGet("a"), TxnGet("b"), TxnGet("c")})
	if ok || len(res) != 3 {
		t.Fatalf("Txn = %v %v; expected Else branch with 3 results", ok, res)
	}
	if res[0].Value != "1" || res[1].Value != "2" || res[2].Err != rpc.ErrNoKey {
		t.Fatalf("Else results %v", res)
	}

	// LT / GT
	ok, _ = ck.Txn([]Cmp{VersionIs("a", GT, 0), VersionIs("a", LT, 2), ValueIs("b", GT, "1")}, nil, nil)
	if !ok {
		t.Fatalf("LT/GT comparisons failed")
	}

	// Puts inside a transaction bump the version
	ck.Txn(nil, []TxnOp{TxnPut("a", "3"), TxnPut("a", "4")}, nil)
	if v, ver, err := ck.Get("a"); err != rpc.OK || v != "4" || ver != 3 {
		t.Fatalf("Get(a) = (%v, %v, %v); expected (4, 3, OK)", v, ver, err)
	}

	// versioned Puts and transactions see the same versions
	if err := ck.Put("a", "5", 3); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}
	if ok, _ := ck.Txn([]Cmp{VersionIs("a", EQ, 4)}, nil, nil); !ok {
		t.Fatalf("Txn didn't see version 4")
	}
}

// Bank transfers: clients move money between accounts with read-modify-
// write transactions guarded by version comparisons, while auditors read
// all accounts in one transaction. The total must never change, over an
// unreliable, partitioned network.
func TestTxnBankExt(t *testing.T) {
	const (
		NACCT   = 6
		INITIAL = 100
		NCLNT   = 5
		NSEC    = 5
	)
	ts := MakeTest(t, "Ext transactions bank", NCLNT, 5, false, false, true, -1, false)
	tester.AnnotateTest("TestTxnBankExt", 5)
	defer ts.Cleanup()

	accts := make([]string, NACCT)
	readAll := make([]TxnOp, NACCT)
	init := make([]TxnOp, NACCT)
	for i := range accts {
		accts[i] = "acct" + strconv.Itoa(i)
		readAll[i] = TxnGet(accts[i])
		init[i] = TxnPut(accts[i], strconv.Itoa(INITIAL))
	}
	ck := clerkOf(ts.MakeClerk())
	ck.Txn(nil, init, nil)

	audit := func(ck *Clerk) {
		_, res := ck.Txn(nil, readAll, nil)
		sum := 0
		for _, r := range res {
			n, _ := strconv.Atoi(r.Value)
			sum += n
		}
		if sum != NACCT*INITIAL {
			ts.Fatalf("audit: total is %d; expected %d (%v)", sum, NACCT*INITIAL, res)
		}
	}

	ch := make(chan bool)
	go ts.Partitioner(Gid, ch)
	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ick kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		ck := clerkOf(ick)
		res := kvtest.ClntRes{}
		for {
			select {
			case <-done:
				return res
			default:
			}
			if me == 0 {
				audit(ck)
				res.Nok++
				continue
			}
			a, b := rand.Intn(NACCT), rand.Intn(NACCT-1)
			if b >= a {
				b++
			}
			_, cur := ck.Txn(nil, []TxnOp{TxnGet(accts[a]), TxnGet(accts[b])}, nil)
			na, _ := strconv.Atoi(cur[0].Value)
			nb, _ := strconv.Atoi(cur[1].Value)
			x := rand.Intn(10)
			ok, _ := ck.Txn(
				[]Cmp{VersionIs(accts[a], EQ, cur[0].Version), VersionIs(accts[b], EQ, cur[1].Version)},
				[]TxnOp{TxnPut(accts[a], strconv.Itoa(na-x)), TxnPut(accts[b], strconv.Itoa(nb+x))},
				nil)
			if ok {
				res.Nok++
			} else {
				res.Nmaybe++ // lost the race; retry with fresh balances
			}
		}
	})

	ch <- true
	<-ch
	ts.Group(Gid).ConnectAll()
	audit(ck)
	transfers := 0
	for _, r := range rs[1:] {
		transfers += r.Nok
	}
	if transfers == 0 || rs[0].Nok == 0 {
		t.Fatalf("no progress: %d transfers, %d audits", transfers, rs[0].Nok)
	}
}

// A clerk whose versioned Put is a transaction: compare the version,
// then put; otherwise report why not. Transactions subsume versioned
// Puts.
type txnPutClerk struct {
	*Clerk
}

func (ck txnPutClerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	ok, res := ck.Txn([]Cmp{VersionIs(key, EQ, version)}, []TxnOp{TxnPut(key, value)}, []TxnOp{TxnGet(key)})
	if ok {
		return rpc.OK
	}
	if res[0].Err == rpc.ErrNoKey {
		return rpc.ErrNoKey
	}
	return rpc.ErrVersion
}

type txnPutClerkMaker struct {
	ts *Test
}

func (m txnPutClerkMaker) MakeClerk() kvtest.IKVClerk {
	clnt := m.ts.Config.MakeClient()
	ck := txnPutClerk{MakeClerk(clnt, m.ts.Group(Gid).SrvNames()).(*Clerk)}
	return &kvtest.TestClerk{ck, clnt, m.ts.Config}
}

func (m txnPutClerkMaker) DeleteClerk(ck kvtest.IKVClerk) {
	m.ts.DeleteClient(ck.(*kvtest.TestClerk).Clnt)
}

// Run the concurrent versioned-Put workload of Lab 4 with Puts
// implemented as transactions, and check linearizability.
func TestTxnVersionedPutExt(t *testing.T) {
	const NCLNT = 5
	ts := MakeTest(t, "Ext versioned Put as a transaction", NCLNT, 5, false, false, true, -1, false)
	tester.AnnotateTest("TestTxnVersionedPutExt", 5)
	defer ts.Cleanup()

	tt := kvtest.MakeTest(t, ts.Config, false, txnPutClerkMaker{ts})
	ck := tt.MakeClerk()

	ch := make(chan bool)
	go ts.Partitioner(Gid, ch)
	rs := tt.SpawnClientsAndWait(NCLNT, 3*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return tt.OneClientPut(me, ck, []string{"k"}, done)
	})
	ch <- true
	<-ch
	ts.Group(Gid).ConnectAll()

	res := kvtest.ClntRes{}
	tt.CheckPutConcurrent(ck, "k", rs, &res, true) // exactly-once: every OK Put happened
	if res.Nmaybe != 0 {
		t.Fatalf("%d Puts returned ErrMaybe", res.Nmaybe)
	}
	tt.CheckPorcupine()
}

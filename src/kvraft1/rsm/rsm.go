package rsm

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Me  int
	Id  uuid.UUID
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	pending     map[uuid.UUID]chan any
	lastApplied int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[uuid.UUID]chan any),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.listenApplyCh()
	go rsm.watchSnapshot()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) listenApplyCh() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			// Apply under rsm.mu, so that watchSnapshot sees the state
			// and lastApplied of the same moment.
			rsm.mu.Lock()
			rep := rsm.sm.DoOp(op.Req)
			ch, ok := rsm.pending[op.Id]
			if ok {
				ch <- rep
			} else {
				// println("pending not found", op.Id.String())
			}

			rsm.lastApplied = max(rsm.lastApplied, msg.CommandIndex)
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			rsm.applySnapshot(msg.Snapshot, msg.SnapshotIndex)
		}
	}
}

func (rsm *RSM) applySnapshot(snapshot []byte, index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	rsm.sm.Restore(snapshot)
	rsm.lastApplied = max(rsm.lastApplied, index)
}

func (rsm *RSM) watchSnapshot() {
	for {
		time.Sleep(100 * time.Millisecond)

		if rsm.maxraftstate == -1 {
			continue
		}

		if rsm.rf.PersistBytes() > rsm.maxraftstate {
			rsm.mu.Lock()
			snapshot := rsm.sm.Snapshot()
			lastApplied := rsm.lastApplied
			rsm.mu.Unlock()

			rsm.rf.Snapshot(lastApplied, snapshot)
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	op := Op{
		Me:  rsm.me,
		Id:  uuid.New(),
		Req: req,
	}

	// Register before Start: the entry may commit and be applied before
	// Start even returns, and listenApplyCh drops the result of an op
	// nobody is waiting for.
	ch := make(chan any, 1)
	rsm.mu.Lock()
	rsm.pending[op.Id] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.pending, op.Id)
		rsm.mu.Unlock()
	}()

	_, term, isLeader := rsm.Raft().Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	for {
		select {
		case rep := <-ch:
			// println("submited op", op.Id.String())
			return rpc.OK, rep
		case <-time.After(500 * time.Millisecond):
			stateTerm, stateIsLeader := rsm.rf.GetState()
			// If leader changed, return ErrWrongLeader to client so it can try another server.
			// ! As figure 8, if the leader has become a follower, term changed, then it become leader again
			// ! All not commited log with old term will wait one current term log to commit
			// ! Without NOP, if the test has no new log to commit, this submit will wait forever
			// ! So we need to check both term and isLeader by 'and' but not 'or'. Once term changed will trigger failed
			if stateTerm != term || !stateIsLeader {
				// println("submited op", op.Id.String(), "failed")
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

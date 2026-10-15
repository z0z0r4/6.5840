package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	log []LogEntry

	state int

	lastHeartbeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	println("Server", rf.me, "received RequestVote RPC from server", args.CandidateId, "for term", args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// Become follower
		rf.state = Follower
		println("Server", rf.me, "became follower for term, meet larger term", rf.currentTerm)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if len(rf.log) == 0 || (args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) {
			// Up to date, grant vote
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			return
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

type RequestVoteResult struct {
	Term        int
	VoteGranted bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		// println("Server", rf.me, "ticker running")
		rf.mu.Lock()
		if rf.state != Leader && rf.lastHeartbeat.Add(time.Duration(300+rand.Intn(300))*time.Millisecond).Before(time.Now()) {
			// Be candidate, start election
			rf.state = Candidate
			rf.currentTerm++

			// Vote for self
			rf.votedFor = rf.me
			rf.lastHeartbeat = time.Now()

			vote_lock := sync.Mutex{}
			votes := 1

			election_term := rf.currentTerm
			last_log_index := len(rf.log) - 1
			last_log_term := -1
			if len(rf.log) > 0 {
				last_log_term = rf.log[len(rf.log)-1].Term
			}
			rf.mu.Unlock()

			// Boardcast RequestVote RPCs to all other servers
			// ismarjor, maxTerm := rf.boardcastRequestVote()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				args := &RequestVoteArgs{}
				args.CandidateId = rf.me
				args.Term = election_term
				args.LastLogIndex = last_log_index
				args.LastLogTerm = last_log_term

				reply := &RequestVoteReply{}

				go func(server int, args *RequestVoteArgs) {
					ok := rf.sendRequestVote(server, args, reply)
					if !ok {
						println("Server", rf.me, "failed to send RequestVote RPC to server", server)
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						// Become follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						println("Server", rf.me, "became follower for term, meet larger term", rf.currentTerm)
					}

					if reply.Term != election_term || reply.Term != rf.currentTerm || rf.state != Candidate {
						return
					}

					if reply.VoteGranted {
						vote_lock.Lock()

						votes++

						if votes > len(rf.peers)/2 {
							// Become leader
							rf.state = Leader
							println("Server", rf.me, "became leader for term", rf.currentTerm)
						} else {
							println("Server", rf.me, "received vote from server", server, "for term", election_term, "total votes:", votes)
						}
						vote_lock.Unlock()
					} else {
						println("Server", rf.me, "was rejected from server", server, "for term", election_term)
					}
				}(i, args)
			}
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type LogEntry struct {
	Term    int
	Command interface{}
}

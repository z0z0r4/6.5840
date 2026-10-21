package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	state int

	votes int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat time.Time
	// Randomized once per election, re-drawing it on every tick would make it close to the minimum
	electionTimeout time.Duration

	lastIncludeIndex int
	lastIncludeTerm  int
	snapshot         []byte

	applyCh     chan raftapi.ApplyMsg
	applierCond *sync.Cond

	// A snapshot installed from the leader that applier still has to deliver.
	pendingSnapshot bool
}

// Logical index -> physical index in rf.log
func (rf *Raft) logIdx(index int) int {
	return index - rf.lastIncludeIndex
}

// Return logical log length, including any snapshot entries.
func (rf *Raft) logEndLocked() int {
	return len(rf.log) + rf.lastIncludeIndex
}

// Get the log entry by logical index
func (rf *Raft) getLogLocked(index int) LogEntry {
	if index == rf.lastIncludeIndex {
		// Dummy log entry
		return LogEntry{Term: rf.lastIncludeTerm}
	}
	if index <= rf.lastIncludeIndex || index >= rf.logEndLocked() {
		println("Server", rf.me, "getLogLocked: index out of bounds, lastIncludeIndex:", rf.lastIncludeIndex, "logEnd:", rf.logEndLocked(), "requested index:", index)
		panic("getLogLocked: index out of bounds")
	}
	return rf.log[rf.logIdx(index)]
}

// Trim the log, removing entries from 1 to index (inclusive, [1, index]).
// This is used when a snapshot is created and the log can be truncated.
func (rf *Raft) trimLogLocked(index int) {
	if index <= rf.lastIncludeIndex || index >= rf.logEndLocked() {
		panic("trimLogLocked: index out of bounds")
	}

	keepFrom := index - rf.lastIncludeIndex + 1
	newLog := make([]LogEntry, 0, len(rf.log)-keepFrom+1)
	newLog = append(newLog, rf.log[0])
	newLog = append(newLog, rf.log[keepFrom:]...)

	rf.log = newLog
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
func (rf *Raft) persistLocked() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		println("Server", rf.me, "failed to read persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		println("Server", rf.me, "read persisted state: term", rf.currentTerm, "votedFor", rf.votedFor, "log length", rf.logEndLocked())
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) BecomeLeaderLocked() {
	rf.state = Leader
	println("Server", rf.me, "became leader for term", rf.currentTerm)

	// Initialize nextIndex and matchIndex for each follower
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.logEndLocked()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) resetElectionTimeoutLocked() {
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) BecomeFollowerLocked(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persistLocked()
	println("Server", rf.me, "became follower for term", rf.currentTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		// Snapshot is older than the last included index, ignore
		return
	}

	if index > rf.logEndLocked()-1 {
		println("Server", rf.me, "received Snapshot for index", index, "but log length is only", rf.logEndLocked())
		return
	}

	lastIncludeTerm := rf.getLogLocked(index).Term

	rf.trimLogLocked(index)
	rf.snapshot = snapshot
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = lastIncludeTerm

	println("Server", rf.me, "created snapshot up to index", index, "term", rf.lastIncludeTerm, "log length after trim", rf.logEndLocked())

	rf.persistLocked()
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.BecomeFollowerLocked(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// Reset heartbeat timer
	rf.lastHeartbeat = time.Now()

	// Stale or duplicate snapshot, already covered by what we have
	if args.LastIncludedIndex <= rf.lastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// Update snapshot
	rf.snapshot = args.Data

	// Keep the following entries only if our log agrees with the snapshot,
	// otherwise discard the whole log.
	if rf.logEndLocked()-1 >= args.LastIncludedIndex && rf.getLogLocked(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.trimLogLocked(args.LastIncludedIndex)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	// Update lastIncludeIndex and lastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm

	// If the service has already applied past the snapshot, delivering it would roll its state back
	deliver := args.LastIncludedIndex > rf.lastApplied

	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	rf.persistLocked()

	println("Server", rf.me, "installed snapshot from leader", args.LeaderId, "up to index", rf.lastIncludeIndex, "term", rf.lastIncludeTerm, "log length after trim", rf.logEndLocked())

	// Let applier deliver it, so that it can't be reordered with applied entries
	if deliver {
		rf.pendingSnapshot = true
		rf.applierCond.Signal()
	}

	rf.mu.Unlock()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// println("Server", rf.me, "received RequestVote RPC from server", args.CandidateId, "for term", args.Term)
	if args.Term > rf.currentTerm {
		rf.BecomeFollowerLocked(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// index 0 is dummy log entry
		last_index := rf.logEndLocked() - 1
		last_term := rf.getLogLocked(last_index).Term
		upToDate := false
		// First check term, if equal, then check log index and term
		if args.LastLogTerm > last_term {
			upToDate = true
		} else if args.LastLogTerm == last_term && args.LastLogIndex >= last_index {
			upToDate = true
		}
		if upToDate {
			// Up to date, grant vote
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persistLocked()

			rf.lastHeartbeat = time.Now()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) addLogLocked(entires []LogEntry, startIndex int) {
	for i, entry := range entires {
		index := startIndex + i

		if index < rf.logEndLocked() {
			if rf.getLogLocked(index).Term != entry.Term {
				// Conflict, delete the existing entry and all that follow it
				rf.log = rf.log[:index-rf.lastIncludeIndex]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persistLocked()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	if rf.snapshot != nil {
		msg := raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.lastIncludeTerm,
			SnapshotIndex: rf.lastIncludeIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
	} else {
		rf.mu.Unlock()
	}

	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.pendingSnapshot {
			rf.applierCond.Wait()
		}

		if rf.pendingSnapshot {
			rf.pendingSnapshot = false
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludeTerm,
				SnapshotIndex: rf.lastIncludeIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		entries := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.getLogLocked(rf.lastApplied)
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			entries = append(entries, applyMsg)
		}
		rf.mu.Unlock()

		for _, applyMsg := range entries {
			rf.applyCh <- applyMsg
		}
		println("Server", rf.me, "applied", len(entries), "log entries")
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// Update term and become follower
		rf.BecomeFollowerLocked(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		println("Server", rf.me, "rejected AppendEntries RPC from leader", args.LeaderId, "for term", args.Term, "due to stale term")
		return
	}

	// Reset heartbeat timer
	rf.lastHeartbeat = time.Now()

	// Check prev log index and term
	if args.PrevLogIndex >= rf.logEndLocked() || (args.PrevLogIndex >= 0 && rf.getLogLocked(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm

		// Find out the conflicting term and its first index to help the leader optimize its nextIndex
		if args.PrevLogIndex >= rf.logEndLocked() {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.logEndLocked()
		} else {
			conflictTerm := rf.getLogLocked(args.PrevLogIndex).Term
			conflictIndex := args.PrevLogIndex
			for conflictIndex > rf.lastIncludeIndex && rf.getLogLocked(conflictIndex-1).Term == conflictTerm {
				conflictIndex--
			}
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = conflictIndex
		}
		println("Server", rf.me, "rejected AppendEntries RPC from leader", args.LeaderId, "for term", args.Term, "due to log inconsistency at PrevLogIndex", args.PrevLogIndex)
		return
	}

	// reset heartbeat timer
	rf.lastHeartbeat = time.Now()

	if len(args.Entries) > 0 {
		rf.addLogLocked(args.Entries, args.PrevLogIndex+1)
		println("Server", rf.me, "added", len(args.Entries), "log entries [", args.PrevLogIndex+1, "-", args.PrevLogIndex+len(args.Entries), "] from leader", args.LeaderId, "for term", args.Term)
	} else {
		// println("Server", rf.me, "received heartbeat from leader", args.LeaderId, "for term", args.Term)
	}

	// Update commit index
	// println("Server", rf.me, "received AppendEntries RPC from leader", args.LeaderId, "for term", args.Term, "with LeaderCommit", args.LeaderCommit, "current commitIndex", rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logEndLocked()-1)
		// println("Server", rf.me, "updating commitIndex to", rf.commitIndex)
		rf.applierCond.Signal()
	} else {
		// println("Server", rf.me, "not updating commitIndex, LeaderCommit", args.LeaderCommit, "current commitIndex", rf.commitIndex)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesToServer(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		// We may have stepped down since this was scheduled, don't send entries under a newer term
		rf.mu.Unlock()
		return
	}
	next_index := rf.nextIndex[server]
	if next_index <= rf.lastIncludeIndex {
		// Need to send InstallSnapshot RPC instead of AppendEntries
		go rf.sendInstallSnapshotToServer(server)
		rf.mu.Unlock()
		return
	}

	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = next_index - 1
	args.PrevLogTerm = rf.getLogLocked(args.PrevLogIndex).Term
	// Copy: the RPC encodes args after we release the lock, while rf.log may be modified
	args.Entries = append([]LogEntry(nil), rf.log[next_index-rf.lastIncludeIndex:]...)
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	var reply AppendEntriesReply

	// println("Server", rf.me, "sending AppendEntries RPC to server", server, "with PrevLogIndex", args.PrevLogIndex, "PrevLogTerm", args.PrevLogTerm, "entries length", len(args.Entries), "LeaderCommit", args.LeaderCommit)
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		// println("Server", rf.me, "failed to send AppendEntries RPC to server", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerLocked(reply.Term)
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			println("Server", rf.me, "updated matchIndex for server", server, "to", rf.matchIndex[server])
		}

		// Check if the log entry has been committed by a majority of servers
		for n := rf.logEndLocked() - 1; n > rf.commitIndex; n-- {
			// !Consider §5.4.2 Figure 8, old term entries should not be committed by new term leaders,
			// They can only be committed when the leader commits an entry behind them in the current
			if rf.getLogLocked(n).Term != rf.currentTerm {
				break
			}

			// If the log entry is from the current term, consider it for commitment
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.applierCond.Signal()
				break
			}
		}

		// update next_index
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		// println("Server", rf.me, "updated nextIndex for server", server, "to", rf.nextIndex[server])
	} else {
		// Decrease next_index and retry
		if reply.ConflictTerm == -1 {
			// If the conflict term is -1, it means the prev log entry doesn't exist
			// Which means follower's log is too short,
			// so we will send missing entries starting from the follower's log length
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// Find the last index of the conflicting term in the log
			conflictIndex := -1
			for i := rf.logEndLocked() - 1; i >= rf.lastIncludeIndex; i-- {
				if rf.getLogLocked(i).Term == reply.ConflictTerm {
					// Found the last index of the conflicting term
					conflictIndex = i
					break
				} else if rf.getLogLocked(i).Term < reply.ConflictTerm {
					// No entry with the conflicting term exists in leader's log,
					// so we can set nextIndex to the first entry with the conflicting term
					break
				}
			}
			if conflictIndex != -1 {
				// Set nextIndex to the index after the last entry of the conflicting
				rf.nextIndex[server] = conflictIndex + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
		println("Server", rf.me, "decremented nextIndex for server", server, "to", rf.nextIndex[server])
	}
}

func (rf *Raft) sendInstallSnapshotToServer(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludeIndex,
		LastIncludedTerm:  rf.lastIncludeTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		println("Server", rf.me, "failed to send InstallSnapshot RPC to server", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerLocked(reply.Term)
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	println("Server", rf.me, "updated nextIndex and matchIndex for server", server, "after InstallSnapshot RPC to", rf.nextIndex[server], rf.matchIndex[server])
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
	// Your code here (3B).
	rf.mu.Lock()

	log_term := rf.currentTerm
	index := rf.logEndLocked()

	// println("Server", rf.me, "received Start command for term", log_term, "at index", index)

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, log_term, false
	}

	logEntry := LogEntry{
		Term:    log_term,
		Command: command,
	}

	// Add Log to local log
	rf.log = append(rf.log, logEntry)
	if rf.matchIndex[rf.me] < index {
		rf.matchIndex[rf.me] = index
	}
	rf.persistLocked()
	// println("Server", rf.me, "add log entry at index", index, "for term", log_term)

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntriesToServer(i)
	}

	return index, log_term, true
}

func (rf *Raft) sendRequestVoteToServer(server int, candidateId int, term int, lastLogIndex int, lastLogTerm int) {
	rf.mu.Lock()
	if rf.state != Candidate || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	args := &RequestVoteArgs{}
	args.CandidateId = candidateId
	args.Term = term
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	reply := &RequestVoteReply{}

	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		// println("Server", rf.me, "failed to send RequestVote RPC to server", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerLocked(reply.Term)
		return
	}

	if reply.Term != rf.currentTerm || rf.state != Candidate {
		return
	}

	if reply.VoteGranted {
		rf.votes++

		if rf.votes > len(rf.peers)/2 {
			// Become leader
			rf.BecomeLeaderLocked()
		} else {
			println("Server", rf.me, "received vote from server", server, "for term", args.Term, "total votes:", rf.votes)
		}
	} else {
		println("Server", rf.me, "was rejected from server", server, "for term", args.Term)
	}
}

func (rf *Raft) electionLocked() {
	// Be candidate, start election
	rf.state = Candidate
	rf.currentTerm++

	// Vote for self
	rf.votedFor = rf.me
	rf.persistLocked()
	rf.votes = 1

	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeoutLocked()

	candidateId := rf.me
	term := rf.currentTerm
	lastLogIndex := rf.logEndLocked() - 1
	lastLogTerm := rf.getLogLocked(lastLogIndex).Term

	// Boardcast RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sendRequestVoteToServer(i, candidateId, term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) boardcastHeartbeatLocked() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntriesToServer(i)
	}
}

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			rf.electionLocked()
		}
		if rf.state == Leader && time.Since(rf.lastHeartbeat) > 100*time.Millisecond {
			rf.boardcastHeartbeatLocked()
			rf.lastHeartbeat = time.Now()
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) restore() {
	if rf.persister.RaftStateSize() != 0 {
		rf.readPersist(rf.persister.ReadRaftState())

		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex

		println("Server", rf.me, "restored state: term", rf.currentTerm, "votedFor", rf.votedFor, "log length", rf.logEndLocked(), "lastIncludeIndex", rf.lastIncludeIndex, "lastIncludeTerm", rf.lastIncludeTerm)
	}

	if rf.persister.SnapshotSize() != 0 {
		rf.snapshot = rf.persister.ReadSnapshot()

		println("Server", rf.me, "restored snapshot: lastIncludeIndex", rf.lastIncludeIndex, "lastIncludeTerm", rf.lastIncludeTerm)
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
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeoutLocked()
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.snapshot = nil

	rf.applierCond = sync.NewCond(&rf.mu)

	// Add dummy log entry at index 0, so that the first real log entry is at index 1
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.restore()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

type LogEntry struct {
	Term    int
	Command interface{}
}

package paxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.5840/paxos/types"
)

type Instance struct {
	ProposalAcceptNumber types.ProposalID
	Op                   OP
	Decided              bool
	ReadValue            string
}

type Server struct {
	own_number int
	peers      []string
	majority   int
	statePath  string

	insts         map[int]Instance
	kv            map[string]string
	requests      map[string]RequestRecord
	checkpoint    Checkpoint
	pinnedReads   map[int]bool
	pendingWrites map[string]*pendingProposal
	pendingCount  int

	phase                  leaderPhase
	catchingUp             bool
	proposalSlot           chan struct{}
	leader                 int
	leaderProposalNumber   types.ProposalID
	promisedProposalNumber types.ProposalID

	heartbeatTicker   *time.Ticker
	lastHeartbeatTime time.Time

	leaseGrantUntil time.Time
	readLeaseUntil  time.Time

	max_seq   int
	applied   int
	round_num uint64

	mu sync.Mutex
}

// Try to applyLocked all decided OP in order from applied+1 until the first undecided OP
func (s *Server) applyLocked() {
	defer s.compactLocked()
	for {
		next_seq_apply := s.applied + 1

		inst, exists := s.insts[next_seq_apply]
		if !exists || !inst.Decided {
			break
		}

		if inst.Op.ReqID != "" {
			if _, exists := s.requests[inst.Op.ReqID]; exists {
				s.applied++
				continue
			}
			s.requests[inst.Op.ReqID] = RequestRecord{Op: inst.Op, Seq: next_seq_apply}
		}
		switch inst.Op.Action {
		case "set":
			s.kv[inst.Op.Key] = inst.Op.Value
		case "get":
			inst.ReadValue = s.kv[inst.Op.Key]
			s.insts[next_seq_apply] = inst
		case "nop":
			// do nothing
		default:
			fmt.Println("Unknown action:", inst.Op.Action)
		}

		s.applied++
	}
}

func (s *Server) driveProposal(op OP) proposalResult {
	s.mu.Lock()
	if !s.readyLocked() {
		result := proposalResult{status: proposalRedirect, leader: s.leader}
		s.mu.Unlock()
		return result
	}

	// 检查重复请求
	if old, exists := s.requests[op.ReqID]; op.ReqID != "" && exists {
		status := proposalApplied
		if old.Op != op {
			status = proposalConflict
		}
		s.mu.Unlock()
		return proposalResult{status: status}
	}

	// The lease only permits local reads; otherwise use the normal proposal.
	if op.Action == "get" && time.Now().Before(s.readLeaseUntil) {
		result := proposalResult{status: proposalApplied, value: s.kv[op.Key]}
		s.mu.Unlock()
		return result
	}

	proposalNumber, seq := s.leaderProposalNumber, s.max_seq+1
	if op.Action == "get" {
		s.pinnedReads[seq] = true
	}

	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if op.Action == "get" {
			delete(s.pinnedReads, seq)
			if seq <= s.checkpoint.Seq {
				delete(s.insts, seq)
			}
		}
	}()

	for {
		s.mu.Lock()
		// 释放锁后重新检查
		active := s.readyLocked() && s.inLeaderProposalNumberLocked(proposalNumber)
		leader := s.leader
		s.mu.Unlock()

		if !active {
			return proposalResult{status: proposalRedirect, leader: leader}
		}

		if s.broadcastAccept(seq, proposalNumber, op) >= s.majority {
			s.announceDecision(seq, Instance{ProposalAcceptNumber: proposalNumber, Op: op, Decided: true})

			s.mu.Lock()
			result := s.proposedResultLocked(seq, op)
			s.mu.Unlock()

			return result
		}

		// Retry the same slot and value until chosen or leadership changes.
		time.Sleep(retryInterval)
	}
}

// Recheck heartbeat expiration after acquiring the proposal slot.
func (s *Server) electLeader() {
	s.proposalSlot <- struct{}{}
	defer func() { <-s.proposalSlot }()

	s.mu.Lock()
	if s.readyLocked() || time.Since(s.lastHeartbeatTime) < heartbeatTimeout {
		s.mu.Unlock()
		return
	}
	s.catchupRoundNumLocked(s.promisedProposalNumber)
	n := s.makeProposalNumberLocked()
	// Failed Prepare/recovery must not keep receiving leader heartbeats forever.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.readyLocked() {
			s.invalidateLeaderLocked(n, true)
		}
	}()
	applied := s.applied
	s.mustPersistLocked("round")
	s.mu.Unlock()

	// Prepare phase
	count, values, highest, checkpoint := s.broadcastPrepare(n, applied)
	if count < s.majority {
		s.mu.Lock()
		s.catchupRoundNumLocked(highest)
		s.mu.Unlock()
		return
	}

	// Recover phase
	s.mu.Lock()
	if !s.inLeaderProposalNumberLocked(n) {
		s.mu.Unlock()
		return
	}
	s.phase = leaderRecovering
	s.installCheckpointLocked(checkpoint)
	s.mustPersistLocked("election checkpoint")
	applied = s.applied
	end := s.max_seq
	s.mu.Unlock()

	for seq := range values {
		if seq > end {
			end = seq
		}
	}
	// Recover only the suffix after this candidate's applied prefix.
	for seq := applied + 1; seq <= end; seq++ {
		s.mu.Lock()
		if s.recoveryInterruptedLocked(n) {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		op := OP{Action: "nop"}
		if old, exists := values[seq]; exists {
			op = old.Value
		}
		accepted := s.broadcastAccept(seq, n, op)
		if accepted < s.majority {
			return
		}
		s.broadcastDecide(seq, Instance{ProposalAcceptNumber: n, Op: op, Decided: true})
	}

	s.mu.Lock()
	if s.inLeaderProposalNumberLocked(n) {
		s.phase = leaderReady
		s.lastHeartbeatTime = time.Now()
	}
	s.mu.Unlock()
}

func (s *Server) Set(args *ClientSetRequest, reply *ClientSetReply) error {
	if args.ReqID == "" {
		return fmt.Errorf("write requires ReqID")
	}
	ctx, cancel := handlerWaitContext(args.TimeoutMs)
	defer cancel()
	result := s.waitForProposal(ctx, OP{ReqID: args.ReqID, Action: "set", Key: args.Key, Value: args.Value})
	reply.Status = result.status
	reply.Result = result.status == proposalApplied
	reply.LeaderAddress = s.redirectAddress(result)
	return nil
}

func (s *Server) Get(args *ClientGetRequest, reply *ClientGetResponse) error {
	ctx, cancel := handlerWaitContext(args.TimeoutMs)
	defer cancel()
	result := s.waitForProposal(ctx, OP{Action: "get", Key: args.Key})
	reply.Status = result.status
	reply.Result = result.status == proposalApplied
	reply.Value = result.value
	reply.LeaderAddress = s.redirectAddress(result)
	return nil
}

func Serve(me int, peers []string) {
	gob.Register("")

	dataDir := os.Getenv("PAXOS_DATA_DIR")
	if dataDir == "" {
		dataDir = "paxos-data"
	}
	s, err := newServer(me, peers, dataDir)
	if err != nil {
		log.Fatal(err)
	}

	if err := rpc.Register(s); err != nil {
		log.Fatal(err)
	}
	l, err := net.Listen("tcp", peers[me])
	if err != nil {
		log.Fatal(err)
	}

	// Start the heartbeat ticker
	s.StartHeartbeat()

	// Start the health check ticker
	s.StartHealthCheck()

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

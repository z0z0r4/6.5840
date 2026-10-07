package paxos

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type PaxosInstance struct {
	NP        int
	NA        int
	Op        OP
	Decided   bool
	ReadValue string
}

type Server struct {
	own_number int
	peers      []string
	majority   int

	insts map[int]PaxosInstance
	kv    map[string]string

	max_seq   int
	applied   int
	round_num int
	statePath string

	mu   sync.Mutex
	cond sync.Cond
}

func requestContext(deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	if deadlineUnixNano == 0 {
		return context.Background(), func() {}
	}
	return context.WithDeadline(context.Background(), time.Unix(0, deadlineUnixNano))
}

// noteSeq records the largest instance number this server has heard about.
// The caller must hold s.mu.
func (s *Server) noteSeq(seq int) {
	if seq > s.max_seq {
		s.max_seq = seq
	}
}

// The caller must hold s.mu.
func (s *Server) makeProposalNumberLocked() int {
	round := s.round_num
	s.round_num++
	return round*len(s.peers) + s.own_number
}

// Try to applyLocked all decided OP in order from applied+1 until the first undecided OP
func (s *Server) applyLocked() {
	for {
		next_seq_apply := s.applied + 1

		inst, exists := s.insts[next_seq_apply]
		if !exists || !inst.Decided {
			break
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
		s.cond.Broadcast()

		fmt.Println("Server", s.own_number, "applied op", inst.Op, "seq:", next_seq_apply)
	}
}

func (s *Server) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noteSeq(args.Seq)

	reply.Seq = args.Seq

	if _, exists := s.insts[args.Seq]; !exists {
		// create a new instance for the given instance number
		s.insts[args.Seq] = PaxosInstance{NP: -1, NA: -1, Op: OP{}}
	}

	if args.ProposalNumber > s.insts[args.Seq].NP {
		// set np
		inst := s.insts[args.Seq]
		inst.NP = args.ProposalNumber
		s.insts[args.Seq] = inst
		if err := s.persistLocked(); err != nil {
			log.Fatalf("persist Prepare: %v", err)
		}

		reply.Prepared = true
		reply.AcceptedProposalNumber = s.insts[args.Seq].NA
		reply.Value = s.insts[args.Seq].Op
	}

	fmt.Println("Server", s.own_number, "received Prepare for seq", args.Seq, "proposal number", args.ProposalNumber, "prepared:", reply.Prepared)

	return nil
}

type PrepareResult struct {
	peer     string
	prepared bool
	reply    PrepareReply
}

func (s *Server) broadcastPrepare(seq int, proposalNumber int) (int, int, int, bool, OP) {
	ch := make(chan PrepareResult, len(s.peers))

	// the proposer is also an acceptor: run the local handler directly
	var self PrepareReply
	s.Prepare(&PrepareArgs{Seq: seq, ProposalNumber: proposalNumber}, &self)
	ch <- PrepareResult{peer: s.peers[s.own_number], prepared: self.Prepared, reply: self}

	for _, peer := range s.peers {
		if peer == s.peers[s.own_number] {
			continue
		}
		go func(peer string) {
			var reply PrepareReply
			ok := call(peer, "Server.Prepare", &PrepareArgs{Seq: seq, ProposalNumber: proposalNumber}, &reply)
			ch <- PrepareResult{peer: peer, prepared: ok && reply.Prepared, reply: reply}
		}(peer)
	}

	prepared_count := 0
	network_error_count := 0
	reject_count := 0
	responses := 0

	highest_accepted_proposal_number := -1
	var chosen_op OP = OP{}
	has_chosen_op := false

	for responses < len(s.peers) {
		result := <-ch
		responses++
		if result.prepared {
			prepared_count++

			// pick value
			if result.reply.AcceptedProposalNumber > highest_accepted_proposal_number {
				highest_accepted_proposal_number = result.reply.AcceptedProposalNumber
				chosen_op = result.reply.Value
				has_chosen_op = true
			}
		} else {
			if result.reply.Prepared {
				network_error_count++
			} else {
				reject_count++
			}
		}
		if prepared_count >= s.majority || prepared_count+(len(s.peers)-responses) < s.majority {
			break
		}
	}

	return prepared_count, network_error_count, reject_count, has_chosen_op, chosen_op
}

func (s *Server) Accept(args *AcceptArgs, reply *AcceptReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noteSeq(args.Seq)

	reply.Seq = args.Seq

	if _, exists := s.insts[args.Seq]; !exists {
		// create a new instance for the given instance number
		s.insts[args.Seq] = PaxosInstance{NP: -1, NA: -1, Op: OP{}}
	}

	inst := s.insts[args.Seq]

	if inst.Decided && inst.Op != args.Value {
		return nil
	}
	if args.ProposalNumber >= inst.NP {
		inst.NP = args.ProposalNumber
		inst.NA = args.ProposalNumber
		inst.Op = args.Value
		s.insts[args.Seq] = inst
		if err := s.persistLocked(); err != nil {
			log.Fatalf("persist Accept: %v", err)
		}

		reply.Accepted = true
	} else {
		reply.Accepted = false
	}

	fmt.Println("Server", s.own_number, "received Accept for seq", args.Seq, "proposal number", args.ProposalNumber, "accepted:", reply.Accepted)

	return nil
}

type AcceptResult struct {
	peer     string
	ok       bool
	accepted bool
}

func (s *Server) broadcastAccept(seq int, proposalNumber int, value OP) (int, int, int) {
	ch := make(chan AcceptResult, len(s.peers))

	var self_reply AcceptReply
	err := s.Accept(&AcceptArgs{Seq: seq, ProposalNumber: proposalNumber, Value: value}, &self_reply)
	if err == nil {
		ch <- AcceptResult{peer: s.peers[s.own_number], ok: true, accepted: self_reply.Accepted}
	} else {
		ch <- AcceptResult{peer: s.peers[s.own_number], ok: false, accepted: false}
	}

	for _, peer := range s.peers {
		if peer == s.peers[s.own_number] {
			continue
		}
		go func(peer string) {
			var reply AcceptReply
			ok := call(peer, "Server.Accept", &AcceptArgs{Seq: seq, ProposalNumber: proposalNumber, Value: value}, &reply)
			if ok {
				ch <- AcceptResult{peer: peer, ok: true, accepted: reply.Accepted}
			} else {
				ch <- AcceptResult{peer: peer, ok: false, accepted: false}
			}
		}(peer)
	}

	accepted_count := 0
	network_error_count := 0
	reject_count := 0
	responses := 0
	for responses < len(s.peers) {
		result := <-ch
		responses++
		if result.ok {
			if result.accepted {
				accepted_count++
			} else {
				reject_count++
			}
		} else {
			network_error_count++
		}
		if accepted_count >= s.majority || accepted_count+(len(s.peers)-responses) < s.majority {
			break
		}
	}

	return accepted_count, network_error_count, reject_count
}

func (s *Server) Decide(args *DecideArgs, reply *DecideReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seq := args.Seq
	s.noteSeq(seq)

	if inst, exists := s.insts[seq]; exists && inst.Decided {
		if inst.Op != args.Instance.Op {
			return fmt.Errorf("instance %d already decided a different operation", seq)
		}
		reply.Result = true
		return nil
	}

	np := args.Instance.NP
	if current, exists := s.insts[seq]; exists && current.NP > np {
		np = current.NP
	}
	s.insts[seq] = PaxosInstance{
		NP:      np,
		NA:      args.Instance.NA,
		Op:      args.Instance.Op,
		Decided: true,
	}

	fmt.Println("Server", s.own_number, "decided op", args.Instance.Op, "seq:", seq)

	s.applyLocked()
	if err := s.persistLocked(); err != nil {
		log.Fatalf("persist Decide: %v", err)
	}
	reply.Result = true
	return nil
}

func (s *Server) broadcastDecide(seq int, instance PaxosInstance) int {
	ch := make(chan bool, len(s.peers))

	var self_reply DecideReply
	err := s.Decide(&DecideArgs{Seq: seq, Instance: instance}, &self_reply)
	if err == nil {
		ch <- true
	} else {
		ch <- false
	}

	for _, peer := range s.peers {
		if peer == s.peers[s.own_number] {
			continue
		}
		go func(peer string) {
			var reply DecideReply
			ok := call(peer, "Server.Decide", &DecideArgs{Seq: seq, Instance: instance}, &reply)
			ch <- ok && reply.Result
		}(peer)
	}

	success_count := 0
	responses := 0
	for responses < len(s.peers) {
		result := <-ch
		responses++
		if result {
			success_count++
		}
		if success_count >= s.majority || success_count+(len(s.peers)-responses) < s.majority {
			break
		}
	}

	return success_count
}

// Decide one log slot, adopting any previously accepted value.
func (s *Server) proposeAt(seq int, op OP) OP {
	chosen, _ := s.proposeAtContext(context.Background(), seq, op)
	return chosen
}

func (s *Server) proposeAtContext(ctx context.Context, seq int, op OP) (OP, bool) {
	for {
		if err := ctx.Err(); err != nil {
			return OP{}, false
		}
		s.mu.Lock()
		if inst, exists := s.insts[seq]; exists && inst.Decided {
			s.mu.Unlock()
			return inst.Op, true
		}
		s.mu.Unlock()

		// stage 1: prepare
		s.mu.Lock()
		n := s.makeProposalNumberLocked()
		if err := s.persistLocked(); err != nil {
			log.Fatalf("persist proposal number: %v", err)
		}
		s.mu.Unlock()
		prepared_count, _, _, has_chosen_op, chosen_op := s.broadcastPrepare(seq, n)
		if err := ctx.Err(); err != nil {
			return OP{}, false
		}

		if prepared_count < s.majority {
			continue
		}
		if !has_chosen_op {
			chosen_op = op
		}

		// stage 2: accept
		accepted_count, _, _ := s.broadcastAccept(seq, n, chosen_op)
		if err := ctx.Err(); err != nil {
			return OP{}, false
		}

		if accepted_count >= s.majority {
			inst := PaxosInstance{NP: n, NA: n, Op: chosen_op, Decided: true}
			s.broadcastDecide(seq, inst)
			return chosen_op, true
		}
	}
}

// Fill in any missing decided instances up to the given sequence number by proposing nop
func (s *Server) waitAppliedContext(ctx context.Context, seq int) bool {
	for {
		if err := ctx.Err(); err != nil {
			return false
		}
		s.mu.Lock()
		if s.applied >= seq {
			s.mu.Unlock()
			return true
		}

		timedOut := false
		timer := time.AfterFunc(100*time.Millisecond, func() {
			s.mu.Lock()
			timedOut = true
			s.cond.Broadcast()
			s.mu.Unlock()
		})
		for s.applied < seq && !timedOut {
			if err := ctx.Err(); err != nil {
				break
			}
			s.cond.Wait()
		}
		timer.Stop()
		if s.applied >= seq {
			s.mu.Unlock()
			return true
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return false
		}

		missing_seq := s.applied + 1
		inst, exists := s.insts[missing_seq]
		s.mu.Unlock()

		if !exists || !inst.Decided {
			if _, ok := s.proposeAtContext(ctx, missing_seq, OP{Action: "nop"}); !ok {
				return false
			}
		}
	}
}

// Try to propose a given value, return whether the proposal was successful and the sequence number
func (s *Server) propose(op OP) (bool, int) {
	return s.proposeContext(context.Background(), op)
}

func (s *Server) proposeContext(ctx context.Context, op OP) (bool, int) {
	retry_times_limit := 5
	retry_times := 0

	seq := 0

	for retry_times < retry_times_limit {
		if err := ctx.Err(); err != nil {
			return false, seq
		}
		s.mu.Lock()
		// seq move forward only here
		// if the proposal failed (not decided and applied), we will find a empty slot
		seq = s.max_seq + 1
		s.mu.Unlock()

		chosen_op, ok := s.proposeAtContext(ctx, seq, op)
		if !ok {
			return false, seq
		}

		if chosen_op == op {
			if !s.waitAppliedContext(ctx, seq) {
				return false, seq
			}
			break
		}
		retry_times++
	}

	return retry_times < retry_times_limit, seq
}

func (s *Server) Set(args *ClientSetRequest, reply *ClientSetReply) error {
	ctx, cancel := requestContext(args.DeadlineUnixNano)
	defer cancel()
	ok, seq := s.proposeContext(ctx, OP{Action: "set", Key: args.Key, Value: args.Value})

	if !ok {
		reply.Result = false
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inst, exists := s.insts[seq]

	if !exists {
		reply.Result = false
		return nil
	}

	reply.Result = inst.Decided && s.applied >= seq

	return nil
}

func (s *Server) Get(args *ClientGetRequest, reply *ClientGetResponse) error {
	ctx, cancel := requestContext(args.DeadlineUnixNano)
	defer cancel()
	ok, seq := s.proposeContext(ctx, OP{Action: "get", Key: args.Key})

	if !ok {
		reply.Result = false
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inst, exists := s.insts[seq]

	if !exists {
		reply.Result = false
		return nil
	}

	reply.Result = inst.Decided && s.applied >= seq

	if reply.Result {
		reply.Value = inst.ReadValue
	}

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
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

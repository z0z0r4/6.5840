package consensus

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"6.5840/paxos/api"
	"6.5840/paxos/kv"
	"6.5840/paxos/transport"
)

type OP = Command
type Server struct{ *Node }
type ClientSetRequest = api.ClientSetRequest
type ClientSetReply = api.ClientSetReply
type ClientGetRequest = api.ClientGetRequest
type ClientGetResponse = api.ClientGetResponse

func newRequestID() string            { return uuid.NewString() }
func command(op kv.Operation) Command { return kv.Encode(op) }
func decodeCommand(cmd Command) kv.Operation {
	op, err := kv.Decode(cmd)
	if err != nil {
		panic(err)
	}
	return op
}

func values(s *Server) map[string]string {
	store := kv.New()
	if err := store.Restore(s.machine.Snapshot()); err != nil {
		panic(err)
	}
	return store.Values()
}

func checkpointValues(c Checkpoint) map[string]string {
	store := kv.New()
	if err := store.Restore(c.State); err != nil {
		panic(err)
	}
	return store.Values()
}

func newServer(me int, peers []string, dir string) (*Server, error) {
	node, err := New(Config{ID: me, Peers: peers, DataDir: dir, Machine: kv.NewStateMachine(kv.New())})
	if err != nil {
		return nil, err
	}
	return &Server{node}, nil
}

func (s *Server) waitForProposal(ctx context.Context, op Command) Result {
	if ctx.Err() != nil {
		return Result{Status: Expired}
	}
	ticket := s.Submit(op)
	defer ticket.Release()
	select {
	case <-ctx.Done():
		return Result{Status: Expired}
	case <-ticket.Done():
		return ticket.Result()
	}
}

func (s *Server) Set(args *ClientSetRequest, reply *ClientSetReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(max(int64(1), args.TimeoutMs))*time.Millisecond)
	if args.TimeoutMs == 0 {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	}
	defer cancel()
	result := s.waitForProposal(ctx, command(kv.Operation{ReqID: args.ReqID, Action: "set", Key: args.Key, Value: args.Value}))
	reply.Status = result.Status
	reply.Result = result.Status == Applied
	return nil
}

func (s *Server) Get(args *ClientGetRequest, reply *ClientGetResponse) error {
	result := s.waitForProposal(context.Background(), command(kv.Operation{Action: "get", Key: args.Key}))
	reply.Status = result.Status
	reply.Result = result.Status == Applied
	reply.Value = result.Value
	return nil
}

var callWithContext = transport.CallWithContext

type TestRPCServer struct {
	*Server
	rejectAccept atomic.Bool
}

func (s *TestRPCServer) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if s.rejectAccept.Load() {
		reply.Seq = args.Seq
		reply.Accepted = false
		return nil
	}
	return s.Server.Accept(args, reply)
}

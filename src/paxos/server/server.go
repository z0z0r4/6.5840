// Package server adapts the KV service to a generic Paxos node.
package server

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

	"6.5840/paxos/api"
	"6.5840/paxos/consensus"
	"6.5840/paxos/kv"
	"6.5840/paxos/statemachine"
)

const serverWaitTimeout = 10 * time.Second

// Server exposes the KV RPCs alongside the peer protocol. The state machine is
// supplied to Node at construction; it is never accessed outside the node lock.
type Server struct{ *consensus.Node }

// New constructs a KV server with the default in-memory backend.
func New(me int, peers []string, dataDir string) (*Server, error) {
	return NewWithStorage(me, peers, dataDir, kv.New())
}

// NewWithStorage supplies the KV backend independently of consensus.
// The server takes exclusive ownership of storage after construction.
func NewWithStorage(me int, peers []string, dataDir string, storage kv.Storage) (*Server, error) {
	if storage == nil {
		return nil, fmt.Errorf("KV storage is required")
	}
	config := consensus.Config{ID: me, Peers: peers, DataDir: dataDir, Machine: kv.NewStateMachine(storage)}
	if storage.Kind() == kv.New().Kind() {
		config.SnapshotUpgrade = upgradeKVSnapshot
	}
	node, err := consensus.New(config)
	if err != nil {
		return nil, err
	}
	return &Server{Node: node}, nil
}

func handlerWaitContext(timeoutMs int64) (context.Context, context.CancelFunc) {
	timeout := serverWaitTimeout
	if timeoutMs > 0 && timeoutMs < serverWaitTimeout.Milliseconds() {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (s *Server) wait(ctx context.Context, cmd statemachine.Command) consensus.Result {
	if ctx.Err() != nil {
		return consensus.Result{Status: consensus.Expired}
	}
	ticket := s.Node.Submit(cmd)
	defer ticket.Release()
	select {
	case <-ctx.Done():
		return consensus.Result{Status: consensus.Expired}
	case <-ticket.Done():
		return ticket.Result()
	}
}

func (s *Server) Set(args *api.ClientSetRequest, reply *api.ClientSetReply) error {
	if args.ReqID == "" {
		return fmt.Errorf("write requires ReqID")
	}
	ctx, cancel := handlerWaitContext(args.TimeoutMs)
	defer cancel()
	result := s.wait(ctx, kv.Encode(kv.Operation{ReqID: args.ReqID, Action: "set", Key: args.Key, Value: args.Value}))
	reply.Status = result.Status
	reply.Result = result.Status == consensus.Applied
	reply.Error = result.Error
	reply.LeaderAddress = s.Node.RedirectAddress(result)
	return nil
}

func (s *Server) Get(args *api.ClientGetRequest, reply *api.ClientGetResponse) error {
	ctx, cancel := handlerWaitContext(args.TimeoutMs)
	defer cancel()
	result := s.wait(ctx, kv.Encode(kv.Operation{Action: "get", Key: args.Key}))
	reply.Status = result.Status
	reply.Result = result.Status == consensus.Applied
	reply.Value = result.Value
	reply.Error = result.Error
	reply.LeaderAddress = s.Node.RedirectAddress(result)
	return nil
}

func Serve(me int, peers []string) {
	gob.Register("")
	dataDir := os.Getenv("PAXOS_DATA_DIR")
	if dataDir == "" {
		dataDir = "paxos-data"
	}
	s, err := New(me, peers, dataDir)
	if err != nil {
		log.Fatal(err)
	}
	registry := rpc.NewServer()
	if err := registry.RegisterName("Server", s); err != nil {
		log.Fatal(err)
	}
	l, err := net.Listen("tcp", peers[me])
	if err != nil {
		log.Fatal(err)
	}
	s.StartHeartbeat()
	s.StartHealthCheck()
	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go registry.ServeConn(conn)
	}
}

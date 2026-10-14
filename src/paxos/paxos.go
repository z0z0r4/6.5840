// Package paxos retains the public entry points of the KV service.
// Implementations live in client, server, consensus and kv.
package paxos

import (
	"6.5840/paxos/api"
	"6.5840/paxos/client"
	"6.5840/paxos/server"
)

type Client = client.Client
type Server = server.Server
type ClientSetRequest = api.ClientSetRequest
type ClientSetReply = api.ClientSetReply
type ClientGetRequest = api.ClientGetRequest
type ClientGetResponse = api.ClientGetResponse

var ErrRequestConflict = client.ErrRequestConflict

func Serve(me int, peers []string) { server.Serve(me, peers) }

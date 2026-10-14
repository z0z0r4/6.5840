package main

import (
	"os"
	"strconv"

	"6.5840/paxos/server"
)

func main() {
	me, _ := strconv.Atoi(os.Args[1])
	peers := os.Args[2:]
	server.Serve(me, peers)
}

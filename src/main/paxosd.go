package main

import (
	"os"
	"strconv"

	"6.5840/paxos"
)

func main() {
	me, _ := strconv.Atoi(os.Args[1])
	peers := os.Args[2:]
	paxos.Serve(me, peers)
}

package main

import (
	"os"

	"6.5840/paxos"
)

func main() {
	target_server := os.Args[1]
	// connect to target_server and send a request
	client := &paxos.Client{}
	client.Init(target_server)

	result := client.Send_set("paxos", "hello")
	if result {
		println("Set value: hello")
	} else {
		println("Set failed")
	}

	result, value := client.Send_get("paxos")
	if result {
		println("Get value:", value)
	} else {
		println("Get failed")
	}
}

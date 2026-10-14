package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"unicode"

	paxosclient "6.5840/paxos/client"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Printf("Usage: %s <server1> <server2> <server3> [server ...]\n", os.Args[0])
		fmt.Println("Server count must be odd and at least 3.")
		printHelp()
		return
	}
	serverCount := len(os.Args) - 1
	if serverCount < 3 || serverCount%2 == 0 {
		fmt.Fprintf(os.Stderr, "Server count must be odd and at least 3; got %d.\n", serverCount)
		fmt.Fprintf(os.Stderr, "Usage: %s <server1> <server2> <server3> [server ...]\n", os.Args[0])
		os.Exit(1)
	}

	client := &paxosclient.Client{}
	client.InitPeers(os.Args[1:])
	fmt.Printf("Servers: %s\n", strings.Join(os.Args[1:], ", "))
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for {
		fmt.Print("paxos> ")
		if !scanner.Scan() {
			fmt.Println()
			break
		}
		command, args := nextWord(strings.TrimSpace(scanner.Text()))
		switch strings.ToLower(command) {
		case "":
			continue
		case "set":
			key, value := nextWord(args)
			if key == "" || value == "" {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			if client.Send_set(key, value) {
				fmt.Println("OK")
			} else {
				fmt.Println("Set failed")
			}
		case "get":
			key, extra := nextWord(args)
			if key == "" || extra != "" {
				fmt.Println("Usage: get <key>")
				continue
			}
			if ok, value := client.Send_get(key); ok {
				fmt.Printf("%s\n", value)
			} else {
				fmt.Println("Get failed")
			}
		case "help":
			if args != "" {
				fmt.Println("Usage: help")
				continue
			}
			printHelp()
		case "quit", "exit":
			if args != "" {
				fmt.Printf("Usage: %s\n", strings.ToLower(command))
				continue
			}
			return
		default:
			fmt.Printf("Unknown command %q. Type help for commands.\n", command)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Read input: %v\n", err)
		os.Exit(1)
	}
}

// nextWord splits off a command or key, leaving spaces within the value intact.
func nextWord(line string) (string, string) {
	line = strings.TrimLeftFunc(line, unicode.IsSpace)
	if i := strings.IndexFunc(line, unicode.IsSpace); i >= 0 {
		return line[:i], strings.TrimLeftFunc(line[i:], unicode.IsSpace)
	}
	return line, ""
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  set <key> <value>  Store a value; values may contain spaces (no quotes needed)")
	fmt.Println("  get <key>          Read a value")
	fmt.Println("  help               Show commands")
	fmt.Println("  quit / exit        Exit (EOF also exits)")
}

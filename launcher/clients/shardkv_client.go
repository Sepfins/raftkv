package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"raftkv/kvutil"
	"raftkv/shardkv"
	"strings"
)

func main() {
	ctrlPeers := flag.String("ctrl-peers", "", "comma‑sep controller addresses")
	flag.Parse()
	if *ctrlPeers == "" {
		fmt.Fprintln(os.Stderr, "must set --ctrl-peers")
		os.Exit(1)
	}

	ctrlAddrs := strings.Split(*ctrlPeers, ",")
	ctrlEnds := make([]*kvutil.ClientEnd, len(ctrlAddrs))
	for i, addr := range ctrlAddrs {
		ctrlEnds[i] = &kvutil.ClientEnd{Address: addr}
	}

	makeEnd := func(serverAddr string) *kvutil.ClientEnd {
		return &kvutil.ClientEnd{Address: serverAddr}
	}

	clerk := shardkv.MakeClerk(ctrlEnds, makeEnd)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter commands (Put key value | Get key | Append key suffix). Ctrl+C to exit.")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break // EOF or error
		}
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		op := strings.ToLower(fields[0])
		switch op {
		case "put":
			if len(fields) != 3 {
				fmt.Println("usage: Put key value")
				continue
			}
			clerk.Put(fields[1], fields[2])
			fmt.Println("OK")

		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: Get key")
				continue
			}
			value := clerk.Get(fields[1])
			fmt.Printf("Value: %s\n", value)

		case "append":
			if len(fields) != 3 {
				fmt.Println("usage: Append key suffix")
				continue
			}
			clerk.Append(fields[1], fields[2])
			fmt.Println("OK")

		default:
			fmt.Println("unknown command; supported: Put, Get, Append")
		}
	}
}

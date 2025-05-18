package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"raftkv/kvutil"
	"raftkv/shardctrler"
	"strconv"
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

	clerk := shardctrler.MakeClerk(ctrlEnds)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter commands:")
	fmt.Println("  Join gid:addr1,addr2 ...")
	fmt.Println("  Leave gid1,gid2,...")
	fmt.Println("  Move shard gid")
	fmt.Println("  Query [num]")
	fmt.Println("Ctrl+C to exit.")
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
		case "join":
			if len(fields) < 2 {
				fmt.Println("usage: Join gid:addr1,addr2 ...")
				continue
			}
			servers := map[int][]string{}
			for _, part := range fields[1:] {
				parts := strings.SplitN(part, ":", 2)
				if len(parts) != 2 {
					fmt.Println("Invalid Join format; expected gid:addr1,addr2")
					continue
				}
				gid, err := strconv.Atoi(parts[0])
				if err != nil {
					fmt.Println("Invalid GID:", parts[0])
					continue
				}
				servers[gid] = strings.Split(parts[1], ",")
			}
			clerk.Join(servers)
			fmt.Println("OK")

		case "leave":
			if len(fields) != 2 {
				fmt.Println("usage: Leave gid1,gid2,...")
				continue
			}
			gidStrs := strings.Split(fields[1], ",")
			gids := make([]int, 0, len(gidStrs))
			for _, s := range gidStrs {
				gid, err := strconv.Atoi(s)
				if err != nil {
					fmt.Println("Invalid GID:", s)
					continue
				}
				gids = append(gids, gid)
			}
			clerk.Leave(gids)
			fmt.Println("OK")

		case "move":
			if len(fields) != 3 {
				fmt.Println("usage: Move shard gid")
				continue
			}
			shard, err1 := strconv.Atoi(fields[1])
			gid, err2 := strconv.Atoi(fields[2])
			if err1 != nil || err2 != nil {
				fmt.Println("Invalid shard or gid")
				continue
			}
			clerk.Move(shard, gid)
			fmt.Println("OK")

		case "query":
			num := -1
			if len(fields) == 2 {
				var err error
				num, err = strconv.Atoi(fields[1])
				if err != nil {
					fmt.Println("Invalid number")
					continue
				}
			}
			config := clerk.Query(num)
			fmt.Printf("Config #%d: %+v\n", config.Num, config)

		default:
			fmt.Println("Unknown command; supported: Join, Leave, Move, Query")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Input error:", err)
	}
}

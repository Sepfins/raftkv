package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"raftkv/kvutil"
	"raftkv/raft"
	"strings"
)

func main() {
	me := flag.Int("me", 0, "this server’s index")
	peersStr := flag.String("peers", "", "comma‑sep peer addrs")
	dataDir := flag.String("data", "data", "persister dir")
	flag.Parse()

	addrs := strings.Split(*peersStr, ",")
	peers := make([]*kvutil.ClientEnd, len(addrs))
	for i, addr := range addrs {
		peers[i] = &kvutil.ClientEnd{Address: addr}
	}

	ps := raft.NewPersister(*dataDir, fmt.Sprintf("raft-%d", *me))
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(peers, *me, ps, applyCh)

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(rf); err != nil {
		log.Fatalf("Failed to register Raft RPC server: %v", err)
	}

	listenAddr := addrs[*me]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}
	log.Printf("Raft node %d listening on %s", *me, listenAddr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err == nil {
				go rpcServer.ServeConn(conn)
			} else {
				log.Printf("Accept error: %v", err)
			}
		}
	}()

	select {}
}

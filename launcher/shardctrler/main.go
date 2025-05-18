package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"raftkv/kvutil"
	"raftkv/raft"
	"raftkv/shardctrler"
	"strings"
)

func main() {
	me := flag.Int("me", 0, "controller index")
	peersStr := flag.String("raft-peers", "", "comma‑sep raft peers")
	dataDir := flag.String("data", "data", "persister dir")
	flag.Parse()

	addrs := strings.Split(*peersStr, ",")
	peers := make([]*kvutil.ClientEnd, len(addrs))
	for i, addr := range addrs {
		peers[i] = &kvutil.ClientEnd{Address: addr}
	}

	ps := raft.NewPersister(*dataDir, fmt.Sprintf("ctrl-%d", *me))
	ctrl := shardctrler.StartServer(peers, *me, ps)

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(ctrl); err != nil {
		log.Fatalf("Failed to register Shardctrler RPC server: %v", err)
	}
	if err := rpcServer.Register(ctrl.GetRaft()); err != nil {
		log.Fatalf("Failed to register Raft RPC server: %v", err)
	}

	listenAddr := addrs[*me]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}
	log.Printf("Shardctrler node %d listening on %s", *me, listenAddr)

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

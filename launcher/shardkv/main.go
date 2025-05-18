package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"raftkv/kvutil"
	"raftkv/raft"
	"raftkv/shardkv"
	"strings"
)

func main() {
	gid := flag.Int("gid", 0, "group ID")
	me := flag.Int("me", 0, "server index")
	raftStr := flag.String("raft-peers", "", "comma‑sep raft peers")
	ctrlStr := flag.String("ctrl-peers", "", "comma‑sep controller peers")
	dataDir := flag.String("data", "data", "persister dir")
	maxRS := flag.Int("maxraftstate", 1000, "snapshot threshold")
	flag.Parse()

	raAddrs := strings.Split(*raftStr, ",")
	raftEnds := make([]*kvutil.ClientEnd, len(raAddrs))
	for i, addr := range raAddrs {
		raftEnds[i] = &kvutil.ClientEnd{Address: addr}
	}

	cAddrs := strings.Split(*ctrlStr, ",")
	ctrlEnds := make([]*kvutil.ClientEnd, len(cAddrs))
	for i, addr := range cAddrs {
		ctrlEnds[i] = &kvutil.ClientEnd{Address: addr}
	}

	ps := raft.NewPersister(*dataDir, fmt.Sprintf("kv-%d-%d", *gid, *me))
	makeEnd := func(addr string) *kvutil.ClientEnd {
		return &kvutil.ClientEnd{Address: addr}
	}

	kv := shardkv.StartServer(raftEnds, *me, ps, *maxRS, *gid, ctrlEnds, makeEnd)

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(kv); err != nil {
		log.Fatalf("Failed to register Shardkv RPC server: %v", err)
	}
	if err := rpcServer.Register(kv.GetRaft()); err != nil {
		log.Fatalf("Failed to register Raft RPC server: %v", err)
	}

	listenAddr := raAddrs[*me]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
	}
	log.Printf("Shardkv node %d listening on %s", *me, listenAddr)

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

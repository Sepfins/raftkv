package kvutil

import (
	"net"
	"net/rpc"
	"time"
)

const RpcTimeout = 300 * time.Millisecond

type ClientEnd struct {
	Address string // TCP address like "localhost:8001"
}

func (ce *ClientEnd) Call(serviceMethod string, args interface{}, reply interface{}) bool {
	conn, err := net.DialTimeout("tcp", ce.Address, RpcTimeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	return client.Call(serviceMethod, args, reply) == nil
}

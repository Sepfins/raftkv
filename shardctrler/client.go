package shardctrler

import (
	"github.com/google/uuid"
	"raftkv/kvutil"
	"time"
)

type Clerk struct {
	servers        []*kvutil.ClientEnd
	clientId       string
	sequenceNumber int
	leaderId       int
}

func MakeClerk(servers []*kvutil.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = uuid.New().String()
	ck.sequenceNumber = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	reply := ck.executeCommand(num, nil, nil, -1, OpQuery)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.executeCommand(-1, servers, nil, -1, OpJoin)
}

func (ck *Clerk) Leave(gids []int) {
	ck.executeCommand(-1, nil, gids, -1, OpLeave)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.executeCommand(-1, nil, []int{gid}, shard, OpMove)
}

func (ck *Clerk) executeCommand(num int, servers map[int][]string, gids []int, shard int, op OpType) ExecuteCommandReply {
	args := ExecuteCommandArgs{
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
		Op:             op,
		Num:            num,
		Servers:        servers,
		GIDs:           gids,
		Shard:          shard,
	}

	reply := ExecuteCommandReply{}

	for !ck.servers[ck.leaderId].Call("ShardCtrler.ExecuteCommand", &args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		reply = ExecuteCommandReply{}
		time.Sleep(100 * time.Millisecond)
	}

	ck.sequenceNumber++
	return reply
}

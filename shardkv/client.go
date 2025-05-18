package shardkv

import (
	"github.com/google/uuid"
	"raftkv/kvutil"
	"raftkv/shardctrler"
	"time"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm             *shardctrler.Clerk
	config         shardctrler.Config
	make_end       func(string) *kvutil.ClientEnd
	clientId       string
	sequenceNumber int
	leaderIds      map[int]int
}

func MakeClerk(ctrlers []*kvutil.ClientEnd, make_end func(string) *kvutil.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = uuid.New().String()
	ck.sequenceNumber = 0
	ck.leaderIds = make(map[int]int)
	return ck
}

func (ck *Clerk) executeCommand(key string, value string, op OpType) string {
	args := ExecuteCommandArgs{
		Key:            key,
		Value:          value,
		Op:             op,
		ClientId:       ck.clientId,
		SequenceNumber: ck.sequenceNumber,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			previousLeaderId := ck.leaderIds[gid]
			newLeaderId := previousLeaderId

			for {
				reply := ExecuteCommandReply{}
				srv := ck.make_end(servers[newLeaderId])
				ok := srv.Call("ShardKV.ExecuteCommand", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.sequenceNumber++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == previousLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.executeCommand(key, "", OpGet)
}
func (ck *Clerk) Put(key string, value string) {
	ck.executeCommand(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.executeCommand(key, value, OpAppend)
}

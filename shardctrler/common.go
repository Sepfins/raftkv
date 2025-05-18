package shardctrler

import (
	"fmt"
	"sort"
	"time"
)

const (
	NShards        = 10
	SnapshotSize   = 1 << 20
	RequestTimeout = 1000 * time.Millisecond
)

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type OpType int

const (
	OpJoin OpType = iota
	OpLeave
	OpMove
	OpQuery
)

func (op OpType) String() string {
	switch op {
	case OpJoin:
		return "OpJoin"
	case OpLeave:
		return "OpLeave"
	case OpMove:
		return "OpMove"
	case OpQuery:
		return "OpQuery"
	default:
		return "UnknownOpType"
	}

}

type Err int

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

type Command struct {
	*ExecuteCommandArgs
}

func (command Command) String() string {
	return fmt.Sprintf("Command{%s, %s, %d}", command.Op.String(), command.ClientId, command.SequenceNumber)
}

type OperationState struct {
	HighestSequenceNumber int
	LastReply             *ExecuteCommandReply
}

func deepCopy(original map[int][]string) map[int][]string {
	mapCopy := make(map[int][]string)

	for k, v := range original {
		mapCopy[k] = make([]string, len(v))
		copy(mapCopy[k], v)
	}

	return mapCopy
}

func (config *Config) GetGidToShardMap() map[int][]int {
	shardMap := make(map[int][]int)
	for gid := range config.Groups {
		shardMap[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		shardMap[gid] = append(shardMap[gid], shard)
	}
	return shardMap
}

func getBalancingSource(shardMap map[int][]int) int {
	// shall allocate unassigned shards first
	if shards, ok := shardMap[0]; ok && len(shards) != 0 {
		return 0
	}

	keys := make([]int, 0)
	for k := range shardMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	maxShard, maxGid := -1, -1

	for _, gid := range keys {
		if len(shardMap[gid]) > maxShard {
			maxShard, maxGid = len(shardMap[gid]), gid
		}
	}
	return maxGid
}

func getBalancingTarget(shardMap map[int][]int) int {
	keys := make([]int, 0)
	for k := range shardMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	minShard, minGid := NShards+1, -1

	for _, gid := range keys {
		if gid != 0 && len(shardMap[gid]) < minShard {
			minShard, minGid = len(shardMap[gid]), gid
		}
	}

	return minGid
}

type ExecuteCommandArgs struct {
	ClientId       string
	SequenceNumber int
	Op             OpType
	Servers        map[int][]string // Join
	GIDs           []int            // Leave and Move
	Shard          int              // Move
	Num            int              // Query
}

type ExecuteCommandReply struct {
	Err    Err
	Config Config
}

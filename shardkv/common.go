package shardkv

import (
	"raftkv/shardctrler"
	"time"
)

const (
	RequestTimeout                = 1000 * time.Millisecond
	PoolingInterval               = 100 * time.Millisecond
	MigrationCheckInterval        = 50 * time.Millisecond
	GarbageCollectionInterval     = 50 * time.Millisecond
	CurrentTermEntryCheckInterval = 300 * time.Millisecond
)

type Err int

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrTimeout
	ErrOutOfDate
	ErrNotReady
)

type CommandType int

const (
	Operation CommandType = iota
	Configuration
	TransferShard
	InstallShard
	DeleteShard
	NoOp
)

func (ct CommandType) String() string {
	switch ct {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case TransferShard:
		return "TransferShard"
	case InstallShard:
		return "InstallShard"
	case DeleteShard:
		return "DeleteShard"
	case NoOp:
		return "NoOp"
	default:
		return "UnknownCommandType"
	}
}

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

func (op OpType) String() string {
	switch op {
	case OpGet:
		return "OpGet"
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	default:
		return "UnknownOpType"
	}
}

type ShardStatus int

const (
	ShardInvalid      ShardStatus = iota // Shard not owned, ignore requests
	ShardServing                         // Shard ready for client requests
	ShardPulling                         // Shard being pulled from another group
	ShardTransferring                    // Shard being transferred to another group
	ShardWaitingForGC                    // Shard functional but waiting for post-migration cleanup
)

func (ss ShardStatus) String() string {
	switch ss {
	case ShardInvalid:
		return "ShardInvalid"
	case ShardServing:
		return "ShardServing"
	case ShardPulling:
		return "ShardPulling"
	case ShardTransferring:
		return "ShardTransferring"
	case ShardWaitingForGC:
		return "ShardWaitingForGC"
	default:
		return "UnknownShardStatus"
	}
}

type ExecuteCommandArgs struct {
	Key            string
	Value          string
	Op             OpType
	ClientId       string
	SequenceNumber int
}

type ExecuteCommandReply struct {
	Err   Err
	Value string
}

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err       Err
	ConfigNum int
	Shards    map[int]Shard
}

type Command struct {
	CommandType CommandType
	Data        interface{}
}

func NewOperationCommand(args *ExecuteCommandArgs) Command {
	return Command{
		CommandType: Operation,
		Data:        *args,
	}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{
		CommandType: Configuration,
		Data:        *config,
	}
}

func NewInstallShardCommand(shard *ShardOperationReply) Command {
	return Command{
		CommandType: InstallShard,
		Data:        *shard,
	}
}

func NewDeleteShardCommand(args *ShardOperationArgs) Command {
	return Command{
		CommandType: DeleteShard,
		Data:        *args,
	}
}

func NewBlankCommand() Command {
	return Command{
		CommandType: NoOp,
		Data:        nil,
	}
}

type OperationState struct {
	HighestSequenceNumber int
	LastReply             *ExecuteCommandReply
}

func (state *OperationState) deepCopy() *OperationState {
	return &OperationState{
		HighestSequenceNumber: state.HighestSequenceNumber,
		LastReply:             &ExecuteCommandReply{Err: state.LastReply.Err, Value: state.LastReply.Value},
	}
}

package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"raftkv/kvutil"
	"raftkv/raft"
	"raftkv/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu       sync.RWMutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32
	make_end func(string) *kvutil.ClientEnd
	gid      int
	ctrlers  []*kvutil.ClientEnd
	mck      *shardctrler.Clerk

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	stateMachine   map[int]*Shard
	notifyChannels map[int]chan *ExecuteCommandReply

	previousConfig shardctrler.Config // to check where to pull from
	currentConfig  shardctrler.Config
}

func (kv *ShardKV) getNotifyChannel(index int) chan *ExecuteCommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.notifyChannels[index]; !exists {
		kv.notifyChannels[index] = make(chan *ExecuteCommandReply, 1)
	}
	return kv.notifyChannels[index]
}

func (kv *ShardKV) deleteNotifyChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyChannels, index)
}

func (kv *ShardKV) isDuplicateCommand(clientId string, sequenceNumber int, shardId int) bool {
	if state, exists := kv.stateMachine[shardId].ClientContext[clientId]; exists {
		return sequenceNumber <= state.HighestSequenceNumber
	}
	return false
}

func (kv *ShardKV) canServeShard(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid &&
		(kv.stateMachine[shardId].Status == ShardServing || kv.stateMachine[shardId].Status == ShardWaitingForGC)
}

func (kv *ShardKV) ExecuteCommand(args *ExecuteCommandArgs, reply *ExecuteCommandReply) error {

	kv.mu.RLock()
	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] received %v command from %v", kv.gid, kv.me, args.Op, args.ClientId)
	shardId := key2shard(args.Key)
	if args.Op != OpGet && kv.isDuplicateCommand(args.ClientId, args.SequenceNumber, shardId) {
		lastReply := kv.stateMachine[shardId].ClientContext[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return nil
	}

	if !kv.canServeShard(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return nil
	}
	kv.mu.RUnlock()

	kv.Execute(NewOperationCommand(args), reply)
	return nil
}

func (kv *ShardKV) Execute(command Command, reply *ExecuteCommandReply) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyChannel(index)

	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}

	go kv.deleteNotifyChannel(index)
}

func (kv *ShardKV) RequestShards(args *ShardOperationArgs, reply *ShardOperationReply) error {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] received a request shard for config %d", kv.gid, kv.me, args.ConfigNum)

	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrNotReady
		return nil
	}

	shards := make(map[int]Shard)
	for _, shardId := range args.ShardIds {
		shards[shardId] = *kv.stateMachine[shardId].deepCopy()
	}
	reply.Err, reply.ConfigNum, reply.Shards = OK, kv.currentConfig.Num, shards
	return nil
}

func (kv *ShardKV) DeleteShards(args *ShardOperationArgs, reply *ShardOperationReply) error {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	kv.mu.RLock()
	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] received a delete shard for config %d", kv.gid, kv.me, args.ConfigNum)
	if args.ConfigNum < kv.currentConfig.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return nil
	}
	kv.mu.RUnlock()

	commandReply := &ExecuteCommandReply{}
	kv.Execute(NewDeleteShardCommand(args), commandReply)
	reply.Err = commandReply.Err
	return nil
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				command := message.Command.(Command)
				reply := &ExecuteCommandReply{}

				switch command.CommandType {
				case Operation:
					operation := command.Data.(ExecuteCommandArgs)
					reply = kv.applyOperation(&operation)
				case Configuration:
					config := command.Data.(shardctrler.Config)
					reply = kv.applyConfiguration(&config)
				case InstallShard:
					shardOp := command.Data.(ShardOperationReply)
					reply = kv.applyInstallShard(&shardOp)
				case DeleteShard:
					shardOp := command.Data.(ShardOperationArgs)
					reply = kv.applyDeleteShard(&shardOp)
				case NoOp:
					reply = &ExecuteCommandReply{Err: OK}
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					if notifyChannel, exists := kv.notifyChannels[message.CommandIndex]; exists {
						notifyChannel <- reply
					}
				}
				if kv.isSnapshotNeeded() {
					kv.createSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.lastApplied < message.SnapshotIndex {
					kv.readSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("Invalid ApplyMsg %v", message))
			}
		}

	}
}

func (kv *ShardKV) applyOperation(operation *ExecuteCommandArgs) *ExecuteCommandReply {
	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] applying operation %v", kv.gid, kv.me, operation.Op)
	reply := &ExecuteCommandReply{}
	shardId := key2shard(operation.Key)

	if !kv.canServeShard(shardId) {
		reply.Err = ErrWrongGroup
		return reply
	} else if operation.Op != OpGet && kv.isDuplicateCommand(operation.ClientId, operation.SequenceNumber, shardId) {
		lastReply := kv.stateMachine[shardId].ClientContext[operation.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		return reply
	}

	switch operation.Op {
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine[shardId].Get(operation.Key)
	default:
		var err Err
		switch operation.Op {
		case OpPut:
			err = kv.stateMachine[shardId].Put(operation.Key, operation.Value)
		case OpAppend:
			err = kv.stateMachine[shardId].Append(operation.Key, operation.Value)
		default:
			panic("Unknown operation type")
		}
		reply.Err = err

		kv.stateMachine[shardId].ClientContext[operation.ClientId] = OperationState{
			HighestSequenceNumber: operation.SequenceNumber,
			LastReply:             reply,
		}
	}
	return reply
}

func (kv *ShardKV) applyInstallShard(shardOp *ShardOperationReply) *ExecuteCommandReply {
	if shardOp.ConfigNum < kv.currentConfig.Num {
		return &ExecuteCommandReply{Err: ErrOutOfDate}
	}

	reply := &ExecuteCommandReply{}
	for shardId, shard := range shardOp.Shards {
		if kv.stateMachine[shardId].Status == ShardPulling {
			kvutil.ShardKVLogger.Printf("[GID %d Srv %d] installing shard %d", kv.gid, kv.me, shardId)
			for key, value := range shard.Store {
				kv.stateMachine[shardId].Put(key, value)
			}
			for key, value := range shard.ClientContext {
				kv.stateMachine[shardId].ClientContext[key] = *value.deepCopy()
			}
			kv.stateMachine[shardId].Status = ShardWaitingForGC
		}
	}
	return reply
}

func (kv *ShardKV) applyDeleteShard(shardOp *ShardOperationArgs) *ExecuteCommandReply {
	reply := &ExecuteCommandReply{Err: OK}
	if shardOp.ConfigNum < kv.currentConfig.Num {
		return reply
	}

	for _, shardId := range shardOp.ShardIds {
		if kv.stateMachine[shardId].Status == ShardTransferring {
			kvutil.ShardKVLogger.Printf("[GID %d Srv %d] deleting shard %d", kv.gid, kv.me, shardId)
			kv.stateMachine[shardId] = NewShard()
		} else if kv.stateMachine[shardId].Status == ShardWaitingForGC {
			kv.stateMachine[shardId].Status = ShardServing
		}
	}
	return reply

}

func (kv *ShardKV) applyConfiguration(config *shardctrler.Config) *ExecuteCommandReply {
	reply := &ExecuteCommandReply{}

	currentConfigNum := kv.currentConfig.Num

	if config.Num <= currentConfigNum {
		reply.Err = ErrOutOfDate
		return reply
	}

	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] applying configuration %v", kv.gid, kv.me, config.Num)
	kv.changeShardStatus(config)
	kv.previousConfig = kv.currentConfig
	kv.currentConfig = *config

	reply.Err = OK

	return reply
}

func (kv *ShardKV) changeShardStatus(newConfig *shardctrler.Config) {
	for shardId := range newConfig.Shards {
		if kv.currentConfig.Shards[shardId] != kv.gid && newConfig.Shards[shardId] == kv.gid {
			previousOwner := kv.currentConfig.Shards[shardId]
			if previousOwner != 0 {
				kv.stateMachine[shardId].Status = ShardPulling
			} else {
				kv.stateMachine[shardId].Status = ShardServing
			}
		}

		if kv.currentConfig.Shards[shardId] == kv.gid && newConfig.Shards[shardId] != kv.gid {
			kv.stateMachine[shardId].Status = ShardTransferring
		}
	}
}

func (kv *ShardKV) isSnapshotNeeded() bool {
	size := kv.rf.GetRaftStateSize()
	return kv.maxraftstate != -1 && size > kv.maxraftstate
}

func (kv *ShardKV) createSnapshot(index int) {
	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] creating snapshot at index %d", kv.gid, kv.me, index)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.previousConfig)
	e.Encode(kv.currentConfig)

	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		for i := 0; i < shardctrler.NShards; i++ {
			kv.stateMachine[i] = NewShard()
		}
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var stateMachine map[int]*Shard
	var previousConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&previousConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		panic("Error reading snapshot")
	}

	kv.stateMachine = stateMachine
	kv.previousConfig = previousConfig
	kv.currentConfig = currentConfig
	kvutil.ShardKVLogger.Printf("[GID %d Srv %d] read snapshot", kv.gid, kv.me)
}

func (kv *ShardKV) taskRunner(task func(), interval time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			task()
		}
		time.Sleep(interval)
	}
}

func (kv *ShardKV) retrieveConfig() {
	readyToInstallNext := true

	kv.mu.RLock()
	for _, shard := range kv.stateMachine {
		if shard.Status != ShardServing && shard.Status != ShardInvalid {
			readyToInstallNext = false
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()

	if readyToInstallNext {
		latestConfig := kv.mck.Query(currentConfigNum + 1)

		if latestConfig.Num == currentConfigNum+1 {
			kv.Execute(NewConfigurationCommand(&latestConfig), &ExecuteCommandReply{})
		}
	}
}

func (kv *ShardKV) retrieveShards() {
	kv.mu.RLock()

	var wg sync.WaitGroup
	shards := kv.getShardsByStatus(ShardPulling)

	for previousOwner, shardIds := range shards {
		wg.Add(1)
		kvutil.ShardKVLogger.Printf("[GID %d Srv %d] pulling shards %v from %d",
			kv.gid, kv.me, shardIds, previousOwner)
		go func(servers []string, shardIds []int, configNum int) {
			defer wg.Done()
			args := ShardOperationArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}

			for _, server := range servers {
				end := kv.make_end(server)
				reply := ShardOperationReply{}
				if end.Call("ShardKV.RequestShards", &args, &reply) && reply.Err == OK {
					kv.Execute(NewInstallShardCommand(&reply), &ExecuteCommandReply{})
					break
				}
			}
		}(kv.previousConfig.Groups[previousOwner], shardIds, kv.currentConfig.Num)
	}

	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) garbageCollect() {
	kv.mu.RLock()

	var wg sync.WaitGroup
	shards := kv.getShardsByStatus(ShardWaitingForGC)

	for previousOwner, shardIds := range shards {
		wg.Add(1)
		kvutil.ShardKVLogger.Printf("[GID %d Srv %d] garbage collecting shards %v from %d",
			kv.gid, kv.me, shardIds, previousOwner)
		go func(servers []string, shardIds []int, configNum int) {
			defer wg.Done()
			args := ShardOperationArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}

			for _, server := range servers {
				end := kv.make_end(server)
				reply := ShardOperationReply{}
				if end.Call("ShardKV.DeleteShards", &args, &reply) && reply.Err == OK {
					kv.Execute(NewDeleteShardCommand(&args), &ExecuteCommandReply{})
					break
				}
			}
		}(kv.previousConfig.Groups[previousOwner], shardIds, kv.currentConfig.Num)
	}

	kv.mu.RUnlock()
	wg.Wait()
}

// needed for committing entries from previous terms,
// it should probably be done in the raft layer, but MIT testing
// framework does not accept it.
func (kv *ShardKV) previousTermCommit() {
	if !kv.rf.HasCurrentTermEntry() {
		kv.Execute(NewBlankCommand(), &ExecuteCommandReply{})
	}
}

func (kv *ShardKV) getShardsByStatus(status ShardStatus) map[int][]int {
	shards := make(map[int][]int)
	for shardId, shard := range kv.stateMachine {
		if shard.Status == status {
			previousOwner := kv.previousConfig.Shards[shardId]
			if _, exists := shards[previousOwner]; !exists {
				shards[previousOwner] = make([]int, 0)
			}
			shards[previousOwner] = append(shards[previousOwner], shardId)
		}
	}
	return shards
}

// For rpc purposes
func (kv *ShardKV) GetRaft() *raft.Raft {
	return kv.rf
}

func StartServer(servers []*kvutil.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*kvutil.ClientEnd, make_end func(string) *kvutil.ClientEnd) *ShardKV {
	gob.Register(Command{})
	gob.Register(ExecuteCommandArgs{})
	gob.Register(ExecuteCommandReply{})
	gob.Register(shardctrler.Config{})
	gob.Register(ShardOperationArgs{})
	gob.Register(ShardOperationReply{})
	gob.Register(Shard{})
	gob.Register(OperationState{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = make(map[int]*Shard)
	kv.notifyChannels = make(map[int]chan *ExecuteCommandReply)
	kv.previousConfig = shardctrler.Config{Num: -1}
	kv.currentConfig = shardctrler.Config{Num: -1}

	kv.readSnapshot(persister.ReadSnapshot())

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	go kv.taskRunner(kv.retrieveConfig, PoolingInterval)
	go kv.taskRunner(kv.retrieveShards, MigrationCheckInterval)
	go kv.taskRunner(kv.garbageCollect, GarbageCollectionInterval)
	go kv.taskRunner(kv.previousTermCommit, CurrentTermEntryCheckInterval)

	go kv.applier()
	return kv
}

package shardctrler

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"raftkv/kvutil"
	"raftkv/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	stateMachine    ConfigStateMachine
	operationStates map[string]OperationState
	notifyChannels  map[int]chan *ExecuteCommandReply
}

func (sc *ShardCtrler) ExecuteCommand(args *ExecuteCommandArgs, reply *ExecuteCommandReply) error {
	sc.mu.RLock()
	kvutil.ShardCtrlerLogger.Printf("[Controller %d] received %v command from %v", sc.me, args.Op, args.ClientId)
	if args.Op != OpQuery && sc.isDuplicateCommand(args.ClientId, args.SequenceNumber) {
		lastReply := sc.operationStates[args.ClientId].LastReply
		reply.Err, reply.Config = lastReply.Err, lastReply.Config
		sc.mu.RUnlock()
		return nil
	}
	sc.mu.RUnlock()

	command := Command{args}
	index, _, isLeader := sc.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	notifyCh := sc.getNotifyChannel(index)

	select {
	case result := <-notifyCh:
		reply.Err, reply.Config = result.Err, result.Config
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}

	go sc.deleteNotifyChannel(index)
	return nil
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *ExecuteCommandReply {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, exists := sc.notifyChannels[index]; !exists {
		sc.notifyChannels[index] = make(chan *ExecuteCommandReply, 1)
	}
	return sc.notifyChannels[index]
}

func (sc *ShardCtrler) deleteNotifyChannel(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.notifyChannels, index)
}

func (sc *ShardCtrler) isDuplicateCommand(clientId string, sequenceNumber int) bool {
	if operationState, exists := sc.operationStates[clientId]; exists {
		return operationState.HighestSequenceNumber >= sequenceNumber
	}
	return false
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				command := message.Command.(Command)
				reply := &ExecuteCommandReply{}

				if sc.isDuplicateCommand(command.ClientId, command.SequenceNumber) {
					reply = sc.operationStates[command.ClientId].LastReply
				} else {
					kvutil.ShardCtrlerLogger.Printf("[Controller %d] applying command to the state machine: %v",
						sc.me, command)
					reply = sc.applyCommandToStateMachine(command)
					if command.Op != OpQuery {
						sc.operationStates[command.ClientId] = OperationState{
							HighestSequenceNumber: command.SequenceNumber,
							LastReply:             reply,
						}
					}
				}

				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					if notifyChannel, exists := sc.notifyChannels[message.CommandIndex]; exists {
						notifyChannel <- reply
					}
				}

				if sc.isSnapshotNeeded() {
					sc.takeSnapshot(message.CommandIndex)
				}

				sc.mu.Unlock()
			} else if message.SnapshotValid {
				sc.mu.Lock()
				if sc.lastApplied < message.SnapshotIndex {
					sc.readSnapshot(message.Snapshot)
					sc.lastApplied = message.SnapshotIndex
				}
				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("Invalid message: %v", message))
			}
		}
	}
}

func (sc *ShardCtrler) applyCommandToStateMachine(command Command) *ExecuteCommandReply {
	reply := &ExecuteCommandReply{}
	switch command.Op {
	case OpJoin:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GIDs[0])
	case OpQuery:
		reply.Err, reply.Config = sc.stateMachine.Query(command.Num)
	}
	return reply
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) isSnapshotNeeded() bool {
	size := sc.rf.GetRaftStateSize()
	return sc.maxraftstate != -1 && size > sc.maxraftstate
}

func (sc *ShardCtrler) takeSnapshot(lastIncludedIndex int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(sc.stateMachine)
	e.Encode(sc.operationStates)
	data := w.Bytes()
	sc.rf.Snapshot(lastIncludedIndex, data)
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)

	var stateMachine ConfigStateMachine
	var operationStates map[string]OperationState

	if d.Decode(&stateMachine) != nil || d.Decode(&operationStates) != nil {
		log.Fatalf("Failed to read the snapshot")
	} else {
		sc.stateMachine = stateMachine
		sc.operationStates = operationStates
	}
}

// For rpc purposes
func (sc *ShardCtrler) GetRaft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*kvutil.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	gob.Register(Command{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = SnapshotSize

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.stateMachine = &ConfigMemStore{Configs: []Config{initialConfig()}}
	sc.operationStates = make(map[string]OperationState)
	sc.notifyChannels = make(map[int]chan *ExecuteCommandReply)

	sc.readSnapshot(persister.ReadSnapshot())

	go sc.applier()
	return sc
}

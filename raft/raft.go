package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"raftkv/kvutil"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*kvutil.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// common state
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	state       State
	// leader state
	nextIndex  []int
	matchIndex []int
	//compaction
	LastIncludedIndex int
	LastIncludedTerm  int
	snapshot          []byte

	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	pendingSnapshot *ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	replicateCond  *sync.Cond
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

func (rf *Raft) HasCurrentTermEntry() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getTermAtIndex(rf.logEndIndex()) == rf.currentTerm
}

func (rf *Raft) ChangeState(state State) {
	if rf.state == state {
		return
	}

	kvutil.RaftLogger.Printf("[Node %d] changing state from %v to %v", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.heartbeatTimer.Stop()
	case Candidate:
	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(heartbeatInterval)
		go rf.replicationLoop()
	}
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		kvutil.RaftLogger.Printf("[Node %d] error reading persisted state", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex

		kvutil.RaftLogger.Printf("[Node %d] read persisted state: currentTerm %d, votedFor %d, "+
			"lastIncludedIndex %d, lastIncludedTerm %d", rf.me, rf.currentTerm, rf.votedFor,
			rf.LastIncludedIndex, rf.LastIncludedTerm)
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.LastIncludedIndex {
		kvutil.RaftLogger.Printf("[Node %d] snapshot index %d is outdated, current snapshot starts from %d", rf.me, index, rf.LastIncludedIndex)
		return
	}

	rf.LastIncludedTerm = rf.getTermAtIndex(index)
	rf.log = append([]LogEntry{{Term: rf.LastIncludedTerm}}, rf.log[rf.globalToLocalIndex(index)+1:]...)
	rf.snapshot = snapshot
	rf.LastIncludedIndex = index

	rf.persist()
	kvutil.RaftLogger.Printf("[Node %d] created snapshot at index %d, term %d", rf.me, index, rf.LastIncludedTerm)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	kvutil.RaftLogger.Printf("[Node %d] RequestVote from %d: term=%d, lastLogIdx=%d, lastLogTerm=%d",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	reply.Term, reply.VoteGranted = rf.currentTerm, false

	// Reject if candidate's term is outdated or server has already voted for someone else
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.votedFor, rf.currentTerm = -1, args.Term
		rf.persist()
	}

	logIndex := rf.logEndIndex()
	logTerm := rf.log[len(rf.log)-1].Term

	// Reject if candidate's log is outdated
	if !atLeastUpToDate(logIndex, logTerm, args.LastLogIndex, args.LastLogTerm) {
		return nil
	}

	rf.votedFor = args.CandidateId
	rf.persist()

	kvutil.RaftLogger.Printf("[Node %d] Granting vote to %d for term %d", rf.me, args.CandidateId, rf.currentTerm)

	rf.electionTimer.Reset(randomElectionTimeout())
	reply.VoteGranted = true
	return nil
}

// atLeastUpToDate checks if the candidate's log is at least as up-to-date
// as the current server's log based on Raft's log comparison rules.
//
// A log is considered more up-to-date if:
// 1. Its last log term is greater than the server's last log term.
// 2. If terms are equal, the log with the higher index is more up-to-date.
//
// Returns true if the candidate's log is at least as up-to-date.
func atLeastUpToDate(lastLogIndex, lastLogTerm, candidateLastLogIndex, candidateLastLogTerm int) bool {
	return candidateLastLogTerm > lastLogTerm || (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	kvutil.RaftLogger.Printf("[Node %d] Starting command: %v", rf.me, command)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.localToGlobalIndex(len(rf.log))
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	rf.matchIndex[rf.me] = index
	rf.persist()

	rf.replicateCond.Signal()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CallRequestVote(server int, args RequestVoteArgs) bool {
	reply := RequestVoteReply{}

	if !rf.sendRequestVote(server, &args, &reply) {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.votedFor, rf.currentTerm = -1, reply.Term
		rf.persist()
		return false
	}

	return reply.VoteGranted
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
			rf.mu.Lock()
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			_, isLeader := rf.GetState()
			if isLeader {
				rf.heartbeat(true)
			}
		}
	}
}

func (rf *Raft) startElection() {
	_, isLeader := rf.GetState()

	if !isLeader {
		rf.mu.Lock()

		rf.ChangeState(Candidate)
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()

		votes := 1
		electionWon := false

		lastLogIndex := rf.localToGlobalIndex(len(rf.log) - 1)
		lastLogTerm := rf.log[len(rf.log)-1].Term

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		rf.mu.Unlock()

		for ii := range rf.peers {
			if ii == rf.me {
				continue
			}

			go func(i int) {
				if !rf.CallRequestVote(i, args) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				votes++
				if electionWon || votes <= len(rf.peers)/2 {
					return
				}
				electionWon = true

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}

				rf.ChangeState(Leader)
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.localToGlobalIndex(len(rf.log))
					rf.matchIndex[i] = 0
				}

				rf.heartbeat(true)
			}(ii)
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.pendingSnapshot == nil && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.pendingSnapshot != nil {
			snapshot := rf.pendingSnapshot
			kvutil.RaftLogger.Printf("[Node %d] applying snapshot %d", rf.me, snapshot.SnapshotIndex)
			rf.mu.Unlock()

			rf.applyCh <- *snapshot

			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, snapshot.SnapshotIndex)
			if rf.pendingSnapshot.SnapshotIndex == snapshot.SnapshotIndex {
				rf.pendingSnapshot = nil
			}
			rf.mu.Unlock()
			continue
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied

		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[rf.globalToLocalIndex(lastApplied+1):rf.globalToLocalIndex(commitIndex+1)])
		rf.mu.Unlock()

		for i, entry := range entries {
			kvutil.RaftLogger.Printf("[Node %d] applying log entry %d", rf.me, lastApplied+i+1)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + i + 1,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)

		kvutil.RaftLogger.Printf("[Node %d] applied log entries up to %d", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicationLoop() {
	rf.replicateCond.L.Lock()
	defer rf.replicateCond.L.Unlock()

	for rf.killed() == false {
		rf.replicateCond.Wait()

		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		rf.heartbeat(false)
	}
}

func (rf *Raft) heartbeat(heartbeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if heartbeat || rf.needsReplication(i) {
			go rf.CallAppendEntries(i)
		}
	}

	rf.heartbeatTimer.Reset(heartbeatInterval)
}

func (rf *Raft) needsReplication(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.matchIndex[peer] <= rf.logEndIndex()
}

func (rf *Raft) CallAppendEntries(server int) {
	_, isLeader := rf.GetState()
	if !isLeader {
		return
	}

	rf.mu.RLock()
	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := rf.nextIndex[server] - 1

	if prevLogIndex < rf.LastIncludedIndex {
		rf.mu.RUnlock()
		go rf.CallInstallSnapshot(server)
		return
	}

	prevLogTerm := rf.getTermAtIndex(rf.nextIndex[server] - 1)
	entries := rf.log[rf.globalToLocalIndex(rf.nextIndex[server]):]
	leaderCommit := rf.commitIndex

	kvutil.RaftLogger.Printf("[Node %d] sending AppendEntries to %d, prevLogIndex %d, prevLogTerm %d, entries %v",
		rf.me, server, prevLogIndex, prevLogTerm, entries)

	rf.mu.RUnlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.votedFor, rf.currentTerm = -1, reply.Term
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.commitLogEntries()
		return
	}

	if reply.XTerm == 0 && reply.XIndex == 0 && reply.XLen == 0 {
		return
	}
	if reply.XTerm != 0 {
		hasXTerm := false
		for i := len(rf.log) - 1; i > 0 && rf.log[i].Term >= reply.XTerm; i-- {
			if rf.log[i].Term == reply.XTerm {
				hasXTerm = true
				rf.nextIndex[server] = rf.localToGlobalIndex(i)
				break
			}
		}
		if !hasXTerm {
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		rf.nextIndex[server] = reply.XLen
	}
	return
}

func (rf *Raft) commitLogEntries() {
	n := rf.commitIndex

	for i := len(rf.log) - 1; i >= rf.globalToLocalIndex(rf.commitIndex); i-- {
		if rf.log[i].Term != rf.currentTerm {
			break
		}

		count := 0
		for _, matchIdx := range rf.matchIndex {
			if matchIdx >= rf.localToGlobalIndex(i) {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			n = rf.localToGlobalIndex(i)
			break
		}
	}

	if n > rf.commitIndex {
		rf.commitIndex = n
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	kvutil.RaftLogger.Printf("[Node %d] AppendEntries from %d at term %d", rf.me, args.LeaderId, args.Term)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		kvutil.RaftLogger.Printf("[Node %d] Rejected AppendEntries, term %d outdated, current term is %d",
			rf.me, args.Term, rf.currentTerm)
		return nil
	}

	rf.electionTimer.Reset(randomElectionTimeout())
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.votedFor, rf.currentTerm = -1, args.Term
		rf.persist()
	}

	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = rf.LastIncludedIndex + 1
		kvutil.RaftLogger.Printf("[Node %d] Rejected AppendEntries, prevLogIndex %d is outdated", rf.me, args.PrevLogIndex)
		return nil
	}

	if rf.localToGlobalIndex(len(rf.log)) > args.PrevLogIndex && rf.getTermAtIndex(args.PrevLogIndex) == args.PrevLogTerm {
		appendFrom := 0
		if len(args.Entries) > 0 && rf.localToGlobalIndex(len(rf.log)-1) > args.PrevLogIndex {
			for i := 0; i < len(args.Entries); i++ {
				logIndex := args.PrevLogIndex + i + 1
				if logIndex >= rf.localToGlobalIndex(len(rf.log)) {
					break
				}
				if rf.getTermAtIndex(logIndex) != args.Entries[i].Term {
					rf.log = rf.log[:(rf.globalToLocalIndex(logIndex))]
					break
				}
				appendFrom++
			}
		}
		rf.log = append(rf.log, args.Entries[appendFrom:]...)
		if len(args.Entries[appendFrom:]) > 0 {
			rf.persist()
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.localToGlobalIndex(len(rf.log)-1))
			rf.applyCond.Signal()
		}
		reply.Success = true
		kvutil.RaftLogger.Printf("[Node %d] Accepted AppendEntries ", rf.me)
	} else {
		if rf.localToGlobalIndex(len(rf.log)) <= args.PrevLogIndex {
			reply.XLen = rf.localToGlobalIndex(len(rf.log))
		} else {
			if rf.log[rf.globalToLocalIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
				reply.XTerm = rf.getTermAtIndex(args.PrevLogIndex)
			}
			i := args.PrevLogIndex
			for i > 0 && rf.getTermAtIndex(i) == rf.getTermAtIndex(args.PrevLogIndex) {
				i--
			}
			reply.XIndex = i + 1
		}
		if args.PrevLogIndex == 0 {
		}
		reply.Success = false
		kvutil.RaftLogger.Printf("[Node %d] Rejected AppendEntries, missing log entries", rf.me)
	}
	reply.Term = rf.currentTerm
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) CallInstallSnapshot(server int) bool {
	_, isLeader := rf.GetState()
	if !isLeader {
		return false
	}

	rf.mu.RLock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.snapshot,
	}
	kvutil.RaftLogger.Printf("[Node %d] sending InstallSnapshot to %d", rf.me, server)
	rf.mu.RUnlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.votedFor, rf.currentTerm = -1, reply.Term
		rf.persist()
		return false
	}

	rf.nextIndex[server] = rf.LastIncludedIndex + 1
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	kvutil.RaftLogger.Printf("[Node %d] InstallSnapshot from %d at term %d", rf.me, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		kvutil.RaftLogger.Printf("[Node %d] Rejected InstallSnapshot, term %d outdated, current term is %d",
			rf.me, args.Term, rf.currentTerm)
		return nil
	}

	rf.ChangeState(Follower)
	rf.currentTerm = args.Term
	rf.electionTimer.Reset(randomElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		reply.Term = rf.currentTerm
		kvutil.RaftLogger.Printf("[Node %d] Rejected InstallSnapshot, lastIncludedIndex %d is outdated, current commitIndex is %d",
			rf.me, args.LastIncludedIndex, rf.commitIndex)
		return nil
	}

	rf.pendingSnapshot = &ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      deepCopy(args.Data),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCond.Signal()

	if rf.isInLog(args.LastIncludedIndex) && rf.getTermAtIndex(args.LastIncludedIndex) == args.LastIncludedTerm {
		discardPoint := rf.globalToLocalIndex(args.LastIncludedIndex) + 1
		rf.log = rf.log[discardPoint:]
	} else {
		rf.log = make([]LogEntry, 0)
	}

	rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log...)
	rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()

	reply.Term = rf.currentTerm
	kvutil.RaftLogger.Printf("[Node %d] Accepted InstallSnapshot, lastIncludedIndex %d, lastIncludedTerm %d",
		rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	return nil
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func Make(peers []*kvutil.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	rf.snapshot = nil
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCond = sync.NewCond(&sync.Mutex{})

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.electionTimer = time.NewTimer(time.Duration(rand.Int63n(int64(100 * time.Millisecond))))
	rf.heartbeatTimer = time.NewTimer(heartbeatInterval)

	go rf.ticker()
	go rf.applier()

	kvutil.RaftLogger.Printf("Server %d started up with peers %v", rf.me, rf.peers)
	return rf
}

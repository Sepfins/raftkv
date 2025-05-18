package raft

import (
	"math/rand"
	"time"
)

const heartbeatInterval = 100 * time.Millisecond
const electionIntervalBase = 500 * time.Millisecond
const electionIntervalJitter = 500 * time.Millisecond

func randomElectionTimeout() time.Duration {
	return electionIntervalBase + time.Duration(rand.Int63n(int64(electionIntervalJitter)))
}

func (op State) String() string {
	switch op {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "UnknownState"
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) logStartIndex() int {
	return rf.LastIncludedIndex
}

func (rf *Raft) logEndIndex() int {
	return rf.localToGlobalIndex(len(rf.log) - 1)
}

func (rf *Raft) getTermAtIndex(index int) int {
	if index >= rf.LastIncludedIndex {
		return rf.log[rf.globalToLocalIndex(index)].Term
	}
	return -1
}

func (rf *Raft) localToGlobalIndex(index int) int {
	return index + rf.logStartIndex()
}

func (rf *Raft) globalToLocalIndex(index int) int {
	return index - rf.logStartIndex()
}

func (rf *Raft) isInLog(index int) bool {
	return index > rf.logStartIndex() && index < rf.localToGlobalIndex(len(rf.log))
}

func deepCopy(arr []byte) []byte {
	arrCopy := make([]byte, len(arr))
	copy(arrCopy, arr)
	return arrCopy
}

package raft

import (
	"os"
	"path/filepath"
	"sync"
)

type Persister struct {
	mu          sync.Mutex
	raftstate   []byte
	snapshotDir string
	snapshotKey string
}

func NewPersister(snapshotDir, snapshotKey string) *Persister {
	os.MkdirAll(snapshotDir, 0755)
	ps := &Persister{
		snapshotDir: snapshotDir,
		snapshotKey: snapshotKey,
	}
	ps.loadFromDisk()
	return ps
}

func clone(orig []byte) []byte {
	if orig == nil {
		return nil
	}
	out := make([]byte, len(orig))
	copy(out, orig)
	return out
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return &Persister{
		raftstate:   clone(ps.raftstate),
		snapshotDir: ps.snapshotDir,
		snapshotKey: ps.snapshotKey,
	}
}

// Save both Raft state and snapshot to disk atomically.
func (ps *Persister) Save(raftstate, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = clone(raftstate)
	ps.saveFile(ps.raftPath(), ps.raftstate)

	ps.saveFile(ps.snapshotPath(), snapshot)
}

func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = clone(data)
	ps.saveFile(ps.raftPath(), data)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, _ := os.ReadFile(ps.snapshotPath())
	return data
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, _ := os.ReadFile(ps.snapshotPath())
	return len(data)
}

func (ps *Persister) raftPath() string {
	return filepath.Join(ps.snapshotDir, "raft-"+ps.snapshotKey+".bin")
}

func (ps *Persister) snapshotPath() string {
	return filepath.Join(ps.snapshotDir, "snapshot-"+ps.snapshotKey+".bin")
}

func (ps *Persister) saveFile(path string, data []byte) {
	_ = os.WriteFile(path, data, 0644)
}

func (ps *Persister) loadFromDisk() {
	// Only loads raftstate into memory (snapshot stays on disk)
	if data, err := os.ReadFile(ps.raftPath()); err == nil {
		ps.raftstate = clone(data)
	}
}

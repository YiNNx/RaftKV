package raft

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs EntryList
	d.Decode(&currentTerm)
	d.Decode(&voteFor)
	d.Decode(&logs)
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	if rf.logs.PrevIndex > 0 {
		rf.lastApplied = int64(rf.logs.PrevIndex)
		rf.commitIndex = rf.logs.PrevIndex
	}
	rf.snapshot = rf.persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.logMu.Lock()
	rf.snapshot = snapshot
	rf.logs.tryCutPrefix(index)
	rf.logMu.Unlock()
	rf.HighLightf("SNAPSHOT %d(%d)", rf.logs.PrevIndex, rf.logs.PrevTerm)
}

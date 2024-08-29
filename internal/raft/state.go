package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync/atomic"
	"time"
)

func getRandomElectionTimeout() time.Duration {
	ms := 800 + (rand.Int63() % 2000)
	return time.Duration(ms) * time.Millisecond
}

func getHeartbeatTime() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) grantVote(candidate int) {
	rf.voteFor = candidate
	rf.persist()
	rf.electionTicker.Reset(getRandomElectionTimeout())
}

func (rf *Raft) updateTerm(term int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
	}
}

func (rf *Raft) tryRemoveLogTail(start int) {
	rf.logs.tryCutSuffix(start)
	rf.persist()
}

func (rf *Raft) appendLog(command interface{}, term int) int {
	res := rf.logs.append(command, term)
	rf.persist()
	return res
}

func (rf *Raft) appendLogList(entries []Entry) {
	rf.logs.appendEntries(entries)
	rf.persist()
}

func (rf *Raft) updateLeader(leaderID int) {
	rf.leaderID = leaderID
}

func (rf *Raft) checkRespTerm(term int) bool {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	if term > rf.currentTerm {
		rf.becomeFollower(term)
		return false
	}
	return true
}

func (rf *Raft) getLastApplied() int {
	return int(atomic.LoadInt64(&rf.lastApplied))
}

func (rf *Raft) setLastApplied(n int) {
	atomic.StoreInt64(&rf.lastApplied, int64(n))
}

// >=
func (rf *Raft) getPriorityNum() int {
	return (len(rf.peers) + 1) / 2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (currentTerm int, lastLogTerm int, isLeader bool) {
	rf.stateMu.RLock()
	defer rf.stateMu.RUnlock()

	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	return int(rf.currentTerm), rf.logs.getLastTerm(), rf.leaderID == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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
	d := gob.NewDecoder(r)
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
	rf.HighLightf("SNAPSHOT %d(%d)", rf.logs.PrevIndex, rf.logs.PrevTerm)
	rf.logMu.Unlock()
}

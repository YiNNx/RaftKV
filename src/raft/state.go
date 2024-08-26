package raft

import (
	"math/rand"
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

func (rf *Raft) setSnapshot(snapshot []byte) {
	rf.persist()
	rf.snapshot = snapshot
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

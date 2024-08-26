package raft

import (
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
func (rf *Raft) GetState() (int, bool) {
	rf.stateMu.RLock()
	defer rf.stateMu.RUnlock()
	return int(rf.currentTerm), rf.leaderID == rf.me
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.applyTicker.C:
			func() {
				rf.logMu.RLock()
				defer rf.logMu.RUnlock()

				lastApplied := rf.getLastApplied()
				if rf.commitIndex == lastApplied {
					return
				}
				msgList := make([]ApplyMsg, rf.commitIndex-lastApplied)
				rf.Debugf("apply entry %d - %d", lastApplied+1, rf.commitIndex)
				for i := range msgList {
					entry := rf.logs.getEntry(rf.getLastApplied() + i + 1)
					msgList[i] = ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: entry.Index,
					}
				}
				rf.setLastApplied(rf.commitIndex)

				go func() {
					for _, msg := range msgList {
						rf.applyCh <- msg
					}
				}()
			}()
		}
	}
}

package raft

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// for lab config
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// log state
	logMu    *sync.RWMutex
	logs     EntryList
	snapshot []byte

	commitIndex int
	lastApplied int64

	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	// update after
	// 1. appendEntries failed, set start index - 1
	// 2. appendEntries succeeded, set end index + 1
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	// update after appendEntries succeed, set as end log index
	matchIndex []int

	// node state
	stateMu     *sync.RWMutex
	leaderID    int
	currentTerm int
	voteFor     int

	// flow control
	appendTrigger  chan int
	electionTicker *time.Ticker
	applyTicker    *time.Ticker
	stateCancel    context.CancelFunc
}

func NewRaftInstance(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        int(me),
		dead:      0,
		applyCh:   applyCh,

		stateMu:     &sync.RWMutex{},
		currentTerm: 0,
		voteFor:     -1,
		leaderID:    -1,

		logs:        NewLogList(),
		logMu:       &sync.RWMutex{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,

		appendTrigger:  make(chan int, 100),
		electionTicker: time.NewTicker(getRandomElectionTimeout()),
		applyTicker:    time.NewTicker(getHeartbeatTime()),
	}
	return rf
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
	var snapshot []byte
	d.Decode(&currentTerm)
	d.Decode(&voteFor)
	d.Decode(&logs)
	d.Decode(&snapshot)
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	rf.snapshot = snapshot
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	if rf.killed() || rf.me != rf.leaderID {
		return -1, -1, false
	}

	rf.logMu.Lock()

	defer rf.logMu.Unlock()

	term := rf.currentTerm
	index := rf.appendLog(command, term)
	rf.matchIndex[rf.me] = index
	isLeader := true

	rf.Debugf("START COMMAND %s", rf.logs.getEntry(index))
	rf.appendTrigger <- AllPeers

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.HighLightf("STOP")
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.applyCh)
	close(rf.appendTrigger)
	rf.stateCancel()
	rf.electionTicker.Stop()
	rf.applyTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := NewRaftInstance(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.snapshot != nil {
		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.logs.PrevTerm,
				SnapshotIndex: rf.logs.PrevIndex,
			}
		}()
	}

	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	rf.HighLightf("START")
	return rf
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

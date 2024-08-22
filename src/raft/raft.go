package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// for lab setting
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// persitent state
	currentTerm int64
	voteFor     int64
	logs        LogList

	// volatile state
	commitIndex int64
	lastApplied int64

	// volatile state for leader
	nextIndex  []int64
	matchIndex []int64

	// custom state

	// mutex for the server state machine
	stateMu *sync.Mutex
	// boardcast all goroutines that rf turn into a follower by close the chan
	// re init - being candidate / leader
	// close - being follower
	stateCancel context.CancelFunc
	leaderID    int64

	electionTimeout *time.Ticker
}

func NewRaftInstance(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:        &sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        int64(me),
		dead:      0,
		applyCh:   applyCh,

		currentTerm: 0,
		voteFor:     -1,
		logs:        InitLogList(),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  nil,
		matchIndex: nil,

		stateMu:  &sync.Mutex{},
		leaderID: -1,

		electionTimeout: time.NewTicker(getRandomElectionTimeout()),
	}
	return rf
}

func (rf *Raft) SetLeaderID(leaderID int64) {
	atomic.StoreInt64(&rf.leaderID, leaderID)
}

func (rf *Raft) GetLeaderID() int64 {
	return atomic.LoadInt64(&rf.leaderID)
}

func (rf *Raft) SetCurrentTerm(term int64) {
	atomic.StoreInt64(&rf.currentTerm, term)
}

func (rf *Raft) GetCurrentTerm() int64 {
	return atomic.LoadInt64(&rf.currentTerm)
}

func (rf *Raft) SetVoteFor(voteFor int64) {
	atomic.StoreInt64(&rf.voteFor, voteFor)
}

func (rf *Raft) GetVoteFor() int64 {
	return atomic.LoadInt64(&rf.voteFor)
}

func (rf *Raft) GetPriorityNum() int {
	return (len(rf.peers) + 1) / 2
}

func (rf *Raft) GetLogList() LogList {
	return rf.logs
}

func (rf *Raft) grantVote(candidate int64) {
	rf.SetVoteFor(candidate)
	rf.electionTimeout.Reset(getRandomElectionTimeout())
}

func (rf *Raft) updateTermInfo(term int64, termLeader int64) {
	if term != rf.GetCurrentTerm() {
		rf.SetCurrentTerm(term)
		rf.SetVoteFor(-1)
	}
	rf.SetLeaderID(termLeader)
}

var colors = []string{
	"\u001B[36m", "\u001B[32m", "\u001B[34m", "\u001B[35m", "\u001B[33m",
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	format = colors[rf.me%5] + fmt.Sprintf("[%d][term%d ld%d vote%d] ", rf.me, rf.currentTerm, rf.leaderID, rf.voteFor) + "\u001B[0m" + format
	DPrintf(format, a...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
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

	rf.becomeFollower(rf.currentTerm, -1)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

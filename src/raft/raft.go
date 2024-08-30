package raft

import (
	"context"
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

		leaderID:        -1,
		electionTimeout: time.NewTicker(getRandomTime()),
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
	rf.electionTimeout.Reset(getRandomTime())
}

func (rf *Raft) updateTermInfo(term int64, termLeader int64) {
	if term != rf.GetCurrentTerm() {
		rf.SetCurrentTerm(term)
		rf.SetVoteFor(-1)
	}
	rf.SetLeaderID(termLeader)
}

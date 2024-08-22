package raft

import (
	"context"
	"errors"
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

	// (3A, 3B, 3C)

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
	// re init - being candidator / leader
	// close - being follower
	followerCond chan struct{}
	leaderID     int64

	electionTimeout *time.Ticker
}

func NewRaftInstance(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	return &Raft{
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

		followerCond:    make(chan struct{}),
		leaderID:        -1,
		electionTimeout: time.NewTicker(getRandomTime()),
	}
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

func (rf *Raft) GrantVote(candidate int64) {
	rf.SetVoteFor(candidate)
	rf.electionTimeout.Reset(getRandomTime())
}

// turn into a follower with leader unawareness
func (rf *Raft) becomeFollower(term int64, termLeader int64) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.updateTermInfo(term, termLeader)
	select {
	case <-rf.followerCond:
		return
	default:
		close(rf.followerCond)
	}
}

func (rf *Raft) becomeCandidator() {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.followerCond = make(chan struct{})
	rf.SetLeaderID(-1)
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.electionTimeout.Reset(getRandomTime())
}

func (rf *Raft) becomeLeader() {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.SetLeaderID(rf.me)
}

func (rf *Raft) updateTermInfo(term int64, termLeader int64) {
	if term != rf.GetCurrentTerm() {
		rf.SetCurrentTerm(term)
		rf.SetVoteFor(-1)
	}
	rf.SetLeaderID(termLeader)
}

// start election as a candidate
func (rf *Raft) startElection() {
	for rf.killed() == false {
		voteResChan := make(chan RequestVoteReply, 100)
		voteGrantedNum := 0
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.GetLogList().lastLogIndex,
			LastLogTerm:  rf.GetLogList().lastLogTerm,
		}
		for peer := range rf.peers {
			reply := RequestVoteReply{}
			go func(peer int) {
				ok := rf.sendRequestVote(peer, &args, &reply)
				if ok {
					voteResChan <- reply
				}
			}(peer)
		}
		select {
		case <-rf.followerCond:
			return
		case <-rf.electionTimeout.C:
			break
		case voteRes := <-voteResChan:
			if voteRes.Term > rf.GetCurrentTerm() {
				rf.becomeFollower(voteRes.Term, -1)
				return
			}
			if voteRes.VoteGranted {
				voteGrantedNum++
			}
			if voteGrantedNum >= rf.GetPriorityNum() {
				rf.becomeLeader()
				rf.startHeartbeat()
				return
			}
		}
	}
}

func (rf *Raft) sendHeartbeatToPeers(ctx context.Context, ch chan AppendEntriesReply) {
	rf.sendEntriesToPeers(ctx, ch, nil)
}

func (rf *Raft) sendEntriesToPeers(ctx context.Context, ch chan AppendEntriesReply, command interface{}) {

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		prevLogIndex: rf.logs.lastLogIndex,
		prevLogTerm:  rf.logs.lastLogTerm,
		entries:      nil,
		leaderCommit: rf.commitIndex,
	}
	for peer := range rf.peers {
		go func(peer int) {
			reply, err := rf.sendAppendEntriesWithRetry(ctx, peer, args)
			if err != nil {
				ch <- *reply
			}
		}(peer)
	}
}

func (rf *Raft) sendAppendEntriesWithRetry(ctx context.Context, peer int, args AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, &args, &reply)
	if ok {
		return &reply, nil
	}

	for {
		select {
		case <-time.After(50 * time.Millisecond):
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if ok {
				return &reply, nil
			}
		case <-ctx.Done():
			return nil, errors.New("send entries time out")
		}
	}
}

func (rf *Raft) startHeartbeat() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appendEntriesResChan := make(chan AppendEntriesReply, 100)

	heartbeatTicker := time.NewTicker(getHeartbeatTime())

	rf.sendHeartbeatToPeers(ctx, appendEntriesResChan)
	for rf.killed() == false {
		select {
		case <-rf.followerCond:
			return
		case <-heartbeatTicker.C:
			rf.sendHeartbeatToPeers(ctx, appendEntriesResChan)
		case _ = <-appendEntriesResChan:
			// handle res
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimeout.C:
			rf.becomeCandidator()
			rf.startElection()
		}
	}
}

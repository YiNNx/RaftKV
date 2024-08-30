package raft

import (
	"context"
	"math/rand"
	"time"
)

// no need to set seed
func getRandomElectionTimeout() time.Duration {
	ms := 800 + (rand.Int63() % 2000)
	return time.Duration(ms) * time.Millisecond
}

func getHeartbeatTime() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) grantVote(candidate int) {
	rf.voteFor = candidate
	rf.electionTimeout.Reset(getRandomElectionTimeout())
}

func (rf *Raft) updateTermInfo(term int, termLeader int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
	}
	rf.leaderID = termLeader
}

// ----- FOLLOWER -----

// turn into a follower with leader unawareness
func (rf *Raft) becomeFollower(term int, termLeader int) (stateCtx context.Context) {
	rf.updateTermInfo(term, termLeader)

	if rf.stateCancel != nil {
		rf.stateCancel()
	}
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimeout.C:
			rf.runCandidate()
		}
	}
}

// ----- CANDIDATE -----

func (rf *Raft) becomeCandidate() (stateCtx context.Context) {
	rf.updateTermInfo(rf.currentTerm+1, -1)
	rf.grantVote(rf.me)
	rf.DPrintf("CANDIDATE")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) startElection() (candidateState context.Context, voteReqChan chan RequestVoteReq) {
	rf.stateMu.Lock()
	candidateState = rf.becomeCandidate()
	rf.stateMu.Unlock()

	voteReqArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLogList().lastLogIndex,
		LastLogTerm:  rf.getLogList().lastLogTerm,
	}
	voteReqChan = make(chan RequestVoteReq, len(rf.peers)-1)
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		voteReqChan <- RequestVoteReq{
			args: voteReqArgs,
			peer: peer,
		}
	}
	return candidateState, voteReqChan
}

func (rf *Raft) requestVote(req RequestVoteReq, voteResChan chan RequestVoteRes) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(req.peer, &req.args, &reply)
	if ok {
		voteResChan <- RequestVoteRes{peer: req.peer, reply: reply}
	}
}

// start election as a candidate
func (rf *Raft) runCandidate() {
	candidateState, voteReqChan := rf.startElection()
	voteRespChan := make(chan RequestVoteRes, len(rf.peers)-1)
	voteGrantedNum := 1

	for rf.killed() == false {
		select {
		case <-candidateState.Done():
			return
		case <-rf.electionTimeout.C:
			candidateState, voteReqChan = rf.startElection()
			voteRespChan = make(chan RequestVoteRes, len(rf.peers)-1)
			voteGrantedNum = 1
		case req := <-voteReqChan:
			go rf.requestVote(req, voteRespChan)
		case resp := <-voteRespChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				return
			}
			if resp.reply.VoteGranted {
				voteGrantedNum++
			}
			if voteGrantedNum >= rf.getPriorityNum() {
				rf.runLeader()
				return
			}
		}
	}
}

// ----- LEADER -----

func (rf *Raft) becomeLeader() (stateCtx context.Context) {
	rf.leaderID = rf.me
	rf.DPrintf("LEADER")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) broadcastEntries(ch chan AppendEntriesReq, _ interface{}) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.logs.lastLogIndex,
		PrevLogTerm:  rf.logs.lastLogTerm,
		Entries:      nil, // todo
		LeaderCommit: rf.commitIndex,
	}
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		ch <- AppendEntriesReq{args, peer}
	}
}

func (rf *Raft) broadcastHeartbeat(ch chan AppendEntriesReq) {
	rf.broadcastEntries(ch, nil)
}

func (rf *Raft) appendEntriesWithRetry(ctx context.Context, req AppendEntriesReq, respChan chan AppendEntriesRes) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
	if ok {
		respChan <- AppendEntriesRes{reply: reply, peer: req.peer}
	}
	ticker := time.NewTicker(getHeartbeatTime() / 3)
	for {
		select {
		case <-ticker.C: // todo
			ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
			if ok {
				respChan <- AppendEntriesRes{reply: reply, peer: req.peer}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) runLeader() {
	rf.stateMu.Lock()
	leaderState := rf.becomeLeader()
	rf.stateMu.Unlock()

	heartbeatTicker := time.NewTicker(getHeartbeatTime())
	appendReqChan := make(chan AppendEntriesReq, 100)
	appendRespChan := make(chan AppendEntriesRes, 100)

	go rf.broadcastHeartbeat(appendReqChan)

	for rf.killed() == false {
		select {
		case <-leaderState.Done():
			return
		case <-heartbeatTicker.C:
			go rf.broadcastHeartbeat(appendReqChan)
		case req := <-appendReqChan:
			go rf.appendEntriesWithRetry(leaderState, req, appendRespChan)
		case resp := <-appendRespChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				return
			}
		}
	}
}

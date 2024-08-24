package raft

import (
	"context"
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
	rf.electionTicker.Reset(getRandomElectionTimeout())
}

func (rf *Raft) updateTerm(term int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
	}
}

func (rf *Raft) updateLeader(leaderID int) {
	rf.leaderID = leaderID
}

// ----- FOLLOWER -----

// turn into a follower with leader unawareness
func (rf *Raft) becomeFollower(term int) (stateCtx context.Context) {
	rf.updateTerm(term)

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
		case <-rf.electionTicker.C:
			rf.runCandidate()
		}
	}
}

// ----- CANDIDATE -----

func (rf *Raft) becomeCandidate() (stateCtx context.Context) {
	rf.updateTerm(rf.currentTerm + 1)
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

	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	voteReqArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.logs.getLastIndex(),
		LastLogTerm:  rf.logs.getLastTerm(),
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
		case <-rf.electionTicker.C:
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
	rf.appendTrigger = make(chan int, 100)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs.getLastIndex() + 1
	}

	rf.DPrintf("LEADER")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

const AllPeers = -1

func (rf *Raft) getPeerIndexList(peer int) []int {
	peers := []int{}
	if peer == AllPeers {
		for peer := range rf.peers {
			if peer == int(rf.me) {
				continue
			}
			peers = append(peers, peer)
		}
	} else {
		peers = append(peers, peer)
	}
	return peers
}

func (rf *Raft) appendEntries(ch chan AppendEntriesReq, peer int, heartbeat bool) {
	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	for _, peer := range rf.getPeerIndexList(peer) {
		startIndex := rf.nextIndex[peer]
		endIndex := rf.logs.getLastIndex()
		prevLog := rf.logs.getEntry(startIndex - 1)

		var entries []Entry
		if !heartbeat && startIndex <= endIndex {
			entries = rf.logs.getSlice(startIndex, endIndex)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		ch <- AppendEntriesReq{
			args:       args,
			peer:       peer,
			startIndex: startIndex,
			endIndex:   endIndex,
		}
	}
}

func (rf *Raft) sendEntries(req AppendEntriesReq, respChan chan AppendEntriesRes) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
	if ok {
		respChan <- AppendEntriesRes{
			reply:      reply,
			peer:       req.peer,
			startIndex: req.startIndex,
			endIndex:   req.endIndex,
		}
	}
}

func (rf *Raft) runLeader() {
	rf.stateMu.Lock()
	rf.logMu.Lock()
	leaderState := rf.becomeLeader()
	rf.logMu.Unlock()
	rf.stateMu.Unlock()

	heartbeatTicker := time.NewTicker(getHeartbeatTime())
	defer heartbeatTicker.Stop()

	reqChan := make(chan AppendEntriesReq, 100)
	respChan := make(chan AppendEntriesRes, 100)

	go rf.appendEntries(reqChan, AllPeers, true)

	for rf.killed() == false {
		select {
		case <-leaderState.Done():
			close(rf.appendTrigger)
			return
		case <-heartbeatTicker.C:
			go rf.appendEntries(reqChan, AllPeers, true)
		case peer := <-rf.appendTrigger:
			go rf.appendEntries(reqChan, peer, false)
		case req := <-reqChan:
			go rf.sendEntries(req, respChan)
		case resp := <-respChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				close(rf.appendTrigger)
				return
			}
			go func() {
				rf.stateMu.RLock()
				defer rf.stateMu.RUnlock()

				rf.logMu.Lock()
				defer rf.logMu.Unlock()

				if resp.reply.Success {
					rf.nextIndex[resp.peer] = max(rf.nextIndex[resp.peer], resp.endIndex+1)
					rf.matchIndex[resp.peer] = max(rf.matchIndex[resp.peer], resp.endIndex)
					rf.commitIndex = rf.calculateCommitIndex(resp.endIndex)
					if resp.endIndex >= resp.startIndex {
						rf.DPrintf("append succeed: %+v commit index: %+v", resp, rf.commitIndex)
					}
				} else {
					rf.nextIndex[resp.peer] = resp.startIndex - 1
					rf.appendTrigger <- resp.peer
				}
			}()
		}
	}
}

// improve: cache?
func (rf *Raft) calculateCommitIndex(lastUpdateIndex int) int {
	if rf.commitIndex > lastUpdateIndex || rf.logs.getEntry(lastUpdateIndex).Term != rf.currentTerm {
		return rf.commitIndex
	}
	sum := 0
	for peer := range rf.peers {
		if rf.matchIndex[peer] >= lastUpdateIndex {
			sum++
		}
		if sum >= rf.getPriorityNum() {
			return lastUpdateIndex
		}
	}
	return rf.commitIndex
}

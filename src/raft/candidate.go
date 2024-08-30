package raft

import "context"

type RequestVoteReq struct {
	args RequestVoteArgs
	peer int
}

type RequestVoteRes struct {
	reply RequestVoteReply
	peer  int
}

func (rf *Raft) becomeCandidate() (stateCtx context.Context) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.updateTermInfo(rf.currentTerm+1, -1)
	rf.grantVote(rf.me)
	rf.DPrintf("CANDIDATE")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) startElection() (candidateState context.Context, voteReqChan chan RequestVoteReq) {
	candidateState = rf.becomeCandidate()

	voteReqArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.GetLogList().lastLogIndex,
		LastLogTerm:  rf.GetLogList().lastLogTerm,
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
	voteResChan := make(chan RequestVoteRes, len(rf.peers)-1)
	voteGrantedNum := 1

	for rf.killed() == false {
		select {
		case <-candidateState.Done():
			return
		case <-rf.electionTimeout.C:
			candidateState, voteReqChan = rf.startElection()
			voteResChan = make(chan RequestVoteRes, len(rf.peers)-1)
			voteGrantedNum = 1
		case req := <-voteReqChan:
			go rf.requestVote(req, voteResChan)
		case voteRes := <-voteResChan:
			if voteRes.reply.Term > rf.GetCurrentTerm() {
				rf.becomeFollower(voteRes.reply.Term, -1)
				return
			}
			if voteRes.reply.VoteGranted {
				voteGrantedNum++
			}
			if voteGrantedNum >= rf.GetPriorityNum() {
				rf.runLeader()
				return
			}
		}
	}
}

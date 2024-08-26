package raft

import "context"

// ----- CANDIDATE -----

func (rf *Raft) becomeCandidate() (stateCtx context.Context) {
	rf.updateTerm(rf.currentTerm + 1)
	rf.updateLeader(-1)
	rf.grantVote(rf.me)
	rf.HighLightf("CANDIDATE")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) startElection() (candidateState context.Context, voteReqChan chan VoteReq) {
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

	voteReqChan = make(chan VoteReq, len(rf.peers)-1)
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		voteReqChan <- VoteReq{
			args: voteReqArgs,
			peer: peer,
		}
	}
	return candidateState, voteReqChan
}

func (rf *Raft) requestVote(req VoteReq, voteResChan chan VoteRes) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(req.peer, &req.args, &reply)
	if ok {
		voteResChan <- VoteRes{peer: req.peer, reply: reply}
	}
}

// start election as a candidate
func (rf *Raft) runCandidate() {
	candidateState, voteReqChan := rf.startElection()
	voteRespChan := make(chan VoteRes, len(rf.peers)-1)
	voteGrantedNum := 1

	for rf.killed() == false {
		select {
		case <-candidateState.Done():
			return
		case <-rf.electionTicker.C:
			candidateState, voteReqChan = rf.startElection()
			voteRespChan = make(chan VoteRes, len(rf.peers)-1)
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

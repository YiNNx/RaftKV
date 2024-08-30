package raft

import "context"

func (rf *Raft) becomeCandidate() (stateCtx context.Context) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.updateTermInfo(rf.currentTerm+1, -1)
	rf.grantVote(rf.me)

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

// start election as a candidate
func (rf *Raft) runCandidate() {
	for rf.killed() == false {
		candidateState := rf.becomeCandidate()

		voteReqArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.GetLogList().lastLogIndex,
			LastLogTerm:  rf.GetLogList().lastLogTerm,
		}

		voteReqChan := make(chan int, len(rf.peers)-1)
		for peer := range rf.peers {
			if peer == int(rf.me) {
				continue
			}
			voteReqChan <- peer
		}

		voteResChan := make(chan RequestVoteReply, len(rf.peers)-1)
		voteGrantedNum := 0

		select {
		case <-candidateState.Done():
			return
		case <-rf.electionTimeout.C:
			break
		case peer := <-voteReqChan:
			go func() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &voteReqArgs, &reply)
				if ok {
					voteResChan <- reply
				}
			}()
		case voteRes := <-voteResChan:
			if voteRes.Term > rf.GetCurrentTerm() {
				rf.becomeFollower(voteRes.Term, -1)
				return
			}
			if voteRes.VoteGranted {
				voteGrantedNum++
			}
			if voteGrantedNum >= rf.GetPriorityNum() {
				rf.runLeader()
				return
			}
		}
	}
}

package raft

import "context"

// turn into a follower with leader unawareness
func (rf *Raft) becomeFollower(term int64, termLeader int64) (stateCtx context.Context) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.updateTermInfo(term, termLeader)

	rf.stateCancel()
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

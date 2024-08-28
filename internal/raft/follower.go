package raft

import "context"

// ----- FOLLOWER -----

// turn into a follower with leader unawareness
func (rf *Raft) becomeFollower(term int) (stateCtx context.Context) {
	rf.updateTerm(term)
	rf.updateLeader(-1)

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

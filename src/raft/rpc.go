package raft

func (rf *Raft) checkReqTerm(term int) (bool, *int) {
	rf.stateMu.RLock()
	defer rf.stateMu.RUnlock()
	if term < rf.currentTerm {
		return false, &rf.currentTerm
	}
	return true, nil
}

func (rf *Raft) checkRespTerm(term int) bool {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	if term > rf.currentTerm {
		rf.becomeFollower(term)
		return false
	}
	return true
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if ok, curTerm := rf.checkReqTerm(args.Term); !ok {
		rf.DPrintf("refuse vote for %d", args.CandidateID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log,
	// grant vote
	if rf.voteFor == -1 &&
		((args.LastLogTerm == rf.logs.getLastTerm() && args.LastLogIndex >= rf.logs.getLastIndex()) ||
			args.LastLogTerm > rf.logs.getLastTerm()) {
		rf.grantVote(args.CandidateID)
		reply.VoteGranted = true
	}
	if reply.VoteGranted {
		rf.DPrintf("vote for %d", args.CandidateID)
	} else {
		rf.DPrintf("refuse vote for %d", args.CandidateID)
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok, curTerm := rf.checkReqTerm(args.Term); !ok {
		rf.DPrintf("refuse append by %d", args.LeaderID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.LeaderID != rf.leaderID {
		rf.updateLeader(args.LeaderID)
	}

	rf.logMu.Lock()
	defer rf.logMu.Unlock()

	prevLog := rf.logs.getEntry(args.PrevLogIndex)
	if prevLog == nil || prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.DPrintf("prev log not match")
		return
	}

	rf.electionTicker.Reset(getRandomElectionTimeout())
	reply.Success = true
	reply.Term = rf.currentTerm

	if len(args.Entries) == 0 {
		rf.logs.removeTail(args.PrevLogIndex + 1)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastIndex())
			rf.DPrintf("update commit index: %d", rf.commitIndex)
		}
		return
	}

	leaderEndLog := args.Entries[len(args.Entries)-1]
	leaderStartLog := args.Entries[0]
	endLog := rf.logs.getEntry(leaderEndLog.Index)

	rf.DPrintf("append logs: %d to %d", leaderStartLog.Index, leaderEndLog.Index)

	if endLog == nil || endLog.Term != leaderEndLog.Term {
		rf.logs.removeTail(leaderStartLog.Index)
		rf.logs.appendEntries(args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.getLastIndex())
		rf.DPrintf("update commit index: %d", rf.commitIndex)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

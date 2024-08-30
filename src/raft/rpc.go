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
		rf.becomeFollower(term, -1)
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
		rf.becomeFollower(args.Term, -1)
	}

	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log,
	// grant vote
	if rf.voteFor == -1 &&
		((args.LastLogTerm == rf.getLogList().lastLogTerm && args.LastLogIndex >= rf.getLogList().lastLogIndex) ||
			args.LastLogTerm > rf.getLogList().lastLogTerm) {
		rf.grantVote(args.CandidateID)
		reply.VoteGranted = true
		rf.DPrintf("vote for %d", args.CandidateID)
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
		rf.DPrintf("refuse append entries for %d", args.LeaderID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, args.LeaderID)
	}

	rf.electionTimeout.Reset(getRandomElectionTimeout())

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

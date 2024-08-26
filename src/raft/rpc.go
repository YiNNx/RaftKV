package raft

func (rf *Raft) checkReqTerm(term int) (bool, *int) {
	rf.stateMu.RLock()
	defer rf.stateMu.RUnlock()
	curTerm := rf.currentTerm
	if term < curTerm {
		return false, &curTerm
	}
	return true, nil
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
		rf.Debugf("refuse vote for %d", args.CandidateID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

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
		rf.Debugf("vote for %d", args.CandidateID)
	} else {
		rf.Debugf("refuse vote for %d", args.CandidateID)
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
		rf.Debugf("refuse append by %d", args.LeaderID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	rf.logMu.Lock()
	defer rf.logMu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.LeaderID != rf.leaderID {
		rf.updateLeader(args.LeaderID)
	}

	prevLog := rf.logs.getEntry(args.PrevLogIndex)
	if prevLog == nil || prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.Debugf("prev log %s not match leader's [%d(%d)]", prevLog, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	if len(args.Entries) != 0 {
		leaderEndLog := args.Entries[len(args.Entries)-1]
		endLog := rf.logs.getEntry(leaderEndLog.Index)
		if endLog == nil || endLog.Term != leaderEndLog.Term {
			rf.tryRemoveLogTail(args.PrevLogIndex + 1)
			rf.appendLogList(args.Entries)
		}
		rf.Debugf("append logs: %d to %d", args.PrevLogIndex+1, leaderEndLog.Index)
	}

	if args.LeaderCommit > rf.commitIndex {
		old := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if old != rf.commitIndex {
			rf.Debugf("update commit index %d", rf.commitIndex)
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.electionTicker.Reset(getRandomElectionTimeout())
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if ok, curTerm := rf.checkReqTerm(args.Term); !ok {
		rf.Debugf("refuse to install snapshot by %d", args.LeaderID)
		reply.Term = *curTerm
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	rf.logMu.Lock()
	defer rf.logMu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.LeaderID != rf.leaderID {
		rf.updateLeader(args.LeaderID)
	}
	reply.Term = rf.currentTerm

	if rf.getLastApplied() > args.LastIncludedIndex {
		return
	}

	rf.snapshot = args.Snapshot
	go func() {
		rf.HighLightf("INSTALL SNAPSHOT %d[%d]", args.LastIncludedIndex, args.LastIncludedTerm)
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	rf.logs.tryCutPrefix(args.LastIncludedIndex)
	rf.setLastApplied(args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	if rf.logs.PrevIndex == args.LastIncludedIndex && rf.logs.PrevTerm == args.LastIncludedTerm {
		return
	}
	rf.logs.Logs = []Entry{}
	rf.logs.PrevIndex = args.LastIncludedIndex
	rf.logs.PrevTerm = args.LastIncludedTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

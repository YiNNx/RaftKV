package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int64

	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int64

	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// always refuse to handle the request if the term is expired
	if args.Term < rf.GetCurrentTerm() {
		reply.Term = rf.GetCurrentTerm()
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.GetCurrentTerm() {
		rf.becomeFollower(args.Term, -1)
	}

	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log,
	// grant vote
	if rf.GetVoteFor() != -1 &&
		((args.LastLogTerm == rf.GetCurrentTerm() && args.LastLogIndex >= rf.GetLogList().lastLogIndex) ||
			args.LastLogTerm > rf.GetCurrentTerm()) {
		rf.GrantVote(args.CandidateID)
		reply.VoteGranted = true
	}
	reply.Term = rf.GetCurrentTerm()
}

type AppendEntriesArgs struct {
	Term     int64
	LeaderID int64

	prevLogIndex int64
	prevLogTerm  int64
	entries      []Entry
	leaderCommit int64
}

type AppendEntriesReply struct {
	Term int64

	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).

	// always refuse to handle the request if the term is expired
	if args.Term < rf.GetCurrentTerm() {
		reply.Success = false
		reply.Term = rf.GetCurrentTerm()
		return
	}

	if args.Term == rf.GetCurrentTerm() && rf.me == rf.leaderID {
		panic("brain split!!")
	}

	rf.becomeFollower(args.Term, args.LeaderID)
	rf.electionTimeout.Reset(getRandomTime())

	reply.Success = true
	reply.Term = rf.GetCurrentTerm()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

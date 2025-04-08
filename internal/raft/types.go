package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type VoteReq struct {
	args RequestVoteArgs
	peer string
}

type VoteRes struct {
	reply RequestVoteReply
	peer  string
}

type EntriesReq struct {
	args       AppendEntriesArgs
	peer       string
	startIndex int
	endIndex   int
}

type EntriesRes struct {
	reply      AppendEntriesReply
	peer       string
	startIndex int
	endIndex   int
}

type SnapshotReq struct {
	args InstallSnapshotArgs
	peer string
}

type SnapshotRes struct {
	reply     InstallSnapshotReply
	peer      string
	lastIndex int
}

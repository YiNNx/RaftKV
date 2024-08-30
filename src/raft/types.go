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

type Entry struct {
	Term    int64
	Command interface{}
}

type LogList struct {
	logs         []Entry
	lastLogIndex int64
	lastLogTerm  int64
}

func InitLogList() LogList {
	return LogList{
		logs:         make([]Entry, 1, 100),
		lastLogIndex: 0,
		lastLogTerm:  0,
	}
}

func (l *LogList) IsEmpty() bool {
	return l.lastLogIndex == 0
}

func (l *LogList) AppendLog(command interface{}, index int, term int) {
}

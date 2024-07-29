package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	Map    = 1
	Reduce = 2
	Finish = 3
)

type ArgsAssignMapTask struct {
	WorkerID int
}

type ReplyAssignMapTask struct {
	TaskID int
	Task   *MapTask
}

type ArgsAssignReduceTask struct {
	WorkerID int
}

type ReplyAssignReduceTask struct {
	TaskID int
	Task   *ReduceTask
}

type ArgsFinishMapTask struct {
	WorkerID int
	TaskID   int
}

type ReplyFinishMapTask struct {
}

type ArgsFinishReduceTask struct {
	WorkerID int
	TaskID   int
}

type ReplyFinishReduceTask struct {
}

type ArgsRegisterWorker struct {
}

type ReplyRegisterWorker struct {
	WorkerID int
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

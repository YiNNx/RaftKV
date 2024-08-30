package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	OpID  int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

type FinishArgs struct {
	OpID  int64
}

type FinishReply struct {
}
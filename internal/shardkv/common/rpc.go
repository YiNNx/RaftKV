package common

// Put or Append
type PutAppendArgs struct {
	OpID     int64
	ClientID string

	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	OpID     int64
	ClientID string

	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type PullArgs struct {
	ConfigVersion int
	ShardId       int
}

type PullReply struct {
	Storage map[string]string
	Err     Err
}

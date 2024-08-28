package common

type Request struct {
	OpID string
	Args interface{}
}

type Response struct {
	Err   Err
	Reply interface{}
}

type PutAppendArgs struct {
	Key   string
	Value string
}

type PutAppendReply struct {
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

type FinishArgs struct {
	OpID string
}

type FinishReply struct {
	Err Err
}

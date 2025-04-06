package kvserver

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrSessionExpired = "SessionExpired"
	ErrTimeout        = "ErrTimeout"
)

type Err string

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

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

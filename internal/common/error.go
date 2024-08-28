package common

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrSessionExpired = "session expired"
	ErrTimeout        = "err timeout"
)

type Err string

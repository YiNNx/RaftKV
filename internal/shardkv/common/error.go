package common

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrServerKilled     = "ErrWrongLeader"
	ErrTimeout          = "ErrTimeout"
	ErrInvalidOperation = "ErrInvalidOperation"
	ErrConfigVersion    = "ErrConfigVersion"
)

type Err string

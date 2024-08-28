package kvraft

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Highlightf1(format string, a ...interface{}) {
	format = "\033[38;5;2m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

func Highlightf2(format string, a ...interface{}) {
	format = "\033[38;5;6m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

var colors = []string{
	"\033[38;5;2m",
	"\033[38;5;45m",
	"\033[38;5;6m",
	"\033[38;5;3m",
	"\033[38;5;204m",
	"\033[38;5;111m",
	"\033[38;5;184m",
	"\033[38;5;69m",
}

// note: the debug printf will cause data race
// but it's ok cause it's used for *debug* :)
func (kv *KVServer) Debugf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	prefix := fmt.Sprintf("[%d][kv]", kv.me)
	prefix = colors[kv.me] + prefix + "\033[39;49m"
	format = prefix + " " + format
	DPrintf(format, a...)
}

func (kv *KVServer) HighLightf(format string, a ...interface{}) {
	format = colors[kv.me] + format + "\033[39;49m"
	kv.Debugf(format, a...)
}

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type Op struct {
	OpID string
	Typ  OpType
	Args interface{}
}

func (op *Op) String() string {
	return fmt.Sprintf("[%s] %s %s", op.OpID[:4], op.Typ, op.Args)
}

func NewOp(opID string, opType OpType, args interface{}) Op {
	return Op{
		OpID: opID,
		Typ:  opType,
		Args: args,
	}
}

type OpRes struct {
	Reply interface{}
	Err   Err
}

func NewOpRes(err Err, reply interface{}) OpRes {
	return OpRes{
		Reply: reply,
		Err:   err,
	}
}

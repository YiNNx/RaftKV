package common

import "fmt"

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

package kvserver

import (
	"encoding/gob"
	"sync/atomic"

	"github.com/google/uuid"

	"raftkv/pkg/rpc"
)

type Clerk struct {
	servers     []*rpc.ClientEnd
	cacheLeader uint64
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(TransactionArgs{})
	gob.Register(TransactionReply{})
	gob.Register(CommitArgs{})
	gob.Register(CommitReply{})
	gob.Register(AbortArgs{})
	gob.Register(AbortReply{})
	return ck
}

func (ck *Clerk) MustCall(svcMeth string, args interface{}) interface{} {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := uuid.New().String()
	for {
		DPrintf("[client] try call %d %s %+v", i, svcMeth, args)
		var res Response
		i = i % uint64(len(ck.servers))
		if ck.servers[i].Call(svcMeth, &Request{
			OpID: opID,
			Args: args,
		}, &res) {
			DPrintf("[client] got res from %d: [%s %+v], res %+v", i, svcMeth, args, res)
			err := res.Err
			if err == OK {
				atomic.StoreUint64(&ck.cacheLeader, i)
				return res.Reply
			}
			if err == ErrKeyLocked || err == ErrTxnConflict {
				atomic.StoreUint64(&ck.cacheLeader, i)
				return res.Err
			}
		}
		if res.Err == ErrWrongLeader {
			i++
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := ck.MustCall("KVServer.Get", &args)
	return reply.(GetReply).Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
	}
	ck.MustCall("KVServer."+op, &args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// BeginTransaction starts a new transaction
func (ck *Clerk) BeginTransaction() string {
	reply := ck.MustCall("KVServer.BeginTransaction", nil)
	return reply.(TransactionReply).TxnID
}

// TxnGet gets a value within a transaction
func (ck *Clerk) TxnGet(txnID string, key string) string {
	args := TransactionArgs{
		TxnID: txnID,
		Key:   key,
	}
	reply := ck.MustCall("KVServer.TxnGet", &args)
	return reply.(TransactionReply).Value
}

// TxnPut puts a value within a transaction
func (ck *Clerk) TxnPut(txnID string, key string, value string) {
	args := TransactionArgs{
		TxnID: txnID,
		Key:   key,
		Value: value,
	}
	ck.MustCall("KVServer.TxnPut", &args)
}

// CommitTransaction commits a transaction
func (ck *Clerk) CommitTransaction(txnID string) bool {
	args := CommitArgs{
		TxnID: txnID,
	}
	reply := ck.MustCall("KVServer.CommitTransaction", &args)
	return reply.(CommitReply).Success
}

// AbortTransaction aborts a transaction
func (ck *Clerk) AbortTransaction(txnID string) bool {
	args := AbortArgs{
		TxnID: txnID,
	}
	reply := ck.MustCall("KVServer.AbortTransaction", &args)
	return reply.(AbortReply).Success
}

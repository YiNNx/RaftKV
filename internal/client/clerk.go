package client

import (
	"sync/atomic"

	"github.com/google/uuid"

	"raftkv/internal/common"
	"raftkv/internal/util"
	"raftkv/pkg/rpc"
)

type Clerk struct {
	servers     []*rpc.ClientEnd
	cacheLeader uint64
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) TryCall(svcMeth string, args interface{}) interface{} {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := uuid.New().String()
	// DPrintf("[client] try call [%s] %s %+v", opID[:4], svcMeth, args)
	for {
		var res common.Response
		i = i % uint64(len(ck.servers))
		if ck.servers[i].Call(svcMeth, &common.Request{
			OpID: opID,
			Args: args,
		}, &res) {
			if res.Err != common.ErrWrongLeader {
				util.DPrintf("[client] got res from %d: [%s %+v], res %+v", i, svcMeth, args, res)
			}
			err := res.Err
			if err == common.OK {
				atomic.StoreUint64(&ck.cacheLeader, i)
				return res.Reply
			}
		}
		i++
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
	args := common.GetArgs{Key: key}
	reply := ck.TryCall("KVServer.Get", &args)
	return reply.(common.GetReply).Value
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
	args := common.PutAppendArgs{
		Key:   key,
		Value: value,
	}
	ck.TryCall("KVServer."+op, &args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import (
	"sync/atomic"

	"github.com/google/uuid"

	"6.5840/labrpc"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	cacheLeader uint64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) TryCall(svcMeth string, args interface{}) interface{} {
	i := atomic.LoadUint64(&ck.cacheLeader)
	DPrintf("call: %s %+v", svcMeth, args)
	opID := uuid.New().String()
	for {
		var res Response
		i = i % uint64(len(ck.servers))
		if ck.servers[i].Call(svcMeth, &Request{
			OpID: opID,
			Args: args,
		}, &res) {
			err := res.Err
			if err == OK {
				atomic.StoreUint64(&ck.cacheLeader, i)
				return res.Reply
			}
			DPrintf("call failed: %s", err)
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
	args := GetArgs{Key: key}
	reply := ck.TryCall("KVServer.Get", &args)
	DPrintf("ok: Get %+v - %+v", args, reply)
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
	ck.TryCall("KVServer."+op, &args)
	DPrintf("ok: %s %+v - %+v", op, args, nil)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

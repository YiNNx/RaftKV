package client

//
// Shardctrler clerk.
//

import (
	"encoding/gob"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"raftkv/internal/shardctl/common"
	"raftkv/pkg/rpc"
)

type Clerk struct {
	servers      []*rpc.ClientEnd
	clientID     string
	commandIndex int64
	cacheLeader  uint64
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	gob.Register(common.JoinArgs{})
	gob.Register(common.JoinReply{})
	gob.Register(common.LeaveArgs{})
	gob.Register(common.LeaveReply{})
	gob.Register(common.MoveArgs{})
	gob.Register(common.MoveReply{})
	gob.Register(common.QueryArgs{})
	gob.Register(common.QueryReply{})

	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = uuid.NewString()
	return ck
}

func (ck *Clerk) Query(num int) common.Config {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := atomic.AddInt64(&ck.commandIndex, 1)
	args := &common.QueryArgs{
		OpID:     opID,
		ClientID: ck.clientID,
		Num:      num,
	}
	for {
		i = i % uint64(len(ck.servers))
		var reply common.QueryReply
		// DPrintf("clerk Query %+v to %d", args, i)
		ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err == common.OK {
			atomic.StoreUint64(&ck.cacheLeader, i)
			return reply.Config
		}
		i++
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := atomic.AddInt64(&ck.commandIndex, 1)
	args := &common.JoinArgs{
		OpID:     opID,
		ClientID: ck.clientID,
		Servers:  servers,
	}
	for {
		i = i % uint64(len(ck.servers))
		var reply common.JoinReply
		// DPrintf("clerk Join %+v to %d", args, i)
		ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == common.OK {
			atomic.StoreUint64(&ck.cacheLeader, i)
			return
		}
		i++
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := atomic.AddInt64(&ck.commandIndex, 1)
	args := &common.LeaveArgs{
		OpID:     opID,
		ClientID: ck.clientID,
		GIDs:     gids,
	}
	for {
		i = i % uint64(len(ck.servers))
		var reply common.LeaveReply
		// DPrintf("clerk Leave %+v to %d", args, i)
		ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == common.OK {
			atomic.StoreUint64(&ck.cacheLeader, i)
			return
		}
		i++
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	i := atomic.LoadUint64(&ck.cacheLeader)
	opID := atomic.AddInt64(&ck.commandIndex, 1)
	args := &common.MoveArgs{
		OpID:     opID,
		ClientID: ck.clientID,
		Shard:    shard,
		GID:      gid,
	}
	for {
		i = i % uint64(len(ck.servers))
		var reply common.MoveReply
		// DPrintf("clerk Move %+v to %d", args, i)
		ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err == common.OK {
			atomic.StoreUint64(&ck.cacheLeader, i)
			return
		}
		i++
		time.Sleep(100 * time.Millisecond)
	}
}

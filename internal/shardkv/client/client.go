package client

import (
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	shardctl_client "raftkv/internal/shardctl/client"
	shardctl_common "raftkv/internal/shardctl/common"
	"raftkv/internal/shardkv/common"
	"raftkv/internal/shardkv/util"
	"raftkv/pkg/rpc"
)

type Clerk struct {
	sm       *shardctl_client.Clerk
	configMu *sync.RWMutex
	config   shardctl_common.Config
	make_end func(string) *rpc.ClientEnd

	clientID     string
	commandIndex int64
	cacheLeader  uint64
}

func MakeClerk(ctrlers []*rpc.ClientEnd) *Clerk {
	gob.Register(common.Op{})
	gob.Register(common.PutAppendArgs{})
	gob.Register(common.GetArgs{})
	gob.Register(common.GetReply{})

	ck := new(Clerk)
	ck.sm = shardctl_client.MakeClerk(ctrlers)
	ck.make_end = rpc.MakeClientEnd
	ck.clientID = uuid.NewString()
	ck.config = ck.sm.Query(-1)
	ck.configMu = new(sync.RWMutex)
	return ck
}

func (ck *Clerk) SetConfig(c shardctl_common.Config) {
	ck.configMu.Lock()
	defer ck.configMu.Unlock()
	ck.config = c
}

func (ck *Clerk) GetConfig() shardctl_common.Config {
	ck.configMu.RLock()
	defer ck.configMu.RUnlock()
	return ck.config
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := common.GetArgs{
		OpID:     atomic.AddInt64(&ck.commandIndex, 1),
		ClientID: ck.clientID,
		Key:      key,
	}
	for {
		shard := common.Key2shard(key)
		gid := ck.GetConfig().Shards[shard]
		if servers, ok := ck.GetConfig().Groups[gid]; ok {
			// try each server for the shard.
			for si := atomic.LoadUint64(&ck.cacheLeader); ; si = (si + 1) % uint64(len(servers)) {
				util.DPrintf("clerk to %d-%d Get %+v ", gid, si, args)
				srv := ck.make_end(servers[si])
				var reply common.GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				util.DPrintf("clerk res %+v %+v", ok, reply)
				if ok && (reply.Err == common.OK || reply.Err == common.ErrNoKey) {
					atomic.StoreUint64(&ck.cacheLeader, si)
					return reply.Value
				}
				if ok && (reply.Err == common.ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.SetConfig(ck.sm.Query(-1))
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := common.PutAppendArgs{
		OpID:     atomic.AddInt64(&ck.commandIndex, 1),
		ClientID: ck.clientID,
		Key:      key,
		Value:    value,
		Op:       op,
	}

	for {
		shard := common.Key2shard(key)
		gid := ck.GetConfig().Shards[shard]
		if servers, ok := ck.GetConfig().Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				util.DPrintf("clerk to %d-%d PutAppend %+v ", gid, si, args)
				srv := ck.make_end(servers[si])
				var reply common.PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				util.DPrintf("clerk res %+v", reply)
				if ok && reply.Err == common.OK {
					return
				}
				if ok && reply.Err == common.ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.SetConfig(ck.sm.Query(-1))
		util.DPrintf("ck config %+v ", ck.config)

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

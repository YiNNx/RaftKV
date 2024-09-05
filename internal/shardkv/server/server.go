package server

import (
	"context"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/internal/raft"
	shardctl_client "raftkv/internal/shardctl/client"
	shardctl_common "raftkv/internal/shardctl/common"
	"raftkv/internal/shardkv/common"
	repo "raftkv/internal/shardkv/repository"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

type Op struct {
	OpID     int64
	ClientID string
	Typ      OpType
	Args     interface{}
}

type OpType string

const (
	OpGet          OpType = "Get"
	OpPut          OpType = "Put"
	OpAppend       OpType = "Append"
	OpConfigChange OpType = "ConfigChange"
	OpAddShard     OpType = "AddShard"
)

type AddShard struct {
	Shard   int
	Storage map[string]string
}

type ShardKV struct {
	me      int
	ctx     context.Context
	cancel  context.CancelFunc
	opIndex int64
	status  int32

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	gid int

	configMu   *sync.RWMutex
	prevConfig shardctl_common.Config
	config     shardctl_common.Config
	make_end   func(string) *rpc.ClientEnd
	ctrlers    []*rpc.ClientEnd
	mck        *shardctl_client.Clerk

	mu           *sync.RWMutex
	notifier     *sync.Map //map[string]chan interface{}
	duplicatedOp *sync.Map
	repo         *repo.ShardRepositery
}

func (kv *ShardKV) GetConfig() shardctl_common.Config {
	kv.configMu.RLock()
	defer kv.configMu.RUnlock()
	return kv.config
}

func (kv *ShardKV) Status() bool {
	return atomic.LoadInt32(&kv.status) == 0
}

func (kv *ShardKV) SetStatus(status bool) {
	if status {
		atomic.StoreInt32(&kv.status, 0)
	} else {
		atomic.StoreInt32(&kv.status, -1)
	}
}

func (kv *ShardKV) Get(args *common.GetArgs, reply *common.GetReply) error {
	if kv.GetConfig().Shards[common.Key2shard(args.Key)] != kv.gid || !kv.Status() {
		kv.HighLightf("correct gid: %d", kv.GetConfig().Shards[common.Key2shard(args.Key)])
		reply.Err = common.ErrWrongGroup
		return nil
	}
	res, err := kv.WaitTilApply(Op{args.OpID, args.ClientID, OpGet, args.Key})
	kv.HighLightf("%+v %s", res, err)
	reply.Err = err
	if err == common.OK {
		reply.Value = res.(string)
	}
	return nil
}

func (kv *ShardKV) PutAppend(args *common.PutAppendArgs, reply *common.PutAppendReply) error {
	if kv.GetConfig().Shards[common.Key2shard(args.Key)] != kv.gid || !kv.Status() {
		kv.HighLightf("correct gid: %s", kv.GetConfig().Shards[common.Key2shard(args.Key)])
		reply.Err = common.ErrWrongGroup
		return nil
	}
	_, err := kv.WaitTilApply(Op{args.OpID, args.ClientID, OpType(args.Op), []string{args.Key, args.Value}})
	reply.Err = err
	return nil
}

func (kv *ShardKV) Pull(args *common.PullArgs, reply *common.PullReply) (err error) {
	if kv.IsLeader() {
		reply.Err = common.ErrWrongLeader
		return
	}

	curConfig := kv.GetConfig()
	if args.ConfigVersion != curConfig.Num && args.ConfigVersion-1 != curConfig.Num {
		reply.Err = common.ErrConfigVersion
		return
	}

	reply.Storage = kv.repo.CopyShard(args.ShardId)
	reply.Err = common.OK
	return
}

func (kv *ShardKV) Execute(op Op) (res Res) {
	switch op.Typ {
	case OpGet:
		val, ok := kv.repo.Get(op.Args.(string))
		res.Err = common.ErrNoKey
		if ok {
			res.Err = common.OK
			res.Data = val
		}
		return
	case OpPut:
		kv.repo.Put(op.Args.([]string)[0], op.Args.([]string)[1])
		res.Err = common.OK
		return
	case OpAppend:
		kv.repo.Append(op.Args.([]string)[0], op.Args.([]string)[1])
		res.Err = common.OK
		return
	case OpConfigChange:
		return kv.ExecuteConfigChange(op)
	case OpAddShard:
		args := op.Args.(AddShard)
		kv.repo.AddShard(args.Shard, args.Storage)
		kv.repo.Status[args.Shard] = true
		kv.HighLightf("add shard %d", args.Shard)
		nodeStatus := true
		for shard, gid := range kv.config.Shards {
			if gid == kv.gid && !kv.repo.Status[shard] {
				nodeStatus = false
			}
		}
		res.Err = common.OK
		kv.SetStatus(nodeStatus)
		return
	}
	res.Err = common.ErrInvalidOperation
	return
}

func (kv *ShardKV) ExecuteConfigChange(op Op) (res Res) {
	kv.configMu.Lock()
	defer kv.configMu.Unlock()
	kv.HighLightf("ExecuteConfigChange %+v", op)
	newConfig := op.Args.(shardctl_common.Config)
	if kv.config.Num+1 != newConfig.Num {
		return Res{
			Err:  "invalid config version",
			Data: nil,
		}
	}

	kv.prevConfig = kv.config
	kv.config = newConfig
	status := true
	for shard, gid := range kv.config.Shards {
		if gid != kv.gid {
			continue
		}
		if kv.prevConfig.Shards[shard] == 0 {
			kv.repo.Status[shard] = true
		} else if kv.prevConfig.Shards[shard] != gid {
			kv.repo.Status[shard] = false
			status = false
			if kv.IsLeader() {
				kv.HighLightf("pull shard %d", shard)
				go kv.pullShard(shard, kv.config.Num)
			}
		}
	}
	kv.SetStatus(status)
	return Res{
		Err:  common.OK,
		Data: fmt.Sprintf("excute status %+v", status),
	}
}

func (kv *ShardKV) IsLeader() bool {
	_, _, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) pullShard(shardId int, configVersion int) {
	for kv.IsLeader() {
		kv.configMu.RLock()
		args := common.PullArgs{
			ConfigVersion: configVersion,
			ShardId:       shardId,
		}
		gid := kv.prevConfig.Shards[shardId]
		if servers, ok := kv.prevConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply common.PullReply
				ok := srv.Call("ShardKV.Pull", &args, &reply)
				if ok && (reply.Err == common.OK) {
					kv.configMu.RUnlock()
					kv.HighLightf("start to add shard %d", shardId)
					kv.StartOp(Op{
						OpID:     atomic.AddInt64(&kv.opIndex, 1),
						ClientID: fmt.Sprintf("server-%d", kv.me),
						Typ:      OpAddShard,
						Args: AddShard{
							Shard:   shardId,
							Storage: reply.Storage,
						},
					})
					return
				}
				if ok && (reply.Err == common.ErrConfigVersion) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
	}
	kv.configMu.RUnlock()
	time.Sleep(time.Millisecond * 100)
}

type Res struct {
	Err  common.Err
	Data interface{}
}

func (kv *ShardKV) StartOp(op Op) (chan Res, common.Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, common.ErrWrongLeader
	}
	notifyCh := make(chan Res, 1000)
	kv.notifier.Store(op.ClientID+strconv.Itoa(int(op.OpID)), notifyCh)
	return notifyCh, common.OK
}

func (kv *ShardKV) WaitTilApply(op Op) (interface{}, common.Err) {
	resCh, err := kv.StartOp(op)
	if err != common.OK {
		return nil, err
	}
	for {
		select {
		case <-kv.ctx.Done():
			return nil, common.ErrServerKilled
		case <-time.After(time.Duration(100) * time.Millisecond):
			return nil, common.ErrTimeout
		case res := <-resCh:
			return res.Data, res.Err
		}
	}
}

func (kv *ShardKV) NoOpTicker() {
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-ticker.C:
			if curTerm, lastLogTerm, _ := kv.rf.GetState(); curTerm != lastLogTerm {
				kv.rf.Start(nil)
			}
		}
	}
}

func (kv *ShardKV) CheckConfig() {
	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-ticker.C:
			if kv.IsLeader() {
				oldConfig := kv.GetConfig()
				newConfig := kv.mck.Query(oldConfig.Num + 1)
				if newConfig.Num != oldConfig.Num {
					kv.HighLightf("CONFIG CHANGE!")
					go func() {
						kv.StartOp(Op{
							OpID:     atomic.AddInt64(&kv.opIndex, 1),
							ClientID: fmt.Sprintf("server-%d", kv.me),
							Typ:      OpConfigChange,
							Args:     newConfig,
						})
						kv.HighLightf("start finish")
					}()
				}
			}
		}
	}
}

func (kv *ShardKV) ListenApply() {
	for {
		select {
		case <-kv.ctx.Done():
			return
		case msg := <-kv.applyCh:
			if msg.Command == nil {
				continue
			}
			kv.HighLightf("listen apply %+v", msg)
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				op := msg.Command.(Op)

				var notifyCh chan Res
				if val, ok := kv.notifier.LoadAndDelete(op.ClientID + strconv.Itoa(int(op.OpID))); ok {
					notifyCh = val.(chan Res)
				}

				storedRes, loaded := kv.duplicatedOp.Load(op.ClientID + strconv.Itoa(int(op.OpID)))
				if loaded {
					if notifyCh != nil {
						notifyCh <- storedRes.(Res)
					}
					return
				}
				res := kv.Execute(op)
				kv.HighLightf("exec [%d] %s %+v", op.OpID, op.Typ, op.Args)
				kv.Debugf("exec res %+v", res)
				kv.duplicatedOp.Store(op.ClientID+strconv.Itoa(int(op.OpID)), res)
				if kv.IsLeader() && notifyCh != nil {
					// kv.HighLightf("send %d res to client", msg.CommandIndex)
					notifyCh <- res
				}
			}()
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.Kill()
	kv.cancel()
	kv.repo = repo.NewShardRepositery()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(rpcServer *rpc.Server, servers map[int]*rpc.ClientEnd, me int, persister *persister.Persister, gid int, ctrlers []*rpc.ClientEnd) *ShardKV {
	ctx, cancel := context.WithCancel(context.Background())
	applyCh := make(chan raft.ApplyMsg)
	mck := shardctl_client.MakeClerk(ctrlers)
	kv := &ShardKV{
		me:           me,
		ctx:          ctx,
		cancel:       cancel,
		rf:           raft.Make(rpcServer, servers, me, persister, applyCh),
		applyCh:      applyCh,
		maxraftstate: -1,
		gid:          gid,
		configMu:     new(sync.RWMutex),
		config:       mck.Query(-1),
		make_end:     rpc.MakeClientEnd,
		ctrlers:      ctrlers,
		mck:          mck,
		mu:           new(sync.RWMutex),
		notifier:     new(sync.Map),
		duplicatedOp: new(sync.Map),
		repo:         repo.NewShardRepositery(),
	}
	gob.Register(Op{})
	gob.Register(common.PutAppendArgs{})
	gob.Register(common.PutAppendReply{})
	gob.Register(common.GetArgs{})
	gob.Register(common.GetReply{})
	gob.Register(common.PullArgs{})
	gob.Register(common.PullReply{})
	gob.Register(AddShard{})
	gob.Register(shardctl_common.Config{})
	_ = rpcServer.Register(kv)

	go kv.ListenApply()
	go kv.NoOpTicker()
	go kv.CheckConfig()
	return kv
}

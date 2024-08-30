package server

import (
	"context"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/internal/common"
	"raftkv/internal/raft"
	"raftkv/internal/repository"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

type KVServer struct {
	me     int
	dead   int32 // set by Kill()
	ctx    context.Context
	cancel context.CancelFunc

	mu           *sync.Mutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	notifier     *sync.Map //map[string]chan common.OpRes
	duplicatedOp *sync.Map

	repo *repository.KVRepository

	maxraftstate int // snapshot if log grows this big
}

func (kv *KVServer) Get(req *common.Request, res *common.Response) error {
	opRes := kv.WaitTilApply(req.OpID, common.OpGet, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) Put(req *common.Request, res *common.Response) error {
	opRes := kv.WaitTilApply(req.OpID, common.OpPut, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) Append(req *common.Request, res *common.Response) error {
	opRes := kv.WaitTilApply(req.OpID, common.OpAppend, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) WaitTilApply(opID string, opType common.OpType, args interface{}) common.OpRes {
	op := common.NewOp(opID, opType, args)
	resCh, err := func(op common.Op) (chan common.OpRes, common.Err) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			return nil, common.ErrWrongLeader
		}
		kv.HighLightf("start %s", &op)
		notifyCh := make(chan common.OpRes, 1000)
		kv.notifier.Store(op.OpID, notifyCh)
		return notifyCh, common.OK
	}(op)
	if err != common.OK {
		return common.NewOpRes(err, nil)
	}
	for {
		select {
		case <-time.After(time.Duration(100) * time.Millisecond):
			return common.OpRes{
				Reply: nil,
				Err:   common.ErrTimeout,
			}
		case res := <-resCh:
			return res
		}
	}
}

func (kv *KVServer) NoOpTicker() {
	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	for !kv.killed() {
		select {
		case <-ticker.C:
			if curTerm, lastLogTerm, isLeader := kv.rf.GetState(); isLeader && curTerm != lastLogTerm {
				kv.rf.Start(nil)
			}
		}
	}
}

func (kv *KVServer) ListenApply() {
	for !kv.killed() {
		select {
		case <-kv.ctx.Done():
			return
		case msg := <-kv.applyCh:
			if msg.Command == nil {
				continue
			}
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				op := msg.Command.(common.Op)

				var notifyCh chan common.OpRes
				if val, ok := kv.notifier.Load(op.OpID); ok {
					notifyCh = val.(chan common.OpRes)
				}

				storedRes, loaded := kv.duplicatedOp.Load(op.OpID)
				if loaded {
					if notifyCh != nil {
						notifyCh <- storedRes.(common.OpRes)
					}
					return
				}
				res := kv.Execute(op)
				if _, _, isLeader := kv.rf.GetState(); isLeader {
					kv.Debugf("exec [%d] %s res", msg.CommandIndex, &op)
				}
				kv.duplicatedOp.Store(op.OpID, res)
				if _, _, isLeader := kv.rf.GetState(); notifyCh != nil && isLeader {
					kv.HighLightf("send %d res to client", msg.CommandIndex)
					notifyCh <- res
				}
			}()
		}
	}
}

func (kv *KVServer) Execute(op common.Op) common.OpRes {
	switch op.Typ {
	case common.OpGet:
		key := op.Args.(common.GetArgs).Key
		val := kv.repo.Get(key)
		return common.NewOpRes(common.OK, common.GetReply{Value: val})
	case common.OpPut:
		key := op.Args.(common.PutAppendArgs).Key
		val := op.Args.(common.PutAppendArgs).Value
		kv.repo.Put(key, val)
		return common.NewOpRes(common.OK, nil)
	case common.OpAppend:
		key := op.Args.(common.PutAppendArgs).Key
		val := op.Args.(common.PutAppendArgs).Value
		kv.repo.Append(key, val)
		return common.NewOpRes(common.OK, nil)
	}
	return common.NewOpRes("invalid op", nil)
}

// func (kv *KVServer) Finish(args *FinishArgs, reply *FinishReply) {
// 	kv.duplicatedOp.Delete(args.OpID)
// }

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.cancel()
	kv.repo = repository.NewKVRepositories()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
func StartKVServer(rpcServer *rpc.Server, peers map[int]*rpc.ClientEnd, me int, persister *persister.Persister) *KVServer {
	gob.Register(common.Op{})
	gob.Register(common.PutAppendArgs{})
	gob.Register(common.GetArgs{})
	gob.Register(common.GetReply{})

	applyCh := make(chan raft.ApplyMsg)
	ctx, cancel := context.WithCancel(context.Background())
	kv := &KVServer{
		me:           me,
		ctx:          ctx,
		cancel:       cancel,
		mu:           new(sync.Mutex),
		rf:           raft.Make(rpcServer, peers, me, persister, applyCh),
		applyCh:      applyCh,
		notifier:     new(sync.Map),
		duplicatedOp: new(sync.Map),
		repo:         repository.NewKVRepositories(),
		maxraftstate: -1,
	}
	rpcServer.Register(kv)
	go kv.ListenApply()
	go kv.NoOpTicker()
	return kv
}

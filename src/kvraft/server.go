package kvraft

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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

type KVServer struct {
	me     int
	dead   int32 // set by Kill()
	ctx    context.Context
	cancel context.CancelFunc

	mu           *sync.Mutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	notifier     map[string]chan OpRes
	duplicatedOp *sync.Map

	repo *KVRepositery

	maxraftstate int // snapshot if log grows this big
}

func (kv *KVServer) ListenApply() {
	for !kv.killed() {
		select {
		case <-kv.ctx.Done():
			return
		case msg := <-kv.applyCh:
			go func() {
				op := msg.Command.(Op)
				notifyCh := kv.notifier[op.OpID]
				_, loaded := kv.duplicatedOp.LoadOrStore(op.OpID, true)
				if loaded {
					if notifyCh != nil {
						notifyCh <- OpRes{
							Err: ErrSessionExpired,
						}
					}
					return
				}
				res := kv.PrecessOp(op)
				if notifyCh != nil {
					notifyCh <- res
				}
			}()
		}
	}
}

func (kv *KVServer) PrecessOp(op Op) OpRes {
	switch op.Typ {
	case OpGet:
		key := op.Args.(GetArgs).Key
		val := kv.repo.Get(key)
		return NewOpRes(OK, GetReply{Value: val})
	case OpPut:
		key := op.Args.(PutAppendArgs).Key
		val := op.Args.(PutAppendArgs).Value
		kv.repo.Put(key, val)
		return NewOpRes(OK, nil)
	case OpAppend:
		key := op.Args.(PutAppendArgs).Key
		val := op.Args.(PutAppendArgs).Value
		kv.repo.Append(key, val)
		return NewOpRes(OK, nil)
	}
	return NewOpRes("invalid op", nil)
}

func (kv *KVServer) StartOp(op Op) (chan OpRes, Err) {
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, ErrWrongLeader
	}
	notifyCh := make(chan OpRes, 1)
	kv.notifier[op.OpID] = notifyCh
	return notifyCh, OK
}

func (kv *KVServer) Execute(opID string, opType OpType, args interface{}) OpRes {
	op := NewOp(opID, opType, args)
	resCh, err := kv.StartOp(op)
	if err != OK {
		return NewOpRes(err, nil)
	}
	for {
		select {
		case res := <-resCh:
			return res
		}
	}
}

func (kv *KVServer) Get(req *Request, res *Response) {
	opRes := kv.Execute(req.OpID, OpGet, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return
}

func (kv *KVServer) Put(req *Request, res *Response) {
	opRes := kv.Execute(req.OpID, OpPut, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return
}

func (kv *KVServer) Append(req *Request, res *Response) {
	opRes := kv.Execute(req.OpID, OpAppend, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return
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
	close(kv.applyCh)
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	applyCh := make(chan raft.ApplyMsg)
	ctx, cancel := context.WithCancel(context.Background())
	kv := &KVServer{
		me:           me,
		ctx:          ctx,
		cancel:       cancel,
		mu:           new(sync.Mutex),
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		notifier:     make(map[string]chan OpRes),
		duplicatedOp: new(sync.Map),
		repo:         NewKVRepositories(),
		maxraftstate: maxraftstate,
	}
	go kv.ListenApply()
	return kv
}

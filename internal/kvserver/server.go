package kvserver

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/internal/raft"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

type KVServer struct {
	me   string
	dead int32 // set by Kill()

	mu           *sync.Mutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	notifier     *sync.Map //map[string]chan OpRes
	duplicatedOp *sync.Map

	repo *KVRepositery

	maxraftstate int // snapshot if log grows this big
}

func (kv *KVServer) Get(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpGet, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) Put(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpPut, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) Append(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpAppend, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

// BeginTransaction starts a new transaction
func (kv *KVServer) BeginTransaction(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpBeginTransaction, nil)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

// TxnGet gets a value within a transaction
func (kv *KVServer) TxnGet(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpTxnGet, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

// TxnPut puts a value within a transaction
func (kv *KVServer) TxnPut(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpTxnPut, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

// CommitTransaction commits a transaction
func (kv *KVServer) CommitTransaction(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpCommitTransaction, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

// AbortTransaction aborts a transaction
func (kv *KVServer) AbortTransaction(req *Request, res *Response) error {
	opRes := kv.WaitTilApply(req.OpID, OpAbortTransaction, req.Args)
	res.Reply = opRes.Reply
	res.Err = opRes.Err
	return nil
}

func (kv *KVServer) WaitTilApply(opID string, opType OpType, args interface{}) OpRes {
	op := NewOp(opID, opType, args)
	resCh, err := func(op Op) (chan OpRes, Err) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			return nil, ErrWrongLeader
		}
		kv.HighLightf("start %s", &op)
		notifyCh := make(chan OpRes, 1000)
		kv.notifier.Store(op.OpID, notifyCh)
		return notifyCh, OK
	}(op)
	if err != OK {
		return NewOpRes(err, nil)
	}
	for {
		select {
		case <-time.After(time.Duration(500) * time.Millisecond):
			return OpRes{
				Reply: nil,
				Err:   ErrTimeout,
			}
		case res := <-resCh:
			return res
		}
	}
}

func (kv *KVServer) NoOpTicker() {
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	for !kv.killed() {
		<-ticker.C
		if curTerm, lastLogTerm, _ := kv.rf.GetState(); curTerm != lastLogTerm {
			kv.rf.Start(nil)
		}
	}
}

func (kv *KVServer) ListenApply() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.Command == nil {
			if msg.SnapshotValid {
				kv.readSnapshot(msg.Snapshot)
			}
			continue
		}
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			op := msg.Command.(Op)

			var notifyCh chan OpRes
			if val, ok := kv.notifier.LoadAndDelete(op.OpID); ok {
				notifyCh = val.(chan OpRes)
			}

			storedRes, loaded := kv.duplicatedOp.Load(op.OpID)
			if loaded {
				if notifyCh != nil {
					notifyCh <- storedRes.(OpRes)
				}
				return
			}
			res := kv.Execute(op)
			if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				if err := e.Encode(kv.repo.data); err != nil {
					panic(err)
				}
				kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}
			kv.duplicatedOp.Store(op.OpID, res)
			if _, _, isLeader := kv.rf.GetState(); notifyCh != nil && isLeader {
				kv.HighLightf("send %d res to client", msg.CommandIndex)
				notifyCh <- res
			}
		}()
	}
}

func (kv *KVServer) Execute(op Op) OpRes {
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
	case OpBeginTransaction:
		// Create a new transaction
		txnID := kv.repo.txnManager.CreateTransaction()
		return NewOpRes(OK, TransactionReply{TxnID: txnID})
	case OpTxnGet:
		// Get a value within a transaction
		args := op.Args.(TransactionArgs)
		val, err := kv.repo.TxnGet(args.TxnID, args.Key)
		if err != OK {
			return NewOpRes(err, nil)
		}
		return NewOpRes(OK, TransactionReply{Value: val})
	case OpTxnPut:
		// Put a value within a transaction
		args := op.Args.(TransactionArgs)
		err := kv.repo.TxnPut(args.TxnID, args.Key, args.Value)
		if err != OK {
			return NewOpRes(err, nil)
		}
		return NewOpRes(OK, nil)
	case OpCommitTransaction:
		// Commit a transaction
		args := op.Args.(CommitArgs)
		err := kv.repo.TxnCommit(args.TxnID)
		if err != OK {
			return NewOpRes(err, CommitReply{Success: false})
		}
		return NewOpRes(OK, CommitReply{Success: true})
	case OpAbortTransaction:
		// Abort a transaction
		args := op.Args.(AbortArgs)
		err := kv.repo.TxnAbort(args.TxnID)
		if err != OK {
			return NewOpRes(err, AbortReply{Success: false})
		}
		return NewOpRes(OK, AbortReply{Success: true})
	}
	return NewOpRes("invalid op", nil)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	// var duplicatedOp map[string]any
	var data map[string]map[string]string
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	if e := d.Decode(&data); e != nil {
		data = make(map[string]map[string]string)
	}
	kv.repo.data = data
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
	kv.HighLightf("%+v", kv.repo.data)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.repo = NewKVRepositories()
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
func StartKVServer(rpcServer *rpc.Server, servers map[string]*rpc.ClientEnd, backupPeers map[int]*rpc.ClientEnd, me string, persister *persister.Persister) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
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

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		mu:           new(sync.Mutex),
		rf:           raft.Make(rpcServer, servers, backupPeers, me, persister, applyCh),
		applyCh:      applyCh,
		notifier:     new(sync.Map),
		duplicatedOp: new(sync.Map),
		repo:         NewKVRepositories(),
		maxraftstate: 10000,
	}
	kv.readSnapshot(persister.ReadSnapshot())
	_ = rpcServer.Register(kv)

	go kv.ListenApply()
	go kv.NoOpTicker()
	return kv
}

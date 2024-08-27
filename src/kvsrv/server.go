package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	dataMu       *sync.RWMutex
	data         map[string]string
	duplicatedOp sync.Map
}

func (kv *KVServer) get(key string) string {
	kv.dataMu.RLock()
	defer kv.dataMu.RUnlock()
	return kv.data[key]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Value = kv.get(args.Key)
}

func (kv *KVServer) put(key string, val string) {
	kv.dataMu.Lock()
	defer kv.dataMu.Unlock()
	kv.data[key] = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if _, loaded := kv.duplicatedOp.LoadOrStore(args.OpID, true); !loaded {
		kv.put(args.Key, args.Value)
	}
	reply.Value = kv.get(args.Key)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if val, ok := kv.duplicatedOp.Load(args.OpID); ok {
		reply.Value = val.(string)
		return
	}
	oldVal := kv.get(args.Key)
	kv.put(args.Key, oldVal+args.Value)
	reply.Value = oldVal
	kv.duplicatedOp.Store(args.OpID, reply.Value)
}

func (kv *KVServer) Finish(args *FinishArgs, reply *FinishReply) {
	kv.duplicatedOp.LoadAndDelete(args.OpID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	return kv
}

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
	mu  sync.RWMutex
	kvs map[string]string

	duplicateOp sync.Map
}

func (kv *KVServer) get(key string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.kvs[key]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Value = kv.get(args.Key)
}

func (kv *KVServer) put(key string, val string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvs[key] = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if _, loaded := kv.duplicateOp.LoadOrStore(args.OpID, true); !loaded {
		kv.put(args.Key, args.Value)
	}
	reply.Value = kv.get(args.Key)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if val, ok := kv.duplicateOp.Load(args.OpID); ok {
		reply.Value = val.(string)
		return
	}
	oldVal := kv.get(args.Key)
	kv.put(args.Key, oldVal+args.Value)
	reply.Value = oldVal
	kv.duplicateOp.Store(args.OpID, reply.Value)
}

func (kv *KVServer) Finish(args *FinishArgs, reply *FinishReply) {
	kv.duplicateOp.LoadAndDelete(args.OpID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvs = make(map[string]string)
	return kv
}

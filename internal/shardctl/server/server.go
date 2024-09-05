package server

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/shardctl/common"
	"raftkv/internal/shardctl/repo"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

type ShardCtrler struct {
	me     int
	ctx    context.Context
	cancel context.CancelFunc

	mu           *sync.Mutex
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	notifier     *sync.Map //map[string]chan interface{}
	duplicatedOp *sync.Map
	// Your data here.

	repo repo.ConfigRepositery
}

type OpType string

const (
	OpJoin  OpType = "Join"
	OpLeave OpType = "Leave"
	OpMove  OpType = "Move"
	OpQuery OpType = "Query"
)

type Op struct {
	OpID     int64
	ClientID string
	Typ      OpType
	Args     interface{}
}

func (sc *ShardCtrler) Join(args *common.JoinArgs, reply *common.JoinReply) {
	_, err := sc.WaitTilApply(args.OpID, args.ClientID, OpJoin, args.Servers)
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *common.LeaveArgs, reply *common.LeaveReply) {
	_, err := sc.WaitTilApply(args.OpID, args.ClientID, OpLeave, args.GIDs)
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *common.MoveArgs, reply *common.MoveReply) {
	_, err := sc.WaitTilApply(args.OpID, args.ClientID, OpMove, []int{args.Shard, args.GID})
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *common.QueryArgs, reply *common.QueryReply) {
	res, err := sc.WaitTilApply(args.OpID, args.ClientID, OpQuery, args.Num)
	reply.Err = err
	if err == common.OK {
		reply.Config = res.(common.Config)
	}
}

func (sc *ShardCtrler) WaitTilApply(opID int64, clientID string, opType OpType, args interface{}) (interface{}, common.Err) {
	op := Op{opID, clientID, opType, args}
	resCh, err := func(op Op) (chan interface{}, common.Err) {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		_, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			return nil, common.ErrWrongLeader
		}
		notifyCh := make(chan interface{}, 1000)
		sc.notifier.Store(op.ClientID+strconv.Itoa(int(op.OpID)), notifyCh)
		return notifyCh, common.OK
	}(op)
	if err != common.OK {
		return nil, err
	}
	for {
		select {
		case <-sc.ctx.Done():
			return nil, common.ErrServerKilled
		case <-time.After(time.Duration(100) * time.Millisecond):
			return nil, common.ErrTimeout
		case res := <-resCh:
			return res, common.OK
		}
	}
}

func (sc *ShardCtrler) NoOpTicker() {
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	for {
		select {
		case <-sc.ctx.Done():
			return
		case <-ticker.C:
			if curTerm, lastLogTerm, isLeader := sc.rf.GetState(); isLeader && curTerm != lastLogTerm {
				sc.rf.Start(nil)
			}
		}
	}
}

func (sc *ShardCtrler) ListenApply() {
	for {
		select {
		case <-sc.ctx.Done():
			return
		case msg := <-sc.applyCh:
			if msg.Command == nil {
				continue
			}
			func() {
				sc.mu.Lock()
				defer sc.mu.Unlock()

				op := msg.Command.(Op)

				var notifyCh chan interface{}
				if val, ok := sc.notifier.LoadAndDelete(op.ClientID + strconv.Itoa(int(op.OpID))); ok {
					notifyCh = val.(chan interface{})
				}

				storedRes, loaded := sc.duplicatedOp.Load(op.ClientID + strconv.Itoa(int(op.OpID)))
				if loaded {
					if notifyCh != nil {
						notifyCh <- storedRes
					}
					return
				}
				res := sc.Execute(op)
				// if _, isLeader := sc.rf.GetState(); isLeader {
				// 	sc.HighLightf("exec [%s] %+v", op.Typ, op.Args)
				// 	sc.Debugf("exec  res %+v", sc.repo.getLatestConfig().Shards)
				// }
				sc.duplicatedOp.Store(op.ClientID+strconv.Itoa(int(op.OpID)), res)
				if _, _, isLeader := sc.rf.GetState(); notifyCh != nil && isLeader {
					// sc.HighLightf("send %d res to client", msg.CommandIndex)
					notifyCh <- res
				}
			}()
		}
	}
}

func (sc *ShardCtrler) Execute(op Op) interface{} {
	switch op.Typ {
	case OpQuery:
		return sc.repo.Query(op.Args.(int))
	case OpJoin:
		sc.repo.Join(op.Args.(map[int][]string))
		return nil
	case OpLeave:
		sc.repo.Leave(op.Args.([]int))
		return nil
	case OpMove:
		sc.repo.Move(op.Args.([]int)[0], op.Args.([]int)[1])
		return nil
	}
	return nil
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.rf.Kill()
	sc.cancel()
	sc.repo = repo.NewConfigRepositery()
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(rpcServer *rpc.Server, peers map[int]*rpc.ClientEnd, me int, persister *persister.Persister) *ShardCtrler {
	gob.Register(Op{})
	gob.Register(common.Config{})
	gob.Register(map[int][]string{})

	ctx, cancel := context.WithCancel(context.Background())
	applyCh := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		me:           me,
		ctx:          ctx,
		cancel:       cancel,
		mu:           new(sync.Mutex),
		rf:           raft.Make(rpcServer, peers, me, persister, applyCh),
		applyCh:      applyCh,
		notifier:     new(sync.Map),
		duplicatedOp: new(sync.Map),
		repo:         repo.NewConfigRepositery(),
	}
	_ = rpcServer.Register(sc)
	go sc.ListenApply()
	go sc.NoOpTicker()
	return sc
}

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Highlightf1(format string, a ...interface{}) {
	format = "\033[38;5;2m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

func Highlightf2(format string, a ...interface{}) {
	format = "\033[38;5;6m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

var colors = []string{
	"\033[38;5;2m",
	"\033[38;5;45m",
	"\033[38;5;6m",
	"\033[38;5;3m",
	"\033[38;5;204m",
	"\033[38;5;111m",
	"\033[38;5;184m",
	"\033[38;5;69m",
}

// note: the debug printf will cause data race
// but it's ok cause it's used for *debug* :)
func (sc *ShardCtrler) Debugf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	prefix := fmt.Sprintf("[%d][kv]", sc.me)
	prefix = colors[sc.me] + prefix + "\033[39;49m"
	format = prefix + " " + format
	DPrintf(format, a...)
}

func (sc *ShardCtrler) HighLightf(format string, a ...interface{}) {
	format = colors[sc.me] + format + "\033[39;49m"
	sc.Debugf(format, a...)
}

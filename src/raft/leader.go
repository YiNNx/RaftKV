package raft

import (
	"context"
	"time"
)

type AppendEntriesReq struct {
	args AppendEntriesArgs
	peer int
}

type AppendEntriesRes struct {
	reply AppendEntriesReply
	peer  int
}

func (rf *Raft) becomeLeader() (stateCtx context.Context) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	rf.SetLeaderID(rf.me)

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

func (rf *Raft) broadcastEntries(ch chan AppendEntriesReq, _ interface{}) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		prevLogIndex: rf.logs.lastLogIndex,
		prevLogTerm:  rf.logs.lastLogTerm,
		entries:      nil, // todo
		leaderCommit: rf.commitIndex,
	}
	for peer := range rf.peers {
		if peer == int(rf.me) {
			continue
		}
		ch <- AppendEntriesReq{args, peer}
	}
}

func (rf *Raft) broadcastHeartbeat(ch chan AppendEntriesReq) {
	rf.broadcastEntries(ch, nil)
}

func (rf *Raft) appendEntriesWithRetry(ctx context.Context, req AppendEntriesReq, respChan chan AppendEntriesRes) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
	if ok {
		respChan <- AppendEntriesRes{reply: reply, peer: req.peer}
	}
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
			if ok {
				respChan <- AppendEntriesRes{reply: reply, peer: req.peer}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) runLeader() {
	leaderState := rf.becomeLeader()
	heartbeatTicker := time.NewTicker(getHeartbeatTime())
	reqChan := make(chan AppendEntriesReq, 100)
	respChan := make(chan AppendEntriesRes, 100)

	go rf.broadcastHeartbeat(reqChan)

	for rf.killed() == false {
		select {
		case <-leaderState.Done():
			return
		case <-heartbeatTicker.C:
			go rf.broadcastHeartbeat(reqChan)
		case req := <-reqChan:
			go rf.appendEntriesWithRetry(leaderState, req, respChan)
		case res := <-respChan:
			if res.reply.Term > rf.GetCurrentTerm() {
				rf.becomeFollower(res.reply.Term, -1)
				return
			}
		}
	}
}

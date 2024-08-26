package raft

import (
	"context"
	"time"
)

// ----- LEADER -----

func (rf *Raft) becomeLeader() (stateCtx context.Context) {
	rf.leaderID = rf.me
	close(rf.appendTrigger)
	rf.appendTrigger = make(chan int, 100)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs.getLastIndex() + 1
	}

	rf.HighLightf("LEADER")

	rf.stateCancel()
	stateCtx, rf.stateCancel = context.WithCancel(context.Background())
	return stateCtx
}

const AllPeers = -1

func (rf *Raft) getPeerIndexList(peer int) []int {
	peers := []int{}
	if peer == AllPeers {
		for peer := range rf.peers {
			if peer == int(rf.me) {
				continue
			}
			peers = append(peers, peer)
		}
	} else {
		peers = append(peers, peer)
	}
	return peers
}

func (rf *Raft) appendEntries(ch chan AppendEntriesReq, peer int, _ bool) {
	rf.stateMu.RLock()
	currentTerm := rf.currentTerm
	defer rf.stateMu.RUnlock()

	rf.logMu.RLock()
	defer rf.logMu.RUnlock()

	for _, peer := range rf.getPeerIndexList(peer) {
		startIndex := rf.nextIndex[peer]
		if startIndex > rf.logs.PrevIndex {
			entries := rf.logs.getTail(startIndex)
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				PrevLogTerm:  rf.logs.getEntry(rf.nextIndex[peer] - 1).Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			ch <- AppendEntriesReq{
				args:       args,
				peer:       peer,
				startIndex: rf.nextIndex[peer],
				endIndex:   rf.nextIndex[peer] - 1 + len(entries),
			}
		} else {

		}
	}

}

func (rf *Raft) sendEntries(req AppendEntriesReq, respChan chan AppendEntriesRes) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
	if ok {
		respChan <- AppendEntriesRes{
			reply:      reply,
			peer:       req.peer,
			startIndex: req.startIndex,
			endIndex:   req.endIndex,
		}
	}
}

func (rf *Raft) handleInstallSnapshot(req InstallSnapshotReq, respChan chan InstallSnapshotRes) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(req.peer, &req.args, &reply)
	if ok {
		respChan <- InstallSnapshotRes{
			reply:     reply,
			peer:      req.peer,
			lastIndex: req.args.LastIncludedIndex,
		}
	}
}

func (rf *Raft) runLeader() {
	rf.stateMu.Lock()
	rf.logMu.Lock()

	leaderState := rf.becomeLeader()
	rf.logMu.Unlock()
	rf.stateMu.Unlock()

	heartbeatTicker := time.NewTicker(getHeartbeatTime())
	defer heartbeatTicker.Stop()

	appendReqChan := make(chan AppendEntriesReq, 100)
	appendRespChan := make(chan AppendEntriesRes, 100)
	snapshotReqChan := make(chan InstallSnapshotReq, 100)
	snapshotRespChan := make(chan InstallSnapshotRes, 100)

	go rf.appendEntries(appendReqChan, AllPeers, true)

	for rf.killed() == false {
		select {
		case <-leaderState.Done():
			return
		case <-heartbeatTicker.C:
			go rf.appendEntries(appendReqChan, AllPeers, true)
		case peer := <-rf.appendTrigger:
			go rf.appendEntries(appendReqChan, peer, false)
		case req := <-appendReqChan:
			go rf.sendEntries(req, appendRespChan)
		case req := <-snapshotReqChan:
			go rf.handleInstallSnapshot(req, snapshotRespChan)
		case resp := <-snapshotRespChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				return
			}
			rf.logMu.Lock()
			rf.nextIndex[resp.peer] = max(rf.nextIndex[resp.peer], resp.lastIndex+1)
			rf.matchIndex[resp.peer] = max(rf.matchIndex[resp.peer], resp.lastIndex)
			rf.logMu.Unlock()
		case resp := <-appendRespChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				return
			}
			rf.logMu.Lock()
			if resp.reply.Success {
				if resp.endIndex >= resp.startIndex {
					rf.Debugf("appendEntries ok - peer %d logs%s-%s", resp.peer, rf.logs.getEntry(resp.startIndex), rf.logs.getEntry(resp.endIndex))
				}
				rf.nextIndex[resp.peer] = max(rf.nextIndex[resp.peer], resp.endIndex+1)
				rf.matchIndex[resp.peer] = max(rf.matchIndex[resp.peer], resp.endIndex)
				rf.commitIndex = rf.calculateCommitIndex(resp.endIndex)
			} else {
				rf.nextIndex[resp.peer] = resp.startIndex / 2 // ?
				rf.appendTrigger <- resp.peer
			}
			rf.logMu.Unlock()
		}
	}
}

// improvement - cache?
func (rf *Raft) calculateCommitIndex(lastUpdateIndex int) int {
	if rf.commitIndex >= lastUpdateIndex || rf.logs.getEntry(lastUpdateIndex).Term != rf.currentTerm {
		return rf.commitIndex
	}
	sum := 0
	for peer := range rf.peers {
		if rf.matchIndex[peer] >= lastUpdateIndex {
			sum++
		}
		if sum >= rf.getPriorityNum() {
			rf.Debugf("update commit index: %d", lastUpdateIndex)
			return lastUpdateIndex
		}
	}
	return rf.commitIndex
}

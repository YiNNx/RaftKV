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

func (rf *Raft) appendEntries(peer int, entriesChan chan EntriesReq, snapshotChan chan SnapshotReq) {
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
			entriesChan <- EntriesReq{
				args:       args,
				peer:       peer,
				startIndex: rf.nextIndex[peer],
				endIndex:   rf.nextIndex[peer] - 1 + len(entries),
			}
		} else {
			args := InstallSnapshotArgs{
				Term:              currentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.logs.PrevIndex,
				LastIncludedTerm:  rf.logs.PrevTerm,
				Snapshot:          rf.snapshot,
			}
			snapshotChan <- SnapshotReq{
				args: args,
				peer: peer,
			}
		}
	}
}

func (rf *Raft) sendEntries(req EntriesReq, respChan chan EntriesRes) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(req.peer, &req.args, &reply)
	if ok {
		respChan <- EntriesRes{
			reply:      reply,
			peer:       req.peer,
			startIndex: req.startIndex,
			endIndex:   req.endIndex,
		}
	}
}

func (rf *Raft) sendSnapshot(req SnapshotReq, respChan chan SnapshotRes) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(req.peer, &req.args, &reply)
	if ok {
		respChan <- SnapshotRes{
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

	entriesReqChan := make(chan EntriesReq, 100)
	entriesRespChan := make(chan EntriesRes, 100)
	snapshotReqChan := make(chan SnapshotReq, 100)
	snapshotRespChan := make(chan SnapshotRes, 100)

	go rf.appendEntries(AllPeers, entriesReqChan, snapshotReqChan)

	for rf.killed() == false {
		select {
		case <-leaderState.Done():
			return
		case <-heartbeatTicker.C:
			go rf.appendEntries(AllPeers, entriesReqChan, snapshotReqChan)
		case peer := <-rf.appendTrigger:
			go rf.appendEntries(peer, entriesReqChan, snapshotReqChan)
		case req := <-entriesReqChan:
			go rf.sendEntries(req, entriesRespChan)
		case req := <-snapshotReqChan:
			go rf.sendSnapshot(req, snapshotRespChan)
		case resp := <-snapshotRespChan:
			if ok := rf.checkRespTerm(resp.reply.Term); !ok {
				return
			}
			rf.logMu.Lock()
			rf.nextIndex[resp.peer] = max(rf.nextIndex[resp.peer], resp.lastIndex+1)
			rf.matchIndex[resp.peer] = max(rf.matchIndex[resp.peer], resp.lastIndex)
			rf.logMu.Unlock()
		case resp := <-entriesRespChan:
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

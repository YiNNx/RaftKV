package raft

import (
	"context"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/internal/fault"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

type Raft struct {
	peers     map[int]*rpc.ClientEnd // RPC end points of all peers
	persister *persister.Persister   // Object to hold this peer's persisted state
	me        int                    // this peer's index into peers[]
	dead      int32                  // set by Kill()

	applyChMu *sync.Mutex
	applyCh   chan ApplyMsg

	// log state
	logMu    *sync.RWMutex
	logs     EntryList
	snapshot []byte

	commitIndex int
	lastApplied int64

	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	// update after
	// 1. appendEntries failed, set start index - 1
	// 2. appendEntries succeeded, set end index + 1
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	// update after appendEntries succeed, set as end log index
	matchIndex []int

	// node state
	stateMu     *sync.RWMutex
	leaderID    int
	currentTerm int
	voteFor     int

	// flow control
	appendTrigger  chan int
	electionTicker *time.Ticker
	applyTicker    *time.Ticker
	stateCancel    context.CancelFunc

	// 故障感知与恢复
	faultDetector     *fault.FaultDetector
	healthCheckTicker *time.Ticker

	// 备用节点
	backupPeers map[int]*rpc.ClientEnd // 备用节点的RPC端点
	backupMu    *sync.RWMutex          // 备用节点的互斥锁
}

func NewRaftInstance(peers map[int]*rpc.ClientEnd, me int,
	persister *persister.Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        int(me),
		dead:      0,
		applyCh:   applyCh,
		applyChMu: &sync.Mutex{},

		stateMu:     &sync.RWMutex{},
		currentTerm: 0,
		voteFor:     -1,
		leaderID:    -1,

		logs:        NewLogList(),
		logMu:       &sync.RWMutex{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,

		appendTrigger:  make(chan int, 100),
		electionTicker: time.NewTicker(getRandomElectionTimeout()),
		applyTicker:    time.NewTicker(1 * time.Millisecond),

		// 故障感知与恢复
		healthCheckTicker: time.NewTicker(100 * time.Millisecond),

		// 备用节点
		backupPeers: make(map[int]*rpc.ClientEnd),
		backupMu:    &sync.RWMutex{},
	}

	// 创建故障检测器
	rf.faultDetector = fault.NewFaultDetector(
		100*time.Millisecond, // 心跳超时
		500*time.Millisecond, // 延迟阈值
		0.1,                  // 丢包率阈值
		5*time.Second,        // 滑动窗口大小
		100*time.Millisecond, // 采样间隔
	)

	return rf
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(rpcServer *rpc.Server, peers map[int]*rpc.ClientEnd, me int,
	persister *persister.Persister, applyCh chan ApplyMsg) *Raft {
	rf := NewRaftInstance(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(AppendEntriesArgs{})
	gob.Register(AppendEntriesReply{})
	gob.Register(InstallSnapshotArgs{})
	gob.Register(InstallSnapshotReply{})
	gob.Register(AddServerArgs{})
	gob.Register(AddServerReply{})
	gob.Register(RemoveServerArgs{})
	gob.Register(RemoveServerReply{})
	gob.Register(ConfigChangeCommand{})
	gob.Register(PingArgs{})
	gob.Register(PingReply{})

	if len(rf.snapshot) != 0 {
		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.logs.PrevTerm,
				SnapshotIndex: rf.logs.PrevIndex,
			}
		}()
	}

	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	go rf.startHealthCheck()

	_ = rpcServer.Register(rf)
	rf.HighLightf("START")
	return rf
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.applyTicker.C:
			func() {
				rf.logMu.RLock()
				defer rf.logMu.RUnlock()
				lastApplied := rf.getLastApplied()
				if rf.commitIndex == lastApplied {
					return
				}
				msgList := make([]ApplyMsg, rf.commitIndex-lastApplied)
				rf.HighLightf("apply entry %d - %d", lastApplied+1, rf.commitIndex)
				for i := range msgList {
					entry := rf.logs.getEntry(rf.getLastApplied() + i + 1)
					msgList[i] = ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: entry.Index,
					}
				}
				rf.setLastApplied(rf.commitIndex)

				go func() {
					rf.applyChMu.Lock()
					defer rf.applyChMu.Unlock()
					for _, msg := range msgList {
						if rf.isConfigChangeCommand(msg.Command) {
							rf.applyConfigChange(msg)
						} else {
							rf.applyCh <- msg
						}
					}
				}()
			}()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	if rf.killed() || rf.me != rf.leaderID {
		return -1, -1, false
	}

	rf.logMu.Lock()
	defer rf.logMu.Unlock()

	term := rf.currentTerm
	index := rf.appendLog(command, term)
	rf.matchIndex[rf.me] = index
	isLeader := true

	rf.HighLightf("START COMMAND %s", rf.logs.getEntry(index))
	rf.appendTrigger <- AllPeers

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.HighLightf("STOP")
	atomic.StoreInt32(&rf.dead, 1)
	rf.stateCancel()
	rf.electionTicker.Stop()
	rf.applyTicker.Stop()
	rf.healthCheckTicker.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 启动健康检查
func (rf *Raft) startHealthCheck() {
	go func() {
		for !rf.killed() {
			select {
			case <-rf.healthCheckTicker.C:
				rf.checkNodeHealth()
			}
		}
	}()
}

// 检查节点健康状态
func (rf *Raft) checkNodeHealth() {
	rf.stateMu.RLock()
	defer rf.stateMu.RUnlock()

	// 检查心跳超时
	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		// 记录RPC延迟和丢包率
		start := time.Now()
		ok := rf.sendPing(peerID, &PingArgs{}, &PingReply{})
		latency := time.Since(start)

		// 计算丢包率（这里简化为RPC失败率）
		packetLoss := 0.0
		if !ok {
			packetLoss = 1.0
		}

		// 记录网络指标
		rf.faultDetector.RecordNetworkMetrics(peerID, latency, packetLoss)

		// 更新节点状态
		rf.faultDetector.UpdateNodeStatus(peerID)

		// 获取节点状态
		status := rf.faultDetector.GetNodeStatus(peerID)
		if status == nil {
			continue
		}

		// 根据故障类型和严重程度采取不同措施
		switch status.Severity {
		case fault.Low:
			// 轻微故障，只记录日志
			rf.Debugf("node %d has minor issues: %s", peerID, status.Reason)
		case fault.Medium:
			// 中等故障，调整读写策略
			rf.adjustReadWriteStrategy(peerID, status)
		case fault.High, fault.Critical:
			// 严重故障，需要替换节点
			rf.Debugf("node %d has critical issues: %s", peerID, status.Reason)
			if rf.leaderID == rf.me {
				rf.adjustReplicas()
			}
		}
	}

	// 如果是领导者，检查是否需要调整副本
	if rf.leaderID == rf.me {
		// 获取需要调整的节点
		nodes := rf.faultDetector.GetNodesNeedAdjustment()
		if len(nodes) > 0 {
			rf.adjustReplicas()
		}
	}
}

// 调整读写策略
func (rf *Raft) adjustReadWriteStrategy(peerID int, status *fault.FaultStatus) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	// 根据故障类型调整策略
	switch status.Type {
	case fault.HighLatency:
		// 对于高延迟节点，减少其参与读操作
		// 这里可以通过调整nextIndex和matchIndex来实现
		if rf.nextIndex[peerID] > 0 {
			rf.nextIndex[peerID]--
		}
	case fault.PacketLoss:
		// 对于丢包率高的节点，增加重试次数
		// 这里可以通过调整心跳间隔来实现
		rf.electionTicker.Reset(getRandomElectionTimeout() * 2)
	case fault.TemporaryUnavailable:
		// 对于临时不可用节点，暂时跳过
		// 可以通过调整commitIndex来实现
		if rf.commitIndex > rf.matchIndex[peerID] {
			rf.commitIndex = rf.matchIndex[peerID]
		}
	}
}

// 调整副本
func (rf *Raft) adjustReplicas() {
	// 如果不是领导者，不进行副本调整
	if rf.leaderID != rf.me {
		return
	}

	// 获取故障节点
	faultyNodes := rf.faultDetector.GetFaultyNodes()
	if len(faultyNodes) == 0 {
		return
	}

	// 获取备用节点
	rf.backupMu.RLock()
	backupPeers := make([]int, 0, len(rf.backupPeers))
	for peerID := range rf.backupPeers {
		backupPeers = append(backupPeers, peerID)
	}
	rf.backupMu.RUnlock()

	// 如果没有备用节点，无法进行替换
	if len(backupPeers) == 0 {
		rf.Debugf("no backup peers available for replacement")
		return
	}

	// 对每个故障节点进行替换
	for _, faultyNode := range faultyNodes {
		// 获取故障节点的状态
		status := rf.faultDetector.GetNodeStatus(faultyNode)
		if status == nil || status.Severity < fault.High {
			continue
		}

		// 选择一个备用节点
		if len(backupPeers) == 0 {
			rf.Debugf("no more backup peers available")
			break
		}
		backupNode := backupPeers[0]
		backupPeers = backupPeers[1:]

		// 获取备用节点的地址
		backupPeer := rf.GetBackupPeer(backupNode)
		if backupPeer == nil {
			rf.Debugf("backup peer %d not found", backupNode)
			continue
		}

		// 执行单步成员变更
		// 1. 先添加备用节点
		rf.Debugf("adding backup node %d to replace faulty node %d", backupNode, faultyNode)
		args := &AddServerArgs{
			NewServer: backupPeer.Addr,
		}
		reply := &AddServerReply{}
		ok := rf.sendAddServer(backupNode, args, reply)
		if !ok || reply.Status != OK {
			rf.Debugf("failed to add backup node %d: %v", backupNode, reply.Status)
			continue
		}

		// 2. 等待新节点加入完成
		time.Sleep(2 * rf.faultDetector.HeartbeatTimeout)

		// 3. 移除故障节点
		rf.Debugf("removing faulty node %d", faultyNode)
		removeArgs := &RemoveServerArgs{
			OldServer: rf.peers[faultyNode].Addr,
		}
		removeReply := &RemoveServerReply{}
		ok = rf.sendRemoveServer(faultyNode, removeArgs, removeReply)
		if !ok || removeReply.Status != OK {
			rf.Debugf("failed to remove faulty node %d: %v", faultyNode, removeReply.Status)
			continue
		}

		// 4. 更新节点映射
		rf.stateMu.Lock()
		rf.peers[backupNode] = backupPeer
		delete(rf.peers, faultyNode)
		rf.stateMu.Unlock()

		// 5. 从备用节点列表中移除
		rf.RemoveBackupPeer(backupNode)

		rf.HighLightf("successfully replaced faulty node %d with backup node %d", faultyNode, backupNode)
	}
}

// 添加备用节点
func (rf *Raft) AddBackupPeer(peerID int, peer *rpc.ClientEnd) {
	rf.backupMu.Lock()
	defer rf.backupMu.Unlock()
	rf.backupPeers[peerID] = peer
}

// 获取备用节点
func (rf *Raft) GetBackupPeer(peerID int) *rpc.ClientEnd {
	rf.backupMu.RLock()
	defer rf.backupMu.RUnlock()
	return rf.backupPeers[peerID]
}

// 移除备用节点
func (rf *Raft) RemoveBackupPeer(peerID int) {
	rf.backupMu.Lock()
	defer rf.backupMu.Unlock()
	delete(rf.backupPeers, peerID)
}

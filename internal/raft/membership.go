package raft

import (
	"time"

	"raftkv/pkg/rpc"
)

const (
	// 状态码
	OK         = "OK"
	NOT_LEADER = "NOT_LEADER"
	TIMEOUT    = "TIMEOUT"
)

// 加服务器RPC参数
type AddServerArgs struct {
	NewServer string // 新服务器地址
}

// 加服务器RPC响应
type AddServerReply struct {
	Status     string // OK, NOT_LEADER, TIMEOUT
	LeaderHint string // 如果当前节点不是领导者，提示可能的领导者地址
}

// 移除服务器RPC参数
type RemoveServerArgs struct {
	OldServer string // 要移除的服务器地址
}

// 移除服务器RPC响应
type RemoveServerReply struct {
	Status     string // OK, NOT_LEADER, TIMEOUT
	LeaderHint string // 如果当前节点不是领导者，提示可能的领导者地址
}

// 配置变更命令类型
const (
	CmdAddServer    = "AddServer"
	CmdRemoveServer = "RemoveServer"
)

// 配置变更命令
type ConfigChangeCommand struct {
	Type       string // "AddServer" 或 "RemoveServer"
	ServerAddr string // 服务器地址
}

// 选举超时时间
func GetElectionTimeout() time.Duration {
	return 1000 * time.Millisecond // 设置为1秒
}

// 捕获新服务器的轮数
const CatchupRounds = 3

// AddServer RPC处理函数
func (rf *Raft) AddServer(args *AddServerArgs, reply *AddServerReply) error {
	rf.stateMu.RLock()

	// 1. 检查是否是领导者
	if rf.me != rf.leaderID {
		reply.Status = NOT_LEADER
		if rf.leaderID != -1 && rf.leaderID < len(rf.peers) {
			// 尝试提供领导者的地址
			for id, peer := range rf.peers {
				if id == rf.leaderID {
					reply.LeaderHint = peer.Addr
					break
				}
			}
		}
		rf.stateMu.RUnlock()
		return nil
	}
	rf.stateMu.RUnlock()

	// 2. 捕获新服务器的日志
	// 创建与新服务器的连接
	newClient := rpc.MakeClientEnd(args.NewServer)
	success := rf.catchupNewServer(newClient, GetElectionTimeout())
	if !success {
		reply.Status = TIMEOUT
		return nil
	}

	// 3. 等待之前的配置提交
	success = rf.waitForLastConfigCommitted()
	if !success {
		reply.Status = TIMEOUT
		return nil
	}

	// 4. 添加新配置到日志
	cmd := ConfigChangeCommand{
		Type:       CmdAddServer,
		ServerAddr: args.NewServer,
	}

	// 4. 追加新配置
	index, _, _ := rf.Start(cmd)

	// 等待提交
	committed := rf.waitForIndexCommitted(index, GetElectionTimeout())
	if !committed {
		reply.Status = TIMEOUT
		return nil
	}

	// 5. 返回成功
	reply.Status = OK
	return nil
}

// RemoveServer RPC处理函数
func (rf *Raft) RemoveServer(args *RemoveServerArgs, reply *RemoveServerReply) error {
	rf.stateMu.RLock()

	// 1. 检查是否是领导者
	if rf.me != rf.leaderID {
		reply.Status = NOT_LEADER
		if rf.leaderID != -1 && rf.leaderID < len(rf.peers) {
			// 尝试提供领导者的地址
			for id, peer := range rf.peers {
				if id == rf.leaderID {
					reply.LeaderHint = peer.Addr
					break
				}
			}
		}
		rf.stateMu.RUnlock()
		return nil
	}

	// 检查是否准备移除自己
	isSelfBeingRemoved := false
	for _, peer := range rf.peers {
		if peer.Addr == args.OldServer && peer == rf.peers[rf.me] {
			isSelfBeingRemoved = true
			break
		}
	}
	rf.stateMu.RUnlock()

	// 2. 等待之前的配置提交
	rf.waitForLastConfigCommitted()

	// 3. 追加新配置到日志
	cmd := ConfigChangeCommand{
		Type:       CmdRemoveServer,
		ServerAddr: args.OldServer,
	}

	index, _, _ := rf.Start(cmd)

	// 等待提交
	committed := rf.waitForIndexCommitted(index, GetElectionTimeout())
	if !committed {
		reply.Status = TIMEOUT
		return nil
	}

	// 4. 返回成功
	reply.Status = OK

	// 如果自己被移除，则退位
	if isSelfBeingRemoved {
		rf.HighLightf("I am removed, stepping down")
		rf.stateMu.Lock()
		if rf.leaderID == rf.me {
			rf.leaderID = -1
		}
		rf.stateMu.Unlock()
	}

	return nil
}

// 处理配置变更日志条目
func (rf *Raft) applyConfigChange(entry ApplyMsg) {
	cmd, ok := entry.Command.(ConfigChangeCommand)
	if !ok {
		rf.Debugf("Invalid config change command: %v", entry.Command)
		return
	}

	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()

	switch cmd.Type {
	case CmdAddServer:
		// 解析出服务器地址
		serverAddr := cmd.ServerAddr
		// 创建新的 RPC 客户端连接
		newClient := rpc.MakeClientEnd(serverAddr)

		// 检查服务器是否已存在
		exists := false
		for _, client := range rf.peers {
			if client.Addr == serverAddr {
				exists = true
				break
			}
		}

		if !exists {
			// 添加到 peers 中，这里使用下一个可用的ID
			nextID := 0
			// 找到一个未使用的ID
			for {
				if _, exists := rf.peers[nextID]; !exists {
					break
				}
				nextID++
			}

			rf.peers[nextID] = newClient

			// 如果是领导者，更新 nextIndex 和 matchIndex
			if rf.leaderID == rf.me {
				rf.logMu.Lock()
				// 确保数组长度足够
				for len(rf.nextIndex) <= nextID {
					rf.nextIndex = append(rf.nextIndex, 0)
				}
				for len(rf.matchIndex) <= nextID {
					rf.matchIndex = append(rf.matchIndex, 0)
				}

				// 设置初始值
				rf.nextIndex[nextID] = rf.logs.getLastIndex() + 1
				rf.matchIndex[nextID] = 0
				rf.logMu.Unlock()
			}

			rf.Debugf("Server added: %s with ID %d", serverAddr, nextID)
		}

	case CmdRemoveServer:
		// 查找要移除的服务器ID
		var serverID int = -1
		var serverAddr = cmd.ServerAddr

		for id, client := range rf.peers {
			if client.Addr == serverAddr {
				serverID = id
				break
			}
		}

		if serverID != -1 {
			// 删除服务器
			delete(rf.peers, serverID)

			// 如果是领导者且不是自己被移除，更新 nextIndex 和 matchIndex
			// 这里不需要实际移除数组元素，只需不再使用该索引

			rf.Debugf("Server removed: ID %d, Address %s", serverID, serverAddr)

			// 如果自己被移除，则退位
			if serverID == rf.me {
				rf.Debugf("I am removed from cluster, stepping down")
				// 保持当前任期，但不再是领导者
				rf.Kill()
			}
		}
	}
}

// 等待日志索引被提交
func (rf *Raft) waitForIndexCommitted(index int, timeout time.Duration) bool {
	start := time.Now()
	for {
		rf.logMu.RLock()
		committed := rf.commitIndex >= index
		rf.logMu.RUnlock()

		if committed {
			return true
		}

		if time.Since(start) > timeout {
			return false
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// 等待上一个配置提交
func (rf *Raft) waitForLastConfigCommitted() bool {
	// 在实际实现中，应该记录最后一次配置变更的索引
	// 这里简化为等待所有已知日志提交
	rf.logMu.RLock()
	lastLogIndex := rf.logs.getLastIndex()
	rf.logMu.RUnlock()

	return rf.waitForIndexCommitted(lastLogIndex, GetElectionTimeout())
}

// 捕获新服务器的日志
// 尝试让新服务器赶上当前日志状态
func (rf *Raft) catchupNewServer(newClient *rpc.ClientEnd, timeout time.Duration) bool {
	start := time.Now()
	rounds := 0
	startIndex := 0

	for rounds < CatchupRounds {
		// 检查是否超时
		if time.Since(start) > timeout {
			return false
		}

		// 首先检查是否需要发送快照
		rf.logMu.RLock()
		hasSnapshot := startIndex <= rf.logs.PrevIndex
		rf.logMu.RUnlock()

		var success bool
		var lastIndex int
		if hasSnapshot {
			// 如果有快照，先发送快照
			lastIndex, success = rf.sendSnapshotToNewServer(newClient)
		} else {
			// 没有快照或快照已发送，发送日志条目
			lastIndex, success = rf.sendEntriesToNewServer(newClient, startIndex)
		}

		if !success {
			return false
		}

		startIndex = max(startIndex, lastIndex+1)

		rounds++
	}

	return true
}

// 向新服务器发送快照
func (rf *Raft) sendSnapshotToNewServer(client *rpc.ClientEnd) (lastIndex int, success bool) {
	rf.stateMu.RLock()
	currentTerm := rf.currentTerm
	leaderID := rf.me
	rf.stateMu.RUnlock()

	rf.logMu.RLock()
	args := InstallSnapshotArgs{
		Term:              currentTerm,
		LeaderID:          leaderID,
		LastIncludedIndex: rf.logs.PrevIndex,
		LastIncludedTerm:  rf.logs.PrevTerm,
		Snapshot:          rf.snapshot,
	}
	rf.logMu.RUnlock()

	var reply InstallSnapshotReply
	ok := client.Call("Raft.InstallSnapshot", &args, &reply)

	if ok {
		// 检查任期是否改变
		if reply.Term > currentTerm {
			rf.stateMu.Lock()
			rf.becomeFollower(reply.Term)
			rf.stateMu.Unlock()
			return 0, false
		}
	}

	return args.LastIncludedIndex, ok
}

// 向新服务器发送日志条目
func (rf *Raft) sendEntriesToNewServer(client *rpc.ClientEnd, nextIndex int) (lastIndex int, success bool) {
	rf.stateMu.RLock()
	currentTerm := rf.currentTerm
	leaderID := rf.me
	rf.stateMu.RUnlock()

	rf.logMu.RLock()

	entries := rf.logs.getTail(nextIndex)
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderID:     leaderID,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.logs.getEntry(nextIndex - 1).Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.logMu.RUnlock()

	var reply AppendEntriesReply
	ok := client.Call("Raft.AppendEntries", &args, &reply)

	if ok {
		// 如果失败了，可能是PrevLog不匹配
		// 在实际实现中，应该尝试不同的PrevLogIndex
		if !reply.Success {
			return 0, false
		}
	}

	return nextIndex - 1 + len(entries), ok
}

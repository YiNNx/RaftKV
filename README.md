# Raft 共识

## 共识算法

### 多副本系统的演化

单机 -> 故障可能导致数据直接丢失

定期单机备份 -> 保留一定的数据

多机，主从结构 -> 

e.g. 一台 master，多台 slave，master 负责写，定期给 slave 同步。当 master 故障时，使用 slave 切换 master

### 难题

1. 数据一致性：例如，mater 在数据同步给 slave 前宕机
   - 一致性与性能的平衡
2. 容错恢复能力
   - 自动恢复-> 脑裂问题
   - 人工介入恢复

### 共识算法想解决的问题

在多副本系统中实现一致性

- 具备自动容错恢复能力
- 只要集群中集群宕机不过半，被成功 commit 的操作就不会被丢失

### 以日志作为媒介

复制状态机

> 需要确保基于 Raft 的上层服务是可线性化（linearizable）的

## In Search of an Understandable Consensus Algorithm

### Design

![图 2](https://github.com/maemual/raft-zh_cn/raw/master/images/raft-%E5%9B%BE2.png)

#### State

如图，`currentTerm`、`votedFor`、`log[]` 是需要被持久化的，而 `commitIndex`和 `lastApplied` 是非持久化存储的。

- commitIndex：可以在宕机恢复后很快被重新计算
- lastApplied：实际上取决于状态机是否是持久化的，若状态机为持久化的，则 lastApplied 也应被持久化

#### Term

term 在 Raft 系统中充当 logical clock，从而检测过期情况。例如：

- 如果一个 server 的当前 term 比其他人小，那么他会更新自己的 term 到较大的 term 值
- 如果一个 candidate 或者 leader 发现自己的 term 过期了，那么他会立即恢复成 follower 状态
- 如果一个 server 接收到一个包含过期的 term 的请求，那么他会直接拒绝这个请求

#### 节点通信

集群中的所有节点信息通过静态配置

若需要动态实现 Raft 集群成员变更，则需要实现 One Server ConfChange 或 Joint Consensus，前者实现较为常用

### Leader Election

#### 基本规则

- A server remains in follower state as long as it receives valid RPCs from a leader or candidate.

- Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries) to all followers in order to maintain their authority.

- `election timeout`: a follower receives no communication over a period of time

  then the follower begins an election to choose a new leader

#### 选举过程

*for the follower who begins an election:*

1. currentTerm++, switch state to candidate

2. vote for itself, issues RequestVote RPCs in parallel to each of the other servers in the cluster

3. select

   - case - it wins the election: 

     ​	become the leader and sends heartbeat messages to all of the other servers to establish its authority

   - case - another server establishes itself as leader && term > itself

     ​	become a follower, sync term from the leader

   - case -  a period of time (random) goes by with no winner

     ​	begin another election

*for other follower:*

keep current term

-  only vote for one candidate each term, whose term must be larger than its term
-  on a first-come-first-served basis, and the candidate should have all committed logs

### Log Replication

- If followers crash or run slowly, or if network packets are lost, the leader retries AppendEntries RPCs **indefinitely** (even after it has responded to the client) until all followers eventually store all log entries.
- The leader keeps track of the highest index it knows to be committed, and it includes that index in future AppendEntries RPCs (including heartbeats) so that the other servers eventually find out. 

**Log Matching Property**

1. **If two entries in different logs have the same index and term, then they store the same command.** 

2. **If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.**

> **对于第一条：**
>
> leader 在任期中创建的每一条日志 index 都是唯一的。**（只有前一次成功提交，leader才会尝试对下一批log进行append操作，否则不断尝试重复未成功log的append）**
>
> **对于第二条：**
>
> 一致性检查：append时，只有follower前一条log的term和index与leader一致，才能append成功
>
> 因此可以用归纳法证明：初始的空日志状态满足日志匹配特性，然后一致性检查在日志扩展的时候保护了日志匹配特性。因此，每当附加日志 RPC 返回成功时，领导人就知道跟随者的日志一定是和自己相同的了。

关键点在于，当选后整个系统的 log 状态被 leader 的 log 状态决定，leader 总是只往当前的 log append 日志。leader 正常工作时，日志被一条条 append 到日志尾部并 commit，commit 状态总是顺序更新的。直到 leader 宕机时，未被提交的 logs 则为日志尾部的后 n 条，n>=0 && n <= 当前 term 的 log 条数。

### Safety

Raft 保证系统总是满足以下 safety properties：

1. Election Satefy: 一个 term 至多一个 leader 被选举
2. Leader Append-Only: leader 不会对自己的 log 进行删改，只会进行 append
3. Log Matching: 一条日志是 identical 的（term 与 index 都一致），则该条日志往前的序列都是 identical 的
4. Leader Completeness: leader 总是包含所有已经 commit 的日志
5. State Machine Safety: 所有状态机的生效日志都会是一致的

保证以上 properties，则能保证共识的安全性

在论文的 Leader Election 和 Log Replication 中原有的规则能保证 Election Satefy、Leader Append-Only 与 Log Matching

为了满足 Leader Completeness 需要新增规则：

1. 增加投票限制

   **the voter denies its vote if its own log is more up-to-date than that of the candidate.**

   对 up-to-date 的定义：

   - term 相同，则 index 大的 more up-to-date
   - term 不同，term 大的 more up-to-date

2. 在 leader 当选后，leader 不会尝试统计旧 term 中未 commit 的log来更新 commitIndex，而是在 append entries 时将旧 term 的所有 log 与新 term 的新增 log 一起发出 append

   (对于某些情况，例如 kv 数据库，这样的机制可能会造成查询阻塞，可以使用 no-op log 机制来解决)

> ![image](https://github.com/user-attachments/assets/6b00e09f-3c06-40a1-8a8a-53150498a842)
>
> **Figure 8:**
>
> A time sequence showing why a leader cannot determine commitment using log entries from older terms. 
>
> In (a) S1 is leader and partially replicates the log entry at index 2. 
>
> In (b) S1 crashes; S5 is elected leader for term 3 with votes from S3, S4, and itself, and accepts a different entry at log index 2. 
>
> In (c) S5 crashes; S1 restarts, is elected leader, and continues replication. At this point, the log entry from term 2 has been replicated on a majority of the servers, but it is not committed. 
>
> If S1 crashes as in (d), S5 could be elected leader (with votes from S2, S3, and S4) and overwrite the entry with its own entry from term 3. 
>
> However, if S1 replicates an entry from its current term on a majority of the servers before crashing, as in (e), then this entry is committed (S5 cannot win an election). At this point all preceding entries in the log are committed as well.

### Safety Argument

新增限制后，能够满足所有的一致性条件

以下是论证：

1. Election Satefy

   由过半票决机制保证

2. Leader Append-Only

   由 log replication 机制保证

3. Log Matching

   由于 leader 只可能 append 当前 term 的 log，因此在同一个 term 中，一个 index 对应的 operation 是唯一的

   而 leader log relication 的机制保证，若两个 server 含有同一 term 的 log，说明它们前一 term 的 log 已被这个 term 的 leader  同步过。

   通过数学归纳法：

   term = 0 时，满足 Log Matching；

   term = n 时满足 Log Matching，则 term = n + 1 时也满足 Log Matching；

   因此对于 term>= 0，Log Matching 都被满足。

4. Leader Completeness: leader 总是包含所有已经 commit 的日志

   - 首先，只考虑在投票只投相同 term 且日志更长的 candidate 的情况：

     candidate 要赢得这次选举，则最后一条 log index 必须比过半日志 index 最大值大，否则无法获得过半选票

     因此，投票只投相同 term 且日志更长时，必能满足 Leader Completeness

   - 然后，加入新的规则：

     (a) 投票也可以投更大 term 的 candidate；

     (b) leader 赢得选举后不能单独提交旧 term 的 log，而必须与当前 term 的新 log 一起提交

     加入 (b) 规则后，考虑当前系统中被 commit 过的最大任期值 t，新 leader 要么将 t 更新为当前 term，要么保持不变。

     若 t 更新为当前 term，则当前系统中 log 比过半更长的 server 也是 term 最大的 server；**之后，持有 term 大于 t 的 log 则一定持有 term t 的日志**，投更大的 term 便也等价于投给包含所有 committed log 的 server，因此新 t 值满足了 Leader Completeness。

     在 t = 0 时，系统满足  Leader Completeness；若旧 t 值满足 Leader Completeness，则在新 term 中的 t 值也必将满足 Leader Completeness；归纳法可得，系统总能满足 Leader Completeness。

5. State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

   由 4 规则可以较容易地推出 State Machine Safety 成立。

### Dynamic Membership Changes

较常用的是 One Server ConfChange

- 一次加入或删除一个节点，可以保证变更过程不会发生脑裂

  ![single-server](https://qeesung.github.io/assets/images/raft-single-server.png)

- ConfChange Log 无需被 commit 再 apply，而是直接生效

  这是因为 ConfChange 直接生效不会导致一致性问题，而可以提升一部分性能

- 新 leader 必须提交一条自己的 term 的 no-op 日志, 才允许接着变更日志。

### 日志压缩

将增量 merge 入全量来保存

关键点：

- 不将压缩决定集中在 Leader 上，每个服务器独立地压缩其已提交的日志。
- 快照的并发性：创建一个快照需要耗费很长时间，包括序列化和写入磁盘。序列化和写快照都要与常规操作并发进行，避免服务不可用。

### Client 交互

客户端只将命令发送到 Leader：

- 如果客户端不知道 Leader 是谁，它会和任意一台服务器通信；
- 如果通信的节点不是 Leader，它会告诉客户端 Leader 是谁；

Leader 直到将命令记录、提交和执行到状态机之前，不会做出响应。

这里的问题是如果 Leader 宕机会导致请求超时：

- 客户端重新发出命令到其他服务器上，最终重定向到新的 Leader
- 用新的 Leader 重试请求，直到命令被执行

这留下了一个命令可能被执行两次的风险——Leader 可能在执行命令之后但响应客户端之前宕机，此时客户端再去寻找下一个 Leader，同一个命令就会被执行两次——这是不可接受的！

解决办法是：客户端发送给 Leader 的每个命令都带上一个唯一 id。

- Leader 将唯一 id 写到日志记录中；
- 在 Leader 接受命令之前，先检查其日志中是否已经具有该 id；
- 如果 id 在日志中，说明是重复的请求，则忽略新的命令，返回旧命令的响应；

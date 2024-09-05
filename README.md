# RaftKV

基于 Raft 共识算法与分片策略构建的分布式键值存储系统。

- 完整实现了 Raft 共识算法 Leader Election, Log Replication, Persistence, Snapshot 等流程，构建了稳定的多副本共识系统

- 在 Raft 层上构建线性强一致性的键值数据库，并实现数据分片。构造 ShardCtrler 进行集群管理与分片变更，不同副本系统之间根据配置变更进行分片传输，使数据负载均衡。

- 节点通信基于 golang 原生 rpc 库，利用锁、channel、context 等机制实现了严格的并发安全。

![image-20240906031003138](https://cdn.just-plain.fun/img/image-20240906031003138.png)

## 代码架构

```
├── cmd
│   ├── kv-cli             // 运行 kv 客户端，连接对应的 shard group kv 服务，执行分布式 KV 操作
│   ├── server             // 运行 shardkv server，初始化每个 KV 服务组，负责 shard 的存储与管理
│   └── shard-cli          // 运行 shard 管理客户端，提供 shard 管理相关的操作，如重新分片、负载均衡等
├── internal               
│   ├── raft               // 核心 raft 实现，处理 group 内部的日志同步和状态机管理
│   │   ├── candidate.go   // 处理 Raft 节点的 candidate 状态，包括发起选举和处理选票
│   │   ├── follower.go    // 处理 Raft 节点的 follower 状态，监听 leader 的心跳并接受日志条目
│   │   ├── leader.go      // 处理 Raft 节点的 leader 状态，负责日志复制、心跳广播和集群管理
│   │   ├── log.go         // 实现 Raft Log 的管理与持久化
│   │   ├── raft.go        // 定义 Raft 节点的主要接口和状态机
│   │   ├── rpc.go         // 实现 Raft 集群内的 RPC 通信，包括 AppendEntries、RequestVote 等操作
│   │   ├── state.go       // 管理节点的持久化状态，处理节点角色的变更和快照的生成
│   │   ├── types.go       // Raft 系统通用的数据结构定义
│   │   └── util.go        // 通用工具，辅助日志输出及其他实用功能
│   ├── shardctl           // 分片控制模块，负责 shard 的负载均衡与分发
│   │   ├── client         // 分片客户端，用于与 shard server 进行通信
│   │   ├── common         // 通用结构体，包括分片相关的配置信息和接口参数定义
│   │   ├── repo           // 分片的储存实现，管理 shard 的分配与传输
│   │   ├── server         // 分片服务端，实现 shard 的调度与管理
│   │   └── util           // 通用工具，辅助 shard 操作与数据分发
│   └── shardkv            // 分片 KV 存储模块，基于 Raft 实现的分布式 KV 系统
│       ├── client         // 分片 KV 系统的客户端，处理与 kv server 的交互
│       ├── common         // 通用结构体，定义 shardkv 系统的配置和接口参数
│       ├── repository     // kv 数据的存储实现
│       ├── server         // kv server，负责处理 shard 内的数据请求并与其他 shard group 进行同步
│       └── util           // 辅助工具类，处理日志、错误处理等通用功能
└── pkg                    
    ├── persister          // 节点状态与日志的持久化储存，实现节点快照功能，保证数据一致性
    └── rpc                // rpc 通信库的封     
```

## 运行

运行集群中的单个 Server:

```shell
go run cmd/server/server.go -gid <group id> -id <node id> -nodes <group nodes address list> -ctrlers <ctrlers nodes address list>
```

使用 `-recover true` 来从本地的持久化数据中恢复宕机的服务

使用 shard-cli 改变集群成员与分片：

```
go run cmd/shard-cli/cli.go -ctrlers <addr list>
shardctl cli > query
{0 [0 0 0 0 0 0 0 0 0 0] map[]}
shardctl cli > join 1 :8088,:8089,:8090
<nil>
shardctl cli > query
{1 [1 1 1 1 1 1 1 1 1 1] map[1:[:8088 :8089 :8090]]}
shardctl cli > join 2 :9090,:9091,:9092   
<nil>
shardctl cli > query
{2 [1 1 1 1 1 2 2 2 2 2] map[1:[:8088 :8089 :8090] 2:[:9090 :9091 :9092]]}
shardctl cli > join 3 :9081,:9082,:9083
<nil>
shardctl cli > query
{3 [1 1 1 1 3 2 2 2 3 3] map[1:[:8088 :8089 :8090] 2:[:9090 :9091 :9092] 3:[:9081 :9082 :9083]]}
shardctl cli > leave 1
ok
shardctl cli > query
{4 [2 3 2 3 3 2 2 2 3 3] map[2:[:9090 :9091 :9092] 3:[:9081 :9082 :9083]]}

```

使用 kv-cli 进行数据操作：

```shell
go run cmd/cli/cli.go -addr <addr list>

raftkv cli > GET hello

raftkv cli > PUT hello world
ok
raftkv cli > GET hello
world
raftkv cli > APPEND hello !
ok
raftkv cli > GET hello
world!
```

![Screenshot from 2024-09-06 01-47-40](https://cdn.just-plain.fun/img/2024-09-06-01-47-40.png)


# RaftKV

- 基于 Raft 协议完整实现了 Leader Election, Log Replication, Persistence, Snapshot 等流程，构建了一个稳定的多副本共识系统

- 在 Raft 层上构建具备高容错性的键值数据库，实现了操作的线性强一致性

- 节点通信基于 golang 原生 rpc 库

## 运行

运行 Server:

在 `config.toml` 中添加集群配置，运行节点：

```shell
go run cmd/server/server.go -c config.toml -id <node id> 
```

使用 `-recover true` 来从本地的持久化数据中恢复宕机的服务

使用 cli 进行数据操作：

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

![image-20240829224117943](https://cdn.just-plain.fun/img/image-20240829224117943.png)

## 代码架构

```
.
├── cmd
│   ├── cli                // 运行 kv clerk，连接 server 对输入进行执行
│   └── server             // 运行 kv server，初始化 server 及集群 rpc 连接
├── internal               
│   ├── server             // kv server, 对于每个操作请求，调用底层 raft 进行日志同步，并处理保证操作的线性强一致性
│   ├── client             // kv clerk
│   ├── common             // 通用结构体，包括配置、rpc 接口参数定义等
│   ├── raft               // 核心 raft 实现
│   │   ├── raft.go        // 定义 Raft 节点与对上层服务暴露的接口
│   │   ├── log.go         // 对 Raft Log 的完善实现
│   │   ├── rpc.go         // 实现了 AppendEntries、RequestVote、InstallSnapshot 等操作
│   │   ├── candidate.go   // 处于 candidate 状态的节点操作，开启选举并处理状态机变更
│   │   ├── follower.go    // 处于 follower 状态的节点操作，当 ticker 过期时成为 candidate
│   │   ├── leader.go      // 处于 leader 状态的节点操作，负责发送心跳，进行 AppendEntries 广播与节点的 InstallSnapshot 处理等，维护整个集群的日志一致性
│   │   ├── state.go       // 状态变更及持久化等相关操作，保证线程安全性
│   │   ├── types.go       // 通用结构体
│   │   └── util.go        // 实现了易于错误排查的日志输出
│   ├── repository         // kv 储存的实现
│   └── util                
├── pkg                    
│   ├── persister          // 实现节点状态与日志的持久化储存
│   └── rpc                // 原生 rpc 库的封装
└── README.md

```


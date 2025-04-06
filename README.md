# RaftKV

- 基于 Raft 协议完整实现了 Leader Election, Log Replication, Persistence, Snapshot 等流程，构建了一个稳定的多副本共识系统

- 在 Raft 层上构建具备高容错性的键值数据库，实现了操作的线性强一致性

- 节点通信基于 golang 原生 rpc 库

## 运行

运行 Server:

```shell
go run cmd/server/server.go -nodes localhost:8080,localhost:8081,localhost:8082 -id 0 
```

使用 `-recover true` 来从本地的持久化数据中恢复宕机的服务

使用 cli 进行数据操作：

```shell
go run cmd/server/server.go -nodes localhost:8080,localhost:8081,localhost:8082 -id 0

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

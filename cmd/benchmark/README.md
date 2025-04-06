# RaftKV 性能测试框架

这个性能测试框架用于对 RaftKV 分布式键值存储系统进行全面的性能测试，支持模拟各种分布式环境下的场景。

## 功能特性

- 支持本地多进程模式和分布式模式
- 自动管理集群的启动和停止
- 支持多种工作负载类型（读重、写重、混合、扫描）
- 实时性能监控和报告
- 模拟网络分区、节点故障等故障场景
- 可配置的测试参数

## 使用方法

### 本地模式测试

在本地模式下，测试框架会自动启动多个服务器进程，并管理它们的生命周期：

```shell
go run cmd/benchmark/benchmark.go -mode=local -duration=120 -clients=20 -workload=mixed
```

### 分布式模式测试

在分布式模式下，需要先手动在不同机器上启动服务器，然后运行测试框架：

1. 在各个服务器节点启动服务：

```shell
# 在节点1上
go run cmd/server/server.go -nodes <所有节点地址列表> -id 0

# 在节点2上
go run cmd/server/server.go -nodes <所有节点地址列表> -id 1

# 在节点3上
go run cmd/server/server.go -nodes <所有节点地址列表> -id 2
```

2. 在控制节点上运行测试框架：

```shell
go run cmd/benchmark/benchmark.go -mode=distributed -config=my-config.json -duration=300
```

### 命令行参数

- `-config`: 配置文件路径 (默认: "cmd/benchmark/config.json")
- `-mode`: 测试模式: local (单机多进程) 或 distributed (分布式) (默认: "local")
- `-duration`: 测试持续时间(秒) (默认: 60)
- `-workload`: 工作负载类型: read-heavy, write-heavy, mixed, scan (默认: "mixed")
- `-clients`: 客户端数量 (默认: 10)
- `-report-rate`: 性能报告间隔(秒) (默认: 5)

### 配置文件

配置文件以 JSON 格式定义测试环境和场景。默认配置文件位于 `cmd/benchmark/config.json`，包含以下内容：

```json
{
  "mode": "local",
  "serverNodes": [
    { "id": 0, "address": "localhost:8080" },
    { "id": 1, "address": "localhost:8081" },
    { "id": 2, "address": "localhost:8082" }
  ],
  "clientNodes": ["localhost"],
  "scenarios": {
    "networkPartition": {
      "enabled": true,
      "startAfterSeconds": 30,
      "durationSeconds": 15,
      "nodeIndices": [2, 3]
    },
    "nodeFailure": {
      "enabled": false,
      "startAfterSeconds": 20,
      "durationSeconds": 10,
      "nodeIndex": 1
    },
    "highLoad": {
      "enabled": false,
      "startAfterSeconds": 40,
      "durationSeconds": 20,
      "clientMultiplier": 5
    }
  },
  "cleanup": true
}
```

## 工作负载类型

- `read-heavy`: 读取操作占主导 (80% 读取, 10% 写入, 10% 追加)
- `write-heavy`: 写入操作占主导 (30% 读取, 40% 写入, 30% 追加)
- `mixed`: 平衡的混合负载 (50% 读取, 30% 写入, 20% 追加)
- `scan`: 主要是连续读取操作 (90% 读取, 5% 写入, 5% 追加)

## 性能指标

测试框架会收集并报告以下性能指标：

- 总请求数
- 每秒查询次数 (QPS)
- 平均请求延迟
- 延迟分布 (P50, P90, P99)
- 请求类型分布
- 错误率

## 测试场景

### 网络分区

模拟网络分区情况，将集群分割成两部分，测试系统的可用性和恢复能力。

### 节点故障

模拟单个节点故障，测试系统的故障转移能力。

### 高负载压力

在测试过程中突然增加客户端数量，测试系统在高负载下的性能和稳定性。

## 示例用法

### 基本性能测试

```shell
go run cmd/benchmark/benchmark.go -duration=60 -clients=10
```

### 长时间稳定性测试

```shell
go run cmd/benchmark/benchmark.go -duration=3600 -clients=5 -report-rate=60
```

### 写入密集型测试

```shell
go run cmd/benchmark/benchmark.go -workload=write-heavy -clients=20
```

### 网络分区测试

创建一个配置文件，启用网络分区场景，然后运行：

```shell
go run cmd/benchmark/benchmark.go -config=partition-test.json
``` 
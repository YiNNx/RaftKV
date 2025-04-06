# RaftKV 性能测试框架

这个性能测试框架用于对 RaftKV 分布式键值存储系统进行全面的性能测试，支持模拟各种分布式环境下的场景。

## 功能特性

- 支持本地多进程模式和分布式模式
- 自动管理集群的启动和停止
- 支持多种工作负载类型（读重、写重、混合、扫描）
- 实时性能监控和报告
- 模拟网络分区、节点故障等故障场景
- 模拟节点频繁宕机和恢复场景
- 可配置的测试参数
- 生成可视化性能报告和图表

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
- `-result-dir`: 性能测试结果保存目录 (默认: "benchmark-results")
- `-test-name`: 测试名称，会作为结果子目录的名称 (默认: 自动生成时间戳名称)
- `-visualization`: 是否生成可视化报告 (默认: true)

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
    },
    "frequentFailure": {
      "enabled": false,
      "startAfterSeconds": 15,
      "durationSeconds": 30,
      "failureCycleSeconds": 3,
      "recoveryCycleSeconds": 2,
      "nodeIndices": [1, 2, 3],
      "failOneByOne": true
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

### 频繁节点故障

模拟节点频繁宕机和恢复的情况，测试系统在极端条件下的可靠性和自愈能力。支持两种模式：

1. **依次故障模式**：按照配置的节点列表顺序，依次使节点宕机和恢复
2. **同时故障模式**：同时使所有指定节点宕机，然后同时恢复

可配置参数包括：
- 故障周期（秒）：节点处于宕机状态的持续时间
- 恢复周期（秒）：节点处于恢复状态的持续时间
- 测试持续时间（秒）：整个频繁故障测试的持续时间
- 故障节点列表：需要参与测试的节点索引列表

## 可视化性能报告

测试框架支持生成可视化的性能报告，通过HTML和交互式图表直观展示系统性能。

### 可视化特性

- 查看QPS随时间的变化趋势
- 查看延迟（平均，P50，P90，P99）随时间的变化
- 查看操作类型分布及其随时间的变化
- 查看错误率趋势图
- 查看整体性能摘要

### 访问报告

每次测试完成后，会在指定的结果目录下生成报告：

```
benchmark-results/
  └── raftkv-benchmark-20230424-153022/  # 测试时间戳作为目录名
      ├── index.html           # 主报告页面（摘要信息）
      └── performance_charts.html # 包含所有交互式图表的页面
```

在浏览器中打开 `index.html` 查看整体摘要信息，点击"查看交互式性能图表"链接可以查看详细的性能图表。

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

### 频繁故障测试

创建一个配置文件，启用频繁故障场景，然后运行：

```shell
go run cmd/benchmark/benchmark.go -config=frequent-failure-test.json
```

### 自定义结果目录和测试名称

```shell
go run cmd/benchmark/benchmark.go -result-dir=/tmp/raftkv-results -test-name=high-load-test
``` 
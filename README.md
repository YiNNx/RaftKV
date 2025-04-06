# RaftKV

## 运行

运行三个 Server:

```shell
go run cmd/server/server.go -nodes localhost:8080,localhost:8081,localhost:8082 -id 0

go run cmd/server/server.go -nodes localhost:8080,localhost:8081,localhost:8082 -id 1

go run cmd/server/server.go -nodes localhost:8080,localhost:8081,localhost:8082 -id 2
```

使用 `-recover true` 来从本地的持久化数据中恢复宕机的服务

使用 cli 进行数据操作：

```shell
go run cmd/kv-cli/cli.go -nodes :8080,:8081,:8082,:8083

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

## 性能测试

RaftKV提供了一个全面的性能测试框架，用于评估系统在不同工作负载和故障场景下的性能表现。

### 基本性能测试

```shell
go run cmd/benchmark/benchmark.go -duration=60 -clients=10
```

### 测试场景

支持以下测试场景：
- 网络分区测试（集群分裂）
- 节点故障恢复测试
- 高负载压力测试
- 节点频繁故障测试（模拟频繁宕机和恢复）

### 频繁故障测试

测试系统在节点频繁宕机和恢复的极端条件下的可靠性：

```shell
go run cmd/benchmark/benchmark.go -config=cmd/benchmark/config.json
```

在配置文件中启用 frequentFailure 场景：

```json
"frequentFailure": {
  "enabled": true,
  "startAfterSeconds": 5,
  "durationSeconds": 30,
  "failureCycleSeconds": 3,
  "recoveryCycleSeconds": 2,
  "nodeIndices": [1, 2, 3],
  "failOneByOne": true
}
```

### 可视化性能报告

测试框架自动生成交互式的性能可视化报告，包括：

- QPS趋势图
- 延迟分布图
- 操作类型分布图
- 错误率趋势图

所有图表整合在一个HTML页面中，方便直观地分析系统性能。

```shell
# 生成可视化报告
go run cmd/benchmark/benchmark.go -duration=120 -test-name=my-benchmark

# 查看报告
# 打开 benchmark-results/my-benchmark/index.html
```

![性能报告示例](https://example.com/performance-report.png)

### 详细用法

查看 [性能测试文档](cmd/benchmark/README.md) 获取更多详细信息。
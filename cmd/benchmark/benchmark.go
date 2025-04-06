package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"raftkv/cmd/benchmark/config"
	"raftkv/cmd/benchmark/load"
	"raftkv/internal/kvserver"
	"raftkv/pkg/rpc"
)

var (
	configPath   = flag.String("config", "cmd/benchmark/config.json", "配置文件路径")
	mode         = flag.String("mode", "local", "测试模式: local (单机多进程) 或 distributed (分布式)")
	duration     = flag.Int("duration", 60, "测试持续时间(秒)")
	workloadType = flag.String("workload", "mixed", "工作负载类型: read-heavy, write-heavy, mixed, scan")
	clientCount  = flag.Int("clients", 10, "客户端数量")
	reportRate   = flag.Int("report-rate", 5, "性能报告间隔(秒)")
)

// 服务器进程信息
type ServerProcess struct {
	Cmd    *exec.Cmd
	ID     int
	Addr   string
	Active bool
}

// 性能测试管理器
type BenchmarkManager struct {
	Config       *config.Config
	Servers      []*ServerProcess
	ClientNodes  []string
	LoadGen      *load.Generator
	Results      *load.Results
	StopChan     chan struct{}
	ServerWg     sync.WaitGroup
	ClientWg     sync.WaitGroup
	ReportTicker *time.Ticker
}

// 创建新的性能测试管理器
func NewBenchmarkManager(cfg *config.Config) *BenchmarkManager {
	return &BenchmarkManager{
		Config:   cfg,
		Servers:  make([]*ServerProcess, len(cfg.ServerNodes)),
		StopChan: make(chan struct{}),
		Results:  load.NewResults(),
	}
}

// 启动所有服务器
func (bm *BenchmarkManager) StartServers() error {
	log.Println("启动服务器...")

	// 构造节点地址列表字符串
	var nodeAddrs []string
	for _, node := range bm.Config.ServerNodes {
		nodeAddrs = append(nodeAddrs, node.Address)
	}
	nodeAddrsStr := strings.Join(nodeAddrs, ",")

	// 启动每个服务器进程
	for i, node := range bm.Config.ServerNodes {
		if bm.Config.Mode == "local" {
			// 本地模式：启动进程
			cmd := exec.Command("go", "run", "cmd/server/server.go",
				"-id", fmt.Sprintf("%d", node.ID),
				"-nodes", nodeAddrsStr)

			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Start()
			if err != nil {
				return fmt.Errorf("启动服务器 %d 失败: %v", node.ID, err)
			}

			bm.Servers[i] = &ServerProcess{
				Cmd:    cmd,
				ID:     node.ID,
				Addr:   node.Address,
				Active: true,
			}

			log.Printf("服务器 %d 启动在 %s\n", node.ID, node.Address)
		} else {
			// 分布式模式：假设服务器已经运行
			bm.Servers[i] = &ServerProcess{
				ID:     node.ID,
				Addr:   node.Address,
				Active: true,
			}
			log.Printf("使用现有服务器 %d 在 %s\n", node.ID, node.Address)
		}
	}

	// 等待服务器完全启动
	time.Sleep(3 * time.Second)
	return nil
}

// 停止所有服务器
func (bm *BenchmarkManager) StopServers() {
	log.Println("停止服务器...")

	for _, server := range bm.Servers {
		if server.Cmd != nil && server.Active {
			if runtime.GOOS == "windows" {
				_ = server.Cmd.Process.Kill()
			} else {
				_ = server.Cmd.Process.Signal(os.Interrupt)
			}
			log.Printf("已发送终止信号到服务器 %d\n", server.ID)
		}
	}

	// 等待服务器进程结束
	for _, server := range bm.Servers {
		if server.Cmd != nil && server.Active {
			_ = server.Cmd.Wait()
			log.Printf("服务器 %d 已终止\n", server.ID)
		}
	}
}

// 启动客户端负载
func (bm *BenchmarkManager) StartClients() {
	log.Printf("启动 %d 个客户端...\n", *clientCount)

	// 准备服务器地址列表
	var serverAddrs []string
	for _, server := range bm.Servers {
		if server.Active {
			serverAddrs = append(serverAddrs, server.Addr)
		}
	}

	// 创建RPC客户端
	var rpcEnds []*rpc.ClientEnd
	for _, addr := range serverAddrs {
		rpcEnds = append(rpcEnds, rpc.MakeClientEnd(addr))
	}

	// 初始化负载生成器
	workload := load.WorkloadFromString(*workloadType)
	bm.LoadGen = load.NewGenerator(
		*clientCount,
		workload,
		func() kvserver.Clerk {
			return *kvserver.MakeClerk(rpcEnds)
		},
		bm.Results,
	)

	// 启动负载生成
	bm.LoadGen.Start()
}

// 停止客户端负载
func (bm *BenchmarkManager) StopClients() {
	log.Println("停止客户端...")
	if bm.LoadGen != nil {
		bm.LoadGen.Stop()
	}
}

// 周期性报告性能
func (bm *BenchmarkManager) StartReporting() {
	log.Printf("开始性能监控，每 %d 秒报告一次\n", *reportRate)
	bm.ReportTicker = time.NewTicker(time.Duration(*reportRate) * time.Second)

	go func() {
		for {
			select {
			case <-bm.ReportTicker.C:
				bm.ReportPerformance(false)
			case <-bm.StopChan:
				return
			}
		}
	}()
}

// 停止性能报告
func (bm *BenchmarkManager) StopReporting() {
	if bm.ReportTicker != nil {
		bm.ReportTicker.Stop()
	}
	close(bm.StopChan)
}

// 报告性能
func (bm *BenchmarkManager) ReportPerformance(final bool) {
	stats := bm.Results.GetStats()

	if final {
		fmt.Println("\n======= 最终性能报告 =======")
	} else {
		fmt.Println("\n------- 阶段性能报告 -------")
	}

	fmt.Printf("总请求数: %d\n", stats.TotalOps)
	fmt.Printf("每秒请求数 (QPS): %.2f\n", stats.QPS)
	fmt.Printf("平均延迟: %.2f ms\n", stats.AvgLatency)
	fmt.Printf("延迟分布:\n")
	fmt.Printf("  P50: %.2f ms\n", stats.P50Latency)
	fmt.Printf("  P90: %.2f ms\n", stats.P90Latency)
	fmt.Printf("  P99: %.2f ms\n", stats.P99Latency)
	fmt.Printf("请求类型分布:\n")
	fmt.Printf("  读取: %d (%.1f%%)\n", stats.ReadOps, stats.ReadOpsPercent)
	fmt.Printf("  写入: %d (%.1f%%)\n", stats.WriteOps, stats.WriteOpsPercent)
	fmt.Printf("  追加: %d (%.1f%%)\n", stats.AppendOps, stats.AppendOpsPercent)
	fmt.Printf("错误: %d (%.2f%%)\n", stats.Errors, stats.ErrorRate)

	if final {
		fmt.Println("============================")
	} else {
		fmt.Println("----------------------------")
	}
}

// 模拟网络分区
func (bm *BenchmarkManager) SimulateNetworkPartition(duration time.Duration, nodeIndices []int) {
	log.Println("模拟网络分区...")

	// 将特定节点标记为不可用
	for _, idx := range nodeIndices {
		if idx >= 0 && idx < len(bm.Servers) {
			bm.Servers[idx].Active = false
			log.Printf("节点 %d 暂时不可用\n", bm.Servers[idx].ID)
		}
	}

	// 等待指定的分区持续时间
	time.Sleep(duration)

	// 恢复网络连接
	for _, idx := range nodeIndices {
		if idx >= 0 && idx < len(bm.Servers) {
			bm.Servers[idx].Active = true
			log.Printf("节点 %d 重新可用\n", bm.Servers[idx].ID)
		}
	}

	log.Println("网络分区结束，所有节点已恢复")
}

// 运行基准测试
func (bm *BenchmarkManager) RunBenchmark() {
	// 启动性能监控
	bm.StartReporting()

	// 启动客户端负载
	bm.StartClients()

	// 运行指定的持续时间
	log.Printf("测试将持续运行 %d 秒...\n", *duration)

	// 如果配置了网络分区，在适当时间模拟分区
	if bm.Config.Scenarios.NetworkPartition.Enabled {
		partitionTime := time.Duration(bm.Config.Scenarios.NetworkPartition.StartAfterSeconds) * time.Second
		partitionDuration := time.Duration(bm.Config.Scenarios.NetworkPartition.DurationSeconds) * time.Second

		time.AfterFunc(partitionTime, func() {
			bm.SimulateNetworkPartition(partitionDuration, bm.Config.Scenarios.NetworkPartition.NodeIndices)
		})
	}

	// 等待测试结束
	time.Sleep(time.Duration(*duration) * time.Second)

	// 停止客户端和报告
	bm.StopClients()
	bm.StopReporting()

	// 输出最终报告
	bm.ReportPerformance(true)
}

// 清理测试环境
func (bm *BenchmarkManager) Cleanup() {
	// 停止所有服务器
	if bm.Config.Mode == "local" {
		bm.StopServers()
	}

	// 如果配置了数据清理，执行清理操作
	if bm.Config.Cleanup {
		log.Println("清理测试数据...")
		os.RemoveAll("/tmp/kvraft")
	}
}

func main() {
	flag.Parse()

	// 加载配置文件
	cfg, err := config.LoadConfigFromFile(*configPath)
	if err != nil {
		// 如果配置文件不存在，创建默认配置
		dirPath := filepath.Dir(*configPath)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			os.MkdirAll(dirPath, 0755)
		}

		cfg = config.DefaultConfig()
		cfg.Mode = *mode
		config.SaveConfigToFile(cfg, *configPath)
		log.Printf("已创建默认配置文件: %s\n", *configPath)
	}

	// 以命令行参数覆盖配置
	if *mode != "local" {
		cfg.Mode = *mode
	}

	// 创建测试管理器
	manager := NewBenchmarkManager(cfg)

	// 启动服务器（仅在本地模式下）
	if cfg.Mode == "local" {
		if err := manager.StartServers(); err != nil {
			log.Fatalf("启动服务器失败: %v", err)
		}
	}

	// 设置清理操作（当程序退出时执行）
	defer manager.Cleanup()

	// 运行基准测试
	manager.RunBenchmark()
}

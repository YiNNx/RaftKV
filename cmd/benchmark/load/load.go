package load

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"raftkv/internal/kvraft"
)

// 工作负载类型
type WorkloadType int

const (
	ReadHeavy  WorkloadType = iota // 读多写少
	WriteHeavy                     // 写多读少
	Mixed                          // 混合读写
	Scan                           // 扫描操作
)

// 从字符串解析工作负载类型
func WorkloadFromString(workloadType string) WorkloadType {
	switch workloadType {
	case "read-heavy":
		return ReadHeavy
	case "write-heavy":
		return WriteHeavy
	case "scan":
		return Scan
	default:
		return Mixed
	}
}

// 操作类型
type OpType int

const (
	Get OpType = iota
	Put
	Append
)

// 操作请求
type Operation struct {
	Type  OpType
	Key   string
	Value string
}

// 操作结果
type OpResult struct {
	Type      OpType
	StartTime time.Time
	EndTime   time.Time
	Latency   time.Duration
	Error     error
}

// 性能统计结果
type Results struct {
	mu            *sync.Mutex
	results       []OpResult
	startTime     time.Time
	opsCount      int32
	lastOpsCount  int32
	laststartTime time.Time
	readOps       int32
	writeOps      int32
	appendOps     int32
	errors        int32
}

// 创建性能统计对象
func NewResults() *Results {
	return &Results{
		mu:            new(sync.Mutex),
		results:       make([]OpResult, 0, 10000),
		startTime:     time.Now(),
		laststartTime: time.Now(),
	}
}

// 添加操作结果
func (r *Results) AddResult(result OpResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.results = append(r.results, result)
	r.opsCount++

	switch result.Type {
	case Get:
		r.readOps++
	case Put:
		r.writeOps++
	case Append:
		r.appendOps++
	}

	if result.Error != nil {
		r.errors++
	}
}

// 性能统计数据
type Stats struct {
	TotalOps         int
	QPS              float64
	CurrentQPS       float64
	AvgLatency       float64
	P50Latency       float64
	P90Latency       float64
	P99Latency       float64
	ReadOps          int
	WriteOps         int
	AppendOps        int
	ReadOpsPercent   float64
	WriteOpsPercent  float64
	AppendOpsPercent float64
	Errors           int
	ErrorRate        float64
}

// 获取性能统计数据
func (r *Results) GetStats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()

	if len(r.results) == 0 {
		return Stats{}
	}

	// 按延迟排序用于计算百分位数
	latencies := make([]float64, 0, len(r.results))
	var totalLatency time.Duration

	for _, result := range r.results {
		latency := result.EndTime.Sub(result.StartTime)
		latencies = append(latencies, float64(latency.Microseconds())/1000.0)
		totalLatency += latency
	}

	sort.Float64s(latencies)

	// 计算平均延迟和百分位数
	avgLatency := float64(totalLatency.Microseconds()) / 1000.0 / float64(len(r.results))

	p50Index := int(float64(len(latencies)) * 0.5)
	p90Index := int(float64(len(latencies)) * 0.9)
	p99Index := int(float64(len(latencies)) * 0.99)

	p50Latency := latencies[p50Index]
	p90Latency := latencies[p90Index]
	p99Latency := latencies[p99Index]

	// 计算QPS（每秒查询次数）
	qps := float64(r.opsCount) / now.Sub(r.startTime).Seconds()

	// 计算总操作数
	lastOpsCount := int(r.lastOpsCount)
	laststartTime := r.laststartTime
	r.lastOpsCount = int32(r.opsCount)
	r.laststartTime = now

	// 计算操作类型百分比
	totalOps := int(r.opsCount)
	readOpsPercent := 0.0
	writeOpsPercent := 0.0
	appendOpsPercent := 0.0
	errorRate := 0.0
	if totalOps > 0 {
		readOpsPercent = float64(r.readOps) / float64(totalOps) * 100
		writeOpsPercent = float64(r.writeOps) / float64(totalOps) * 100
		appendOpsPercent = float64(r.appendOps) / float64(totalOps) * 100
		errorRate = float64(r.errors) / float64(totalOps) * 100
	}

	return Stats{
		TotalOps:         totalOps,
		QPS:              qps,
		CurrentQPS:       (float64(r.lastOpsCount) - float64(lastOpsCount)) / r.laststartTime.Sub(laststartTime).Seconds(),
		AvgLatency:       avgLatency,
		P50Latency:       p50Latency,
		P90Latency:       p90Latency,
		P99Latency:       p99Latency,
		ReadOps:          int(r.readOps),
		WriteOps:         int(r.writeOps),
		AppendOps:        int(r.appendOps),
		Errors:           int(r.errors),
		ReadOpsPercent:   readOpsPercent,
		WriteOpsPercent:  writeOpsPercent,
		AppendOpsPercent: appendOpsPercent,
		ErrorRate:        errorRate,
	}
}

// 负载生成器
type Generator struct {
	clientCount    int
	workloadType   WorkloadType
	clerkFactory   func() kvraft.Clerk
	results        *Results
	stopChan       chan struct{}
	wg             sync.WaitGroup
	keySpace       []string
	valueSpace     []string
	keySpaceSize   int
	valueSpaceSize int
}

// 创建负载生成器
func NewGenerator(
	clientCount int,
	workloadType WorkloadType,
	clerkFactory func() kvraft.Clerk,
	results *Results,
) *Generator {
	const keySpaceSize = 1000
	const valueSpaceSize = 100

	// 生成测试用的键和值空间
	keySpace := make([]string, keySpaceSize)
	valueSpace := make([]string, valueSpaceSize)

	for i := 0; i < keySpaceSize; i++ {
		keySpace[i] = generateRandomString(8)
	}

	for i := 0; i < valueSpaceSize; i++ {
		valueSpace[i] = generateRandomString(32)
	}

	return &Generator{
		clientCount:    clientCount,
		workloadType:   workloadType,
		clerkFactory:   clerkFactory,
		results:        results,
		stopChan:       make(chan struct{}),
		keySpace:       keySpace,
		valueSpace:     valueSpace,
		keySpaceSize:   keySpaceSize,
		valueSpaceSize: valueSpaceSize,
	}
}

// 生成随机字符串
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成下一个操作请求
func (g *Generator) nextOperation() Operation {
	var opType OpType
	var opProb float64

	switch g.workloadType {
	case ReadHeavy:
		opProb = rand.Float64()
		if opProb < 0.8 {
			opType = Get
		} else if opProb < 0.9 {
			opType = Put
		} else {
			opType = Append
		}
	case WriteHeavy:
		opProb = rand.Float64()
		if opProb < 0.3 {
			opType = Get
		} else if opProb < 0.7 {
			opType = Put
		} else {
			opType = Append
		}
	case Mixed:
		opProb = rand.Float64()
		if opProb < 0.5 {
			opType = Get
		} else if opProb < 0.8 {
			opType = Put
		} else {
			opType = Append
		}
	case Scan:
		// Scan工作负载使用连续的键查询
		opProb = rand.Float64()
		if opProb < 0.9 {
			opType = Get
		} else if opProb < 0.95 {
			opType = Put
		} else {
			opType = Append
		}
	}

	keyIndex := rand.Intn(g.keySpaceSize)
	valueIndex := rand.Intn(g.valueSpaceSize)

	return Operation{
		Type:  opType,
		Key:   g.keySpace[keyIndex],
		Value: g.valueSpace[valueIndex],
	}
}

// 执行客户端工作循环
func (g *Generator) clientWorker(id int) {
	defer g.wg.Done()

	// 创建KV客户端
	clerk := g.clerkFactory()

	// 为随机生成器使用不同的种子
	localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		select {
		case <-g.stopChan:
			return
		default:
			// 继续执行
		}

		// 获取下一个操作
		op := g.nextOperation()

		// 记录开始时间
		startTime := time.Now()
		var err error

		// 执行操作
		switch op.Type {
		case Get:
			_ = clerk.Get(op.Key)
		case Put:
			clerk.Put(op.Key, op.Value)
		case Append:
			clerk.Append(op.Key, op.Value)
		}

		// 记录结束时间和结果
		endTime := time.Now()
		result := OpResult{
			Type:      op.Type,
			StartTime: startTime,
			EndTime:   endTime,
			Latency:   endTime.Sub(startTime),
			Error:     err,
		}

		go g.results.AddResult(result)

		// 添加一些随机间隔，避免所有客户端同时发送请求
		time.Sleep(time.Millisecond * time.Duration(10+localRand.Intn(10)))
	}
}

// 启动负载生成
func (g *Generator) Start() {
	// 启动客户端工作线程
	for i := 0; i < g.clientCount; i++ {
		g.wg.Add(1)
		go g.clientWorker(i)
	}
	log.Printf("已启动 %d 个客户端\n", g.clientCount)
}

// 停止负载生成
func (g *Generator) Stop() {
	close(g.stopChan)
	g.wg.Wait()
}

package fault

import (
	"sync"
	"time"
)

// 故障类型
type FaultType int

const (
	NoFault              FaultType = iota
	NodeFailure                    // 节点宕机
	NetworkPartition               // 网络分区
	HighLatency                    // 高延迟
	PacketLoss                     // 丢包
	TemporaryUnavailable           // 临时不可用
)

// 故障严重程度
type Severity int

const (
	Low Severity = iota
	Medium
	High
	Critical
)

// 网络指标
type NetworkMetrics struct {
	Latency    time.Duration
	PacketLoss float64
	Timestamp  time.Time
}

// 滑动窗口
type SlidingWindow struct {
	windowSize int
	metrics    []NetworkMetrics
	mu         sync.RWMutex
}

// 创建新的滑动窗口
func NewSlidingWindow(size int) *SlidingWindow {
	return &SlidingWindow{
		windowSize: size,
		metrics:    make([]NetworkMetrics, 0, size),
	}
}

// 添加新的网络指标
func (sw *SlidingWindow) AddMetric(metric NetworkMetrics) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.metrics = append(sw.metrics, metric)
	if len(sw.metrics) > sw.windowSize {
		sw.metrics = sw.metrics[1:]
	}
}

// 计算平均延迟
func (sw *SlidingWindow) AverageLatency() time.Duration {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	if len(sw.metrics) == 0 {
		return 0
	}

	var sum time.Duration
	for _, m := range sw.metrics {
		sum += m.Latency
	}
	return sum / time.Duration(len(sw.metrics))
}

// 计算平均丢包率
func (sw *SlidingWindow) AveragePacketLoss() float64 {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	if len(sw.metrics) == 0 {
		return 0
	}

	var sum float64
	for _, m := range sw.metrics {
		sum += m.PacketLoss
	}
	return sum / float64(len(sw.metrics))
}

// 故障状态
type FaultStatus struct {
	Type      FaultType
	Severity  Severity
	StartTime time.Time
	Duration  time.Duration
	NodeID    int
	// 故障原因
	Reason string
	// 历史记录
	History []FaultStatus
}

// 故障检测器
type FaultDetector struct {
	mu sync.RWMutex
	// 节点状态映射
	nodeStatus map[int]*FaultStatus
	// 网络指标滑动窗口
	networkMetrics map[int]*SlidingWindow
	// 心跳超时时间
	HeartbeatTimeout time.Duration
	// 延迟阈值
	latencyThreshold time.Duration
	// 丢包率阈值
	packetLossThreshold float64
	// 临时故障持续时间阈值
	temporaryFaultDuration time.Duration
	// 网络分区检测时间
	partitionDetectionTime time.Duration
}

// 创建新的故障检测器
func NewFaultDetector(
	heartbeatTimeout time.Duration,
	latencyThreshold time.Duration,
	packetLossThreshold float64,
	temporaryFaultDuration time.Duration,
	partitionDetectionTime time.Duration,
) *FaultDetector {
	return &FaultDetector{
		nodeStatus:             make(map[int]*FaultStatus),
		networkMetrics:         make(map[int]*SlidingWindow),
		HeartbeatTimeout:       heartbeatTimeout,
		latencyThreshold:       latencyThreshold,
		packetLossThreshold:    packetLossThreshold,
		temporaryFaultDuration: temporaryFaultDuration,
		partitionDetectionTime: partitionDetectionTime,
	}
}

// 记录网络指标
func (fd *FaultDetector) RecordNetworkMetrics(nodeID int, latency time.Duration, packetLoss float64) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	// 获取或创建滑动窗口
	window, exists := fd.networkMetrics[nodeID]
	if !exists {
		window = NewSlidingWindow(10) // 使用10个样本的滑动窗口
		fd.networkMetrics[nodeID] = window
	}

	// 添加新的网络指标
	window.AddMetric(NetworkMetrics{
		Latency:    latency,
		PacketLoss: packetLoss,
		Timestamp:  time.Now(),
	})
}

// 分析故障类型和严重程度
func (fd *FaultDetector) AnalyzeFault(nodeID int) (FaultType, Severity, string) {
	window, exists := fd.networkMetrics[nodeID]
	if !exists {
		return NoFault, Low, "no metrics available"
	}

	// 获取平均指标
	avgLatency := window.AverageLatency()
	// log.Printf("node %d latency %f s", nodeID,avgLatency.Seconds())
	avgPacketLoss := window.AveragePacketLoss()

	// 分析故障类型和严重程度
	if avgLatency > fd.latencyThreshold*2 {
		if avgPacketLoss > fd.packetLossThreshold {
			return NetworkPartition, High, "high latency and packet loss"
		}
		return HighLatency, Medium, "high latency"
	}

	if avgPacketLoss > fd.packetLossThreshold {
		return PacketLoss, Medium, "high packet loss"
	}

	// 检查临时不可用
	status, exists := fd.nodeStatus[nodeID]
	if exists && status.Type == NodeFailure {
		duration := time.Since(status.StartTime)
		if duration < fd.temporaryFaultDuration {
			return TemporaryUnavailable, Low, "temporary unavailability"
		}
		return NodeFailure, High, "node failure"
	}

	return NoFault, Low, "normal"
}

// 更新节点状态
func (fd *FaultDetector) UpdateNodeStatus(nodeID int) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	// 分析故障
	faultType, severity, reason := fd.AnalyzeFault(nodeID)

	// 获取或创建状态
	status, exists := fd.nodeStatus[nodeID]
	if !exists {
		status = &FaultStatus{
			Type:      NoFault,
			Severity:  Low,
			StartTime: time.Now(),
			History:   make([]FaultStatus, 0),
		}
		fd.nodeStatus[nodeID] = status
	}

	// 如果故障类型或严重程度发生变化，记录历史
	if status.Type != faultType || status.Severity != severity {
		status.History = append(status.History, *status)
		if len(status.History) > 10 { // 保留最近10条历史记录
			status.History = status.History[1:]
		}
	}

	// 更新状态
	status.Type = faultType
	status.Severity = severity
	status.Duration = time.Since(status.StartTime)
	status.NodeID = nodeID
	status.Reason = reason

	// 如果故障恢复，重置开始时间
	if faultType == NoFault {
		status.StartTime = time.Now()
	}
}

// 获取节点状态
func (fd *FaultDetector) GetNodeStatus(nodeID int) *FaultStatus {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.nodeStatus[nodeID]
}

// 获取所有故障节点
func (fd *FaultDetector) GetFaultyNodes() []int {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var faultyNodes []int
	for nodeID, status := range fd.nodeStatus {
		if status.Type != NoFault && status.Severity >= High {
			faultyNodes = append(faultyNodes, nodeID)
		}
	}
	return faultyNodes
}

func (fd *FaultDetector) RemoveNode(nodeID int) {
	delete(fd.nodeStatus, nodeID)
}

// 获取需要调整的节点
func (fd *FaultDetector) GetNodesNeedAdjustment() []int {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var nodes []int
	for nodeID, status := range fd.nodeStatus {
		if status.Type != NoFault && status.Severity >= Medium {
			nodes = append(nodes, nodeID)
		}
	}
	return nodes
}

// 预测故障
func (fd *FaultDetector) PredictFaults() map[int]FaultType {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	predictions := make(map[int]FaultType)
	for nodeID, window := range fd.networkMetrics {
		if len(window.metrics) < 5 {
			continue
		}

		// 分析最近5个样本的趋势
		recentMetrics := window.metrics[len(window.metrics)-5:]
		latencyTrend := 0
		packetLossTrend := 0

		for i := 1; i < len(recentMetrics); i++ {
			if recentMetrics[i].Latency > recentMetrics[i-1].Latency {
				latencyTrend++
			} else {
				latencyTrend--
			}

			if recentMetrics[i].PacketLoss > recentMetrics[i-1].PacketLoss {
				packetLossTrend++
			} else {
				packetLossTrend--
			}
		}

		// 如果延迟和丢包率都呈上升趋势，预测可能发生故障
		if latencyTrend >= 3 && packetLossTrend >= 3 {
			predictions[nodeID] = NetworkPartition
		} else if latencyTrend >= 3 {
			predictions[nodeID] = HighLatency
		} else if packetLossTrend >= 3 {
			predictions[nodeID] = PacketLoss
		}
	}

	return predictions
}

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// 服务器节点配置
type ServerNode struct {
	ID      int    `json:"id"`
	Address string `json:"address"`
}

// 网络分区场景配置
type NetworkPartitionScenario struct {
	Enabled           bool  `json:"enabled"`
	StartAfterSeconds int   `json:"startAfterSeconds"`
	DurationSeconds   int   `json:"durationSeconds"`
	NodeIndices       []int `json:"nodeIndices"`
}

// 节点故障场景配置
type NodeFailureScenario struct {
	Enabled           bool `json:"enabled"`
	StartAfterSeconds int  `json:"startAfterSeconds"`
	DurationSeconds   int  `json:"durationSeconds"`
	NodeIndex         int  `json:"nodeIndex"`
}

// 高负载场景配置
type HighLoadScenario struct {
	Enabled           bool `json:"enabled"`
	StartAfterSeconds int  `json:"startAfterSeconds"`
	DurationSeconds   int  `json:"durationSeconds"`
	ClientMultiplier  int  `json:"clientMultiplier"`
}

// 节点频繁故障场景配置
type FrequentFailureScenario struct {
	Enabled              bool  `json:"enabled"`
	StartAfterSeconds    int   `json:"startAfterSeconds"`
	DurationSeconds      int   `json:"durationSeconds"`
	FailureCycleSeconds  int   `json:"failureCycleSeconds"`  // 故障周期（秒）
	RecoveryCycleSeconds int   `json:"recoveryCycleSeconds"` // 恢复周期（秒）
	NodeIndices          []int `json:"nodeIndices"`          // 要故障的节点索引列表
	FailOneByOne         bool  `json:"failOneByOne"`         // 是否依次故障而非同时故障
}

// 测试场景配置
type TestScenarios struct {
	NetworkPartition NetworkPartitionScenario `json:"networkPartition"`
	NodeFailure      NodeFailureScenario      `json:"nodeFailure"`
	HighLoad         HighLoadScenario         `json:"highLoad"`
	FrequentFailure  FrequentFailureScenario  `json:"frequentFailure"`
}

// 性能测试配置
type Config struct {
	Mode        string        `json:"mode"`
	ServerNodes []ServerNode  `json:"serverNodes"`
	ClientNodes []string      `json:"clientNodes"`
	Scenarios   TestScenarios `json:"scenarios"`
	Cleanup     bool          `json:"cleanup"`
}

// 创建默认配置
func DefaultConfig() *Config {
	return &Config{
		Mode: "local",
		ServerNodes: []ServerNode{
			{ID: 0, Address: "localhost:8080"},
			{ID: 1, Address: "localhost:8081"},
			{ID: 2, Address: "localhost:8082"},
			{ID: 3, Address: "localhost:8083"},
			{ID: 4, Address: "localhost:8084"},
		},
		ClientNodes: []string{
			"localhost",
		},
		Scenarios: TestScenarios{
			NetworkPartition: NetworkPartitionScenario{
				Enabled:           false,
				StartAfterSeconds: 30,
				DurationSeconds:   15,
				NodeIndices:       []int{3, 4},
			},
			NodeFailure: NodeFailureScenario{
				Enabled:           false,
				StartAfterSeconds: 20,
				DurationSeconds:   10,
				NodeIndex:         2,
			},
			HighLoad: HighLoadScenario{
				Enabled:           false,
				StartAfterSeconds: 40,
				DurationSeconds:   20,
				ClientMultiplier:  5,
			},
			FrequentFailure: FrequentFailureScenario{
				Enabled:              false,
				StartAfterSeconds:    15,
				DurationSeconds:      30,
				FailureCycleSeconds:  3, // 3秒故障
				RecoveryCycleSeconds: 2, // 2秒恢复
				NodeIndices:          []int{1, 2, 3},
				FailOneByOne:         true,
			},
		},
		Cleanup: true,
	}
}

// 从文件加载配置
func LoadConfigFromFile(filePath string) (*Config, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

// 将配置保存到文件
func SaveConfigToFile(config *Config, filePath string) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("配置序列化失败: %v", err)
	}

	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %v", err)
	}

	return nil
}

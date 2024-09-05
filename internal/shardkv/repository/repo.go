package repo

import (
	"sync"

	"raftkv/internal/shardkv/common"
)

type ShardRepositery struct {
	dataMu *sync.RWMutex
	data   map[int]*ShardData
	Status map[int]bool
}

func NewShardRepositery() *ShardRepositery {
	return &ShardRepositery{
		dataMu: &sync.RWMutex{},
		data:   map[int]*ShardData{},
		Status: map[int]bool{},
	}
}

func (repo *ShardRepositery) AddShard(shard int, kv map[string]string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()

	shardData := NewKVRepositories()
	shardData.data = kv
	repo.data[shard] = shardData
}

func (repo *ShardRepositery) CopyShard(shard int) (kv map[string]string) {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()

	shardData, ok := repo.data[shard]
	if !ok {
		return make(map[string]string)
	}

	kv = make(map[string]string)
	shardData.dataMu.RLock()
	defer shardData.dataMu.RUnlock()
	for k, v := range shardData.data {
		kv[k] = v
	}
	return
}

func (repo *ShardRepositery) Get(key string) (string, bool) {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()

	shardData, ok := repo.data[common.Key2shard(key)]
	if !ok {
		return "", ok
	}
	return shardData.Get(key)
}

func (repo *ShardRepositery) Put(key string, val string) {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	_, ok := repo.data[common.Key2shard(key)]
	if !ok {
		repo.data[common.Key2shard(key)] = NewKVRepositories()
	}
	repo.data[common.Key2shard(key)].Put(key, val)
}

func (repo *ShardRepositery) Append(key string, val string) {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	_, ok := repo.data[common.Key2shard(key)]
	if !ok {
		repo.data[common.Key2shard(key)] = NewKVRepositories()
	}
	repo.data[common.Key2shard(key)].Append(key, val)
}

type ShardData struct {
	dataMu *sync.RWMutex
	data   map[string]string
}

func NewKVRepositories() *ShardData {
	return &ShardData{
		dataMu: &sync.RWMutex{},
		data:   map[string]string{},
	}
}

func (repo *ShardData) Get(key string) (string, bool) {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	val, ok := repo.data[key]
	return val, ok
}

func (repo *ShardData) Put(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	repo.data[key] = val
}

func (repo *ShardData) Append(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	oldVal := repo.data[key]
	repo.data[key] = oldVal + val
}

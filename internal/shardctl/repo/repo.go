package repo

import (
	"container/heap"
	"raftkv/internal/shardctl/common"
	"raftkv/internal/shardctl/util"
	"sort"
	"sync"
)

type ConfigRepositery struct {
	mu      *sync.Mutex
	configs []common.Config
}

func NewConfigRepositery() ConfigRepositery {
	configs := make([]common.Config, 1)
	configs[0].Groups = map[int][]string{}
	return ConfigRepositery{
		mu:      &sync.Mutex{},
		configs: configs,
	}
}

func (repo *ConfigRepositery) getLatestConfig() *common.Config {
	return &repo.configs[len(repo.configs)-1]
}

func (repo *ConfigRepositery) Join(groups map[int][]string) common.Config {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	oldConfig := repo.getLatestConfig()
	newConfig := oldConfig.Incr()

	gShardList := map[int][]int{}
	for shard, gid := range oldConfig.Shards {
		if _, ok := gShardList[gid]; !ok {
			gShardList[gid] = []int{}
		}
		gShardList[gid] = append(gShardList[gid], shard)
	}

	minShardNumPQ := util.NewPQ(false)
	maxShardNumPQ := util.NewPQ(true)
	for gid, shard := range gShardList {
		if gid == 0 {
			continue
		}
		shardNum := len(shard)
		item := &util.Item{
			Value:    gid,
			Priority: shardNum,
		}
		heap.Push(&minShardNumPQ, item)
		heap.Push(&maxShardNumPQ, item)
	}

	groupKeys := make([]int, 0, len(groups))
	for k := range groups {
		groupKeys = append(groupKeys, k)
	}
	sort.Ints(groupKeys)

	for _, gid := range groupKeys {
		if len(gShardList[0]) != 0 {
			gShardList[gid] = gShardList[0]
			for shard := range gShardList[gid] {
				newConfig.Shards[shard] = gid
			}
			gShardList[0] = nil
		}
		newConfig.Groups[gid] = groups[gid]
		item := &util.Item{
			Value:    gid,
			Priority: len(gShardList[gid]),
		}
		heap.Push(&minShardNumPQ, item)
		heap.Push(&maxShardNumPQ, item)
	}

	for maxShardNumPQ.Peak() != nil && maxShardNumPQ.Peak().Priority-minShardNumPQ.Peak().Priority > 1 {
		maxItem := heap.Pop(&maxShardNumPQ).(*util.Item)
		minItem := heap.Pop(&minShardNumPQ).(*util.Item)
		maxItem.Priority--
		minItem.Priority++

		shard := gShardList[maxItem.Value][len(gShardList[maxItem.Value])-1]
		gShardList[maxItem.Value] = gShardList[maxItem.Value][:len(gShardList[maxItem.Value])-1]
		newConfig.Shards[shard] = minItem.Value

		heap.Push(&minShardNumPQ, minItem)
		heap.Push(&maxShardNumPQ, maxItem)
	}
	repo.configs = append(repo.configs, newConfig)
	return newConfig
}

func (repo *ConfigRepositery) Leave(gids []int) common.Config {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	oldConfig := repo.getLatestConfig()
	newConfig := oldConfig.Incr()

	gidSet := util.NewSet[int]()
	for _, gid := range gids {
		gidSet.Add(gid)
		delete(newConfig.Groups, gid)
	}

	gShardNums := map[int]int{}
	for _, gid := range oldConfig.Shards {
		old := gShardNums[gid]
		gShardNums[gid] = old + 1
	}

	minShardNumPQ := util.NewPQ(false)
	for gid := range oldConfig.Groups {
		if !gidSet.Contains(gid) {
			heap.Push(&minShardNumPQ, &util.Item{
				Value:    gid,
				Priority: gShardNums[gid],
			})
		}
	}

	for shard, gid := range oldConfig.Shards {
		if minShardNumPQ.Len() == 0 {
			newConfig.Shards[shard] = 0
		} else if gidSet.Contains(gid) {
			item := heap.Pop(&minShardNumPQ).(*util.Item)
			newConfig.Shards[shard] = item.Value
			item.Priority++
			heap.Push(&minShardNumPQ, item)
		}
	}
	repo.configs = append(repo.configs, newConfig)

	return newConfig
}

func (repo *ConfigRepositery) Move(shard int, gid int) common.Config {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	new := repo.getLatestConfig().Incr()
	new.Shards[shard] = gid
	repo.configs = append(repo.configs, new)

	return new
}

func (repo *ConfigRepositery) Query(num int) common.Config {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	if num == -1 || num >= len(repo.configs) {
		return *repo.getLatestConfig()
	}
	return repo.configs[num]
}

package common

import shardctl_common "raftkv/internal/shardctl/common"

func Key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctl_common.NShards
	return shard
}

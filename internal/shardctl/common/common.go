package common

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Incr() Config {
	groups := make(map[int][]string, len(c.Groups))
	for k, v := range c.Groups {
		groups[k] = v
	}
	return Config{
		Num:    c.Num + 1,
		Shards: c.Shards,
		Groups: groups,
	}
}

const (
	OK              = "OK"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrTimeout      = "ErrTimeout"
	ErrServerKilled = "ErrServerKilled"
)

type Err string

type JoinArgs struct {
	OpID     int64
	ClientID string

	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	OpID     int64
	ClientID string

	GIDs []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	OpID     int64
	ClientID string

	Shard int
	GID   int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	OpID     int64
	ClientID string

	Num int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}

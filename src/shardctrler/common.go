package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
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

func CopyConfig(cfg *Config) Config {
	res := Config{}
	res.Num = cfg.Num + 1
	for i := 0; i < len(cfg.Shards); i++ {
		res.Shards[i] = cfg.Shards[i]
	}
	res.Groups = make(map[int][]string)
	for k, v := range cfg.Groups {
		res.Groups[k] = v
	}
	return res
}

func GetRandomKey(mp map[int][]string) int {
	for k := range mp {
		return k
	}
	return 0
}

func GetGIDWithMinimumShards(g2s map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func GetGIDWithMaximumShards(g2s map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(g2s[gid]) > max {
			index, max = gid, len(g2s[gid])
		}
	}
	return index
}

func (cfg *Config) ReAllocGID() {
	if len(cfg.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	g2s := make(map[int][]int)
	for g := range cfg.Groups {
		g2s[g] = []int{}
	}
	for s, g := range cfg.Shards {
		_, find := g2s[g]
		if g != 0 && find {
			g2s[g] = append(g2s[g], s)
		}
	}

	// for leave
	for i := 0; i < NShards; i++ {
		if _, ok := cfg.Groups[cfg.Shards[i]]; !ok {
			gid := GetGIDWithMinimumShards(g2s)
			cfg.Shards[i] = gid
			g2s[gid] = append(g2s[gid], i)
		}
	}
	// for join
	for {
		source, target := GetGIDWithMaximumShards(g2s), GetGIDWithMinimumShards(g2s)
		if source != 0 && len(g2s[source])-len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}

	// update cfg.Shards
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	cfg.Shards = newShards
}

const (
	OK = "OK"
)

type Err string

type CommandArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	CommandId uint64
	GIDs      []int
	Num       int // desired config number
	Config    Config
	Shard     int
	GID       int
	Type      OpType
}

type CommandReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

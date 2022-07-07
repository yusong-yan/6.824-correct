package shardkv

import "raft/src/raft"

func (kv *ShardKV) Getrf() *raft.Raft {
	return kv.rf
}

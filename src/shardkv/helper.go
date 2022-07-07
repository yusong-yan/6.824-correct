package shardkv

import "6.824/src/raft"

func (kv *ShardKV) Getrf() *raft.Raft {
	return kv.rf
}

package shardctrler

import "raft/src/raft"

func (sc *ShardCtrler) Getrf() *raft.Raft {
	return sc.rf
}

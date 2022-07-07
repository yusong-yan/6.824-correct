package shardctrler

import "6.824/src/raft"

func (sc *ShardCtrler) Getrf() *raft.Raft {
	return sc.rf
}

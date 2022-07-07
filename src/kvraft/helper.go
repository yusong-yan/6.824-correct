package kvraft

import "raft/src/raft"

func (kv *KVServer) Getrf() *raft.Raft {
	return kv.rf
}

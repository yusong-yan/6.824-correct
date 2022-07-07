package kvraft

import "6.824/src/raft"

func (kv *KVServer) Getrf() *raft.Raft {
	return kv.rf
}

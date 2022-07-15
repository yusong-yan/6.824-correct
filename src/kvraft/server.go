package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"raft/labgob"
	"raft/labrpc"
	"raft/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpTask string
	Key    string
	Value  string
	Client int64
	Id     int64
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	// Your definitions here.
	storage *MemoryKV
	idTable map[int64]map[int64]Op
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = NewMemoryKV()
	kv.idTable = map[int64]map[int64]Op{}
	go kv.listenApplyCh()
	return kv
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.CommandId}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.existId(args.CommandId, args.ClientId) {
		// this get operation has been commit to the database
		reply.Value, reply.Err = kv.storage.Get(args.Key)
		return
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) listenApplyCh() {
	for !kv.killed() {
		applyMessage := <-kv.applyCh
		curOp := applyMessage.Command.(Op)
		kv.mu.Lock()
		//if the operation hasn't been commit to database, just do it
		//otherwise, skip it
		if !kv.existId(curOp.Id, curOp.Client) {
			if curOp.OpTask == Appendd && kv.storage.Found(curOp.Key) {
				kv.storage.Append(curOp.Key, curOp.Value)
			} else if curOp.OpTask == Putt || curOp.OpTask == Appendd {
				kv.storage.Put(curOp.Key, curOp.Value)
			}
			_, exist := kv.idTable[curOp.Client]
			if !exist {
				// if haven't seend this client before, add it to id table
				kv.idTable[curOp.Client] = map[int64]Op{}
			}
		}
		kv.idTable[curOp.Client][curOp.Id] = curOp

		kv.mu.Unlock()
	}
}

func (kv *KVServer) existId(id int64, client int64) bool {
	_, exist := kv.idTable[client]
	if exist {
		_, exist = kv.idTable[client][id]
		if exist {
			return true
		}
	}
	return false
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	// Your definitions here.
	storage     *MemoryKV
	latestTime  map[int64]int64
	waitChannel map[int64]chan bool
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storage = NewMemoryKV()
	kv.latestTime = make(map[int64]int64)
	kv.waitChannel = make(map[int64]chan bool)
	go kv.listenApplyCh()
	return kv
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	// never seen this client before, then setup
	kv.mu.RLock()
	if kv.dupCommand(args.CommandId, args.CommandId) {
		reply.Value, reply.Err = kv.storage.Get(args.Key)
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.CommandId}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	c := kv.startWaitChannelL(args.ClientId)
	timer := time.After(90 * time.Millisecond)
	select {
	case <-timer:
		// timeout!
		kv.deleteWaitChannelL(args.ClientId)
		reply.Err = ErrTimeout
	case <-c:
		// this op has be processed!
		kv.mu.Lock()
		reply.Value, reply.Err = kv.storage.Get(args.Key)
		kv.deleteWaitChannel(args.ClientId)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) listenApplyCh() {
	for !kv.killed() {
		applyMessage := <-kv.applyCh
		curOp := applyMessage.Command.(Op)
		kv.mu.Lock()
		//check if this command is in the database or not
		if !kv.dupCommand(curOp.Id, curOp.Client) {
			if curOp.OpTask == Appendd && kv.storage.Found(curOp.Key) {
				kv.storage.Append(curOp.Key, curOp.Value)
			} else if curOp.OpTask == Putt || curOp.OpTask == Appendd {
				kv.storage.Put(curOp.Key, curOp.Value)
			}
			kv.latestTime[curOp.Client] = curOp.Id
		}
		if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMessage.CommandTerm == currentTerm {
			c, ok := kv.waitChannel[curOp.Client]
			if ok {
				c <- true
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) startWaitChannelL(clientId int64) chan bool {
	c := make(chan bool, 1)
	kv.mu.Lock()
	kv.waitChannel[clientId] = c
	kv.mu.Unlock()
	return c
}

func (kv *KVServer) deleteWaitChannel(clientId int64) {
	delete(kv.waitChannel, clientId)
}
func (kv *KVServer) deleteWaitChannelL(clientId int64) {
	kv.mu.Lock()
	delete(kv.waitChannel, clientId)
	kv.mu.Unlock()
}

func (kv *KVServer) dupCommand(commandId int64, clientId int64) bool {
	latestId, exist := kv.latestTime[clientId]
	return exist && commandId <= latestId
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

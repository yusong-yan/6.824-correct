package kvraft

import (
	"bytes"
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
	Seq    int64
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
	persister   *raft.Persister
	lastApplied int
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.storage = NewMemoryKV()
	kv.latestTime = make(map[int64]int64)
	kv.waitChannel = make(map[int64]chan bool)
	kv.lastApplied = 0
	kv.replaceSnapshot(persister.ReadSnapshot())
	kv.persister = persister
	go kv.listenApplyCh()
	return kv
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	// never seen this client before, then setup
	kv.mu.RLock()
	if args.Op != Gett && kv.dupCommand(args.CommandId, args.ClientId) {
		// reply.Value, reply.Err = kv.storage.Get(args.Key)
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	// // prevent leaders memory increase too much
	// // this
	// if kv.needSnapShot() {
	// 	reply.Err = ErrWrongLeader
	// 	kv.mu.RUnlock()
	// 	return
	// }
	kv.mu.RUnlock()
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.CommandId, nrand()}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	c := kv.startWaitChannelL(op.Seq)
	timer := time.After(100 * time.Millisecond)
	select {
	case <-timer:
		// timeout!
		kv.deleteWaitChannelL(op.Seq)
		reply.Err = ErrTimeout
	case <-c:
		// this op has be processed!
		kv.mu.Lock()
		reply.Value, reply.Err = kv.storage.Get(args.Key)
		kv.deleteWaitChannel(op.Seq)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) listenApplyCh() {
	for applyMessage := range kv.applyCh {
		if kv.killed() {
			return
		}
		if applyMessage.CommandValid {
			curOp := applyMessage.Command.(Op)
			kv.mu.Lock()
			if applyMessage.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = applyMessage.CommandIndex
			//check if this command is in the database or not
			if curOp.OpTask != Gett && !kv.dupCommand(curOp.Id, curOp.Client) {
				if curOp.OpTask == Appendd {
					kv.storage.Append(curOp.Key, curOp.Value)
				} else if curOp.OpTask == Putt {
					kv.storage.Put(curOp.Key, curOp.Value)
				}
				kv.latestTime[curOp.Client] = curOp.Id
			}
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMessage.CommandTerm == currentTerm {
				c, ok := kv.waitChannel[curOp.Seq]
				if ok {
					c <- true
				}
			}
			if kv.needSnapShot() {
				kv.takeSnapShot(applyMessage.CommandIndex)
			}
			kv.mu.Unlock()
		} else if applyMessage.SnapshotValid {
			kv.mu.Lock()
			kv.replaceSnapshot(applyMessage.Snapshot)
			kv.lastApplied = applyMessage.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) startWaitChannelL(seq int64) chan bool {
	c := make(chan bool, 1)
	kv.mu.Lock()
	kv.waitChannel[seq] = c
	kv.mu.Unlock()
	return c
}

func (kv *KVServer) deleteWaitChannel(seq int64) {
	delete(kv.waitChannel, seq)
}
func (kv *KVServer) deleteWaitChannelL(seq int64) {
	kv.mu.Lock()
	delete(kv.waitChannel, seq)
	kv.mu.Unlock()
}

func (kv *KVServer) dupCommand(commandId int64, clientId int64) bool {
	latestId, exist := kv.latestTime[clientId]
	return exist && commandId <= latestId
}

func (kv *KVServer) needSnapShot() bool {
	return kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()/kv.maxraftstate) > 0.8
}

func (kv *KVServer) takeSnapShot(index int) {
	snapShot := kv.saveState()
	kv.rf.Snapshot(index, snapShot)
}

func (kv *KVServer) replaceSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var latestTime map[int64]int64
	var lastApplied int
	if d.Decode(&storage) != nil ||
		d.Decode(&latestTime) != nil ||
		d.Decode(&lastApplied) != nil {
		log.Fatal("error")
	} else {
		kv.latestTime = latestTime
		kv.storage.SetKV(storage)
		kv.lastApplied = lastApplied
	}
}

func (kv *KVServer) saveState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.storage.GetKV())
	e.Encode(kv.latestTime)
	e.Encode(kv.lastApplied)
	return w.Bytes()
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

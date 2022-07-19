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

type Op struct {
	OpTask    string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
	Seq       int64
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
	//lastApplied int
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
	kv.installSnapshot(persister.ReadSnapshot())
	kv.persister = persister
	go kv.listenApplyCh()
	return kv
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	op := Op{}
	op.OpTask = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId
	op.CommandId = args.CommandId
	op.Seq = nrand()

	kv.mu.Lock()
	if kv.dupCommand(args.CommandId, args.ClientId) {
		reply.Value, reply.Err = kv.storage.Get(args.Key)
		kv.mu.Unlock()
		return
	}
	c := kv.startWaitChannel(op.Seq)
	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		go kv.deleteWaitChannelL(op.Seq)
		reply.Err = ErrWrongLeader
	} else {
		timer := time.After(99 * time.Millisecond)
		select {
		case <-timer:
			go kv.deleteWaitChannelL(op.Seq)
			reply.Err = ErrTimeout
		case <-c:
			// this has been apply to database
			kv.mu.Lock()
			reply.Err = OK
			if args.Op == Gett {
				reply.Value, reply.Err = kv.storage.Get(args.Key)
			}
			kv.deleteWaitChannel(op.Seq)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) listenApplyCh() {
	for applyMessage := range kv.applyCh {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		if applyMessage.CommandValid {
			curOp := applyMessage.Command.(Op)
			if !kv.dupCommand(curOp.CommandId, curOp.ClientId) {
				if curOp.OpTask == Appendd {
					kv.storage.Append(curOp.Key, curOp.Value)
				} else if curOp.OpTask == Putt {
					kv.storage.Put(curOp.Key, curOp.Value)
				}
				kv.latestTime[curOp.ClientId] = curOp.CommandId
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
		} else if applyMessage.SnapshotValid {
			kv.installSnapshot(applyMessage.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) startWaitChannel(seq int64) chan bool {
	c := make(chan bool, 1)
	kv.waitChannel[seq] = c
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

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage map[string]string
	var latestTime map[int64]int64
	// var record map[int64]map[int64]bool
	if d.Decode(&storage) != nil ||
		d.Decode(&latestTime) != nil {
		log.Fatal("error")
	} else {
		kv.storage.SetKV(storage)
		kv.latestTime = latestTime
	}
}

func (kv *KVServer) saveState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.storage.GetKV())
	e.Encode(kv.latestTime)
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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const Debug1 = 1

func DPrintf1(format string, a ...interface{}) (n int, err error) {
	if Debug1 > 0 {
		log.Printf(format, a...)
	}
	return
}

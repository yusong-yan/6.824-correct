package shardctrler

import (
	"sync"
	"time"

	"raft/labgob"
	"raft/labrpc"
	"raft/raft"
)

type OpType string

const (
	QUERY   OpType = "QUERY"
	JOIN    OpType = "JOIN"
	LEAVE   OpType = "LEAVE"
	MOVE    OpType = "MOVE"
	TIMEOUT        = 100 // set time out to 100 millsecond.
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.

	configs []Config // indexed by config num

	chans      map[int64]chan bool
	latestTime map[int64]uint64
}

type Op struct {
	// Your data here.
	Type    OpType
	GID     []int
	Shard   int
	Num     int
	Servers map[int][]string

	ClientId  int64
	CommandId uint64
	ServerSeq int64
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.dupCommand(args.CommandId, args.ClientId) {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		Type:      args.Type,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		ServerSeq: nrand(),
		Shard:     args.Shard,
		Num:       args.Num,
	}
	if args.Type == LEAVE {
		op.GID = args.GIDs
	} else if args.Type == MOVE {
		op.GID = make([]int, 1)
		op.GID[0] = args.GID
	}

	rec_chan := make(chan bool, 1)
	sc.mu.Lock()
	sc.chans[op.ServerSeq] = rec_chan
	sc.mu.Unlock()
	if _, _, ok1 := sc.rf.Start(op); ok1 {
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
			reply.WrongLeader = true
		case <-rec_chan:
			// this op has be processed!
			if args.Type == QUERY {
				res_idx := op.Num
				if op.Num == -1 || op.Num >= len(sc.configs) {
					res_idx = len(sc.configs) - 1
				}
				reply.Config = sc.configs[res_idx]
			}
		}
	} else {
		reply.WrongLeader = true
	}
	sc.mu.Lock()
	delete(sc.chans, op.ServerSeq)
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// check whether need to process
func (sc *ShardCtrler) dupCommand(commandId uint64, clientId int64) bool {
	latestId, exist := sc.latestTime[clientId]
	return exist && commandId <= latestId
}

func (sc *ShardCtrler) listenApplyCh() {
	for command := range sc.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			if !sc.dupCommand(op.CommandId, op.ClientId) {
				switch op.Type {
				case JOIN:
					new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
					for k, v := range op.Servers {
						new_config.Groups[k] = v
					}
					new_config.ReAllocGID()
					sc.configs = append(sc.configs, new_config)
				case LEAVE:
					new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
					for _, i := range op.GID {
						delete(new_config.Groups, i)
					}
					new_config.ReAllocGID()
					sc.configs = append(sc.configs, new_config)
				case MOVE:
					new_config := CopyConfig(&sc.configs[len(sc.configs)-1])
					new_config.Shards[op.Shard] = op.GID[0]
					sc.configs = append(sc.configs, new_config)
				default:
				}
				sc.mu.Lock()
				sc.latestTime[op.ClientId] = op.CommandId
				sc.mu.Unlock()
			}
			if currentTerm, isLeader := sc.rf.GetState(); isLeader && command.CommandTerm == currentTerm {
				c, ok := sc.chans[op.ServerSeq]
				if ok {
					c <- true
				}
			}
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.chans = make(map[int64]chan bool)
	sc.latestTime = make(map[int64]uint64)

	go sc.listenApplyCh()
	return sc
}

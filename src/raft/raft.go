package raft

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	"raft/labgob"
	"raft/labrpc"
)

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh       chan ApplyMsg
	applyCond     *sync.Cond   // used to wakeup applier goroutine after committing new entries
	tryAppendCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	state         int

	currentTerm int
	votedFor    int
	raftLog     *raftLog // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	hasSnapshot bool

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(90) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := 600 - 300
	return time.Duration(300+r.Intn(diff)) * time.Millisecond
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		tryAppendCond:  make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		raftLog:        newLogs(),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)

	for i := 0; i < len(peers); i++ {
		if i != rf.me {
			rf.tryAppendCond[i] = sync.NewCond(&sync.Mutex{})
			// start a peer's replicator goroutine to replicate entries in the background
			go rf.appendThread(i)
		}
	}
	rf.commitIndex = rf.raftLog.dummyIndex()
	rf.lastApplied = rf.commitIndex
	rf.hasSnapshot = false
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()
	return rf
}

//receive appending command from upper KV layer
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := Entry{}
	newLog.Command = command
	newLog.Index = rf.raftLog.lastIndex() + 1
	newLog.Term = rf.currentTerm
	rf.raftLog.append(newLog)
	rf.persist()
	rf.BroadcastAppend(Append)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			if rf.state != StateLeader {
				rf.StartElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			if rf.state == StateLeader {
				go rf.BroadcastAppend(HeartBeat)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) needAppend(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	ret := rf.state == StateLeader && rf.matchIndex[peer] < rf.raftLog.lastIndex()
	return ret
}

func (rf *Raft) appendThread(peer int) {
	rf.tryAppendCond[peer].L.Lock()
	defer rf.tryAppendCond[peer].L.Unlock()
	for !rf.killed() {
		// we might recevied N Appending request, but we don't need
		// to do len(peers)*N RPC, because first few RPCs might push
		// all the new entry from logs to other replica, then needReplicating
		// will be false
		for !rf.needAppend(peer) {
			rf.tryAppendCond[peer].Wait()
			if rf.killed() {
				return
			}
		}
		rf.appendOneRound(peer)
	}
}

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for !rf.hasSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		msgs := make([]ApplyMsg, 0)
		if rf.hasSnapshot {
			msgs = append(msgs, ApplyMsg{
				SnapshotValid: true,
				CommandValid:  false,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.raftLog.dummyTerm(),
				SnapshotIndex: rf.raftLog.dummyIndex(),
			})
			rf.hasSnapshot = false
		}
		if lastApplied < commitIndex {
			logSlice := rf.raftLog.slice(lastApplied+1, commitIndex+1)
			for _, entry := range logSlice {
				msgs = append(msgs, ApplyMsg{
					SnapshotValid: false,
					CommandValid:  true, // if entry.Command == nil then is false
					Command:       entry.Command,
					CommandTerm:   entry.Term,
					CommandIndex:  entry.Index,
				})
			}
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) persist() {
	data := rf.SaveState()
	rf.persister.SaveRaftState(data)
}
func (rf *Raft) SaveState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.raftLog.getLogs())
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var logs []Entry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("error")
	} else {
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
		rf.raftLog.setLogs(logs)
	}
}

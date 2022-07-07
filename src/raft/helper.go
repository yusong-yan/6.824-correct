package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"raft/test/labgob"
)

func (rf *Raft) ChangeState(state int) {
	rf.state = state
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := 600 - 300
	return time.Duration(300+r.Intn(diff)) * time.Millisecond
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// just signal replicator goroutine to send entries in batch
		rf.tryAppendCond[peer].Signal()
	}
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := false
	term := rf.currentTerm
	if rf.state == StateLeader {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) GetState2() (int, string) {
	rf.mu.Lock()
	Term := rf.currentTerm
	var State string
	if rf.state == StateFollower {
		State = "Follower"
	} else if rf.state == StateCandidate {
		State = "Candidate"
	} else {
		State = "Leader"
	}
	rf.mu.Unlock()
	return Term, State
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.raftLog.getLogs())
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

func (rf *Raft) GetLogs() []Entry {
	return rf.raftLog.getLogs()
}

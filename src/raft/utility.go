package raft

import (
	"log"
	"sync/atomic"
)

// for testing, crash a server
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
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

func Min(a int, b int) int {
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

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const Debug1 = true

func DPrintf1(format string, a ...interface{}) (n int, err error) {
	if Debug1 {
		log.Printf(format, a...)
	}
	return
}

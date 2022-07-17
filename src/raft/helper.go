package raft

import (
	"sync/atomic"
)

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
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

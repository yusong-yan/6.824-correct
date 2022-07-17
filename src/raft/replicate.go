package raft

//HeartBeat
func (rf *Raft) BroadcastAppend(job int) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if job == HeartBeat {
			// leader will try to send heartbeat constantly
			go rf.appendOneRound(peer)
		} else {
			// activate replicator thread for this peer
			rf.tryAppendCond[peer].Signal()
		}
	}
}

//One peer fix, sending RPC
func (rf *Raft) appendOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.raftLog.dummyIndex() {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.raftLog.dummyIndex(),
			LastIncludedTerm:  rf.raftLog.dummyTerm(),
			Snapshot:          rf.persister.ReadSnapshot(),
		}
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			rf.processInstallSnapshotReply(peer, args, reply)
			rf.mu.Unlock()
		}
	} else {
		if prevLogIndex > rf.raftLog.lastIndex() {
			panic("revLogIndex > rf.raftLog.lastIndex()")
		}
		// just entries can catch up
		args := &AppendEntriesArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.raftLog.getEntry(prevLogIndex).Term,
			Entries:      make([]Entry, rf.raftLog.lastIndex()-prevLogIndex),
			LeaderCommit: rf.commitIndex,
		}

		copy(args.Entries, rf.raftLog.sliceFrom(prevLogIndex+1))
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			// Here, we might activate more replicateOneRound depend on
			// whether we can fix this peer's log in this round
			rf.mu.Lock()
			rf.processAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) processAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = StateFollower
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if reply.Term == rf.currentTerm && rf.state == StateLeader &&
		args.Term == rf.currentTerm && args.PrevLogIndex == rf.nextIndex[peer]-1 {
		if reply.Success {
			newNext := len(args.Entries) + args.PrevLogIndex + 1
			newMatch := len(args.Entries) + args.PrevLogIndex
			if newNext > rf.nextIndex[peer] {
				rf.nextIndex[peer] = newNext
			}
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
			}
			rf.advanceCommitIndexForLeader()
		} else {
			// here we are sure that reply.ConflictIndex will be
			// greater or equal to one from the logic of HandleAppendEntries
			rf.nextIndex[peer] = reply.ConflictIndex
		}
		if rf.nextIndex[peer] < rf.raftLog.lastIndex()+1 {
			rf.tryAppendCond[peer].Signal()
		}
	}
}
func (rf *Raft) advanceCommitIndexForLeader() {
	if rf.state != StateLeader {
		return
	}
	for i := rf.raftLog.lastIndex(); i > rf.commitIndex; i-- {
		num := 0
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				num++
			}
		}
		//from raft paper (Rules for Servers, leader, last bullet point)
		if num+1 > (len(rf.peers)/2) && rf.raftLog.getEntry(i).Term == rf.currentTerm {
			rf.commitIndex = i
			rf.applyCond.Signal()
			return
		}
	}

}

//Handle the received RPC
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	rf.state = StateFollower
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.PrevLogIndex < rf.raftLog.dummyIndex() {
		reply.Term, reply.Success = 0, false
		reply.ConflictIndex = rf.raftLog.dummyIndex() + 1
		return
	}
	if !rf.raftLog.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.raftLog.lastIndex()
		if args.PrevLogIndex > lastIndex {
			reply.ConflictIndex = lastIndex + 1
		} else {
			dummyIndex := rf.raftLog.dummyIndex()
			abandondRound := rf.raftLog.getEntry(args.PrevLogIndex).Term
			index := args.PrevLogIndex
			for index > dummyIndex+1 && rf.raftLog.getEntry(index).Term == abandondRound {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}
	// This is old codes, which will fail under none fifo channel
	// {
	// // we connect entries with logs, by minimize the delete of rf.logs
	// rf.raftLog.trunc(args.PrevLogIndex + 1)
	// rf.raftLog.append(args.Entries...)
	// }

	for index, entry := range args.Entries {
		if rf.raftLog.convertIndex(entry.Index) >= rf.raftLog.len() || rf.raftLog.getEntry(entry.Index).Term != entry.Term {
			rf.raftLog.trunc(entry.Index)
			rf.raftLog.append(args.Entries[index:]...)
			break
		}
	}
	// raft paper (AppendEntries RPC, 5)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.raftLog.lastIndex())
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

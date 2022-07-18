package raft

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.raftLog.dummyIndex() {
		//println("Raft already trim the log at index:", index)
		return
	}
	rf.raftLog.setLogs(rf.raftLog.sliceFrom(index))
	rf.raftLog.clearDummyEntryCommand()
	rf.persister.SaveStateAndSnapshot(rf.SaveState(), snapshot)
}

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.state = StateFollower
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.LastIncludedIndex > rf.raftLog.lastIndex() {
		newlog := make([]Entry, 1)
		rf.raftLog.setLogs(newlog)
	} else {
		rf.raftLog.setLogs(rf.raftLog.sliceFrom(args.LastIncludedIndex))
	}
	rf.raftLog.clearDummyEntryCommand()
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.raftLog.setDummyIndex(args.LastIncludedIndex)
	rf.raftLog.setDummyTerm(args.LastIncludedTerm)
	rf.persister.SaveStateAndSnapshot(rf.SaveState(), args.Snapshot)

	rf.hasSnapshot = true
	rf.applyCond.Signal()
	// this implementation ensure entries with index smaller or equal to commitIndex
	// will be applied before applying snapshot. And all entriess with bigger Index will
	// be applied after
}

func (rf *Raft) processInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = StateFollower
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if rf.state == StateLeader && args.Term == rf.currentTerm {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

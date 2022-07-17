package raft

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// // outdated snapshot
	// if lastIncludedIndex <= rf.commitIndex {
	// 	return false
	// }

	// if lastIncludedIndex > rf.raftLog.lastIndex() {
	// 	newlog := make([]Entry, 1)
	// 	rf.raftLog.setLogs(newlog)
	// } else {
	// 	rf.raftLog.setLogs(rf.raftLog.sliceFrom(lastIncludedIndex))
	// 	rf.raftLog.clearDummyEntryCommand()
	// }
	// rf.commitIndex = lastIncludedIndex
	// rf.lastApplied = lastIncludedIndex
	// rf.raftLog.setDummyIndex(lastIncludedIndex)
	// rf.raftLog.setDummyTerm(lastIncludedTerm)

	// rf.persister.SaveStateAndSnapshot(rf.SaveState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
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

	rf.ChangeState(StateFollower)
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
	// // OLD synchronized version
	// // In this case we can't go rf.applyCh <- applyMsg, because this can't make sure the atomic of applyMsgs,
	// // this is because the commitIndex can be changed very quicky, so the applier might push
	// // an applyMsg that has commitIndex higher than lastIncludedIndex. This is why here we need to wait
	// // this SnapshotMsg to be pushed, but it is slow, the optimazition can be make a hasSnapshot instance in rafe
	// // that store this information then single applyChan, if we find a validSnapshot we will put this
	// // msg in the begining of the sending entries.
	// applyMsg := ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot:      args.Snapshot,
	// 	SnapshotIndex: args.LastIncludedIndex,
	// 	SnapshotTerm:  args.LastIncludedTerm,
	// }
	// rf.applyCh <- applyMsg
}

func (rf *Raft) processInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.ChangeState(StateFollower)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if rf.state == StateLeader && args.Term == rf.currentTerm {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}

}

package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

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
	if index < rf.raftLog.dummyIndex() {
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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	reply.Term = rf.currentTerm
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	// outdated snapshot
	if args.LastIncludedIndex <= rf.raftLog.dummyIndex() {
		return
	}
	if args.LastIncludedIndex < rf.commitIndex {
		return
	}

	if args.LastIncludedIndex > rf.raftLog.lastIndex() {
		newlog := make([]Entry, 1)
		rf.raftLog.setLogs(newlog)
	} else {
		rf.raftLog.setLogs(rf.raftLog.sliceFrom(args.LastIncludedIndex))
		rf.raftLog.clearDummyEntryCommand()
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.raftLog.setDummyIndex(args.LastIncludedIndex)
	rf.raftLog.setDummyTerm(args.LastIncludedTerm)

	rf.persister.SaveStateAndSnapshot(rf.SaveState(), args.Snapshot)
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) processInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.ChangeState(StateFollower)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if reply.Term == rf.currentTerm && args.Term == rf.currentTerm {
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
	}

}

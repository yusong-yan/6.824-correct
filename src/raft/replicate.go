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
		// only snapshot can catch up
		// request := rf.genInstallSnapshotRequest()
		// rf.mu.RUnlock()
		// response := new(InstallSnapshotResponse)
		// if rf.sendInstallSnapshot(peer, request, response) {
		// 	rf.mu.Lock()
		// 	rf.handleInstallSnapshotResponse(peer, request, response)
		// 	rf.mu.Unlock()
		// }
		print(rf.nextIndex[peer])
		panic("weird")
	}
	if prevLogIndex > rf.raftLog.lastIndex() {
		println("prevLogIndex > rf.raftLog.lastIndex()")
		prevLogIndex = rf.raftLog.lastIndex() + 1
	}
	// just entries can catch up
	args := new(AppendEntriesArgs)
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.raftLog.getEntry(prevLogIndex).Term
	args.Entries = make([]Entry, rf.raftLog.lastIndex()-prevLogIndex)
	args.LeaderCommit = rf.commitIndex
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
func (rf *Raft) processAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state != StateLeader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.ChangeState(StateFollower)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if reply.Term == rf.currentTerm {
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
		} else if reply.ConflictIndex != 0 {
			// we find conflict and conflict is bigger than 1, and then step back one by one
			rf.nextIndex[peer] = reply.ConflictIndex
			// go rf.appendOneRound(peer)
			rf.tryAppendCond[peer].Signal()
		}
	}
}
func (rf *Raft) advanceCommitIndexForLeader() {
	// find smallest match, and start from there
	latestGrantedIndex := rf.matchIndex[0]
	for _, ServerMatchIndex := range rf.matchIndex {
		latestGrantedIndex = min(latestGrantedIndex, ServerMatchIndex)
	}
	// move forward, until lost the majority
	for i := latestGrantedIndex + 1; i <= rf.raftLog.lastIndex(); i++ {
		granted := 1
		for Server, ServerMatchIndex := range rf.matchIndex {
			if Server != rf.me && ServerMatchIndex >= i {
				granted++
			}
		}
		if granted < len(rf.peers)/2+1 {
			break
		}
		latestGrantedIndex = i
	}
	//from raft paper (Rules for Servers, leader, last bullet point)
	if latestGrantedIndex > rf.commitIndex &&
		rf.raftLog.getEntry(latestGrantedIndex).Term == rf.currentTerm &&
		rf.state == StateLeader {
		rf.commitIndex = latestGrantedIndex
		rf.applyCond.Signal()
	}
}

//Handle the received RPC
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.raftLog.dummyIndex(), rf.raftLog.lastIndex(), args, reply)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.PrevLogIndex < rf.raftLog.dummyIndex() {
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.raftLog.dummyIndex())
		panic("weird2")
		// return
	}

	if !rf.raftLog.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.raftLog.lastIndex()
		if args.PrevLogIndex > lastIndex {
			reply.ConflictIndex = lastIndex + 1
		} else {
			dummyIndex := rf.raftLog.dummyIndex()
			abandondRound := rf.raftLog.getEntry(args.PrevLogIndex).Term
			index := args.PrevLogIndex - 1
			for index > dummyIndex+1 && rf.raftLog.getEntry(index).Term == abandondRound {
				index--
			}
			reply.ConflictIndex = index
			// response.ConflictIndex = request.PrevLogIndex
		}
		return
	}
	// we connect entries with logs, by minimize the delete of rf.logs
	rf.raftLog.trunc(args.PrevLogIndex + 1)
	rf.raftLog.append(args.Entries...)
	// raft paper (AppendEntries RPC, 5)
	rf.commitIndex = min(args.LeaderCommit, rf.raftLog.lastIndex())
	rf.applyCond.Signal()

	reply.Term, reply.Success = rf.currentTerm, true
}

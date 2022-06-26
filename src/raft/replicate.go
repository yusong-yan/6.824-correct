package raft

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) advanceCommitIndexForLeader() {
	latestGrantedIndex := rf.matchIndex[0]
	for _, ServerMatchIndex := range rf.matchIndex {
		latestGrantedIndex = min(latestGrantedIndex, ServerMatchIndex)
	}

	for i := latestGrantedIndex + 1; i <= rf.getLastLog().Index; i++ {
		granted := 1
		for Server, ServerMatchIndex := range rf.matchIndex {
			// if !rf.PeerAlive[Server] {continue}
			if Server != rf.me && ServerMatchIndex >= i {
				granted++
			}
		}
		if granted < len(rf.peers)/2+1 {
			break
		}
		latestGrantedIndex = i
	}

	if latestGrantedIndex > rf.commitIndex &&
		rf.logs[latestGrantedIndex-rf.getFirstLog().Index].Term == rf.currentTerm &&
		rf.state == StateLeader {
		rf.commitIndex = latestGrantedIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ChangeState(StateFollower)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if reply.Term == rf.currentTerm {
		if reply.Success {
			rf.nextIndex[peer] = len(args.Entries) + args.PrevLogIndex + 1
			rf.matchIndex[peer] = len(args.Entries) + args.PrevLogIndex
			// if newnext > rf.nextIndex
			//update commitIndex
			rf.advanceCommitIndexForLeader()
		}
		if reply.ConflictIndex != 0 {
			//conflictValid
			rf.nextIndex[peer] = reply.ConflictIndex - 1
			rf.replicatorCond[peer].Signal()
		}
	}

	// 2b
}
func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {
	//2b
	args := new(AppendEntriesArgs)
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.logs[prevLogIndex-rf.getFirstLog().Index].Term
	args.Entries = make([]Entry, rf.getLastLog().Index-prevLogIndex)
	args.LeaderCommit = rf.commitIndex
	copy(args.Entries, rf.logs[prevLogIndex+1:])
	return args
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// only snapshot can catch up
		// request := rf.genInstallSnapshotRequest()
		// rf.mu.RUnlock()
		// response := new(InstallSnapshotResponse)
		// if rf.sendInstallSnapshot(peer, request, response) {
		// 	rf.mu.Lock()
		// 	rf.handleInstallSnapshotResponse(peer, request, response)
		// 	rf.mu.Unlock()
		// }
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) matchLog(requestPrevTerm int, requestPrevIndex int) bool {
	// there is no such entry exist because there is no such index
	if requestPrevIndex > rf.getLastLog().Index {
		return false
	}
	// check the index, if the term is the same. If the same then we find it
	targetLog := rf.logs[requestPrevIndex-rf.getFirstLog().Index]
	if requestPrevIndex == targetLog.Index && requestPrevTerm == targetLog.Term {
		return true
	}
	return false
}

func (rf *Raft) HandleAppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if request.PrevLogIndex > lastIndex {
			response.ConflictIndex = lastIndex + 1
		} else {
			response.ConflictIndex = request.PrevLogIndex
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)
			break
		}
	}

	//advanceCommitIndexForFollower
	rf.commitIndex = min(request.LeaderCommit, rf.getLastLog().Index)
	rf.applyCond.Signal()

	response.Term, response.Success = rf.currentTerm, true
}

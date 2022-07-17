package raft

//Sending election RPC
func (rf *Raft) StartElection() {
	//Yusong
	rf.state = StateCandidate
	rf.currentTerm += 1
	lastLog := rf.raftLog.lastEntry()
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = lastLog.Index
	args.LastLogTerm = lastLog.Term
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)
	// use Closure
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check if the term is equal to make sure that we are still in current round
				// check Candiate status to make sure we don't process following code if we are leader
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							// prevent figure 8 happened
							// here we don;t need to wait for new client request to commit entires with old term
							// go rf.Start(nil)
							rf.state = StateLeader
							lastLogIndex := rf.raftLog.lastIndex()
							for i := 0; i < len(rf.peers); i++ {
								// if we don't set rf.matchIndex[i] == 0, there will be error in unreliable test
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = lastLogIndex + 1
							}
							rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
							rf.BroadcastAppend(HeartBeat)
						}
					} else if reply.Term > rf.currentTerm {
						rf.state = StateFollower
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

//Handle received RPC
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = StateFollower
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	reply.Term = rf.currentTerm
	//from raft paper (Figure 2, RequestVote RPC, 2)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.raftLog.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}

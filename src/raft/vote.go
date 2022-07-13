package raft

//Sending election RPC
func (rf *Raft) StartElection() {
	//Yusong
	rf.ChangeState(StateCandidate)
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
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, reply, peer, args, rf.currentTerm)
				// check if the term is equal to make sure that we are still in current round
				// check Candiate status to make sure we don't process following code if we are leader
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.ChangeState(StateLeader)
							lastLogIndex := rf.raftLog.lastIndex()
							for i := 0; i < len(rf.peers); i++ {
								// if we don't set rf.matchIndex[i] == 0, there will be error in unreliable test
								rf.matchIndex[i] = 0
								rf.nextIndex[i] = lastLogIndex + 1
							}
							rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
							rf.BroadcastAppend(HeartBeat)
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
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
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.raftLog.dummyIndex(), rf.raftLog.lastEntry(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
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

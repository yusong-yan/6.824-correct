package raft

const (
	HeartBeat = iota + 1
	Append
)

const (
	StateLeader = iota + 1
	StateCandidate
	StateFollower
)
const (
	DidNotWin = iota + 1
	Win
)
const (
	Connect = iota + 1
	Disconnect
)
const (
	Ok = iota + 1
	Fail
)

type ApplyMsg struct {
	Command      interface{}
	CommandValid bool
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Command interface{}
	Term    int
	Id      int
}

type ClientMessageArgs struct {
	Message int
}

type ClientMessageReply struct {
	Message int
	Status  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Entry
	PrevLogIndex int //index of log entry immediately precedingnew ones
	PrevLogTerm  int //term of PrevLogIndex entry
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Conflict      bool
	ConflictIndex int
	Term          int
	Success       bool
}

type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	State       int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

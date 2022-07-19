package raft

type raftLog struct {
	logs []Entry
}

func newLogs() *raftLog {
	raftLog := &raftLog{
		logs: make([]Entry, 1),
	}
	return raftLog
}

func (l *raftLog) getLogs() []Entry {
	return l.logs
}

func (l *raftLog) setLogs(newlogs []Entry) {
	l.logs = make([]Entry, len(newlogs))
	copy(l.logs, newlogs)
}

func (l *raftLog) clearDummyEntryCommand() {
	l.logs[0].Command = nil
}

func (l *raftLog) setDummyIndex(index int) {
	l.logs[0].Index = index
}
func (l *raftLog) setDummyTerm(term int) {
	l.logs[0].Term = term
}
func (l *raftLog) dummyIndex() int {
	return l.logs[0].Index
}
func (l *raftLog) dummyTerm() int {
	return l.logs[0].Term
}

func (l *raftLog) getEntry(index int) Entry {
	return l.logs[l.convertIndex(index)]
}

func (l *raftLog) lastIndex() int {
	return l.logs[len(l.logs)-1].Index
}
func (l *raftLog) lastTerm() int {
	return l.logs[len(l.logs)-1].Term
}

func (l *raftLog) lastEntry() Entry {
	return l.logs[len(l.logs)-1]
}

func (l *raftLog) convertIndex(index int) int {
	if index < l.dummyIndex() {
		panic("current index is smaller than dummy Index")
	}
	return index - l.dummyIndex()
}

func (l *raftLog) append(ents ...Entry) int {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	l.logs = append(l.logs, ents...)
	return l.lastIndex()
}

func (l *raftLog) trunc(high int) int {
	l.logs = l.sliceTo(high)
	return l.lastIndex()
}

func (l *raftLog) sliceFrom(low int) []Entry {
	return l.logs[l.convertIndex(low):]
}

func (l *raftLog) sliceTo(high int) []Entry {
	return l.logs[:l.convertIndex(high)]
}

func (l *raftLog) slice(low int, high int) []Entry {
	return l.logs[l.convertIndex(low):l.convertIndex(high)]
}

func (l *raftLog) len() int {
	return len(l.logs)
}

// raft paper (search log match)
func (l *raftLog) matchLog(Term int, Index int) bool {
	// if Index is bigger than LastIndex, then this entry doesn't exist
	// else if this index has different term, then also doesn't exist
	return Index <= l.lastIndex() && Term == l.getEntry(Index).Term
}

// raft paper (5.41 in the end)
func (l *raftLog) isLogUpToDate(requestLastTerm int, requestLastIndex int) bool {
	mylastLog := l.lastEntry()
	//from raft paper
	return (requestLastTerm > mylastLog.Term ||
		mylastLog.Term == requestLastTerm && requestLastIndex >= mylastLog.Index)
}

package raft

import (
	"fmt"
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type EntryList struct {
	Logs      []Entry
	PrevIndex int
	PrevTerm  int
}

func NewLogList() EntryList {
	return EntryList{
		Logs:      make([]Entry, 1, 100),
		PrevIndex: -1,
		PrevTerm:  -1,
	}
}

func (l *Entry) String() string {
	return fmt.Sprintf("[%d(%d)]", l.Index, l.Term)
}

func (l *EntryList) String() string {
	res := ""
	for _, log := range l.Logs {
		res += log.String()
	}
	res += fmt.Sprintf(" prev [%d(%d)]", l.PrevIndex, l.PrevTerm)
	return res
}

func (l *EntryList) getRawIndex(index int) int {
	return index - (l.PrevIndex + 1)
}

func (l *EntryList) getLastIndex() int {
	if len(l.Logs) > 0 {
		return l.Logs[len(l.Logs)-1].Index
	}
	return l.PrevIndex
}

func (l *EntryList) getLastTerm() int {
	if len(l.Logs) > 0 {
		return l.Logs[len(l.Logs)-1].Term
	}
	return l.PrevTerm
}

func (l *EntryList) getTail(start int) []Entry {
	start = l.getRawIndex(start)
	if start < 0 {
		return []Entry{}
	}
	return l.Logs[start:]
}

func (l *EntryList) getEntry(index int) *Entry {
	if index == l.PrevIndex {
		return &Entry{
			Index: l.PrevIndex,
			Term:  l.PrevTerm,
		}
	}
	rawIndex := l.getRawIndex(index)
	if rawIndex < 0 || rawIndex >= len(l.Logs) {
		return nil
	}
	return &l.Logs[rawIndex]
}

func (l *EntryList) tryCutPrefix(prefixEnd int) {
	rawEnd := l.getRawIndex(prefixEnd)
	if rawEnd > len(l.Logs)-1 {
		rawEnd = len(l.Logs) - 1
	}
	if rawEnd < 0 {
		return
	}
	prevLog := l.Logs[rawEnd]
	l.Logs = l.Logs[rawEnd+1:]
	l.PrevIndex = prevLog.Index
	l.PrevTerm = prevLog.Term
}

func (l *EntryList) tryCutSuffix(suffixStart int) {
	rawStart := l.getRawIndex(suffixStart)
	if rawStart >= len(l.Logs) {
		return
	}
	if rawStart < 0 {
		rawStart = 0
	}
	l.Logs = l.Logs[:rawStart]
}

func (l *EntryList) append(command interface{}, term int) int {
	newEntry := Entry{
		Index:   l.getLastIndex() + 1,
		Term:    term,
		Command: command,
	}
	l.Logs = append(l.Logs, newEntry)
	return newEntry.Index
}

func (l *EntryList) appendEntries(entries []Entry) {
	l.Logs = append(l.Logs, entries...)
}

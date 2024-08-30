package raft

import "fmt"

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type EntryList struct {
	Logs      []Entry
	LastIndex int
	LastTerm  int
}

func NewLogList() EntryList {
	return EntryList{
		Logs: make([]Entry, 1, 100),
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
	return res
}

func (l *EntryList) getLastIndex() int {
	return l.Logs[len(l.Logs)-1].Index
}

func (l *EntryList) getLastTerm() int {
	return l.Logs[len(l.Logs)-1].Term
}

func (l *EntryList) getSlice(start int, end int) []Entry {
	return l.Logs[start : end+1]
}

func (l *EntryList) getEntry(index int) *Entry {
	if index < 0 || index >= len(l.Logs) {
		return nil
	}
	return &l.Logs[index]
}

func (l *EntryList) tryRemoveTail(start int) {
	if start == 0 || start >= len(l.Logs) {
		return
	}
	l.Logs = l.Logs[:start]
	l.LastIndex = start - 1
	l.LastTerm = l.Logs[start-1].Term
}

func (l *EntryList) append(command interface{}, term int) int {
	l.LastIndex++
	l.LastTerm = term
	newEntry := Entry{
		Index:   l.LastIndex,
		Term:    term,
		Command: command,
	}
	l.Logs = append(l.Logs, newEntry)
	return l.LastIndex
}

func (l *EntryList) appendEntries(entries []Entry) {
	l.Logs = append(l.Logs, entries...)
	l.LastIndex += len(entries)
	l.LastTerm = l.Logs[l.LastIndex].Term
}

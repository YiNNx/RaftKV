package raft

import "fmt"

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type EntryList struct {
	logs      []Entry
	lastIndex int
	lastTerm  int
}

func InitLogList() EntryList {
	return EntryList{
		logs: make([]Entry, 1, 100),
	}
}

func (l *Entry) String() string {
	return fmt.Sprintf("[%d(%d)]", l.Index, l.Term)
}

func (l *EntryList) getLastIndex() int {
	return l.lastIndex
}

func (l *EntryList) getLastTerm() int {
	return l.lastTerm
}

func (l *EntryList) getSlice(start int, end int) []Entry {
	return l.logs[start : end+1]
}

func (l *EntryList) getEntry(index int) *Entry {
	if index < 0 || index >= len(l.logs) {
		return nil
	}
	return &l.logs[index]
}

func (l *EntryList) removeTail(start int) {
	if start >= len(l.logs) {
		return
	}
	l.logs = l.logs[:start]
	l.lastIndex = start - 1
	l.lastTerm = l.logs[start-1].Term
}

func (l *EntryList) append(command interface{}, term int) int {
	l.lastIndex++
	l.lastTerm = term
	newEntry := Entry{
		Index:   l.lastIndex,
		Term:    term,
		Command: command,
	}
	l.logs = append(l.logs, newEntry)
	return l.lastIndex
}

func (l *EntryList) appendEntries(entries []Entry) {
	l.logs = append(l.logs, entries...)
	l.lastIndex += len(entries)
	l.lastTerm = l.logs[l.lastIndex].Term
}

package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

var colors = []string{
	"\033[38;5;2m",
	"\033[38;5;45m",
	"\033[38;5;6m",
	"\033[38;5;3m",
	"\033[38;5;204m",
	"\033[38;5;111m",
	"\033[38;5;184m",
	"\033[38;5;69m",
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	colorPrefix := colors[rf.me] + fmt.Sprintf("[%d][term%d ld%d vote%d]", rf.me, rf.currentTerm, rf.leaderID, rf.voteFor) + "\033[39;49m"
	if rf.leaderID == rf.me {
		colorPrefix = "\033[4m" + colorPrefix + "\033[0m"
	}
	format = colorPrefix + " " + format
	DPrintf(format, a...)
}

func init() {
	log.SetFlags(log.Lmicroseconds)
}

package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = true
const Colored = false

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

// note: the debug printf will cause data race
// but it's ok cause it's used for *debug* :)
func (rf *Raft) Debugf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	prefix := fmt.Sprintf("[%d][term%d ld%d commit%d]", rf.me, rf.currentTerm, rf.leaderID, rf.commitIndex)
	if Colored {
		prefix = colors[rf.me] + prefix + "\033[39;49m"
		if rf.leaderID == rf.me {
			prefix = "\033[4m" + prefix + "\033[0m"
		}
	}
	format = prefix + " " + format
	DPrintf(format, a...)
}

func (rf *Raft) HighLightf(format string, a ...interface{}) {
	if Colored {
		format = colors[rf.me] + format + "\033[39;49m"
	}
	rf.Debugf(format, a...)
}

func init() {
	log.SetFlags(log.Lmicroseconds)
}

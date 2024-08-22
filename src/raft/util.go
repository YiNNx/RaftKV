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
	"\u001B[36m", "\u001B[32m", "\u001B[34m", "\u001B[35m", "\u001B[33m",
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	format = colors[rf.me%5] + fmt.Sprintf("[%d][term%d ld%d vote%d] ", rf.me, rf.currentTerm, rf.leaderID, rf.voteFor) + "\u001B[0m" + format
	DPrintf(format, a...)
}

package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// no need to set seed
func getRandomTime() time.Duration {
	ms := 50 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

func getHeartbeatTime() time.Duration {
	return time.Duration(50) * time.Millisecond
}

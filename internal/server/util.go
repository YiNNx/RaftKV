package server

import (
	"fmt"

	"raftkv/internal/util"
)

// note: the debug printf will cause data race
// but it's ok cause it's used for *debug* :)
func (kv *KVServer) Debugf(format string, a ...interface{}) {
	if !util.Debug {
		return
	}
	prefix := fmt.Sprintf("[%d][kv]", kv.me)
	prefix = util.LogHight[kv.me] + prefix + "\033[39;49m"
	format = prefix + " " + format
	util.DPrintf(format, a...)
}

func (kv *KVServer) HighLightf(format string, a ...interface{}) {
	format = util.LogHight[kv.me] + format + "\033[39;49m"
	kv.Debugf(format, a...)
}

package util

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func Highlightf1(format string, a ...interface{}) {
	format = "\033[38;5;2m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

func Highlightf2(format string, a ...interface{}) {
	format = "\033[38;5;6m" + format + "\033[39;49m"
	DPrintf(format, a...)
}

var LogHight = []string{
	"\033[38;5;2m",
	"\033[38;5;45m",
	"\033[38;5;6m",
	"\033[38;5;3m",
	"\033[38;5;204m",
	"\033[38;5;111m",
	"\033[38;5;184m",
	"\033[38;5;69m",
}

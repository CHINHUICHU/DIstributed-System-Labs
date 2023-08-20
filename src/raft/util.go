package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

var (
	start = time.Now()
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Timestamp() int64 {
	return time.Since(start).Abs().Milliseconds()
}

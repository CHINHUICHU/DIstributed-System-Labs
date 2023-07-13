package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Entry
	if d.Decode(&term) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&log) == nil {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		fmt.Printf("--------server read persist me %v term %v-----\n", rf.me, rf.currentTerm)
		for i, e := range rf.log {
			fmt.Printf("persist log, me %v, index %v cmd %v term %v\n", rf.me, i, e.Command, e.Term)
		}
		rf.mu.Unlock()
	}
}

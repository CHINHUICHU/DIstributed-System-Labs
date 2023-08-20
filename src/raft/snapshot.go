package raft

import (
	"fmt"
	"time"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index >= rf.commitIndex || index < rf.lastIncludedIndex {
		return
	}

	fmt.Printf("me %v snapshot: log len %v, commit index %v, last applied %v, old snapshot index %v new snapshot index %v\n", rf.me, len(rf.log), rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, index)
	idx := rf.raftToLogIndex(index)
	rf.lastIncludedTerm = rf.log[idx].Term
	rf.lastIncludedIndex = index
	// remove from seen
	for i := 0; i <= idx; i++ {
		delete(rf.seen, rf.log[i].Command)
	}
	rf.log = rf.log[idx:]
	rf.latestSnapshot = snapshot
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm
	rf.latestSnapshot = args.Data
	if idx := rf.raftToLogIndex(args.LastIncludedIndex); idx >= 0 && idx < len(rf.log) && rf.log[idx].Term == args.LastIncludedTerm {
		for i := 0; i < idx; i++ {
			delete(rf.seen, rf.log[i].Command)
		}
		rf.log = rf.log[idx:]
	} else {
		rf.log = make([]Entry, 0)
		rf.seen = make(map[interface{}]int)
		rf.log = append(rf.log, Entry{})
	}
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.latestSnapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	fmt.Printf("server %v apply snapshot at index %v, term %v\n", rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
	fmt.Printf("----- check log after snapshot -----\n")
	for i, e := range rf.log {
		fmt.Printf("me %v log index %v command %v\n", rf.me, i+args.LastIncludedIndex, e.Command)
	}
	fmt.Printf("----- check log after snapshot -----\n")
	go func() {
		rf.applych <- applyMsg
	}()
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

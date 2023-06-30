package raft

import (
	"fmt"
	"sync/atomic"
)

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	fmt.Printf("I am dead, me %v, role %v, term %v, time %v\n", rf.me, rf.Role(), rf.CurrentTerm(), Timestamp())
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.CurrentTerm()
	var isLeader bool = rf.Role() == Leader
	if isLeader {
		fmt.Printf("- GetState: I am leader me %v in term %v time %v\n", rf.me, term, Timestamp())
		// Your code here (2A).
	}
	return term, isLeader
}

func (rf *Raft) Role() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) SetRole(role Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) Log(i int) Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[i]
}

func (rf *Raft) SetLog(i int, e Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log[i] = Entry{
		Command: e.Command,
		Term:    e.Term,
	}
}

func (rf *Raft) LogLen() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) CommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) SetCommitIndex(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = i
}

func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) SetLastApplied(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = i
}

func (rf *Raft) AppendEntry(e Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, e)
}

func (rf *Raft) NextIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server]
}

func (rf *Raft) SetNextIndex(server int, i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = i
}

func (rf *Raft) SetMatchIndex(server int, i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[server] = i
}

func (rf *Raft) MatchIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[server]
}

func (rf *Raft) truncateLog(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = rf.log[:i]
}

func (rf *Raft) appendLog(entries []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) retrieveLog(start int, end int) []Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[start:end]
}

func (rf *Raft) isLeaderReady() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex != nil && rf.matchIndex != nil && rf.role == Leader
}

func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log) // last log
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.matchIndex[i] = -1
		}
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}
}

func (rf *Raft) Seen() map[interface{}]int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.seen
}

func (rf *Raft) SetSeen(key interface{}, i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.seen[key] = i
}

func (rf *Raft) DeleteSeen(cmd interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	delete(rf.seen, cmd)
}

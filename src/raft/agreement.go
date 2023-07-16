package raft

import (
	"fmt"
	"time"
)

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.Role() == Leader

	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	// Your code here (2B).
	if !rf.killed() && rf.matchIndex != nil && rf.nextIndex != nil {
		// append entry to local log
		defer func() {
			rf.persist()
			rf.mu.Unlock()
		}()

		term = rf.currentTerm

		entry := Entry{
			Term:    term,
			Command: command,
		}

		var index int

		if i, ok := rf.seen[command]; ok {

			fmt.Printf("client retried...., previous entry info term %v, index %v, command %v\n", rf.log[i].Term, i, command)

			e := Entry{
				Term:    term,
				Command: command,
			}
			rf.log[i] = e
			index = i
		} else {
			rf.log = append(rf.log, entry)
			index = len(rf.log) - 1
			rf.nextIndex[rf.me] = index + 1
			rf.matchIndex[rf.me] = index
			rf.seen[entry.Command] = index
		}

		// fmt.Printf("------- leader %v check log in term %v -------\n", rf.me, rf.currentTerm)
		// for i, e := range rf.log {
		// 	// if len(rf.log) > 0 {
		// 	// 	e := rf.log[len(rf.log)-1]
		// 	fmt.Printf("me %v index %v, command %v, term %v\n", rf.me, i, e.Command, e.Term)
		// 	// }
		// }
		// fmt.Printf("------- leader %v check log in term %v finished-------\n", rf.me, rf.currentTerm)

		return index + 1, term, isLeader
	}
	return index + 1, term, isLeader
}

func (rf *Raft) reachAgreement() {
	for !rf.killed() {
		if rf.isLeaderReady() {
			startAppendTerm := rf.CurrentTerm()
			for i := range rf.peers {
				if i != rf.me && rf.CurrentTerm() == startAppendTerm && rf.Role() == Leader {
					go rf.appendLogRoutine(i)
					time.Sleep(AppendInterval)
				}
			}
		}
	}
}

func (rf *Raft) appendLogRoutine(i int) {
	// this routine will at least run once for heartbeat
	rf.mu.Lock()
	shouldContinue := true && rf.role == Leader && rf.nextIndex != nil && rf.matchIndex != nil
	rf.mu.Unlock()
	for shouldContinue {
		rf.mu.Lock()
		if rf.role != Leader || rf.nextIndex == nil || rf.matchIndex == nil {
			rf.mu.Unlock()
			return
		}
		ni := rf.nextIndex[i]
		term := rf.currentTerm
		ll := len(rf.log)
		if ni > ll {
			ni = ll
		}
		entries := rf.log[ni:]
		prevLogIdx := ni - 1
		prevLogTerm := 0
		if prevLogIdx >= 0 {
			prevLogTerm = rf.log[prevLogIdx].Term
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()
		replied := make(chan bool, 1)
		success := true
		start := time.Now()
		go func() {
			ok := rf.sendAppendEntries(i, args, reply)
			replied <- ok
		}()
		for {
			if v, ok := <-replied; ok {
				success = v
				break
			} else if time.Since(start) > RpcTimeout {
				success = false
				break
			}
			time.Sleep(CheckInterval)
		}
		term = rf.CurrentTerm()
		rf.mu.Lock()
		isOutdated := term != args.Term ||
			rf.role != Leader ||
			rf.nextIndex == nil ||
			rf.matchIndex == nil ||
			rf.nextIndex[i] != args.PrevLogIndex+1
		rf.mu.Unlock()
		// the RPC outdated
		if isOutdated {
			return
		}
		if success {
			if reply.Term > term {
				rf.mu.Lock()
				rf.role = Follower
				rf.currentTerm = reply.Term
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			if reply.Success {
				mi := args.PrevLogIndex + len(args.Entries)
				fmt.Printf("append success, leader %v update server %v match index to %v time %v\n", rf.me, i, mi, Timestamp())
				rf.matchIndex[i] = mi
				rf.nextIndex[i] = mi + 1
				if ll-1 < rf.nextIndex[i] || len(args.Entries) == 0 {
					shouldContinue = false
				}
			} else {
				index := -1
				for i, e := range rf.log {
					if e.Term == reply.ConflictTerm {
						index = i + 1
					}
				}
				if index == -1 {
					index = reply.ConflictIndex
				}
				fmt.Printf("append failed, leader %v set follower %v nextIndex %v\n", rf.me, i, reply.ConflictIndex)
				rf.nextIndex[i] = index
			}
			rf.mu.Unlock()
			time.Sleep(AppendInterval)
		}
	}
}

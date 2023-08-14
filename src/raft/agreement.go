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
	ready := rf.matchIndex != nil && rf.nextIndex != nil
	// Your code here (2B).
	if !rf.killed() && ready {
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

		if raftIndex, ok := rf.seen[command]; ok {
			logIndex := rf.raftToLogIndex(raftIndex)
			rf.log[logIndex] = entry
			index = raftIndex
		} else {
			rf.log = append(rf.log, entry)
			index = rf.logToRaftIndex(len(rf.log) - 1)
			rf.nextIndex[rf.me] = index + 1
			rf.matchIndex[rf.me] = index
			rf.seen[entry.Command] = index
		}

		fmt.Printf("Leader %v check log, index %v, command %v\n", rf.me, index, command)
		return index, term, isLeader
	} else {
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

func (rf *Raft) reachAgreement() {
	for !rf.killed() {
		start := rf.CurrentTerm()
		for i := range rf.peers {
			rf.mu.Lock()
			if i != rf.me && rf.nextIndex != nil && rf.matchIndex != nil && rf.role == Leader && start == rf.currentTerm {
				rf.mu.Unlock()
				go rf.appendLogRoutine(i)
				time.Sleep(AppendInterval)
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) appendLogRoutine(i int) {
	// this routine will at least run once for heartbeat
	shouldContinue := true
	for shouldContinue {
		rf.mu.Lock()
		isValid := rf.role == Leader && rf.nextIndex != nil && rf.matchIndex != nil
		if isValid && rf.raftToLogIndex(rf.nextIndex[i]) >= 0 {
			if next := rf.logToRaftIndex(len(rf.log)); rf.nextIndex[i] > next {
				rf.nextIndex[i] = next
			}
			nextLogIndex := rf.raftToLogIndex(rf.nextIndex[i])
			entries := rf.log[nextLogIndex:]
			prevLogTerm := 0
			if nextLogIndex-1 >= 0 {
				prevLogTerm = rf.log[nextLogIndex-1].Term
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			replied := make(chan bool, 1)
			start := time.Now()

			go func(args *AppendEntriesArgs, reply *AppendEntriesReply, replied chan bool) {
				replied <- rf.sendAppendEntries(i, args, reply)
			}(args, reply, replied)

			go func(replied chan bool) {
				for {
					if time.Since(start) > RpcTimeout {
						replied <- false
						return
					}
					time.Sleep(CheckInterval)
				}
			}(replied)

			rf.mu.Unlock()
			if <-replied {
				rf.mu.Lock()
				isOutdated := rf.currentTerm != args.Term ||
					rf.role != Leader ||
					rf.nextIndex == nil ||
					rf.matchIndex == nil ||
					rf.nextIndex[i] != args.PrevLogIndex+1
				if !isOutdated {
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.nextIndex = nil
						rf.matchIndex = nil
						rf.currentTerm = reply.Term
						rf.persist()
					} else if reply.Success {
						mi := args.PrevLogIndex + len(args.Entries)
						fmt.Printf("append success, leader %v update server %v match index to %v time %v\n", rf.me, i, mi, Timestamp())
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
						if rf.logToRaftIndex(len(rf.log)-1) < rf.nextIndex[i] || len(args.Entries) == 0 {
							shouldContinue = false
						}
					} else {
						index := -1
						for i, e := range rf.log {
							if e.Term == reply.ConflictTerm {
								index = rf.logToRaftIndex(i) + 1
							}
						}
						if index == -1 {
							index = reply.ConflictIndex
						}
						rf.nextIndex[i] = index
						// fmt.Printf("append failed, leader %v set server %v nextIndex %v\n", rf.me, i, index)
					}
				}
				rf.mu.Unlock()
			}
		} else if isValid && rf.raftToLogIndex(rf.nextIndex[i]) < 0 {
			// should install snapshot
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.latestSnapshot,
			}
			reply := &InstallSnapshotReply{}
			replied := make(chan bool, 1)
			start := time.Now()
			// fmt.Printf("reason why we need to install snapshot nextIndex[i] %v lastIncludeIndex %v\n", rf.nextIndex[i], rf.lastIncludedIndex)
			rf.mu.Unlock()
			// fmt.Printf("leader %v send install snapshot rpc to server %v, args: index %v term %v\n", rf.me, i, args.LastIncludedIndex, args.LastIncludedTerm)
			go func(args *InstallSnapshotArgs, reply *InstallSnapshotReply, ch chan bool) {
				ok := rf.sendInstallSnapshot(i, args, reply)
				replied <- ok
			}(args, reply, replied)
			go func(ch chan bool) {
				for {
					if time.Since(start) > RpcTimeout {
						replied <- false
						return
					}
					time.Sleep(CheckInterval)
				}
			}(replied)
			if <-replied {
				rf.mu.Lock()
				isOutdated := rf.currentTerm != args.Term || rf.role != Leader || rf.lastIncludedIndex != args.LastIncludedIndex
				if !isOutdated {
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.nextIndex = nil
						rf.matchIndex = nil
						rf.currentTerm = reply.Term
						rf.persist()
					} else {
						if rf.lastIncludedIndex > rf.matchIndex[i] {
							rf.nextIndex[i] = rf.lastIncludedIndex + 1
							rf.matchIndex[i] = rf.lastIncludedIndex
							fmt.Printf("leader %v install snapshot success, set server %v match index = %v\n", rf.me, i, rf.lastIncludedIndex)
						}
					}
				}
				rf.mu.Unlock()
			}
			return
		} else {
			rf.mu.Unlock()
			return
		}
		time.Sleep(AppendInterval)
	}
}

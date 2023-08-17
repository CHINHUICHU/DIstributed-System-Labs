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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader && rf.nextIndex != nil && rf.matchIndex != nil && !rf.killed()
	if !isLeader {
		return index, term, isLeader
	}

	defer rf.persist()

	term = rf.currentTerm
	entry := Entry{
		Term:    term,
		Command: command,
	}

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

	// fmt.Printf("Leader %v check log, index %v, command %v, term %v\n", rf.me, index, command, rf.currentTerm)
	return index, term, isLeader
}

func (rf *Raft) reachAgreement() {
	for !rf.killed() {
		rf.mu.Lock()
		start := rf.currentTerm
		rf.mu.Unlock()
		for i := range rf.peers {
			rf.mu.Lock()
			if i != rf.me && rf.nextIndex != nil && rf.matchIndex != nil && rf.role == Leader && start == rf.currentTerm {
				rf.mu.Unlock()
				go rf.appendLogRoutine(i, start)
				time.Sleep(RpcInterval)
			} else if rf.role != Leader || rf.currentTerm != start {
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) appendLogRoutine(i int, start int) {
	// this routine will at least run once for heartbeat
	for {
		rf.mu.Lock()
		isValid := rf.role == Leader && rf.nextIndex != nil && rf.matchIndex != nil && !rf.killed() && rf.currentTerm == start
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
			replied := make(chan struct{}, 1)

			go func(args *AppendEntriesArgs, reply *AppendEntriesReply, replied chan struct{}) {
				rf.sendAppendEntries(i, args, reply)
				replied <- struct{}{}
			}(args, reply, replied)
			rf.mu.Unlock()

		loop:
			for {
				select {
				case <-replied:
					// fmt.Printf("append time %v\n", time.Since(start).Abs().Milliseconds())
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
							rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
							if rf.logToRaftIndex(len(rf.log)-1) < rf.nextIndex[i] || len(args.Entries) == 0 {
								rf.mu.Unlock()
								return
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
						}
					}
					rf.mu.Unlock()
				case <-time.After(RpcTimeout):
					break loop
				default:
					time.Sleep(CheckInterval)
				}
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
			replied := make(chan struct{}, 1)
			rf.mu.Unlock()
			go func(args *InstallSnapshotArgs, reply *InstallSnapshotReply, ch chan struct{}) {
				rf.sendInstallSnapshot(i, args, reply)
				replied <- struct{}{}
			}(args, reply, replied)

		loop2:
			for {
				select {
				case <-replied:
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
				case <-time.After(RpcTimeout * 2):
					break loop2
				default:
					time.Sleep(CheckInterval)
				}
			}
			return
		} else {
			rf.mu.Unlock()
			return
		}
		time.Sleep(RpcInterval)
	}
}

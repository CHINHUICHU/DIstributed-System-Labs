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
	term = rf.CurrentTerm()
	// Your code here (2B).
	if !rf.killed() && rf.isLeaderReady() {
		// append entry to local log
		fmt.Printf("- Leader (me: %v) is alive in term %v and start to append entry to local log, time %v\n", rf.me, term, Timestamp())
		entry := Entry{
			Term:    term,
			Command: command,
		}

		var index int

		if i, ok := rf.Seen()[command]; ok {

			fmt.Printf("client retried...., previous entry info term %v, index %v, command %v\n", rf.Log(i).Term, i, command)

			e := Entry{
				Term:    rf.CurrentTerm(),
				Command: command,
			}

			rf.SetLog(i, e)
			index = i
		} else {
			rf.appendLog([]Entry{entry})
			index = rf.LogLen() - 1
			rf.SetNextIndex(rf.me, index+1)
			rf.SetMatchIndex(rf.me, index)
		}

		fmt.Printf("-------Leader %v update matchIndex to %v-----------\n", rf.me, index)

		fmt.Printf("------- leader %v check log in term %v -------\n", rf.me, rf.CurrentTerm())
		ll := rf.LogLen()
		for i := 0; i < ll; i++ {
			e := rf.Log(i)
			fmt.Printf("index %v, command %v, term %v\n", i, e.Command, e.Term)
		}
		fmt.Printf("------- leader %v check log in term %v finished-------\n", rf.me, rf.CurrentTerm())

		return index + 1, term, isLeader
	}
	return index + 1, term, isLeader
}

func (rf *Raft) reachAgreement() {
	for !rf.killed() {
		if rf.Role() == Leader && rf.isLeaderReady() {
			for i := range rf.peers {
				if i != rf.me {
					ni := rf.NextIndex(i)
					go rf.appendLogRoutine(i, ni)
					time.Sleep(HeartBeatInterval)
				}
			}
		}
	}
}

func (rf *Raft) appendLogRoutine(i int, ni int) {
	term := rf.CurrentTerm()
	ll := rf.LogLen()
	if ni > ll {
		ni = ll
	}
	entries := rf.retrieveLog(ni, ll)
	prevLogIdx := ni - 1
	prevLogTerm := 0
	if prevLogIdx >= 0 {
		prevLogTerm = rf.Log(prevLogIdx).Term
	}
	li := rf.CommitIndex()
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: li,
	}
	reply := &AppendEntriesReply{}
	// fmt.Printf("leader %v send out AE rpc to server %v in term %v, time %v\n", rf.me, i, rf.CurrentTerm(), Timestamp())
	ok := rf.sendAppendEntries(i, args, reply)
	term = rf.CurrentTerm()
	if term != args.Term {
		fmt.Printf("leader %v send server %v append entries RPC outdated in term %v\n", rf.me, i, term)
		return
	}
	if ok {
		// fmt.Printf("leader %v AE rpc to server %v succeeded term %v, time %v\n", rf.me, i, rf.CurrentTerm(), Timestamp())
		if reply.Term > term {
			rf.SetRole(Follower)
			rf.SetCurrentTerm(reply.Term)
			fmt.Printf("- **Term changed** LEADER STEP DOWN when append log, IS HB? %v: %v step down in term %v time %v\n", len(entries) == 0, rf.me, reply.Term, Timestamp())
			return
		}
		if reply.Success {
			mi := args.PrevLogIndex + len(args.Entries)
			rf.SetNextIndex(i, mi+1)
			rf.SetMatchIndex(i, mi)
			if len(args.Entries) > 0 {
				fmt.Printf("- LEADER (me %v) APPEND SUCCESS (HB? %v) to server %v: term %v, set ni = %v, mi = %v, time %v\n", rf.me, len(entries) == 0, i, reply.Term, rf.NextIndex(i), rf.MatchIndex(i), Timestamp())
			}
		} else {
			fmt.Printf("-------LEADER APPEND FAILED------\n")
			fmt.Printf("leader %v, index %v, term %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			fmt.Printf("follower %v, conflict index %v\n", i, reply.ConflictIndex)
			fmt.Printf("-------LEADER APPEND FAILED------\n")
			ll := rf.LogLen()
			index := -1
			logs := rf.retrieveLog(0, ll)
			for i, e := range logs {
				if e.Term == reply.ConflictTerm {
					index = i + 1
				}
			}
			if index == -1 {
				index = reply.ConflictIndex
			}
			fmt.Printf("leader %v set follower %v nextIndex %v\n", rf.me, i, reply.ConflictIndex)
			rf.SetNextIndex(i, index)
		}
	} else {
		fmt.Printf("leader %v AE rpc to server %v fai/led term %v, time %v\n", rf.me, i, rf.CurrentTerm(), Timestamp())
	}
}

package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// AE RPC step1: process term
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
		rf.matchIndex = nil
		rf.nextIndex = nil
	} else if rf.role == Candidate {
		rf.role = Follower
		rf.matchIndex = nil
		rf.nextIndex = nil
	}

	reply.Term = rf.currentTerm
	rf.lastContact = time.Now()

	if rf.raftToLogIndex(args.PrevLogIndex) < 0 {
		fmt.Printf("this is old append entries RPC, ignore\n")
		return
	}

	// process RPC
	// AE RPC step 2: check if entry match at prevLogIndex and prevLogTerm
	if lastRaftIndex := rf.logToRaftIndex(len(rf.log) - 1); args.PrevLogIndex > lastRaftIndex {
		fmt.Printf("leader (me %v)'s preLogIndex %v out of my (me %v) log len (%v) range time (append log and immediately send hb)%v\n", args.LeaderId, args.PrevLogIndex, rf.me, lastRaftIndex+1, Timestamp())
		reply.ConflictIndex = lastRaftIndex + 1
		return
	} else if args.PrevLogIndex > rf.logToRaftIndex(0) {
		logIndex := rf.raftToLogIndex(args.PrevLogIndex)
		if e := rf.log[logIndex]; e.Term != args.PrevLogTerm {
			fmt.Printf("log conflict: leader %v me %v log index %v leader term %v my term %v time %v \n", args.LeaderId, rf.me, args.PrevLogIndex, e.Term, args.PrevLogTerm, Timestamp())
			reply.ConflictTerm = e.Term
			for i, entry := range rf.log {
				if entry.Term == e.Term {
					reply.ConflictIndex = rf.logToRaftIndex(i)
					return
				}
			}
		}
	}

	newEntries := args.Entries
	match := 0

	// fmt.Printf("leader %v, new entries len %v, leader prev log index %v\n", args.LeaderId, len(newEntries), args.PrevLogIndex)
	// fmt.Printf("-------leader's log to append------\n")
	// for i, e := range newEntries {
	// 	fmt.Printf("leader %v, index %v, command %v, term %v, time %v\n", args.LeaderId, i+args.PrevLogIndex+1, e.Command, e.Term, Timestamp())
	// }
	// fmt.Printf("-------leader's log to append------\n")

	// AE RPC step 3: truncate follower's if conflicting
	for i := 0; i < len(args.Entries); i++ {
		logIdx := rf.raftToLogIndex(args.PrevLogIndex + i + 1)
		if logIdx >= 0 && logIdx < len(rf.log) {
			if e := rf.log[logIdx]; e.Term != args.Entries[i].Term || e.Command != args.Entries[i].Command {
				for i := logIdx; i < len(rf.log); i++ {
					delete(rf.seen, rf.log[i].Command)
				}
				rf.log = rf.log[:logIdx]
				break
			} else {
				match++
			}
		}
	}

	newEntries = newEntries[match:]

	for _, e := range newEntries {
		if prevIdx, ok := rf.seen[e.Command]; ok && prevIdx > 0 && prevIdx < rf.logToRaftIndex(len(rf.log)) &&
			rf.log[rf.raftToLogIndex(prevIdx)].Command == e.Command {
			entry := Entry{
				Term:    reply.Term,
				Command: e.Command,
			}
			rf.log[rf.raftToLogIndex(prevIdx)] = entry
		} else {
			rf.log = append(rf.log, e)
			rf.seen[e.Command] = rf.logToRaftIndex(len(rf.log) - 1)
		}
	}

	// AE PRC step 4: append log
	// if len(rf.log) > 0 {
	// 	e := rf.log[len(rf.log)-1]
	// 	fmt.Printf("me %v index %v, command %v, term %v\n", rf.me, len(rf.log)-1, e.Command, e.Term)
	// }
	// if args.Entries != nil {
	// 	fmt.Printf("-------follower check log------\n")

	// 	for i, e := range rf.log {
	// 		fmt.Printf("me %v, index %v, command %v, term %v, time %v\n", rf.me, rf.logToRaftIndex(i), e.Command, e.Term, Timestamp())
	// 	}
	// 	fmt.Printf("-------follower check log------\n")
	// }

	// AE PRC step 5: check commit index
	if ci := rf.commitIndex; args.LeaderCommit > ci {
		ci = args.LeaderCommit
		lastIdx := rf.logToRaftIndex(len(rf.log) - 1)
		if lastIdx < ci {
			ci = lastIdx
		}
		rf.commitIndex = ci
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

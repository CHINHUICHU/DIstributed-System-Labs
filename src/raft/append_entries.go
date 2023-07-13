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
		// fmt.Printf("- leader's term outdated leader %v, leader's term %v, me: %v, my term: %v time: %v\n", args.LeaderId, args.Term, rf.me, rf.currentTerm, Timestamp())
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		// fmt.Printf("- role changed **Term changed** leader term higher than me: %v, leader's term: %v time: %v\n", rf.me, args.Term, Timestamp())
	} else if rf.role == Candidate {
		// fmt.Printf("- role changed I (me %v) am candidate in term %v, received hb, convert to follower, time %v\n", rf.me, rf.currentTerm, Timestamp())
		rf.role = Follower
	}

	ll := len(rf.log)
	reply.Term = rf.currentTerm
	rf.lastContact = time.Now()
	// fmt.Printf("%v received Append Entries RPC from leader %v in term %v, ***reset election timer*** time %v\n", rf.me, args.LeaderId, reply.Term, Timestamp())

	// process RPC
	// AE RPC step 2: check if entry match at prevLogIndex and prevLogTerm
	if args.PrevLogIndex >= ll {
		fmt.Printf("leader (me %v)'s preLogIndex %v out of my (me %v) log len (%v) range time (append log and immediately send hb)%v\n", args.LeaderId, args.PrevLogIndex, rf.me, ll, Timestamp())
		reply.ConflictIndex = ll
		return
	}

	if args.PrevLogIndex >= 0 {
		if e := rf.log[args.PrevLogIndex]; e.Term != args.PrevLogTerm {
			fmt.Printf("log conflict: leader %v me %v log index %v leader term %v my term %v time %v \n", args.LeaderId, rf.me, args.PrevLogIndex, e.Term, args.PrevLogTerm, Timestamp())
			reply.ConflictTerm = e.Term
			for i, entry := range rf.log {
				if entry.Term == e.Term {
					reply.ConflictIndex = i
					return
				}
			}
		}
	}

	newEntries := args.Entries
	match := 0

	// fmt.Printf("leader %v, new entries len %v, leader commit index %v\n", args.LeaderId, len(newEntries), args.LeaderCommit)

	// AE RPC step 3: truncate follower's if conflicting
	for i := 0; i < len(args.Entries) && args.PrevLogIndex+i+1 < ll; i++ {
		if e := rf.log[args.PrevLogIndex+i+1]; e.Term != args.Entries[i].Term || e.Command != args.Entries[i].Command {
			fmt.Print("------Entry conflict, need to truncate------\n")
			fmt.Printf("LEADER: %v, command %v, term %v, index %v\n", args.LeaderId, args.Entries[i].Command, args.Entries[i].Term, i)
			fmt.Printf("SERVER: %v, command %v, term %v, index %v\n", rf.me, e.Command, e.Term, args.PrevLogIndex+i+1)
			fmt.Print("------Entry conflict, need to truncate------\n")
			rf.log = rf.log[:args.PrevLogIndex+i+1]
			// update seen when truncate log
			for i := args.PrevLogIndex + i + 1; i < len(rf.log); i++ {
				delete(rf.seen, rf.log[i].Command)
			}
			rf.log = rf.log[:args.PrevLogIndex+i+1]
			break
		} else {
			match++
		}
	}

	newEntries = newEntries[match:]

	for _, e := range newEntries {
		ll = len(rf.log)
		if prevIdx, ok := rf.seen[e.Command]; ok && prevIdx > 0 && prevIdx < ll &&
			rf.log[prevIdx].Command == e.Command {
			entry := Entry{
				Term:    reply.Term,
				Command: e.Command,
			}
			rf.log[prevIdx] = entry
		} else {
			rf.log = append(rf.log, e)
			idx := len(rf.log) - 1
			rf.seen[e.Command] = idx
		}
	}

	// AE PRC step 4: append log
	if len(rf.log) > 0 {
		e := rf.log[len(rf.log)-1]
		fmt.Printf("me %v index %v, command %v, term %v\n", rf.me, len(rf.log)-1, e.Command, e.Term)
	}
	// if args.Entries != nil {
	// 	fmt.Printf("-------follower check log------\n")

	// 	for i, e := range rf.log {
	// 		fmt.Printf("me %v, index %v, command %v, term %v, time %v\n", rf.me, i, e.Command, e.Term, Timestamp())
	// 	}
	// 	fmt.Printf("-------follower check log------\n")
	// }

	// AE PRC step 5: check commit index
	if ci := rf.commitIndex; args.LeaderCommit > ci {
		ci = args.LeaderCommit
		lastIdx := len(rf.log) - 1
		if lastIdx < ci {
			ci = lastIdx
		}
		rf.commitIndex = ci
		fmt.Printf("server %v update commit index to min(leader ci: %v, last idx of new entry: %v) in term %v time %v\n", rf.me, args.LeaderCommit, lastIdx, rf.currentTerm, Timestamp())
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

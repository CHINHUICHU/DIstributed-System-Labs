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
	ct := rf.CurrentTerm()
	if args.Term < ct {
		fmt.Printf("- leader's term outdated leader %v, leader's term %v, me: %v, my term: %v time: %v\n", args.LeaderId, args.Term, rf.me, ct, Timestamp())
		reply.Term = ct
		return
	} else if args.Term > ct {
		rf.SetCurrentTerm(args.Term)
		rf.SetRole(Follower)
		fmt.Printf("- **Term changed** leader term higher than me: %v, leader's term: %v time: %v\n", rf.me, args.Term, Timestamp())
	} else if rf.Role() == Candidate {
		fmt.Printf("- I (me %v) am candidate in term %v, received hb, convert to follower, time %v\n", rf.me, ct, Timestamp())
		rf.SetRole(Follower)
	}

	ll := rf.LogLen()
	reply.Term = rf.CurrentTerm()
	rf.SetLastContact(time.Now())
	fmt.Printf("%v received Append Entries RPC from leader %v in term %v, ***reset election timer*** time %v\n", rf.me, args.LeaderId, reply.Term, Timestamp())

	// process RPC
	// AE RPC step 2: check if entry match at prevLogIndex and prevLogTerm
	if args.PrevLogIndex >= ll {
		fmt.Printf("leader (me %v)'s preLogIndex %v out of my (me %v) log len (%v) range time (append log and immediately send hb)%v\n", args.LeaderId, args.PrevLogIndex, rf.me, rf.LogLen(), Timestamp())
		reply.ConflictIndex = rf.LogLen()
		return
	}
	if args.PrevLogIndex >= 0 {
		if e := rf.Log(args.PrevLogIndex); e.Term != args.PrevLogTerm {
			fmt.Printf("log conflict: leader %v me %v log index %v leader term %v my term %v time %v \n", args.LeaderId, rf.me, args.PrevLogIndex, e.Term, args.PrevLogTerm, Timestamp())
			reply.ConflictTerm = e.Term
			ll := rf.LogLen()
			logs := rf.retrieveLog(0, ll)
			for i, entry := range logs {
				if entry.Term == e.Term {
					reply.ConflictIndex = i
					return
				}
			}
		}
	}

	newEntries := args.Entries
	match := 0

	fmt.Printf("leader %v, new entries len %v\n", args.LeaderId, len(newEntries))

	// AE RPC step 3: truncate follower's if conflicting
	for i := 0; i < len(args.Entries) && args.PrevLogIndex+i+1 < ll; i++ {
		fmt.Printf("- server starts processing logs... me: %v, leader's term: %v time: %v\n", rf.me, args.Term, Timestamp())
		if e := rf.Log(args.PrevLogIndex + i + 1); e.Term != args.Entries[i].Term || e.Command != args.Entries[i].Command {
			fmt.Print("------Entry conflict, need to truncate------\n")
			fmt.Printf("LEADER: %v, command %v, term %v, index %v\n", args.LeaderId, args.Entries[i].Command, args.Entries[i].Term, i)
			fmt.Printf("SERVER: %v, command %v, term %v, index %v\n", rf.me, e.Command, e.Term, args.PrevLogIndex+i+1)
			fmt.Print("------Entry conflict, need to truncate------\n")
			rf.truncateLog(args.PrevLogIndex + i + 1)
			break
		} else {
			match++
		}
	}

	newEntries = newEntries[match:]

	for _, e := range newEntries {
		ll = rf.LogLen()
		if prevIdx, ok := rf.Seen()[e.Command]; ok && prevIdx > 0 && prevIdx < ll &&
			rf.Log(prevIdx).Command == e.Command {
			entry := Entry{
				Term:    reply.Term,
				Command: e.Command,
			}
			rf.SetLog(prevIdx, entry)
		} else {
			// fmt.Printf("----Server did not seen the command, append log: me %v command %v term %v-----\n", rf.me, e.Command, e.Term)
			rf.appendLog([]Entry{e})
		}
	}

	// AE PRC step 4: append log
	// rf.appendLog(newEntries)
	if args.Entries != nil {
		// fmt.Printf("server %v finished append log in term %v, now log len = %v (li %v) time %v\n", rf.me, ct, rf.LogLen(), args.LeaderCommit, Timestamp())
		fmt.Printf("-------follower check log------\n")
		rf.mu.Lock()
		for i, e := range rf.log {
			fmt.Printf("me %v, index %v, command %v, term %v, time %v\n", rf.me, i, e.Command, e.Term, Timestamp())
		}
		rf.mu.Unlock()
		fmt.Printf("-------follower check log------\n")
	}

	// AE PRC step 5: check commit index
	if ci := rf.CommitIndex(); args.LeaderCommit > ci {
		ci = args.LeaderCommit
		lastIdx := rf.LogLen() - 1
		if lastIdx < ci {
			ci = lastIdx
		}
		rf.SetCommitIndex(ci)
		fmt.Printf("server %v update commit index to min(leader ci: %v, last idx of new entry: %v) in term %v time %v\n", rf.me, args.LeaderCommit, lastIdx, ct, Timestamp())
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

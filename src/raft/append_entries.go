package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *RequestVoteReply) {
	cf := rf.CurrentTerm()

	if args.Term < cf {
		fmt.Printf("- leader hb outdated, me: %v, term: %v time: %v\n", rf.me, cf, time.Now().UnixMilli())
		return
	} else if args.Term > cf {
		rf.SetCurrentTerm(args.Term)
		rf.SetRole(Follower)
		fmt.Printf("- leader term higher than me: %v, leader's term: %v time: %v\n", rf.me, args.Term, time.Now().UnixMilli())
	} else if rf.Role() == Candidate {
		fmt.Printf("- I am candidate in term %v, received hb, convert to follower, time %v\n", cf, time.Now().UnixMilli())
		rf.SetRole(Follower)
	}

	rf.SetLastContact(time.Now())
	reply.Term = rf.CurrentTerm()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

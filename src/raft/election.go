package raft

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	ElectionTimeout = 400 + (rand.Int63() % 400)
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) VotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) Vote(c int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = c
}

func (rf *Raft) LastContact() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastContact
}

func (rf *Raft) SetLastContact(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastContact = t
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B)
	cf := rf.CurrentTerm()

	if args.Term < cf {
		fmt.Printf("- candidate %v term is outdated, term should be %v, me is %v, time: %v\n", args.CandidateId, cf, rf.me, time.Now().UnixMilli())
		return
	} else if args.Term > cf {
		fmt.Printf("- Got vote request from candidate %v, me: %v, role %v, term: %v, time: %v\n", args.CandidateId, rf.me, rf.Role(), cf, time.Now().UnixMilli())
		rf.SetRole(Follower)
		rf.SetCurrentTerm(args.Term)
		rf.Vote(-1)
	}

	if votedFor := rf.VotedFor(); votedFor == -1 || votedFor == args.CandidateId {
		fmt.Printf("- %v vote to %v in term %v (vote granted) time %v \n", rf.me, args.CandidateId, rf.CurrentTerm(), time.Now().UnixMilli())
		rf.Vote(args.CandidateId)
		reply.VoteGranted = true
		rf.SetLastContact(time.Now())
	}
	reply.Term = rf.CurrentTerm()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	if !rf.killed() && rf.Role() == Candidate {
		rf.SetCurrentTerm(rf.CurrentTerm() + 1)
		rf.Vote(rf.me)
		rf.SetLastContact(time.Now())
		atomic.StoreInt32(&rf.votes, 0)
		atomic.AddInt32(&rf.votes, 1)
		cf := rf.CurrentTerm()
		for i := range rf.peers {
			if rf.Role() == Candidate && i != rf.me {
				go func(i int) {
					fmt.Printf("- Initial RV: candidate %v request vote from server %v in term: %v time %v\n", rf.me, i, cf, time.Now().UnixMilli())
					args := &RequestVoteArgs{
						Term:        cf,
						CandidateId: rf.me,
					}
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(i, args, reply)
					newcf := rf.CurrentTerm()
					if newcf != args.Term {
						return
					}
					if ok {
						if reply.Term > newcf {
							fmt.Printf("- Valid reply term: candidate %v got higher reply term %v from server %v time %v\n", rf.me, reply.Term, i, time.Now().UnixMilli())
							rf.SetRole(Follower)
							rf.SetCurrentTerm(reply.Term)
						}
						if reply.VoteGranted {
							fmt.Printf("- Vote Granted: candidate %v got vote from server %v in term %v time %v\n", rf.me, i, cf, time.Now().UnixMilli())
							atomic.AddInt32(&rf.votes, 1)
						}
					}
				}(i)
			}
		}
		fmt.Printf("- before WG: start check the final result for possibile candidate %v in term max(%v or %v) time %v\n", rf.me, cf, rf.CurrentTerm(), time.Now().UnixMilli())

		time.Sleep(time.Duration(50) * time.Millisecond)

		result := int(atomic.LoadInt32(&rf.votes))
		fmt.Printf("- start check the final result for possibile candidate %v in term max(%v or %v) time %v\n", rf.me, cf, rf.CurrentTerm(), time.Now().UnixMilli())
		if rf.Role() == Candidate && rf.CurrentTerm() == cf {
			if result > len(rf.peers)/2 {
				fmt.Printf("- ELECTED AS LEADER: %v become leader with vote %v in term %v time %v\n", rf.me, result, cf, time.Now().UnixMilli())
				rf.SetRole(Leader)
			} else {
				fmt.Printf("- NOT ELECTED AS LEADER: %v is still candidate with vote %v in term %v time %v\n", rf.me, result, cf, time.Now().UnixMilli())
			}
		} else {
			fmt.Printf("- Candidate %v convert to follower in term %v time %v\n", rf.me, rf.CurrentTerm(), time.Now().UnixMilli())
		}
	}
}

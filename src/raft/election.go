package raft

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	ElectionTimeout = 400 + (rand.Int63() % 500)
)

const (
	WaitForVoteFinishedBreak = 50
	DelayToSendHeartbeat     = 100
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
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
	currentTerm := rf.CurrentTerm()

	if args.Term < currentTerm {
		// fmt.Printf("- candidate %v term is outdated, term should be %v, me is %v, time: %v\n", args.CandidateId, currentTerm, rf.me, Timestamp())
		reply.Term = currentTerm
		return
	} else if args.Term > currentTerm {
		fmt.Printf("-**Term changed** Got vote request from candidate %v, candidate term %v, me: %v, my term: %v, time: %v\n", args.CandidateId, args.Term, rf.me, currentTerm, Timestamp())
		rf.SetRole(Follower)
		rf.SetCurrentTerm(args.Term)
		rf.Vote(-1)
	}

	// check if candidate's log is more up-to-date
	logLen := rf.LogLen()
	isUpToDate := true
	if logLen > 0 {
		lastEntry := rf.Log(logLen - 1)
		isUpToDate = args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIdx >= logLen-1)
	}
	if votedFor := rf.VotedFor(); (votedFor == -1 || votedFor == args.CandidateId) && isUpToDate {
		// fmt.Printf("before voting: server %v's vote for %v \n", rf.me, rf.VotedFor())
		rf.Vote(args.CandidateId)
		reply.VoteGranted = true
		rf.SetLastContact(time.Now())
		fmt.Printf("after voting: %v grant vote to candidate %v in term %v, ***reset election timer***, time %v\n", rf.me, rf.VotedFor(), rf.CurrentTerm(), Timestamp())
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
	if rf.Role() == Candidate {
		ct := rf.CurrentTerm()
		fmt.Printf("**Term changed** server %v as candidate, increment term %v to %v time %v ***reset election timer*** \n", rf.me, ct, ct+1, Timestamp())
		rf.SetCurrentTerm(ct + 1)
		rf.Vote(rf.me)                // vote for itself
		rf.SetLastContact(time.Now()) // reset election timer
		atomic.StoreInt32(&rf.votes, 1)
		ct = rf.CurrentTerm()
		for i := range rf.peers {
			if rf.Role() == Candidate && i != rf.me {
				go func(i int) {
					lastLogIdx := -1
					lastLogTerm := 0
					if ll := rf.LogLen(); ll > 0 {
						lastLogIdx = ll - 1
						lastLogTerm = rf.Log(lastLogIdx).Term
					}

					args := &RequestVoteArgs{
						Term:        ct,
						CandidateId: rf.me,
						LastLogIdx:  lastLogIdx,
						LastLogTerm: lastLogTerm,
					}
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(i, args, reply)
					newCurrentTerm := rf.CurrentTerm()
					if newCurrentTerm != args.Term {
						return
					}
					if ok {
						if reply.Term > newCurrentTerm {
							rf.SetRole(Follower)
							fmt.Printf("**Term changed** Candidate convert to follower, me %v my term %v, receiver %v, receiver term %v, time %v\n", rf.me, newCurrentTerm, i, reply.Term, Timestamp())
							rf.SetCurrentTerm(reply.Term)
							return
						}
						if reply.VoteGranted {
							atomic.AddInt32(&rf.votes, 1)
						}
					}
				}(i)
			}
		}

		time.Sleep(time.Duration(WaitForVoteFinishedBreak) * time.Millisecond)
		// calculate election result
		result := int(atomic.LoadInt32(&rf.votes))
		if rf.Role() == Candidate && rf.CurrentTerm() == ct {
			if result > len(rf.peers)/2 {
				fmt.Printf("- ### ELECTED AS LEADER: %v become leader with vote %v in term %v my log len %v time %v\n", rf.me, result, ct, rf.LogLen(), Timestamp())
				rf.SetRole(Leader)
				rf.initLeaderState()
				time.Sleep(75 * time.Millisecond)
				go rf.reachAgreement()
			}
		}
	}
}

package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

const (
	HeartBeatInterval = 150
)

var (
	ElectionTimeout = 400 + (rand.Int63() % 400)
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	role        Role
	lastContact time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	votes       int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.CurrentTerm()
	var isleader bool = rf.Role() == Leader
	fmt.Printf("GetState: I am leader ? %v, me %v in term %v time %v\n", isleader, rf.me, term, time.Now().UnixMilli())
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		timeout := time.Duration(ElectionTimeout) * time.Millisecond

		for {
			elapsedTime := time.Since(rf.LastContact())
			if elapsedTime > timeout && rf.Role() != Leader {
				cf := rf.CurrentTerm()
				fmt.Printf("- START ELECTION: election timeout in term %v, %v start election, time %v\n", cf, rf.me, time.Now().UnixMilli())
				rf.SetRole(Candidate)
				go rf.startElection()
				break
			}
			time.Sleep(time.Duration(30) * time.Millisecond)
		}
	}
}

func (rf *Raft) startElection() {
	if !rf.killed() && rf.Role() == Candidate {
		rf.SetCurrentTerm(rf.CurrentTerm() + 1)
		rf.Vote(rf.me)
		rf.SetLastContact(time.Now())
		atomic.StoreInt32(&rf.votes, 0)
		atomic.AddInt32(&rf.votes, 1)
		// var wg sync.WaitGroup
		cf := rf.CurrentTerm()
		for i := range rf.peers {
			if rf.Role() == Candidate && i != rf.me {
				// wg.Add(1)
				go func(i int) {
					// defer wg.Done()
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
		// wg.Wait()

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

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		cf := rf.CurrentTerm()
		for i := range rf.peers {
			if rf.Role() == Leader {
				go func(i int) {
					fmt.Printf("- Initial HB: %v send hb to %v in term %v time %v\n", rf.me, i, cf, time.Now().UnixMilli())
					args := &AppendEntriesArgs{
						Term: cf,
					}
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)
					newcf := rf.CurrentTerm()
					if newcf != args.Term {
						return
					}
					if ok {
						if reply.Term > newcf {
							rf.SetRole(Follower)
							rf.SetCurrentTerm(reply.Term)
							fmt.Printf("- LEADER STEP DOWN: %v step down in term %v time %v\n", rf.me, reply.Term, time.Now().UnixMilli())
						}
					}
				}(i)
				time.Sleep(HeartBeatInterval * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) Role() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) SetRole(role Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.lastContact = time.Now()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// go rf.startElection()

	go rf.sendHeartbeat()

	return rf
}

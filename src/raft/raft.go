package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
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
const (
	HeartBeatInterval = 100 * time.Millisecond
	CheckInterval     = 10 * time.Millisecond
)

var (
	Ticker = 50 + rand.Int63()%300
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

	log         []Entry
	commitIndex int
	lastApplied int

	applych chan ApplyMsg

	nextIndex  []int
	matchIndex []int
	seen       map[interface{}]int
}

type Entry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		time.Sleep(time.Duration(Ticker) * time.Millisecond)
		fmt.Printf("### %v reset election timeout time %v\n", rf.me, Timestamp())
		timeout := time.Duration(ElectionTimeout) * time.Millisecond

		for {
			rf.mu.Lock()
			elapsed := time.Since(rf.lastContact)
			role := rf.role
			rf.mu.Unlock()
			if elapsed > timeout && role != Leader {
				// fmt.Printf("start election, me %v, my role %v term %v time %v\n", rf.me, rf.Role(), rf.CurrentTerm(), Timestamp())
				// rf.SetRole(Candidate)
				rf.mu.Lock()
				rf.role = Candidate
				rf.mu.Unlock()
				// fmt.Printf("### %v start election, elapsed time %v, timeout %v , time %v\n", rf.me, time.Since(rf.LastContact()).Milliseconds(), timeout, Timestamp())
				go rf.startElection()
				break
			}
			time.Sleep(CheckInterval)
		}
	}
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

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		votedFor:    -1,
		lastContact: time.Now(),
		applych:     applyCh,
		log:         make([]Entry, 0),
		seen:        make(map[interface{}]int),
		lastApplied: -1,
		commitIndex: -1,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	// only leader run the routines

	go rf.checkCommitIndex()

	go rf.updateSeen()

	return rf
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyIndex := rf.lastApplied + 1
			e := rf.log[rf.lastApplied]
			am := ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: applyIndex,
			}
			fmt.Printf("server %v apply msg, command %v index %v term %v\n", rf.me, am.Command, am.CommandIndex, rf.currentTerm)
			rf.mu.Unlock()
			rf.applych <- am
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(CheckInterval)
	}
}

func (rf *Raft) checkCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		ll := len(rf.log)
		rf.mu.Unlock()
		if rf.isLeaderReady() && ll > 0 {
			rf.mu.Lock()
			var idx int
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				count := 0
				for p := range rf.peers {
					if rf.matchIndex[p] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					idx = i
					break
				}
			}

			if rf.log[idx].Term == rf.currentTerm && idx > rf.commitIndex {
				fmt.Printf("- leader (me %v) increase commit index to %v in term %v time %v\n", rf.me, idx, rf.currentTerm, Timestamp())
				rf.commitIndex = idx
			}
			rf.mu.Unlock()
			time.Sleep(CheckInterval)
		}
	}
}

func (rf *Raft) updateSeen() {
	for !rf.killed() {
		rf.mu.Lock()
		for i, e := range rf.log {
			if idx, ok := rf.seen[e.Command]; !ok || idx != i {
				rf.seen[e.Command] = i
			}
		}

		for k, v := range rf.seen {
			if v < 0 || v >= len(rf.log) || rf.log[v].Command != k {
				delete(rf.seen, k)
			}
		}

		rf.mu.Unlock()
		time.Sleep(CheckInterval)
	}
}

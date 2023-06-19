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
	HeartBeatInterval = 150
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

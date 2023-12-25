package raft

//
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
	"log"
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

const (
	Follower = iota
	Candidate
	Leader
)

type entry struct {
	command int
	term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A

	currentTerm int
	votedFor    int
	log         []entry
	state       int
	timeout     time.Duration
	lastTime    time.Time
	r           rand.Rand
	voteCount   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

type up_to_date struct {
	Term         int
	LastLogIndex int
}

func equal_or_greater(a up_to_date, b up_to_date) bool {

	if a.LastLogIndex != 0 || b.LastLogIndex != 0 {
		log.Fatalf("impossible")
	}

	if a.Term != b.Term {
		return a.Term >= b.Term
	} else {
		return a.LastLogIndex >= b.LastLogIndex
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(candidate *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurrentTerm = rf.currentTerm

	if candidate.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < candidate.Term {
		rf.currentTerm = candidate.Term
		rf.convert_to_follower_and_clear_votedFor()
	}
	if rf.votedFor == -1 || rf.votedFor == candidate.CandidateId {
		if (equal_or_greater(up_to_date{candidate.Term, candidate.LastLogIndex}, up_to_date{rf.currentTerm, len(rf.log)})) {

			rf.show_peer_state()

			rf.convert_to_follower_and_clear_votedFor()
			rf.votedFor = candidate.CandidateId
			reply.VoteGranted = true
			rf.refresh_timeout()
		} else {
			reply.VoteGranted = false
		}
	}

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
func (rf_candidate *Raft) sendRequestVote(server int, candidate *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf_candidate.peers[server].Call("Raft.RequestVote", candidate, reply)

	if !ok {
		return
	}

	rf_candidate.mu.Lock()
	defer rf_candidate.mu.Unlock()

	if rf_candidate.state != Candidate || rf_candidate.currentTerm != candidate.Term {
		// log.Printf("Request vote is stale. rf_candidate.id = %v, rf_candidate.currentTerm = %v, candidate.Term = %v\n", rf_candidate.me, rf_candidate.currentTerm, candidate.Term)
		return
	}

	rf_candidate.show_peer_send_request_vote()

	// log.Printf("Request vote: rf_candidate.currentTerm = %v, reply.CurrentTerm = %v reply.VoteGranted = %v\n", rf_candidate.currentTerm, reply.CurrentTerm, reply.VoteGranted)
	if rf_candidate.currentTerm < reply.CurrentTerm {
		rf_candidate.currentTerm = reply.CurrentTerm
		rf_candidate.convert_to_follower_and_clear_votedFor()
		return
	}

	if reply.VoteGranted == true {

		rf_candidate.show_server_votes_for_candidate(server)

		rf_candidate.voteCount++
		if rf_candidate.voteCount > len(rf_candidate.peers)/2 {
			rf_candidate.show_peer_becomes_leader()
			rf_candidate.state = Leader

			HeartBeat := func(rf *Raft) {
				for rf.killed() == false {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					rf.show_leader_state()
					leader := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}

					rf.mu.Unlock()

					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						reply := AppendEntriesReply{}
						go rf.sendAppendEntries(i, &leader, &reply)
					}

					time.Sleep(time.Duration(100) * time.Millisecond)
				}
			}

			go HeartBeat(rf_candidate)
		}
	}

	// rf_candidate.debug()

}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []int
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(leader *AppendEntriesArgs, reply *AppendEntriesReply) {

	// TODO: problem may be potential: RPC is not from current leader

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if leader.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.currentTerm = leader.Term
	rf.convert_to_follower_and_clear_votedFor()
	rf.votedFor = leader.LeaderId

	rf.refresh_timeout()

	// 5.3

	// HeartBeat
	if len(leader.Entries) == 0 {
		reply.Success = true
		return
	}

}

func (rf *Raft) sendAppendEntries(server int, leader *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", leader, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("Peer %v send AppendEntries to Peer %v, leader's term = %v, server's term = %v leader.Term = %v\n", rf.me, server, rf.currentTerm, reply.Term, leader.Term)

	if rf.state != Leader || rf.currentTerm != leader.Term {
		return
	}

	rf.show_send_append_entries(server, reply.Term)

	// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convert_to_follower_and_clear_votedFor()
		return
	}

	rf.show_peer_recognizes_the_leader_as_legitimate(server)

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// Follower or Candidate
		if rf.state != Leader && rf.timeout_elapse() {
			// start election
			rf.show_peer_timeout_elapse()

			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.timeout = rf.get_election_timeout()
			rf.refresh_timeout()

			rf.show_peer_begins_election()

			candidate := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}

			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				reply := RequestVoteReply{}
				go rf.sendRequestVote(i, &candidate, &reply)
			}
		} else {
			rf.mu.Unlock()
		}

		// periodly heartbeat
		time.Sleep(time.Duration(10) * time.Millisecond)

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).

	rf.r = *rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)))
	rf.state = Follower
	rf.timeout = rf.get_election_timeout()
	rf.refresh_timeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// temporarily set 250-550ms
func (rf *Raft) get_election_timeout() time.Duration {
	return time.Duration(250+rf.r.Intn(300)) * time.Millisecond
}

func (rf *Raft) refresh_timeout() {
	rf.lastTime = time.Now()
}

func (rf *Raft) timeout_elapse() bool {
	return time.Since(rf.lastTime) > rf.timeout
}

func (rf *Raft) convert_to_follower_and_clear_votedFor() {
	rf.state = Follower
	rf.votedFor = -1
	rf.voteCount = 0
	// raft peer shouldn't refresh timeout here, which may cause available problem.
}

// debug

func get_state(state int) string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Impossible"
}

func (rf *Raft) show_peer_state() {
	log.Printf("Peer %v: state = %s Term = %v votedFor = %v voteCount = %v \n", rf.me, get_state(rf.state), rf.currentTerm, rf.votedFor, rf.voteCount)
}

func (rf *Raft) show_peer_timeout_elapse() {
	log.Printf("Peer %v timeout elapses: state = %s Term = %v \n", rf.me, get_state(rf.state), rf.currentTerm)
}

func (rf *Raft) show_leader_state() {
	log.Printf("Peer %v is leader: Term = %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) show_server_votes_for_candidate(server int) {
	log.Printf("Peer %v votes for Peer %v, Term = %v, voteCount = %v\n", server, rf.me, rf.currentTerm, rf.voteCount)
}

func (rf *Raft) show_peer_becomes_leader() {
	log.Printf("Peer %v becomes leader, Term = %v, voteCount = %v, count of peers = %v\n", rf.me, rf.currentTerm, rf.voteCount, len(rf.peers))
}

func (rf *Raft) show_peer_send_request_vote() {
	log.Printf("Peer %v is sending request vote, Term = %v, state = %v\n", rf.me, rf.currentTerm, get_state(rf.state))
}

func (rf *Raft) show_peer_begins_election() {
	log.Printf("Peer %v begins election, Term = %v timeout = %v\n", rf.me, rf.currentTerm, rf.timeout)
}

func (rf *Raft) show_peer_recognizes_the_leader_as_legitimate(server int) {
	log.Printf("Peer %v recognizes the leader Peer %v as legitimate, Term = %v\n", server, rf.currentTerm, rf.currentTerm)
}

func (rf *Raft) show_send_append_entries(server int, server_term int) {
	log.Printf("Peer %v send AppendEntries to Peer %v, state = %v, leader's term = %v, server's term = %v\n", rf.me, server, get_state(rf.state), rf.currentTerm, server_term)
}

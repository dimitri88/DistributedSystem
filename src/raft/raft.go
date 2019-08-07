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
	"io/ioutil"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

//Commenting
type State int

// import "bytes"
// import "labgob"
const (
	Follower  State = 1
	Candidate State = 2
	Leader    State = 3
	max             = 500
	min             = 300
)

// Log Entry
type Entry struct {
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex                     // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartBeat   bool
	apllyCh     chan ApplyMsg
	currIndex   int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       State   //1: Follower, 2:Candidate, 3: Leader
	currentTerm int     //Need to be persisted
	votedFor    int     //Need to be persisted
	log         []Entry //Need to be persisted
	voteCounter int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}
	if rf.votedFor != -1 && (args.Term == rf.currentTerm && rf.votedFor != args.CandidateID) {
		return
	}

	if args.LastLogTerm < rf.log[rf.currIndex].Term {
		return
	} else if args.LastLogTerm == rf.log[rf.currIndex].Term && args.LastLogIndex < rf.currIndex {
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	log.Printf("%v has voted for %v at Term:%v, Total Servers: %v \n", rf.me, args.CandidateID, rf.currentTerm, len(rf.peers))

	return
}

//AppendEntriesArgs Comment
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

//AppendEntriesReply  example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

//AppendEntries Appending
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("trying to grab lock for %v", rf.me)
	rf.Lock()
	log.Printf("lock grabbed for %v", rf.me)
	log.Printf("###################### %v", len(args.Entries))
	defer rf.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if len(args.Entries) == 0 {
		log.Printf("%v has set headbeat to true", rf.me)
		rf.heartBeat = true
		if rf.state == Candidate {
			rf.state = Follower
		}
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) toFollower(term int) {
	// Your code here, if desired.
	rf.currentTerm = term
	rf.state = Follower
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(ioutil.Discard)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.apllyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.currentTerm = 0
	rf.currIndex = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{
		Term:    0,
		Command: nil,
	}
	rf.state = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	timeOut := rand.Intn(max-min) + min
	rf.heartBeat = false
	go func() {
		for {
			time.Sleep(time.Duration(timeOut) * time.Millisecond)
			rf.Lock()
			hb := rf.heartBeat
			state := rf.state
			rf.Unlock()
			if !hb && state == Follower {
				log.Printf("%v has become the candidate", rf.me)
				//do stuff, shoud start election
				rf.Lock()
				rf.currentTerm++
				rf.state = Candidate
				rf.votedFor = rf.me
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  me,
					LastLogIndex: rf.currIndex,
					LastLogTerm:  rf.log[rf.currIndex].Term,
				}
				rf.voteCounter = 0
				for i := range peers {
					if i != me {
						go func(i int, args RequestVoteArgs) {
							var res RequestVoteReply
							ok := rf.sendRequestVote(i, &args, &res)
							if ok {
								rf.Lock()
								if rf.currentTerm == args.Term {
									if res.VoteGranted {
										rf.voteCounter++
										if rf.voteCounter > len(rf.peers)/2 {
											rf.state = Leader
											log.Printf("%v has become the leader", rf.me)
											rf.voteCounter = 0
											// rf.Lock()
											// args := AppendEntriesArgs{
											// 	Term:     rf.currentTerm,
											// 	LeaderID: me,
											// 	Entries:  make([]Entry, 0),
											// }
											// for i := range rf.peers {
											// 	log.Printf("looping %v to send results", rf.me)
											// 	if i != me {
											// 		go func(i int, args AppendEntriesArgs) {
											// 			var res AppendEntriesReply
											// 			log.Printf("%v has sent out the heartbeat as leader", rf.me)
											// 			ok := rf.sendAppendEntries(i, &args, &res)
											// 			if ok {
											// 				rf.Lock()
											// 				if args.Term == rf.currentTerm {
											// 					if !res.Success && args.Term < res.Term {
											// 						rf.toFollower(res.Term)
											// 					}
											// 				}
											// 				rf.Unlock()
											// 			}
											// 		}(i, args)
											// 	}
											// }
											// rf.Unlock()
										}
									} else if res.Term > rf.currentTerm {
										rf.toFollower(res.Term)
									}
								}
								rf.Unlock()
							}
						}(i, args)
					}
				}
				rf.Unlock()
			} else {
				rf.Lock()
				rf.heartBeat = false
				rf.Unlock()
			}

		}

	}()

	go func() {
		for {
			if rf.state == Leader {
				rf.Lock()
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
					Entries:  make([]Entry, 0),
				}
				for i := range rf.peers {
					if i != me {
						go func(i int, args AppendEntriesArgs) {
							var res AppendEntriesReply
							log.Printf("%v has sent out the heartbeat as leader", rf.me)
							ok := rf.sendAppendEntries(i, &args, &res)
							if ok {
								rf.Lock()
								if args.Term == rf.currentTerm {
									if !res.Success && args.Term < res.Term {
										rf.toFollower(res.Term)
									}
								}
								rf.Unlock()

							}
						}(i, args)
					}
				}
				rf.Unlock()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return rf
}

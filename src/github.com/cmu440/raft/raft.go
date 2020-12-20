//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

type Status int

const (
	LEADER Status = iota
	FOLLOWER
	CANDIDATE
)

type PersistentState struct {
	currentTerm int
	votedFor    int
	log         []LogEntry
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type VolatileStateOnAllServer struct {
	commitIndex int
	lastApplied int
}

func (rf *Raft) initVolatileStateOnAllServer() {
	rf.volatileStateOnAllServer.commitIndex = 0
	rf.volatileStateOnAllServer.lastApplied = 0
}

type VolatileStateOnLeader struct {
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) initVolatileStateOnLeader() {
	if rf.getStatus() != LEADER {
		rf.logger.Fatal("not leader call the initVolatileStateOnLeader, me is ", rf.me)
	}

	lastLogIndex := rf.getLastLogIndex()

	rf.volatileStateOnLeader.nextIndex = make([]int, len(rf.peers))
	for index := range rf.volatileStateOnLeader.nextIndex {
		rf.volatileStateOnLeader.nextIndex[index] = lastLogIndex + 1
	}

	rf.volatileStateOnLeader.matchIndex = make([]int, len(rf.peers))
	for index := range rf.volatileStateOnLeader.matchIndex {
		rf.volatileStateOnLeader.matchIndex[index] = 0
	}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (3A, 3B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	applyCh     chan ApplyCommand
	status      Status
	heartBeatCh chan bool
	// election timeout in milli second
	electionTimeoutMilli int
	// heartbeat interval in milli second
	heartBeatIntervalMilli int
	statusCh               chan Status
	stepDownCh             chan int
	startAgreementCh       chan int

	persistentState          PersistentState
	volatileStateOnAllServer VolatileStateOnAllServer
	volatileStateOnLeader    VolatileStateOnLeader
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	me := rf.me
	term := rf.getCurrentTerm()
	isLeader := rf.getStatus() == LEADER
	// Your code here (3A)
	return me, term, isLeader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B)
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B)
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (3A)
	Term        int
	VoteGranted int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B)
	currentTerm := rf.getCurrentTerm()
	if args.Term > currentTerm && (rf.getStatus() == LEADER || rf.getStatus() == CANDIDATE) {
		rf.persistentState.currentTerm = args.Term
		rf.logger.Println("RequestVote step down")
		rf.stepDownCh <- args.Term
		currentTerm = rf.getCurrentTerm()
	}

	if rf.getStatus() == FOLLOWER {
		// only follower need heartbeat to refresh the election timeout
		rf.heartBeatCh <- true
	}
	if args.Term == currentTerm {
		// vote for one candidate within one single term
		reply.VoteGranted = rf.persistentState.votedFor
		reply.Term = currentTerm
		rf.logger.Println("vote for ", reply.VoteGranted)
		return
	}
	if args.Term > currentTerm {
		lastLogTerm := rf.getLastLogTerm()
		lastLogIndex := rf.getLastLogIndex()

		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// ensure the candidate's log is as complete as this server
			reply.Term = currentTerm
			reply.VoteGranted = args.CandidateId
			rf.persistentState.votedFor = args.CandidateId
			rf.setCurrentTerm(args.Term)
			rf.logger.Println("vote for ", reply.VoteGranted)
			return
		}
	}
	// args.Term < currentTerm and other not vote cases,
	// e.g. candidate's log is not as completed as this server
	reply.Term = currentTerm
	reply.VoteGranted = -1
	rf.logger.Println("not vote for ", args.CandidateId)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B)
	currentTerm := rf.getCurrentTerm()
	if args.Term > currentTerm {
		rf.persistentState.currentTerm = args.Term
		if rf.getStatus() == LEADER {
			rf.logger.Println("AppendEntries step down")
			rf.stepDownCh <- args.Term
			currentTerm = rf.getCurrentTerm()
		}
	}

	if rf.getStatus() == CANDIDATE {
		rf.persistentState.currentTerm = args.Term
		rf.logger.Println("AppendEntries step down")
		rf.stepDownCh <- args.Term
		currentTerm = rf.getCurrentTerm()
	}

	if rf.getStatus() == FOLLOWER {
		// only follower need heartbeat to refresh the election timeout
		rf.heartBeatCh <- true
	}

	rf.mux.Lock()
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// even if the Entries' length is 0 (heartbeat)
	if args.LeaderCommit > rf.volatileStateOnAllServer.commitIndex {
		rf.volatileStateOnAllServer.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.persistentState.log)-1)))
	}
	rf.mux.Unlock()
	go rf.doCommit()

	if len(args.Entries) == 0 {
		reply.Term = currentTerm
		reply.Success = true
		return
	}

	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex != -1 {
		rf.mux.Lock()
		// if log does not contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		if len(rf.persistentState.log) <= args.PrevLogIndex ||
			(len(rf.persistentState.log) > args.PrevLogIndex && rf.persistentState.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.Term = currentTerm
			reply.Success = false
			defer rf.mux.Unlock()
			return
		}
		rf.mux.Unlock()
	}

	rf.mux.Lock()

	i := args.PrevLogIndex + 1
	j := 0
	for i < len(rf.persistentState.log) && j < len(args.Entries) {
		// If an existing entry conflicts with a new one (same index but different terms)
		if rf.persistentState.log[i].Term != args.Entries[j].Term {
			// delete the existing entry and all that follow it
			rf.persistentState.log = rf.persistentState.log[:i]
			break
		}
		i++
		j++
	}

	// Append any new entries not already in the log
	if j < len(args.Entries) {
		rf.persistentState.log = append(rf.persistentState.log, args.Entries[j:]...)
	}

	rf.mux.Unlock()

	reply.Term = currentTerm
	reply.Success = true
	return
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteResult chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.Term > rf.getCurrentTerm() {
			// step down
			rf.logger.Println("sendRequestVote step down server ", server, " has higher term ", reply.Term)
			rf.stepDownCh <- reply.Term
			voteResult <- false
			return
		}
		if reply.VoteGranted == rf.me {
			// get a vote
			rf.logger.Println("server ", rf.me, " received vote term", args.Term)
			voteResult <- true
			return
		}
	}
	voteResult <- false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) != 0 {
		rf.logger.Println("sendAppendEntries ", args.Entries, " to server ", server)
	}
	for {
		// retry indefinitely
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			if reply.Term > rf.getCurrentTerm() {
				rf.logger.Println("sendAppendEntries step down server ", server, " has higher term ", reply.Term)
				// step down
				rf.stepDownCh <- reply.Term
				// return true to finish, otherwise would decrease the nextIndex and retry
				return true
			}
			return reply.Success
		}
	}
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.getStatus() == LEADER
	if !isLeader {
		return index, term, isLeader
	}

	rf.logger.Println("PutCommand ", command)

	term = rf.getCurrentTerm()

	rf.mux.Lock()
	index = len(rf.persistentState.log)
	newLog := LogEntry{Index: index, Term: term, Command: command}
	rf.persistentState.log = append(rf.persistentState.log, newLog)
	rf.volatileStateOnLeader.matchIndex[rf.me] = len(rf.persistentState.log) - 1
	rf.volatileStateOnLeader.nextIndex[rf.me] = len(rf.persistentState.log)
	//rf.logger.Println("update matchIndex at server", rf.me, " to value", len(rf.persistentState.log) - 1)
	rf.mux.Unlock()

	// start the agreement
	rf.startAgreementCh <- index
	// TODO: check return immediately

	return index, term, isLeader
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	// Your code here, if desired
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
//
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (3A, 3B)
	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan bool)
	// election time out range [1s, 2s)
	rf.electionTimeoutMilli = randInt(1000, 2000)
	// heartbeat interval range [0.1s, 0.2s)
	rf.heartBeatIntervalMilli = randInt(100, 200)
	// log index begin at 1
	rf.statusCh = make(chan Status, 1)
	rf.stepDownCh = make(chan int)
	rf.startAgreementCh = make(chan int)
	rf.persistentState.currentTerm = 0
	rf.persistentState.votedFor = -1
	rf.persistentState.log = make([]LogEntry, 1)
	rf.initVolatileStateOnAllServer()
	// original, server is follower
	rf.status = FOLLOWER
	rf.statusCh <- FOLLOWER

	go rf.handleStatus()

	return rf
}

func (rf *Raft) handleStatus() {
	for {
		switch <-rf.statusCh {
		case FOLLOWER:
			rf.doFollower()
		case LEADER:
			rf.doLeader()
		case CANDIDATE:
			rf.doCandidate()
		}
	}
}

func (rf *Raft) doCommit() {
	rf.mux.Lock()
	// If commitIndex > lastApplied
	for rf.volatileStateOnAllServer.commitIndex > rf.volatileStateOnAllServer.lastApplied {
		//  increment lastApplied, apply one command to state machine
		rf.volatileStateOnAllServer.lastApplied++
		rf.logger.Println("server ", rf.me, " last applied ", rf.volatileStateOnAllServer.lastApplied)
		logEntry := rf.persistentState.log[rf.volatileStateOnAllServer.lastApplied]
		rf.applyCh <- ApplyCommand{Index: logEntry.Index, Command: logEntry.Command}
	}
	rf.mux.Unlock()
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func (rf *Raft) doFollower() {
	// followers check if there is heartbeats from the leader within election timeout
	// otherwise became candidate
	rf.setStatus(FOLLOWER)
	rf.clearStepDownCh()
	for {
		select {
		case <-rf.heartBeatCh:
			//rf.logger.Println("receive heartbeat")
		case <-time.After(time.Duration(rf.electionTimeoutMilli) * time.Millisecond):
			// election timeout, became candidate
			rf.statusCh <- CANDIDATE
			rf.logger.Println("election timeout, became candidate")
			return
		}
	}
}
func (rf *Raft) clearStepDownCh() {
	for {
		select {
		case <-rf.stepDownCh:
		default:
			return
		}
	}
}

func (rf *Raft) doLeader() {
	rf.status = LEADER
	rf.initVolatileStateOnLeader()

	stopHeartBeats := make(chan bool)
	stopStartAgreement := make(chan bool)
	// send heartbeat
	go rf.sendHeartBeats(stopHeartBeats)
	go rf.handleStartAgreement(stopStartAgreement)

	for {
		select {
		case term := <-rf.stepDownCh:
			if term > rf.getCurrentTerm() && rf.getStatus() == LEADER {
				// step down
				rf.logger.Println("step down")
				stopHeartBeats <- true
				stopStartAgreement <- true
				rf.setStatus(FOLLOWER)
				rf.statusCh <- FOLLOWER
				return
			}
		}
	}
}

func (rf *Raft) sendHeartBeats(stop chan bool) {
	for {
		select {
		case <-time.After(time.Duration(rf.heartBeatIntervalMilli) * time.Millisecond):
			// send heartbeat after a time interval
			args := AppendEntriesArgs{}
			args.Term = rf.getCurrentTerm()
			args.LeaderId = rf.me
			//args.Entries = make([]LogEntry, 0)
			args.LeaderCommit = rf.volatileStateOnAllServer.commitIndex
			args.PrevLogIndex = -1
			args.PrevLogTerm = -1
			args.Entries = make([]LogEntry, 0)
			for index := range rf.peers {
				if index != rf.me {
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(index, &args, &reply)
				}
			}
		case <-stop:
			return
		}
	}
}

func (rf *Raft) handleStartAgreement(stop chan bool) {
	for {
		select {
		case <-rf.startAgreementCh:
			// check if last log index >= nextIndex for a follower
			rf.logger.Println("start agreement")
			lastLogIndex := rf.getLastLogIndex()
			for server, nextIndex := range rf.volatileStateOnLeader.nextIndex {
				if server == rf.me {
					continue
				}
				if lastLogIndex >= nextIndex {
					// send AppendEntries RPC and try indefinitely
					go rf.appendEntriesToOneServer(server)
				}
			}
		case <-stop:
			return
		}
	}
}

func (rf *Raft) appendEntriesToOneServer(server int) {
	lastLogIndex := rf.getLastLogIndex()

	currentTerm := rf.getCurrentTerm()
	endIndex := len(rf.persistentState.log)

	rf.mux.Lock()

	leaderId := rf.me
	nextIndex := rf.volatileStateOnLeader.nextIndex[server]
	prevLogIndex := nextIndex - 1

	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.persistentState.log[prevLogIndex].Term
	}

	entries := rf.persistentState.log[nextIndex:endIndex]

	LeaderCommit := rf.volatileStateOnAllServer.commitIndex

	rf.mux.Unlock()

	args := AppendEntriesArgs{}
	args.Term = currentTerm
	args.Entries = entries
	args.PrevLogTerm = prevLogTerm
	args.PrevLogIndex = prevLogIndex
	args.LeaderId = leaderId
	args.LeaderCommit = LeaderCommit

	for lastLogIndex >= nextIndex {
		reply := AppendEntriesReply{}
		// retry indefinitely in the RPC sender
		shouldStop := rf.sendAppendEntries(server, &args, &reply)
		if shouldStop {
			rf.mux.Lock()
			// update the server's info
			// notice because of the network delay, the entries may be updated already
			if rf.volatileStateOnLeader.nextIndex[server] < endIndex {
				rf.volatileStateOnLeader.nextIndex[server] = endIndex
				rf.volatileStateOnLeader.matchIndex[server] = endIndex - 1
				rf.logger.Println("update matchIndex at server ", server, " to value ", endIndex-1)
			}
			rf.mux.Unlock()
			break
		}
		// decrease the nextIndex as well as other related argsf
		if args.PrevLogIndex == -1 {
			rf.logger.Fatal("last index should not be less than -1")
		}

		args.PrevLogIndex--
		rf.mux.Lock()
		if prevLogIndex >= 0 {
			args.PrevLogTerm = rf.persistentState.log[prevLogIndex].Term
		} else {
			args.PrevLogTerm = -1
		}
		args.Entries = rf.persistentState.log[args.PrevLogIndex+1 : endIndex]

		nextIndex = rf.volatileStateOnLeader.nextIndex[server]
		rf.mux.Unlock()
		lastLogIndex = rf.getLastLogIndex()
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	rf.mux.Lock()
	for server, N := range rf.volatileStateOnLeader.matchIndex {
		if server == rf.me {
			continue
		}
		if N > rf.volatileStateOnAllServer.commitIndex && rf.persistentState.log[N].Term == currentTerm {
			sum := 0
			for _, matchIndex := range rf.volatileStateOnLeader.matchIndex {
				if matchIndex >= N {
					sum++
				}
			}
			if sum > len(rf.peers)/2 {
				rf.volatileStateOnAllServer.commitIndex = N
				rf.logger.Println("update commitIndex to ", N)
			}
		}
	}

	rf.mux.Unlock()
	go rf.doCommit()
}

func (rf *Raft) doCandidate() {
	rf.setStatus(CANDIDATE)
	for {
		select {
		case term := <-rf.stepDownCh:
			if term >= rf.getCurrentTerm() && rf.getStatus() == CANDIDATE {
				// step down
				rf.logger.Println("step down")
				rf.setStatus(FOLLOWER)
				rf.statusCh <- FOLLOWER
				return
			}
		default:
			// vote for itself
			rf.persistentState.votedFor = rf.me
			// increase current term
			rf.setCurrentTerm(rf.getCurrentTerm() + 1)
			// start election
			voteResult := make(chan int)
			go rf.startElection(voteResult, rf.getCurrentTerm())
			select {
			case result := <-voteResult:
				if result == rf.getCurrentTerm() {
					// 􏰏􏰌􏰈􏰎􏰇􏰁􏰎􏰊􏰎􏰋􏰢􏰎􏰖􏰔􏰁􏰌􏰂􏰂􏰠􏲷􏰌􏰁􏰋􏰈􏰙􏰌􏰔􏰇􏰎􏰁􏰢􏰎􏰁􏰇􏱑􏰽􏰎􏰊􏰌􏰂􏰎􏰡􏰎􏰠􏰖􏰎􏰁􏰏􏰌􏰈􏰎􏰇􏰁􏰎􏰊􏰎􏰋􏰢􏰎􏰖􏰔􏰁􏰌􏰂􏰂􏰠􏲷􏰌􏰁􏰋􏰈􏰙􏰌􏰔􏰇􏰎􏰁􏰢􏰎􏰁􏰇􏱑􏰽􏰎􏰊􏰌􏰂􏰎􏰡􏰎􏰠􏰖􏰎􏰁􏱑􏰽􏰎􏰊􏰌􏰂􏰎􏰡􏰎􏰠􏰖􏰎􏰁received from majority of servers: become leader
					rf.setStatus(LEADER)
					rf.statusCh <- LEADER
					return
				} else {
					rf.logger.Println("currentTerm is ", rf.getCurrentTerm(), " while vote term is ", result)
					// don't receive vote from majority, wait for election timeout and retry
					<-time.After(time.Duration(rf.electionTimeoutMilli) * time.Millisecond)
				}
			case <-time.After(time.Duration(rf.electionTimeoutMilli) * time.Millisecond):
				// reset election timeout, start new election
			}
		}
	}
}

func (rf *Raft) startElection(voteResult chan int, currentTerm int) {
	rf.logger.Println("start election term", rf.getCurrentTerm())
	// prepare the args
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	args.Term = currentTerm

	accVoteResult := make(chan bool)
	for index := range rf.peers {
		reply := RequestVoteReply{}
		go rf.sendRequestVote(index, &args, &reply, accVoteResult)
	}

	voteSum := 0
	for range rf.peers {
		if <-accVoteResult {
			voteSum++
			if voteSum > len(rf.peers)/2 {
				rf.logger.Println("received vote ", voteSum, " term ", currentTerm)
				voteResult <- currentTerm
				return
			}
		}
	}
	rf.logger.Println("received vote ", voteSum, " term ", currentTerm)
	if voteSum <= len(rf.peers)/2 {
		voteResult <- -1
	}
}

func (rf *Raft) setStatus(status Status) {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	rf.logger.Println("set status to: ", toName(status))
	rf.status = status
}

func (rf *Raft) getStatus() Status {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	return rf.status
}

func (rf *Raft) setCurrentTerm(term int) {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	rf.persistentState.currentTerm = term
}

func (rf *Raft) getCurrentTerm() int {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	return rf.persistentState.currentTerm
}

func (rf *Raft) getLastLogIndex() int {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	lastLogIndex := 0
	if len(rf.persistentState.log) > 1 {
		lastLogIndex = rf.persistentState.log[len(rf.persistentState.log)-1].Index
	}
	return lastLogIndex
}

func (rf *Raft) getLastLogTerm() int {
	defer rf.mux.Unlock()
	rf.mux.Lock()
	lastLogTerm := -1
	if len(rf.persistentState.log) > 1 {
		lastLogTerm = rf.persistentState.log[len(rf.persistentState.log)-1].Term
	}
	return lastLogTerm
}

func toName(status Status) string {
	switch status {
	case LEADER:
		return "leader"
	case CANDIDATE:
		return "candidate"
	case FOLLOWER:
		return "follower"
	}
	return "no such role"
}

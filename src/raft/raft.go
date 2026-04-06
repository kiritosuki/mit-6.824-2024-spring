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
	"bytes"
	"fmt"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Leader             = 1
	Candidate          = 2
	Follower           = 3
	HeartBeatGap       = 125
	ElectionTimeoutMin = 400
	ElectionTimeoutMax = 600
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastHeartBeat time.Time
	applyCh       chan ApplyMsg

	replicatorCond []*sync.Cond // to signal replicator
	applierCond    *sync.Cond   // to signal applier

	stat int

	// persistent state on all servers
	currentTerm int
	voteFor     int
	log         []LogEntry // index 0 is dummy

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders (initialized after election)
	nextIndex  []int
	matchIndex []int

	// snapshot message
	msg *ApplyMsg
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// ensure locked when call this function
func (rf *Raft) toRealIndex(index int) int {
	return index - rf.log[0].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.stat == Leader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// ensure locked when call this function
func (rf *Raft) persist() {
	// Your code here (3C).
	persistState := rf.encodeState()
	snapshot := rf.persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) != 0 {
		rf.persister.Save(persistState, snapshot)
	} else {
		rf.persister.Save(persistState, nil)
	}
}

// restore previously persisted state.
// ensure locked when call this function
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("[error] can't read persist state")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.commitIndex = log[0].Index
		rf.lastApplied = log[0].Index
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if this snapshot from service is outdated, ignore
	if index <= rf.log[0].Index {
		return
	}
	rf.log = append([]LogEntry{}, rf.log[rf.toRealIndex(index):]...)
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if !rf.checkRequestTerm(args, reply) {
		return
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isUpToDate(args) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.lastHeartBeat = time.Now()
	}
}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastLog := rf.log[len(rf.log)-1]
	candidateIndex := args.LastLogIndex
	candidateTerm := args.LastLogTerm
	return candidateTerm > lastLog.Term || (candidateTerm == lastLog.Term && candidateIndex >= lastLog.Index)
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

func (rf *Raft) doRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !rf.checkResponseTerm(args, reply, true) {
		return
	}

	if !reply.VoteGranted {
		return
	}
	// If votes received from majority of servers: become leader
	if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers)/2) &&
		rf.stat == Candidate &&
		rf.currentTerm == args.Term {
		rf.stat = Leader
		lastLogIndex := rf.log[len(rf.log)-1].Index
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		// send initial empty AppendEntries RPCs (heartbeat) to each server immediately
		for i := range rf.peers {
			if i != rf.me {
				args2 := rf.prepareReplicationArgs(i)
				go rf.doReplicate(i, args2)
			}
		}
	}
}

func (rf *Raft) launchElection() {
	rf.currentTerm++
	rf.stat = Candidate
	rf.voteFor = rf.me
	rf.lastHeartBeat = time.Now()
	lastLog := rf.log[len(rf.log)-1]
	voteCount := int32(1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.doRequestVote(id, &args, &voteCount)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
	LogLen        int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stat != Leader {
		return -1, -1, false
	}
	lastIndex := rf.log[len(rf.log)-1].Index
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Index:   lastIndex + 1,
		Command: command,
	})
	// signal all replicators
	for i := range rf.peers {
		if i != rf.me {
			rf.replicatorCond[i].Signal()
		}
	}
	return lastIndex + 1, rf.currentTerm, true
}

// ensure locked when call this function
func (rf *Raft) prepareReplicationArgs(server int) interface{} {
	if rf.nextIndex[server] > rf.log[0].Index {
		realNextIndex := rf.toRealIndex(rf.nextIndex[server])
		entries := make([]LogEntry, len(rf.log[realNextIndex:]))
		copy(entries, rf.log[realNextIndex:])
		prevLogEntry := rf.log[realNextIndex-1]
		return AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogEntry.Index,
			PrevLogTerm:  prevLogEntry.Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
	} else {
		return InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log[0].Index,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
	}
}

func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.stat == Leader && rf.matchIndex[server] < rf.log[len(rf.log)-1].Index) {
			rf.mu.Unlock()
			rf.replicatorCond[server].Wait()
			rf.mu.Lock()
		}
		args := rf.prepareReplicationArgs(server)
		rf.mu.Unlock()
		rf.doReplicate(server, args)
	}
}

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applierCond.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[rf.toRealIndex(lastApplied+1):rf.toRealIndex(commitIndex+1)])
		if rf.msg != nil {
			msg := rf.msg
			rf.msg = nil
			rf.mu.Unlock()
			rf.applyCh <- *msg
		} else {
			rf.mu.Unlock()
		}
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) doReplicate(server int, args interface{}) {
	switch v := args.(type) {
	case AppendEntriesArgs:
		rf.doAppendEntries(server, &v)
	case InstallSnapshotArgs:
		rf.doInstallSnapshot(server, &v)
	default:
		panic("doReplicate() args type not match!")
	}
}

func (rf *Raft) doAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !rf.checkResponseTerm(args, reply, false) {
		return
	}

	// if success, update nextIndex and matchIndex
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		// if there is an N, N > commitIndex && majority matchIndex[i] >= N && log[N].Term == currentTerm
		// set commitIndex = N
		for n := rf.log[len(rf.log)-1].Index; n > rf.commitIndex && n >= rf.log[0].Index; n-- {
			index := n
			if rf.log[rf.toRealIndex(index)].Term != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= index {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[rf.toRealIndex(index)].Term == rf.currentTerm {
				rf.commitIndex = index
				break
			}
		}
	} else {
		// if fail, fast rollback the nextIndex
		if reply.ConflictTerm == -2 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.LogLen
		} else {
			lastIndexOfTerm := -1
			for i := rf.log[len(rf.log)-1].Index; i >= rf.log[0].Index; i-- {
				if rf.log[rf.toRealIndex(i)].Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
				if rf.log[rf.toRealIndex(i)].Term < reply.ConflictTerm {
					break
				}
			}
			if lastIndexOfTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = lastIndexOfTerm + 1
			}
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
	rf.applierCond.Signal()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !rf.checkRequestTerm(args, reply) {
		return
	}

	if rf.stat == Candidate {
		rf.stat = Follower
	}

	rf.lastHeartBeat = time.Now()

	realPrevIndex := rf.toRealIndex(args.PrevLogIndex)
	// realPrevIndex is invalid
	if realPrevIndex < 0 {
		reply.Success = false
		reply.ConflictTerm = -2
		reply.ConflictIndex = rf.log[0].Index + 1
		return
	}
	if realPrevIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.LogLen = rf.log[len(rf.log)-1].Index + 1
		return
	}
	// realPrevIndex is valid
	if rf.log[realPrevIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[realPrevIndex].Term
		realConflictIndex := 0
		for i := realPrevIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[realPrevIndex].Term {
				realConflictIndex = i + 1
				break
			}
		}
		reply.ConflictIndex = rf.log[realConflictIndex].Index
		return
	}
	// match term
	for idx, entry := range args.Entries {
		logIndex := rf.toRealIndex(entry.Index)
		if logIndex >= len(rf.log) || rf.log[logIndex].Term != entry.Term {
			rf.log = append([]LogEntry{}, append(rf.log[:logIndex], args.Entries[idx:]...)...)
			break
		}
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.log[len(rf.log)-1].Index < args.LeaderCommit {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		}
	}
	rf.applierCond.Signal()
}

func (rf *Raft) doInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkResponseTerm(args, reply, false) {
		return
	}

	if args.LastIncludedIndex != rf.log[0].Index {
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.persister.Save(rf.encodeState(), args.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkRequestTerm(args, reply) {
		return
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.lastHeartBeat = time.Now()
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	lastLogIndex := rf.log[len(rf.log)-1].Index

	if args.LastIncludedIndex >= lastLogIndex {
		rf.log = []LogEntry{{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: nil,
		}}
	} else {
		realIdx := rf.toRealIndex(args.LastIncludedIndex)
		if realIdx >= 0 && rf.log[realIdx].Term == args.LastIncludedTerm {
			rf.log = append([]LogEntry{}, rf.log[realIdx:]...)
			rf.log[0].Command = nil
		} else {
			rf.log = []LogEntry{{
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
				Command: nil,
			}}
		}
	}
	rf.persister.Save(rf.encodeState(), args.Data)
	rf.msg = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
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
		rf.mu.Lock()
		if rf.stat == Leader {
			for i := range rf.peers {
				if i != rf.me {
					args := rf.prepareReplicationArgs(i)
					go rf.doReplicate(i, args)
				}
			}
		} else if rf.isElectionTimeout() {
			rf.launchElection()
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatGap * time.Millisecond)
	}
}

func (rf *Raft) isElectionTimeout() bool {
	timeoutRange := ElectionTimeoutMax - ElectionTimeoutMin
	timeout := ElectionTimeoutMin + rand.Intn(timeoutRange)
	return time.Now().After(rf.lastHeartBeat.Add(time.Duration(timeout) * time.Millisecond))
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

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.lastHeartBeat = time.Now()
	rf.stat = Follower
	rf.voteFor = -1
	rf.log = make([]LogEntry, 0)

	// dummy entry to make the index start from 1
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applierCond = sync.NewCond(&rf.mu)
	rf.replicatorCond = make([]*sync.Cond, len(peers))

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range peers {
		rf.nextIndex[i] = 1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	rf.msg = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}

// Warning: this function is not thread-safe
func (rf *Raft) resetNewTermState(targetTerm int) {
	if rf.currentTerm < targetTerm {
		rf.voteFor = -1
	}
	rf.currentTerm = targetTerm
	rf.stat = Follower // reset to follower
}

// Reply false if term < currentTerm (§5.1)
// If RPC request contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkRequestTerm(args, reply RaftRPC) bool {
	term := args.GetTerm()
	defer reply.SetTerm(rf.currentTerm)
	if term < rf.currentTerm {
		return false
	}
	if term > rf.currentTerm {
		rf.resetNewTermState(term)
	}
	return true
}

// If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkResponseTerm(args, reply RaftRPC, isElection bool) bool {
	argsTerm := args.GetTerm()
	replyTerm := reply.GetTerm()
	if replyTerm > argsTerm {
		rf.resetNewTermState(replyTerm)
		rf.lastHeartBeat = time.Now()
		return false
	}
	return isElection || (rf.stat == Leader)
}

type RaftRPC interface {
	GetTerm() int
	SetTerm(int)
}

func (args *AppendEntriesArgs) GetTerm() int {
	return args.Term
}

func (args *AppendEntriesArgs) SetTerm(value int) {
	args.Term = value
}

func (args *RequestVoteArgs) GetTerm() int {
	return args.Term
}

func (args *RequestVoteArgs) SetTerm(value int) {
	args.Term = value
}

func (args *InstallSnapshotArgs) GetTerm() int {
	return args.Term
}

func (args *InstallSnapshotArgs) SetTerm(value int) {
	args.Term = value
}

func (reply *AppendEntriesReply) GetTerm() int {
	return reply.Term
}

func (reply *AppendEntriesReply) SetTerm(value int) {
	reply.Term = value
}

func (reply *RequestVoteReply) GetTerm() int {
	return reply.Term
}

func (reply *RequestVoteReply) SetTerm(value int) {
	reply.Term = value
}

func (reply *InstallSnapshotReply) GetTerm() int {
	return reply.Term
}

func (reply *InstallSnapshotReply) SetTerm(value int) {
	reply.Term = value
}

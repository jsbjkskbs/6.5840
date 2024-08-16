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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
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

	// For 4A:
	CommandTerm int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
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

	//Persistent state on all servers:
	//(Updated on stable storage before responding to RPCs)
	currentTerm int     //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     //candidateId that received vote in current term (or null if none)
	log         []Entry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1) Volatile state on all servers:

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically) Volatile state on leaders: (Reinitialized after election)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	voteMutex      sync.Mutex   // now vote is a stand-alone goroutine, which need another mutex to secure safety
	voteCount      int          // make it easier to count how many peers vote to this server
	electionTicker *time.Ticker // make it easier and more obvious to start an election
	// vote check功能目前独立出一个协程
	// voteCount单独享用一个锁比之前与其他变量共用 raft.mu 这个互斥锁更合理
	// electionTicker作用与之前一致

	heartbeatTicker *time.Ticker // for the fast ops complete, you know
	// Lab 4 Test: ops complete fast enough (4A) ...
	// 操作取决于心跳速度，也就是说不能用Lazy Sync的方法
	// 一般地，心跳为100ms
	// 收到日志后，心跳重置为<33ms

	role int // leader, follower or candicate

	applyCh   chan ApplyMsg // commit log entries (the log you're sure to apply in the state machine)
	applyCond *sync.Cond    // wake up goroutine to commit log entries
	// applyCh单独一个协程
	// 这样AppendEntries相关部分只需要处理commitIndex即可

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// 推迟选举
func (rf *Raft) delayElection() {
	rf.electionTicker.Reset(time.Duration(DelayElectionMinTime+rand.Int63()%DelayElectionRangeSize) * time.Millisecond)
}

func (rf *Raft) resetHeartbeat() {
	rf.heartbeatTicker.Reset(time.Duration(HeartbeatInterval) * time.Millisecond)
}

func (rf *Raft) fastOpt() {
	rf.heartbeatTicker.Reset(time.Duration(FastOperateTime) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("unexpect error: server[%v] read persist failed.", rf.me)
	} else {
		// 3B
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log

		// 3D
		// Reset state machine using snapshot contents (and load
		// snapshot’s cluster configuration)
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// 3D
	// Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.snapshot = data
}

func (rf *Raft) commit() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			// DPrintf("Commiter Wake Up: {id: %v,rf.commitIndex: %v,rf.lastApplied: %v}", rf.me, rf.commitIndex, rf.lastApplied)
		}
		/*
		   sync.Cond.Wait():
		       c.checker.check()
		       t := runtime_notifyListAdd(&c.notify)
		       c.L.Unlock()
		       runtime_notifyListWait(&c.notify, t)
		       c.L.Lock()
		   因此会自动释放锁，并等待信号
		*/

		// 如果有需要提交的日志
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)

		// 注意,可提交的日志必须是"consense".
		// Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs
		// of the leaders for all higher-numbered terms. §5.4

		// State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server
		// will ever apply a different log entry for the same index. §5.4.3
		buffer := []*ApplyMsg{}
        lastApplied := rf.lastApplied
        for rf.commitIndex > lastApplied {
            lastApplied++
            if lastApplied <= rf.lastIncludedIndex {
                continue
            }
            buffer = append(buffer, &ApplyMsg{
                CommandValid: true,
                Command:      rf.log[rf.getPhysicalIndex(lastApplied)].Command,
                CommandIndex: lastApplied,   
                // Lab 4（与Lab 3无关）             
                CommandTerm:  rf.log[rf.getPhysicalIndex(lastApplied)].Term,
            })
        }
        rf.mu.Unlock()

        for _, msg := range buffer {
        
            rf.mu.Lock()
            if msg.CommandIndex != rf.lastApplied+1 {
                rf.mu.Unlock()
                continue
            }
            rf.mu.Unlock()

            rf.applyCh <- *msg

            rf.mu.Lock()
            if msg.CommandIndex != rf.lastApplied+1 {
                rf.mu.Unlock()
                continue
            }
            rf.lastApplied = msg.CommandIndex
            rf.mu.Unlock()
        }
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

	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	if !rf.acceptSnapshot(index) {
		DPrintf("Server reject snapshot[index: %v]: %v with [commitIndex: %v, lastIncludedIndex: %v]", index, rf.me, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("Server accept snapshot[index: %v]: %v[%v] with [commitIndex: %v, lastIncludedIndex: %v]", index, rf.me, getRoleString(rf), rf.commitIndex, rf.lastIncludedIndex)

	rf.snapshot = snapshot
	rf.lastIncludedTerm = rf.log[rf.getPhysicalIndex(index)].Term
	rf.log = rf.log[rf.getPhysicalIndex(index):]

	// if accept(index > rf.lastIncludedIndex) => rf.lastIncludedIndex = max(rf.lastIncludedIndex, index)
	rf.lastIncludedIndex = max(rf.lastIncludedIndex, index)

	rf.commitIndex = max(index, rf.commitIndex)
	rf.lastApplied = max(index, rf.lastApplied)

	if Debug {
		lastAppliedSnapshot := DApplier{}
		lastAppliedSnapshot.ApplyWithSnapshot(rf)
		DApplierPrint(rf, &lastAppliedSnapshot)
	}

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte //raw bytes of the snapshot chunk, starting at offset

	// not used
	Offset int // byte offset where chunk is positioned in the snapshot file
	// Create new snapshot file if first chunk (offset is 0)
	// Write data into snapshot file at given offset

	// not used
	Done bool // true if this is the last chunk
	// Reply and wait for more data chunks if done is false

}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(id int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	defer DRPCPrint(rf, args)
	return rf.peers[id].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DRPCPrint(rf, args)

	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.ConvertToFollower(args.Term)
	rf.delayElection()

	if rf.hasInstalled(args.LastIncludedIndex) {
		DPrintf("Server reject install snapshot[index: %v]: %v with [commitIndex: %v, lastIncludedIndex: %v]", args.LastIncludedIndex, rf.me, rf.commitIndex, rf.lastIncludedIndex)
		reply.Term = rf.currentTerm
		return
	}

	DPrintf("Server accept install snapshot[index: %v]: %v with [commitIndex: %v, lastIncludedIndex: %v]", args.LastIncludedIndex, rf.me, rf.commitIndex, rf.lastIncludedIndex)

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if rf.hasExtraLogEntries(args.LastIncludedIndex) {
		rf.log = rf.log[rf.getPhysicalIndex(args.LastIncludedIndex):]
	} else {
		// Discard the entire log
		// ???
		rf.log = []Entry{
			{
				Term:    rf.lastIncludedTerm,
				Index:   rf.lastIncludedIndex,
				Command: "孩子们，这不好笑",
			},
		}
	}

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if Debug {
		lastAppliedSnapshot := DApplier{}
		lastAppliedSnapshot.ApplyWithSnapshot(rf)
		DApplierPrint(rf, &lastAppliedSnapshot)
	}

	reply.Term = rf.currentTerm
	rf.persist()
}

// AppendEntries RPC
// Arguments:
type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	LeaderCommit int // leader’s commitIndex

	Log []Entry // to store (empty for heartbeat; may send more than one for efficiency)

	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
}

// Results:
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
	// what should three 'X' do ?
	// Case 1: leader doesn't have XTerm:
	//	nextIndex = XIndex
	// Case 2: leader has XTerm:
	//	nextIndex = leader's last entry for XTerm
	// Case 3: follower's log is too short:
	//	nextIndex = XLen
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DRPCPrint(rf, reply)

	// Receiver implementation:

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 确认是当前Leader发的,推迟选举
	rf.delayElection()

	// 任期不一致,立即同步
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.persist()
		// 不要马上return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	// Lab 3C: XTerm, XIndex, XLen
	conflict := false
	// Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries
	// up through the given index. §5.3
	// term与index相同的log不冲突(!conflict)
	if args.PrevLogIndex >= rf.getLogicalIndex(len(rf.log)) {
		// Append any new entries not already in the log

		reply.XTerm = -1
		// XLen: log length
		reply.XLen = rf.getLogicalIndex(len(rf.log))
		conflict = true
	} else if rf.log[rf.getPhysicalIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

		// XTerm: term in the conflicting entry (if any)
		reply.XTerm = rf.log[rf.getPhysicalIndex(args.PrevLogIndex)].Term

		// XIndex: index of first entry with that term (if any)
		rollbackPoint := args.PrevLogIndex
		for rollbackPoint > rf.commitIndex && rf.log[rf.getPhysicalIndex(rollbackPoint)].Term == reply.XTerm {
			rollbackPoint--
		}
		reply.XIndex = rollbackPoint + 1
		reply.XLen = rf.getLogicalIndex(len(rf.log))
		conflict = true
	}

	// 出现日志冲突
	if conflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		DXConflictPrint(rf, args, reply)
		return
	}

	// PrevLog匹配,则同步日志
	// Leader的任何日志都是"可信任的"
	// 所以认为PrevLogIndex之后的日志均为冲突日志也是正确的
	// 无条件从PrevLogIndex开始替换
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	for index, log := range args.Log {
		p := rf.getPhysicalIndex(args.PrevLogIndex) + index + 1
		if p < len(rf.log) && rf.log[p].Term != log.Term {
			rf.log = rf.log[:p]
			rf.log = append(rf.log, args.Log[index:]...)
			break
		} else if p >= len(rf.log) {
			rf.log = append(rf.log, args.Log[index:]...)
			break
		}
	}
	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	// 存在可提交日志
	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry
		rf.commitIndex = min(args.LeaderCommit, rf.getLogicalIndex(len(rf.log)-1))
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	defer DRPCPrint(rf, args)
	return rf.peers[id].Call("Raft.AppendEntries", args, reply)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// Arguments:
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
	LastLogIndex int // index of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// Results:
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DRPCPrint(rf, reply)

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 接下来会投票
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		rf.persist()
	}

	// 有票的情况: 			没投票或已投给此RPC候选人
	// 候选人可信任的情况:	 日志一样或更"新"
	// If votedFor is null or candidateId (haveVoteTicket),
	// and candidate’s log is at least as up-to-date as receiver’s log(isCandidateLogReliable),
	// grant vote (§5.2, §5.4)
	if rf.haveVoteTicket(args) && rf.isCandidateLogReliable(args) {
		rf.ConvertToFollower(args.Term).VoteTo(args.CandidateId)
		rf.delayElection()
		rf.persist()

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	// 没投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
// that the caller passes the address of the rep ly struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	defer DRPCPrint(rf, args)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}

	newLogIndex := rf.getLogicalIndex(len(rf.log))
	rf.log = append(rf.log, Entry{
		Term:    rf.currentTerm,
		Index:   newLogIndex,
		Command: command,
	})
	rf.persist()
	rf.fastOpt()
	return newLogIndex, rf.currentTerm, true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTicker.Stop()
	rf.heartbeatTicker.Stop()

	DPrintf("Server killed:%v", rf.me)
	DServerPrint(rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		<-rf.electionTicker.C
		// If election timeout elapses: start new election
		rf.mu.Lock()

		// 说明Leader没有在下一次选举之前发送任何心跳表示存活,此时必须开始选举
		if rf.role != Leader {
			// if election timeout elapses without receiving AppendEntries
			// RPC from current leader or granting vote to candidate: convert to candidate
			go rf.election()
		}
		// On conversion to candidate, Reset election timer
		rf.delayElection()
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// time.Sleep(time.Duration(50+rand.Int63n()%300) * time.Millsecond)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ConvertToCandidate()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLogicalIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// On conversion to candidate, Send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.checkVote(i, args)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A
	rf.ConvertToFollower(InitTerm).VoteTo(NoVote)
	rf.electionTicker = time.NewTicker(1)
	rf.heartbeatTicker = time.NewTicker(1)
	rf.delayElection()

	// 3B
	rf.log = []Entry{
		{
			Term:    0,
			Index:   -1,
			Command: "孩子们，这不好笑",
		},
	}
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// 3B
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLogicalIndex(len(rf.log))
	}

	DPrintf("server start: %v", rf.me)
	DServerPrint(rf)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit()

	return rf
}

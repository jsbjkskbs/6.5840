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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	CommandTerm  int
	CommandIndex int
	Command      interface{}
}

const (
	NoVote = -1
	// 推迟选举的时间不能太长
	DelayElectionMinTime   = 300
	DelayElectionRangeSize = 100
)

const (
	Follower = iota
	Candidate
	Leader
)

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
	currentTerm int   //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int   //candidateId that received vote in current term (or null if none)
	logEntries  []Log //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1) Volatile state on all servers:

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically) Volatile state on leaders: (Reinitialized after election)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	signal         chan int
	state          int
	electionTicker *time.Ticker

	applyCh chan ApplyMsg // used for logs' submission?
}

func (rf *Raft) delayElection() {
	rf.electionTicker.Reset(time.Duration(DelayElectionMinTime+rand.Int63()%DelayElectionRangeSize) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// AppendEntries RPC
// Arguments:
type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	LogEntries   []Log // to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

// Results:
type AppendEnriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	RollbackIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEnriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.signal <- 1
	rf.currentTerm = args.Term
	rf.state = Follower
	reply.Term = rf.currentTerm
	reply.Success = true

	if len(rf.logEntries)-1 > args.PrevLogIndex { // 任期一致，但Follower日志多余
		rf.logEntries = rf.logEntries[:args.PrevLogIndex+1]
		reply.Success = false
		reply.RollbackIndex = len(rf.logEntries)
		return
	}

	if len(rf.logEntries)-1 < args.PrevLogIndex { // 任期一致，但Follower日志缺失
		rf.logEntries = rf.logEntries[:rf.commitIndex]
		reply.Success = false
		// reply.RollbackIndex = len(rf.logEntries)
		reply.RollbackIndex = rf.commitIndex
		return
	}

	if len(rf.logEntries)-1 == args.PrevLogIndex { // 任期、日志长度一致，但日志任期不一致
		if len(rf.logEntries) != 0 {
			if rf.logEntries[args.PrevLogIndex].CommandTerm != args.PrevLogTerm {
				rf.logEntries = rf.logEntries[:rf.commitIndex]
				reply.Success = false
				reply.RollbackIndex = rf.commitIndex
				return
			}
		}
	}

	if len(args.LogEntries) == 0 {
		// DPrintf("%v receive heartbeat", rf.me)

		// 只有收到无日志心跳并完成全部日志的追加，才能认为日志完整并提交日志
		for rf.lastApplied < args.LeaderCommit {
			rf.lastApplied++
			rf.commitIndex = rf.lastApplied

			// Commit logs. For this lab, report them to the tester
			// Send each newly committed entry on applyCh on each peer.
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
		}
	} else { // 追加日志
		rf.logEntries = append(rf.logEntries, args.LogEntries...)
	}

}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs, reply *AppendEnriesReply) {
	if rf.killed() {
		return
	}

	// Leader本身已同步
	synchronized := 1
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.state != Leader {
			break
		}

		// 3B --
		rf.mu.Lock()
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[i] - 1

		if args.PrevLogIndex == -1 {
			args.LogEntries = make([]Log, len(rf.logEntries))
			args.PrevLogTerm = 0
		} else {
			args.LogEntries = make([]Log, len(rf.logEntries)-rf.nextIndex[i])
			args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].CommandTerm
		}

		if rf.nextIndex[i] < len(rf.logEntries) {
			copy(args.LogEntries, rf.logEntries[rf.nextIndex[i]:])
		}
		rf.mu.Unlock()
		// -- 3B

		go func(peer *labrpc.ClientEnd, i int, synchronized *int, args AppendEntriesArgs, reply AppendEnriesReply) {
			ok := peer.Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if ok {
				if !reply.Success {
					if rf.state != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						// DPrintf("%v find that he is no longer a leader![%v:%v]\n", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						return
					}

					// 追加日志的起始序号(下一轮心跳发送日志项，不必立即处理)
					rf.nextIndex[i] = reply.RollbackIndex
				} else {
					if args.LeaderCommit == rf.commitIndex {
						*synchronized++
					}

					next := args.PrevLogIndex + len(args.LogEntries) + 1
					match := args.PrevLogIndex + len(args.LogEntries)
					if next > rf.nextIndex[i] {
						rf.nextIndex[i] = next
					}
					if match > rf.matchIndex[i] {
						rf.matchIndex[i] = match // 这一步其实没有任何作用，但论文都注释了，就照着写吧
					}

					// 当大多数(>1/2)完成日志同步，则认为全部完成日志同步(由投票机制保障)，此时Leader可以提交日志
					if *synchronized > len(rf.peers)/2 {
						for rf.lastApplied < rf.nextIndex[i] {
							rf.lastApplied++
							rf.commitIndex = rf.lastApplied
							rf.nextIndex[rf.me] = rf.commitIndex

							// Send each newly committed entry on applyCh on each peer.
							rf.applyCh <- ApplyMsg{
								CommandValid: true,
								Command:      rf.logEntries[rf.lastApplied-1].Command,
								CommandIndex: rf.lastApplied,
							}
						}

						// 减少重复次数
						*synchronized = 0
					}
				}
			}
		}(peer, i, &synchronized, *args, *reply)
	}
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
	reply.VoteGranted = false

	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.signal <- 1
		rf.state = Follower
		rf.currentTerm = args.Term

		// 尽可能保证Leader的日志覆盖所有Follower
		if rf.isVoteRequestLogValid(args) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	} else if args.Term == rf.currentTerm {
		rf.signal <- 1
		if (rf.votedFor == args.CandidateId || rf.votedFor == NoVote) &&
			rf.isVoteRequestLogValid(args) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) isVoteRequestLogValid(args *RequestVoteArgs) bool {
	// 只投给日志比自己新或一致的节点
	if len(rf.logEntries) == 0 {
		return true
	}

	if rf.logEntries[len(rf.logEntries)-1].CommandTerm > args.LastLogTerm {
		return false
	}

	if rf.logEntries[len(rf.logEntries)-1].CommandTerm == args.LastLogTerm &&
		len(rf.logEntries)-1 > args.LastLogIndex {
		return false
	}

	return true
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
	// index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = (rf.state == Leader)

	if !isLeader {
		return -1, term, isLeader
	}

	rf.logEntries = append(rf.logEntries, Log{
		CommandTerm: term,
		Command:     command,
	})
	// DPrintf("%v got command: %c", rf.me, fmt.Sprint(command)[0])

	return len(rf.logEntries), term, isLeader
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

	if rf.state == Leader {
		DPrintf("leader[%v] now is dead[%v], %v", rf.me, rf.currentTerm, DLogSprint(&rf.logEntries))
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			// DPrintf("%v heartbeat", rf.me)
			rf.mu.Unlock()
			rf.sendAppendEntries(&args, &AppendEnriesReply{})
			// 本来应该不作send与Unlock的交换，但因为3B添加了一个临界区，为防止死锁而必须修改不属于3B的代码区
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) election() {
	for {
		select {
		case <-rf.electionTicker.C:
			rf.delayElection()
			/*
				不能用rf.signal<-1，因为在同一个select块内
			*/
			// 注: 保证Leader无法进入本代码块
			if rf.state == Follower {
				rf.state = Candidate
			}

			if rf.state == Candidate {
				rf.currentTerm++
			}

			lastLogIndex := len(rf.logEntries) - 1
			lastLogTerm := 0

			if len(rf.logEntries) != 0 {
				lastLogTerm = rf.logEntries[lastLogIndex].CommandTerm
			}

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			rf.votedFor = rf.me
			voteCount := 1

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(args RequestVoteArgs, i int, voteCount *int) {
					reply := RequestVoteReply{}
					if ok := rf.sendRequestVote(i, &args, &reply); ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
						}

						if reply.VoteGranted {
							if args.Term == rf.currentTerm {
								*voteCount++
							}

							if *voteCount > len(rf.peers)/2 {
								if rf.state != Leader {
									DPrintf(
										"%v now is leader{%v/%v}[%v], %v",
										rf.me,
										*voteCount,
										len(rf.peers),
										rf.currentTerm,
										DLogSprint(&rf.logEntries),
									)
								}
								rf.state = Leader

								for i := range rf.nextIndex {
									rf.nextIndex[i] = len(rf.logEntries)
									rf.matchIndex[i] = 0
								}

								appendArgs := AppendEntriesArgs{
									Term:         rf.currentTerm,
									LeaderId:     rf.me,
									PrevLogIndex: len(rf.logEntries) - 1,
									PrevLogTerm:  0,
								}

								// 减少重复次数
								*voteCount = 0

								rf.mu.Unlock()
								rf.sendAppendEntries(&appendArgs, &AppendEnriesReply{})
								// 此处不能send完再Unlock, 因为3B添加了一个等效于顺序执行的临界区
							} else {
								rf.mu.Unlock()
							}
						} else {
							rf.mu.Unlock()
						}
					}
				}(args, i, &voteCount)
			}
		case <-rf.signal:
			// DPrintf("%v delay election", rf.me)
			rf.delayElection()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// 3A
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NoVote
	rf.electionTicker = time.NewTicker(time.Duration(DelayElectionMinTime+(rand.Int63()%DelayElectionRangeSize)) * time.Millisecond)

	rf.signal = make(chan int)

	// 3B
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logEntries = make([]Log, 0)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.election()

	return rf
}

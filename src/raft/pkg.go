package raft

import "time"

const (
	NoVote   = -1
	InitTerm = 0

	// 推迟选举的时间不能太长
	DelayElectionMinTime   = 300
	DelayElectionRangeSize = 100
	HeartbeatInterval      = 100
)

const (
	Follower = iota
	Candidate
	Leader
)

func max(numbers ...int) int {
	r := numbers[0]
	for _, number := range numbers {
		if number > r {
			r = number
		}
	}
	return r
}

func min(numbers ...int) int {
	r := numbers[0]
	for _, number := range numbers {
		if number < r {
			r = number
		}
	}
	return r
}

func (rf *Raft) ConvertToLeader() {
	rf.role = Leader
	DPrintf("New Leader Occured: %v", rf.me)
	DServerPrint(rf)
}

// 只用ConvertToFollower是NoVote
// 如果要附带投票,应该这样调用rf.ConvertToFollower(NewTerm).VoteTo(VoteFor)
func (rf *Raft) ConvertToFollower(term int) *Raft {
	rf.votedFor = NoVote
	rf.currentTerm = term
	rf.role = Follower
	return rf
}

func (rf *Raft) VoteTo(voteFor int) {
	rf.votedFor = voteFor
}

// rf.currentTerm++
// rf.voteCount = 1
func (rf *Raft) ConvertToCandidate() {
	rf.role = Candidate
	rf.voteCount = 1

	// On conversion to candidate, start election:
	// Vote for self
	rf.votedFor = rf.me
	// Increment currentTerm
	rf.currentTerm++
}

// ---------------------------------------------------------------
//							candicate

func (rf *Raft) askForVote(id int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	if ok := rf.sendRequestVote(id, args, reply); !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 存在将一个RPC请求延迟到下一个任期的情况(network lag),尤其是Lab 3C
	if args.Term != rf.currentTerm {
		return false
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	// If AppendEntries RPC received from new leader: convert to follower
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		rf.persist()
	}

	return reply.VoteGranted
}

func (rf *Raft) checkVote(id int, args *RequestVoteArgs) {
	if accept := rf.askForVote(id, args); !accept {
		return
	}

	// 访问、修改voteCount应该加锁(只用rf.mu也可以)
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()

	// 已经选上就直接return
	// 不要访问rf.role,因为不是用rf.mu加锁
	if rf.voteCount > len(rf.peers)/2 {
		return
	}

	rf.voteCount++

	// Election Safety: at most one leader can be elected in a given term. §5.2
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role == Follower {
			return
		}

		// If votes received from majority of servers: become leader
		rf.ConvertToLeader()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLogicalIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		go rf.heartbeat()
	}
}

// ---------------------------------------------------------------

// ---------------------------------------------------------------
//							leader

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		// defer rf.mu.Unlock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		// Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3
		// 所以,Leader只管发信息和append(append在Start中实现)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
			}

			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			sendSnapshot := false
			if args.PrevLogIndex < rf.lastIncludedIndex {
				sendSnapshot = true
			} else if rf.getLogicalIndex(len(rf.log)-1) > args.PrevLogIndex {
				args.Log = rf.log[rf.getPhysicalIndex(args.PrevLogIndex+1):]
			} else {
				args.Log = make([]Entry, 0)
			}

			if sendSnapshot {
				go rf.postInstallSnapshot(i, &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				})
			} else {
				args.PrevLogTerm = rf.log[rf.getPhysicalIndex(args.PrevLogIndex)].Term

				// if len(args.Log) == 0
				// Upon election: send initial empty AppendEntries RPCs
				// (heartbeat) to each server; repeat during idle periods to
				// prevent election timeouts (§5.2)

				go rf.postAppendEntries(i, args)
			}
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) postAppendEntries(id int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(id, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}

	if args.Term != rf.currentTerm {
		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	if reply.Success {
		rf.matchIndex[id] = max(rf.matchIndex[id], args.PrevLogIndex+len(args.Log))
		rf.nextIndex[id] = rf.matchIndex[id] + 1

		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		rf.commitIndex = rf.seekSynchronizedIndex()
		rf.applyCond.Signal()
		return
	}

	// !reply.Success
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		rf.delayElection()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

		DXConflictPrint(rf, args, reply)

		// 这里我们用Lab 3C提到的三个'X'
		// Case 3: follower's log is too short:
		//	nextIndex = XLen
		if reply.XTerm == -1 {
			rf.nextIndex[id] = max(rf.lastIncludedIndex, reply.XLen)
			return
		}

		// try to find XTerm
		rollbackPoint := max(rf.lastIncludedIndex, rf.nextIndex[id]-1)
		for rollbackPoint > rf.lastIncludedIndex && rf.log[rf.getPhysicalIndex(rollbackPoint)].Term > reply.XTerm {
			rollbackPoint--
		}

		if rollbackPoint == rf.lastIncludedIndex && rf.log[rf.getPhysicalIndex(rollbackPoint)].Term > reply.XTerm {
			rf.nextIndex[id] = rf.lastIncludedIndex
		} else if rf.log[rf.getPhysicalIndex(rollbackPoint)].Term != reply.XTerm {
			// Case 1: leader doesn't have XTerm:
			// 	nextIndex = XIndex
			rf.nextIndex[id] = max(rf.lastIncludedIndex, reply.XIndex)
		} else {
			// Case 2: leader has XTerm:
			//	nextIndex = leader's last entry for XTerm
			rf.nextIndex[id] = rollbackPoint + 1
		}
	}
}

// 定向到match过半数的最近index
func (rf *Raft) seekSynchronizedIndex() int {
	synchronizedIndex := rf.getLogicalIndex(len(rf.log) - 1)
	for synchronizedIndex > rf.commitIndex {
		synchronizedPeers := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= synchronizedIndex && rf.log[rf.getPhysicalIndex(synchronizedIndex)].Term == rf.currentTerm {
				synchronizedPeers++
			}
		}
		if synchronizedPeers > len(rf.peers)/2 {
			break
		}
		synchronizedIndex--
	}
	return synchronizedIndex
}

// ---------------------------------------------------------------

// ---------------------------------------------------------------
// 							follower

func (rf *Raft) haveVoteTicket(args *RequestVoteArgs) bool {
	if rf.votedFor == NoVote {
		return true
	}
	if rf.votedFor == args.CandidateId {
		return true
	}
	return false
}

func (rf *Raft) isCandidateLogReliable(args *RequestVoteArgs) bool {
	// 日志任期更"新"
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		return true
	}
	// 日志任期一致,但Candidate更长
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.getLogicalIndex(len(rf.log)-1) {
		return true
	}
	return false
}

// ---------------------------------------------------------------
// 					snapshot

func (rf *Raft) getPhysicalIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) getLogicalIndex(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) acceptSnapshot(index int) bool {
	return rf.commitIndex >= index &&
		index > rf.lastIncludedIndex
}

func (rf *Raft) hasAppliedBySnapshot() bool {
	return rf.lastApplied <= rf.lastIncludedIndex
}

func (rf *Raft) hasInstalled(lastIncludedIndex int) bool {
	return lastIncludedIndex < rf.lastIncludedIndex ||
		lastIncludedIndex < rf.commitIndex
}

func (rf *Raft) hasExtraLogEntries(lastIncludedIndex int) bool {
	return rf.getPhysicalIndex(lastIncludedIndex) < len(rf.log) &&
		rf.log[rf.getPhysicalIndex(lastIncludedIndex)].Term == lastIncludedIndex
}

func (rf *Raft) postInstallSnapshot(id int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(id, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}

	if args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.ConvertToFollower(reply.Term)
		rf.delayElection()
		rf.persist()
		return
	}

	rf.matchIndex[id] = max(rf.matchIndex[id], args.LastIncludedIndex)
	rf.nextIndex[id] = rf.matchIndex[id] + 1
}

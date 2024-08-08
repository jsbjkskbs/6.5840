package raft

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
)

// Debugging
const Debug = false
const IgnoreServer = true
const IgnoreDLogDetails = true
const IgnoreRPC = true
const RandomIgnoreRPC = false
const ShowApplier = false
const ShowConflictX = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func getDebugID(rf *Raft) string {
	return fmt.Sprint(rf.me)
}

type DLogFMT struct {
	term    int
	payload string
}

type DLogSummary struct {
	length  int
	payload string
}

func DLogSprint(rf *Raft) (result string) {
	if Debug {
		DLogs := []DLogFMT{}
		LogSummary := DLogSummary{}
		if rf.mu.TryLock() {
			defer rf.mu.Unlock()
		}
		for _, log := range rf.log {
			dlog := DLogFMT{term: log.Term}
			if IgnoreDLogDetails {
				dlog.payload = fmt.Sprint(log.Command)
			} else {
				dlog.payload = fmt.Sprintf("%c", fmt.Sprint(log.Command)[0])
			}
			DLogs = append(DLogs, dlog)
		}
		if IgnoreDLogDetails {
			logDetails := fmt.Sprint(DLogs)
			hash := md5.Sum([]byte(logDetails))

			LogSummary.length = len(rf.log)
			LogSummary.payload = hex.EncodeToString(hash[:])

		} else {
			LogSummary.length = len(rf.log)
			LogSummary.payload = fmt.Sprint(DLogs)
		}
		result = fmt.Sprintf("%#v", LogSummary)
	}
	return
}

type DServerFMT struct {
	id          int
	term        int
	votedFor    int
	commitIndex int
	lastApplied int
	voteCount   int
	role        string

	lastIncludedIndex int
	lastIncludedTerm  int
}

func DServerSprint(rf *Raft) (result string) {
	if Debug {
		if rf.mu.TryLock() {
			defer rf.mu.Unlock()
		}
		dserver := DServerFMT{
			id:          rf.me,
			term:        rf.currentTerm,
			votedFor:    rf.votedFor,
			commitIndex: rf.commitIndex,
			lastApplied: rf.lastApplied,
			voteCount:   rf.voteCount,

			lastIncludedIndex: rf.lastIncludedIndex,
			lastIncludedTerm:  rf.lastIncludedTerm,
		}

		dserver.role = getRoleString(rf)

		result = fmt.Sprintf("%#v", dserver)
	}
	return
}

func DServerPrint(rf *Raft) {
	if IgnoreServer {
		return
	}
	debugID := getDebugID(rf)
	DPrintf("DEBUG[%v]: %v", debugID, DServerSprint(rf))
	DPrintf("DEBUG[%v]: %v", debugID, DLogSprint(rf))
}

type DApplier struct {
	id          int
	commitIndex int
	lastApplied int

	commandValid bool
	commandIndex int
	commandTerm  int

	snapshotValid     bool
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (d *DApplier) ApplyWithLog(rf *Raft) {
	d.id = rf.me
	d.commitIndex = rf.commitIndex
	d.lastApplied = rf.lastApplied
	d.commandValid = true
	d.commandIndex = rf.lastApplied
	d.commandTerm = rf.log[rf.getPhysicalIndex(rf.lastApplied)].Term
	d.lastIncludedIndex = rf.lastIncludedIndex
	d.lastIncludedTerm = rf.lastIncludedTerm
}

func (d *DApplier) ApplyWithSnapshot(rf *Raft) {
	d.id = rf.me
	d.commitIndex = rf.commitIndex
	d.lastApplied = rf.lastApplied
	d.snapshotValid = true
	d.lastIncludedIndex = rf.lastIncludedIndex
	d.lastIncludedTerm = rf.lastIncludedTerm
}

func DApplierPrint(rf *Raft, d *DApplier) {
	if !Debug {
		return
	}

	if !ShowApplier {
		return
	}

	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	debugID := getDebugID(rf)
	DPrintf("DEBUG[%v]: %#v", debugID, *d)
}

type DRPC struct {
	id      int
	details string
}

func DRPCPrint(rf *Raft, rpc interface{}) {
	if !Debug {
		return
	}

	if IgnoreRPC {
		return
	}

	if RandomIgnoreRPC {
		if rand.Int31()&1 == 0 {
			return
		}
	}

	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	drpc := DRPC{
		id:      rf.me,
		details: fmt.Sprintf("%#v", rpc),
	}
	debugID := getDebugID(rf)
	DPrintf("DEBUG[%v]: %#v", debugID, drpc)
}

// lab 3C
type DXConflict struct {
	id   int
	role string

	XTerm  int
	XIndex int
	XLen   int

	PrevLogIndex int
	PrevLogTerm  int

	ConflictLogMD5 string
}

func DXConflictPrint(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if Debug {
		if !ShowConflictX {
			return
		}

		if rf.mu.TryLock() {
			rf.mu.Unlock()
		}

		logDetails := fmt.Sprint(rf.log[reply.XIndex].Command)
		hash := md5.Sum([]byte(logDetails))
		logSnapshot := hex.EncodeToString(hash[:])
		debugID := getDebugID(rf)

		dXConflict := DXConflict{
			id:     rf.me,
			XTerm:  reply.Term,
			XIndex: reply.XIndex,
			XLen:   reply.XLen,

			PrevLogIndex: args.PrevLogIndex,
			PrevLogTerm:  args.PrevLogTerm,

			ConflictLogMD5: logSnapshot,
		}

		dXConflict.role = getRoleString(rf)

		DPrintf("DEBUG[%v]: %#v", debugID, dXConflict)
	}
}

// 默认已上锁
func getRoleString(rf *Raft) string {
	switch rf.role {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Null"
	}
}

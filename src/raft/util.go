package raft

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
)

// Debugging
const Debug = true
const IgnoreDLogDetails = true
const IgnoreRPC = true
const RandomIgnoreRPC = true
const ShowApplier = false
const ShowConflictX = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
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
	id         int
	term        int
	votedFor    int
	commitIndex int
	lastApplied int
	voteCount   int
	role        string
}

func DServerSprint(rf *Raft) (result string) {
	if Debug {
		if rf.mu.TryLock() {
			defer rf.mu.Unlock()
		}
		dserver := DServerFMT{
			id:         rf.me,
			term:        rf.currentTerm,
			votedFor:    rf.votedFor,
			commitIndex: rf.commitIndex,
			lastApplied: rf.lastApplied,
			voteCount:   rf.voteCount,
		}

		dserver.role = getRoleString(rf)

		result = fmt.Sprintf("%#v", dserver)
	}
	return
}

func DServerPrint(rf *Raft) {
	debugID := rand.Int63() % 1000
	DPrintf("DEBUG[%03d]: %v", debugID, DServerSprint(rf))
	DPrintf("DEBUG[%03d]: %v", debugID, DLogSprint(rf))
}

type DApplier struct {
	id          int
	index       int
	term        int
	commitIndex int
	lastApplied int
}

func DApplierPrint(rf *Raft, lastApplyLog Entry) {
	if !Debug {
		return
	}

	if !ShowApplier {
		return
	}

	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	dapplier := DApplier{
		id:          rf.me,
		index:       lastApplyLog.Index,
		term:        lastApplyLog.Term,
		commitIndex: rf.commitIndex,
		lastApplied: rf.lastApplied,
	}
	debugID := rand.Int63() % 1000
	DPrintf("DEBUG[%03d]: %#v", debugID, dapplier)
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
	debugID := rand.Int63() % 1000
	DPrintf("DEBUG[%03d]: %#v", debugID, drpc)
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
		debugID := rand.Int63() % 1000

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

		DPrintf("DEBUG[%03d]: %#v", debugID, dXConflict)
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

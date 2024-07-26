package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type DLogFMT struct {
	term    int
	payload string
}

func DLogSprint(logEntries *[]Log) (result string) {
	if Debug {
		DLogs := []DLogFMT{}
		for _, log := range *logEntries {
			DLogs = append(DLogs, DLogFMT{
				term:    log.CommandTerm,
				payload: fmt.Sprintf("%c", fmt.Sprint(log.Command)[0]),
			})
		}
		result = fmt.Sprintf("logs: %v; total: %v", DLogs, len(*logEntries))
	}
	return
}

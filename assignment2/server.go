package main

import (
	"fmt"
	"net"
	"strings"
	"bufio"
	"log"
	//"bytes"
	"strconv"
	//"math/rand"
	"io"
	//"time"
    "sync"
    //"sync/atomic"
)


type LogEntry struct {
	term 	int64
	index 	int64
}

const (
	CANDIDATE=0,
	FOLLOWER=1,
	LEADER=2
	)

type Event struct {
	eventType int 		// appendRequest, requestVote

	// Common variables
	term 	int64

	// For append entries event
	leaderId 		int64
	prevLogIndex	int64
	prevLogTerm		int64
	entries			LogEntry
	leaderCommit	int64

	// ForRequest vote
	candidateId		int64
	lastLogIndex	int64
	lastLogTerm		int64
}

type ServerState struct {
	// Persistent state
	server_id	int64
	currentTerm	int64
	votedFor 	int64
	log 		[]log_entry

	// Non-persistent state
	commitIndex	int64
	nextIndex	[]int64
	matchIndex	[]int64
	myState     int 		// CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

	connection 		chan event
}

var server ServerState

func startServer (server ServerState) {

}

func main () {

}
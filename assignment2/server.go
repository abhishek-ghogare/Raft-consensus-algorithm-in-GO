package main

import (
	//"fmt"
	//"net"
	//"strings"
	//"bufio"
	//"log"
	//"bytes"
	//"strconv"
	//"math/rand"
	//"io"
	//"time"
    //"sync"
    //"sync/atomic"
)


type LogEntry struct {
	term 	int64
	index 	int64
}


var TIMEOUTTIME = 1000;	// Timeout in ms

const (
	CANDIDATE=0;
	FOLLOWER=1;
	LEADER=2;
	)

const (
	APPREQ=0;
	VOTEREQ=1;
	APPRESP=2;
	VOTERESP=3;
	TIMEOUT=4;
	)


type Event struct {
	eventType int 		// appendRequest, requestVote, appendResp, requestVoteRest, timeout

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

	// Others
	fromId			int64
}

type ServerState struct {
	// Persistent state
	server_id	int64
	currentTerm	int64
	votedFor 	int64
	log 		[]LogEntry

	// Non-persistent state
	commitIndex	int64
	nextIndex	[]int64
	matchIndex	[]int64
	myState     int 		// CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

	connection 		chan Event
}

func (server *ServerState) startServer () {
	// Initialise the variables and timeout

	for {
		event := <- server.connection
		switch(event.eventType) {
		case APPREQ :

		}
	}
}

func (server *ServerState) follower () {

}

var server ServerState
func main () {

}
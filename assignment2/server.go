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
	"reflect"
)


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

type LogEntry struct {
	term 	int64
	index 	int64
}

/********************************************************************
 *																	*
 *							Input events							*
 *																	*
 ********************************************************************/
type appendRequestEvent struct {
	fromId			int64
	term			int64
	leaderId 		int64
	prevLogIndex	int64
	prevLogTerm		int64
	entries			LogEntry
	leaderCommit	int64
}

type appendRequestRespEvent struct {
	fromId		int64
	term 		int64
	success		bool
}

type requestVoteEvent struct {
	fromId			int64
	term 			int64
	candidateId		int64
	lastLogIndex	int64
	lastLogTerm		int64
}

type requestVoteRespEvent struct {
	fromId		int64
	term 		int64
	success		bool
}

type timeoutEvent struct {

}


/********************************************************************
 *																	*
 *							Output actions							*
 *																	*
 ********************************************************************/
type sendAction struct {
	toId	int64		// for broadcast, set to -1
	event 	interface{}
}

type commitAction struct {
	index 	int64		// for error, set to -1
	data	[]LogEntry
	err		string
}

type logStore struct {
	index 	int64
	data 	[]byte
}

type alarmAction struct {
	time 	int64
}


/********************************************************************
 *																	*
 *							Server status							*
 *																	*
 ********************************************************************/
type ServerState struct {
	// Persistent state
	server_id	int64
	currentTerm	int64
	votedFor 	int64
	log 		[]LogEntry

	// Non-persistent state
	commitIndex	int64		// initialised to -1
	nextIndex	[]int64
	matchIndex	[]int64
	myState     int 		// CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}
}

func (server *ServerState) setupServer ( state int, numberOfNodes int64 ) {
	server.server_id 	= 0
	server.currentTerm 	= 0
	server.votedFor 	= -1
	server.log 			= make([]LogEntry, 0)

	server.commitIndex	= -1
	server.nextIndex	= make([]int64, numberOfNodes)
	server.matchIndex	= make([]int64, numberOfNodes)
	server.myState		= state


    for i := int64(0); i < numberOfNodes; i++ {
		server.nextIndex[i]		= 0
		server.matchIndex[i]	= 0
    }
}


/********************************************************************
 *																	*
 *							Append Request							*
 *																	*
 ********************************************************************/
func (server *ServerState) appendRequest ( event appendRequestEvent ) []interface{} {

	actions []interface{}
	if ( event.term < server.currentTerm ) {
		// This node is not so modern
		// or election has started and this server is in candidate state
		// In all states applicable
		appendResp appendRequestRespEvent{fromId:server.server_id , term:server.currentTerm, success:false}
		resp sendAction{toId:event.fromId , event:appendResp}
		append(actions, resp)
		return actions
	}

	// Reset heartbeat timeout
	Alarm ( heartbeat_time )

	// This server term is not so up-to-date, so update
	currentTerm = term

	switch(server.myState) {
		case LEADER:
			// mystate == leader && term == currentTerm, this is impossible, as no two leaders will be elected at any term
			if ( event.term == server.currentTerm ) {
				return appendRequestRespEvent{fromId:0, term:-1, success:false}
			}
			// continue flow to next case
		case CANDIDATE:
			// Convert to follower if current state is candidate/leader
			server.myState = follower
			// continue flow to next case
		case FOLLOWER:
			if ( len(server.log)-1 < event.prevLogIndex || server.log[event.prevLogIndex].term != event.prevLogTerm ) {
				// Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
				return appendRequestRespEvent{fromId:0, term:server.currentTerm, success:false}
			}

			if( len(server.log)-1 > event.prevLogIndex ) {
				if ( server.log[event.prevLogIndex+1].term != event.entries.term ) {
					// There are garbage entries from last leaders
					// Strip them up to the end
					server.log = server.log[:event.prevLogIndex]
				} else {
					// No need to append, duplecate append request
					server.log = server.log[:event.prevLogIndex+1]	// Trimming remaining entries
					return appendRequestRespEvent{fromId:0, term:server.currentTerm, success:true}
				}
			}

			// Update log if entries are not present
			append(log, LogEntry{term:event.entries.term, index:event.entries.index})

			if ( event.leaderCommit > server.commitIndex ) {
				// If leader has commited entries, so should this server
				server.commitIndex = min(event.leaderCommit, len(server.log)-1)
			}

			return appendRequestRespEvent{fromId:0, term:server.currentTerm, success:true}
			break
	}

	// TODO: handle this last return
	return nil
}


/********************************************************************
 *																	*
 *							Process event							*
 *																	*
 ********************************************************************/
func (server *ServerState) processEvent ( event interface{} ) {
	// Initialise the variables and timeout

	switch(reflect.TypeOf(event).String()) {
		case "appendRequestEvent":
			server.appendRequest(event.(appendRequestEvent))
			break;
		case "appendRequestRespEvent":
			break;
		case "requestVoteEvent":
			break;
		case "requestVoteRespEvent":
			break;
	}
}

var server ServerState
func main () {
	server.setupServer(FOLLOWER,10)
}	
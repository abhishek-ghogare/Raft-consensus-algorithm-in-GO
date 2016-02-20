package main

import (
	"fmt"
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
	//"reflect"
)


var TIMEOUTTIME = int64(1000);	// Timeout in ms

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
	//leaderId 		int64	// same as fromId
	prevLogIndex	int64
	prevLogTerm		int64
	entries			[]LogEntry
	leaderCommit	int64
}

type appendRequestRespEvent struct {
	// TODO :: add last appended index, so that to update on leader side the matchIndex
	fromId		int64
	term 		int64
	success		bool
}

type requestVoteEvent struct {
	fromId			int64
	term 			int64
	// candidateId		int64	// same as fromId
	lastLogIndex	int64
	lastLogTerm		int64
}

type requestVoteRespEvent struct {
	fromId		int64
	term 		int64
	voteGranted	bool
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
	votedFor 	int64		// -1: not voted

	// log is initialised with single empty log, to make life easier in future checking
	// Index starts from 1, as first empty entry is present
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
	server.log 			= append(server.log, LogEntry{term:0, index:0})	// Initialising log with single empty log, to make life easier in future checking

	server.commitIndex	= -1
	server.nextIndex	= make([]int64, numberOfNodes)
	server.matchIndex	= make([]int64, numberOfNodes)
	server.myState		= state


    for i := int64(0); i < numberOfNodes; i++ {
		server.nextIndex[i]		= 0
		server.matchIndex[i]	= 0
    }
}

//  Returns last log entry
func (server *ServerState) getLastLog () LogEntry {
	return server.log[len(server.log) - 1]
}

/********************************************************************
 *																	*
 *							Vote Request							*
 *																	*
 ********************************************************************/
func (server *ServerState) voteRequest ( event requestVoteEvent ) []interface{} {

	actions := make([]interface{}, 0)

	if ( event.term < server.currentTerm ) {
		// In any state, if old termed candidate request vote, tell it to be a follower
		voteResp 	:= requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:false}
		resp 		:= sendAction{toId:event.fromId , event:voteResp}
		actions = append(actions, resp)
		return actions
	} else if event.term > server.currentTerm {
		// Request from more up-to-date node, so lets update our state
		server.currentTerm 	= event.term
		server.myState 		= FOLLOWER
		server.votedFor 	= -1
	}

	// requester_term >= server.current_term
	// If not voted for this term
	if server.votedFor == -1 {
		if event.lastLogTerm > server.getLastLog().term || event.lastLogTerm == server.getLastLog().term && event.lastLogIndex >= server.getLastLog().index {
			server.votedFor = event.fromId

			voteResp 	:= requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:true}
			resp 		:= sendAction{toId:event.fromId , event:voteResp}
			actions = append(actions, resp)
			return actions
		}
	} else {
		// If voted for this term, check if request is from same candidate for which this node has voted
		if server.votedFor == event.fromId {
			// Vote again to same candidate
			voteResp 	:= requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:true}
			resp 		:= sendAction{toId:event.fromId , event:voteResp}
			actions = append(actions, resp)
			return actions
		}
	}

	// For already voted for same term to different candidate, reject all requests
	voteResp := requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:false}
	resp := sendAction{toId:event.fromId , event:voteResp}
	actions = append(actions, resp)
	return actions
}



/********************************************************************
 *																	*
 *							Append Request							*
 *																	*
 ********************************************************************/
func (server *ServerState) appendRequest ( event appendRequestEvent ) []interface{} {

	actions := make([]interface{}, 0)

	if server.currentTerm > event.term {
		// Append request is not from latest leader
		// In all states applicable
		appendResp := appendRequestRespEvent{fromId:server.server_id , term:server.currentTerm, success:false}
		resp := sendAction{toId:event.fromId , event:appendResp}
		actions = append(actions, resp)
		return actions
	}

	switch(server.myState) {
		case LEADER:
			// mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
			if ( event.term == server.currentTerm ) {
				appendResp := appendRequestRespEvent{fromId:server.server_id, term:-1, success:false}
				resp := sendAction{toId:event.fromId , event:appendResp}
				actions = append(actions, resp)
				return actions
			}
			// continue flow to next case for server.currentTerm < event.term
			fallthrough
		case CANDIDATE:
			// Convert to follower if current state is candidate/leader
			server.myState = FOLLOWER
			// continue flow to next case
			fallthrough
		case FOLLOWER:
			// Reset heartbeat timeout
			alarm := alarmAction{time:TIMEOUTTIME}
			actions = append(actions, alarm)

			// TODO:: Check if empty heartbeat append request here

			if server.currentTerm < event.term {
				// This server term is not so up-to-date, so update
				server.currentTerm 	= event.term
				server.votedFor		= -1
				//fmt.Printf("\nUPDATING\n\n")
			}

			if ( server.getLastLog().index < event.prevLogIndex || server.log[event.prevLogIndex].term != event.prevLogTerm ) {
				// Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
				appendResp := appendRequestRespEvent{fromId:server.server_id, term:server.currentTerm, success:false}
				resp := sendAction{toId:event.fromId , event:appendResp}
				actions = append(actions, resp)
				return actions
			}

			if( server.getLastLog().index > event.prevLogIndex ) {
				//if ( server.log[event.prevLogIndex].term != event.entries[0].term ) {
					// There are entries from last leaders
					// Strip them up to the end
					server.log = server.log[:event.prevLogIndex+1]
					fmt.Printf("")
				/*
				} else {
					// No need to append, duplecate append request
					server.log = server.log[:event.prevLogIndex+1]	// Trimming remaining entries
					appendResp := appendRequestRespEvent{fromId:server.server_id, term:server.currentTerm, success:true}
					resp := sendAction{toId:event.fromId , event:appendResp}
					actions = append(actions, resp)
					return actions*/
				//}
			}

			// Update log if entries are not present
			server.log = append(server.log, event.entries...)

			if ( event.leaderCommit > server.commitIndex ) {
				// If leader has commited entries, so should this server
				if event.leaderCommit < int64(len(server.log)-1) {
					server.commitIndex = event.leaderCommit
				} else {
					server.commitIndex = int64(len(server.log)-1)
				}
			}
	}

	// TODO: do commit operations
	appendResp := appendRequestRespEvent{fromId:server.server_id, term:server.currentTerm, success:true}
	resp := sendAction{toId:event.fromId , event:appendResp}
	actions = append(actions, resp)
	return actions
}


/********************************************************************
 *																	*
 *							Process event							*
 *																	*
 ********************************************************************/
func (server *ServerState) processEvent ( event interface{} ) []interface{} {
	// Initialise the variables and timeout

	switch event.(type) {
		case appendRequestEvent:
			return server.appendRequest(event.(appendRequestEvent))
			break;
		case appendRequestRespEvent:
			break;
		case requestVoteEvent:
			return server.voteRequest(event.(requestVoteEvent))
			break;
		case requestVoteRespEvent:
			break;
	}

	return make([]interface{},0)
}

/*
var server ServerState
func main () {
	server.setupServer(FOLLOWER,10)
}*/	
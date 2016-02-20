package main

import (
	//"bufio"
	//"fmt"
	//"net"
	//"strconv"
	//"strings"
	"testing"
	//"time"
	//"reflect"
    //"sync"
)


/****************************************************************************
 *																			*
 *	Utility functions														*
 *																			*
 ****************************************************************************/
func expect(t *testing.T, a interface{}, b interface{}, msg string) {
	//if a.(type) != b.(type) {
	//	t.Errorf("Type mismatch, %v & %v", a.(type), b.(type))
	//} else 
	if a != b {
		t.Errorf("Expected %v, found %v : %v", b, a, msg) // t.Error is visible when running `go test -verbose`
	}
}

/********************************************************************************************
 *																							*
 *									Append Request Testing									*
 *																							*
 ********************************************************************************************/

func TestAppendRequestBasic(t *testing.T) {
	server := ServerState{}
	server.setupServer(FOLLOWER, 11)

	logs	:= make([]LogEntry, 0)
	log 	:= LogEntry{term:0, index:1}
	logs	= append(logs, log)
	log 	= LogEntry{term:0, index:2}
	logs	= append(logs, log)
	log 	= LogEntry{term:1, index:3}
	logs	= append(logs, log)

	event 	:= appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

	actions := server.processEvent(event)

	alarmActionReceived := false
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(t, action.toId, int64(1), "Response sent to wrong node")
			switch action.event.(type) {
			case appendRequestRespEvent:
				e1 := action.event.(appendRequestRespEvent)
				expect(t, e1.success, true, "Append request failed for valid request")
				expect(t, e1.term, int64(1), "Latest term was not sent")
			default:
				t.Errorf("Invalid event returned")
			}
		case alarmAction:
			alarmActionReceived = true
		default:
			t.Errorf("Invalid action returned")
		}
	}
	expect(t, alarmActionReceived, true, "Alarm action not received")
}


func TestAppendRequest_leader_with_old_term(t *testing.T) {
	server := ServerState{}
	server.setupServer(LEADER, 11)

	logs	:= make([]LogEntry, 0)
	log 	:= LogEntry{term:0, index:1}
	logs	= append(logs, log)
	log 	= LogEntry{term:0, index:2}
	logs	= append(logs, log)
	log 	= LogEntry{term:1, index:3}
	logs	= append(logs, log)

	event 	:= appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

	actions := server.processEvent(event)

	alarmActionReceived := false
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(t, action.toId, int64(1), "Response sent to wrong node")
			switch action.event.(type) {
			case appendRequestRespEvent:
				e1 := action.event.(appendRequestRespEvent)
				expect(t, e1.success, true, "Append request failed for valid request")
				expect(t, e1.term, int64(1), "Latest term was not sent")
				expect(t, server.myState, FOLLOWER, "Server state didn't change from leader to follower")
			default:
				t.Errorf("Invalid event returned")
			}
		case alarmAction:
			alarmActionReceived = true
		default:
			t.Errorf("Invalid action returned")
		}
	}
	expect(t, alarmActionReceived, true, "Alarm action not received")
}

// This case will only occur when two leaders exist for same term
// so, fatal error
func TestAppendRequest_leader_with_same_term_as_event_term(t *testing.T) {
	server := ServerState{}
	server.setupServer(LEADER, 11)

	logs	:= make([]LogEntry, 0)
	log 	:= LogEntry{term:0, index:1}
	logs	= append(logs, log)
	log 	= LogEntry{term:0, index:2}
	logs	= append(logs, log)
	log 	= LogEntry{term:0, index:3}
	logs	= append(logs, log)

	event 	:= appendRequestEvent{fromId:1, term:0, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

	actions := server.processEvent(event)

	alarmActionReceived := false
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(t, action.toId, int64(1), "Response sent to wrong node")
			switch action.event.(type) {
			case appendRequestRespEvent:
				e1 := action.event.(appendRequestRespEvent)
				expect(t, e1.success, false, "Append request success for invalid request")
				expect(t, e1.term, int64(-1), "As this is fatal error, -1 was expected")
				expect(t, server.myState, LEADER, "Server state changed, expected state:leader")
			default:
				t.Errorf("Invalid event returned")
			}
		case alarmAction:
			alarmActionReceived = true
		default:
			t.Errorf("Invalid action returned")
		}
	}
	expect(t, alarmActionReceived, false, "Alarm action received from invalid append request")
}




/********************************************************************************************
 *																							*
 *									Vote Request Testing									*
 *																							*
 ********************************************************************************************/

/********************************
 *	Positive test cases			*
 ********************************/
func TestVoteRequestBasic(t *testing.T) {
	server := ServerState{}
	server.setupServer(FOLLOWER, 11)

	event := requestVoteEvent{fromId:1, term:1, lastLogIndex:0, lastLogTerm:0}
	actions := server.processEvent(event)
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(t, action.toId, int64(1), "Response sent to wrong node")
			switch action.event.(type) {
			case requestVoteRespEvent:
				e1 := action.event.(requestVoteRespEvent)
				expect(t, e1.voteGranted, true, "Vote was not granted")
				expect(t, e1.term, int64(1), "Latest term was not sent")
			default:
				t.Errorf("Invalid event returned")
			}
		default:
			t.Errorf("Invalid action returned")
		}
	}
}

func TestVoteRequestLeader(t *testing.T) {
	server := ServerState{}
	server.setupServer(LEADER, 11)

	event := requestVoteEvent{fromId:1, term:1, lastLogIndex:0, lastLogTerm:0}
	actions := server.processEvent(event)
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(t, action.toId, int64(1), "Response sent to wrong node")
			switch action.event.(type) {
			case requestVoteRespEvent:
				e1 := action.event.(requestVoteRespEvent)
				expect(t, e1.voteGranted, true, "Vote was not granted")
				expect(t, e1.term, int64(1), "Latest term was not sent")
			default:
				t.Errorf("Invalid event returned")
			}
		default:
			t.Errorf("Invalid action returned")
		}
	}
}

/********************************
 *	Negative test cases			*
 ********************************/


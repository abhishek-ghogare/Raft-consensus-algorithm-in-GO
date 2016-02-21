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

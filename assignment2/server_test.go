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
func expect(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Errorf("Expected %v, found %v", b, a) // t.Error is visible when running `go test -verbose`
	}
}


// Basic write and read
func TestVoteRequest(t *testing.T) {
	server := ServerState{}
	server.setupServer(FOLLOWER, 11)

	event := requestVoteEvent{fromId:1, term:1, lastLogIndex:0, lastLogTerm:0}
	actions := server.processEvent(event)
	for _, action := range actions {
		switch action.(type) {
		case sendAction :
			action := action.(sendAction)
			expect(T, action.toId, 1)
			break
		}
	}
}
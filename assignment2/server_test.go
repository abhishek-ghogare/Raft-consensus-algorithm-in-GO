package main

import (
    //"bufio"
    //"fmt"
    //"net"
    //"strconv"
    //"strings"
    "testing"
    //"time"
    "reflect"
    //"sync"
)


/****************************************************************************
 *                                                                          *
 *  Utility functions                                                       *
 *                                                                          *
 ****************************************************************************/
func expect(t *testing.T, a interface{}, b interface{}, msg string) {
    //if a.(type) != b.(type) {
    //  t.Errorf("Type mismatch, %v & %v", a.(type), b.(type))
    //} else 
    if a != b {
        t.Errorf("Expected %v, found %v : %v", b, a, msg) // t.Error is visible when running `go test -verbose`
    }
}

/********************************************************************************************
 *                                                                                          *
 *                                  Append Request Testing                                  *
 *                                                                                          *
 ********************************************************************************************/

func TestAppendRequestBasic(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)

    event   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

    actions := server.processEvent(event)

    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, true, "Append request failed for valid request")
                expect(t, e1.term, int(1), "Latest term was not sent")
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

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)

    event   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

    actions := server.processEvent(event)

    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, true, "Append request failed for valid request")
                expect(t, e1.term, int(1), "Latest term was not sent")
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

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:3}
    logs    = append(logs, log)

    event   := appendRequestEvent{fromId:1, term:0, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}

    actions := server.processEvent(event)

    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, false, "Append request success for invalid request")
                expect(t, e1.term, int(-1), "As this is fatal error, -1 was expected")
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


func TestAppendRequest_with_greater_term_as_event_term(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event)

    // append request from 2nd node with term 0
    event   = appendRequestEvent{fromId:2, term:0, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions = server.processEvent(event)


    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(2), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, false, "Append request success for invalid request")
                expect(t, e1.term, int(1), "Append request old leader has changed the currentTerm of server")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
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



func TestAppendRequest_override_some_entries_from_old_leader(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event)

    logs    = make([]LogEntry, 0)       // Creating new logs
    log     = LogEntry{term:2, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:4}
    logs    = append(logs, log)

    // append request from 2nd node with term 2
    event   = appendRequestEvent{fromId:2, term:2, prevLogIndex:2, prevLogTerm:0, entries:logs, leaderCommit:10}
    actions = server.processEvent(event)


    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(2), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, true, "Append request failed for valid request")
                expect(t, e1.term, int(2), "latest term was not returned")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().index, "Server log length doesn't match with index of last log entry")
                expect(t, server.commitIndex, int(len(server.log)-1), "Commit index was not updated")
            default:
                t.Errorf("Invalid event returned")
            }
        case alarmAction:
            alarmActionReceived = true
        default:
            t.Errorf("Invalid action returned")
        }
    }
    expect(t, alarmActionReceived, true, "Alarm action was expected")
}


func TestAppendRequest_holes_in_server_log(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event)

    logs    = make([]LogEntry, 0)       // Creating new logs
    log     = LogEntry{term:2, index:11}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:12}
    logs    = append(logs, log)

    // append request from 2nd node with term 2
    event   = appendRequestEvent{fromId:2, term:2, prevLogIndex:10, prevLogTerm:1, entries:logs, leaderCommit:3}
    actions = server.processEvent(event)


    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(2), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.success, false, "Append request should have failed as there are logs missing from index 5 to 10")
                expect(t, e1.term, int(2), "latest term was not returned")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().index, "Server log length doesn't match with index of last log entry")
            default:
                t.Errorf("Invalid event returned")
            }
        case alarmAction:
            alarmActionReceived = true
        default:
            t.Errorf("Invalid action returned")
        }
    }
    expect(t, alarmActionReceived, true, "Alarm action was expected")
}



/********************************************************************************************
 *                                                                                          *
 *                            Append Request Response Testing                               *
 *                                                                                          *
 ********************************************************************************************/

func TestAppendRequestResponseBasic(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().index+1
    }
    server.matchIndex[server.server_id] = server.getLastLog().index

    event   := appendRequestRespEvent{fromId:1, term:1, success:true, lastLogIndex:2}

    actions = server.processEvent(event)
    
    expect(t, server.matchIndex[1], 2, "Matchindex was not updated")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}

func TestAppendRequestResponse_with_latest_term(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().index+1
    }
    server.matchIndex[server.server_id] = server.getLastLog().index

    event   := appendRequestRespEvent{fromId:1, term:2, success:false, lastLogIndex:2}

    actions = server.processEvent(event)
    
    expect(t, server.myState, FOLLOWER, "State should change from leader to follower")
    expect(t, server.currentTerm, 2, "Current term was not updated")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}

func TestAppendRequestResponse_fill_the_holes_in_log(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().index+1
    }
    server.matchIndex[server.server_id] = server.getLastLog().index

    event   := appendRequestRespEvent{fromId:1, term:1, success:false, lastLogIndex:-1}

    actions = server.processEvent(event)

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, 1, "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestEvent:
                e1 := action.event.(appendRequestEvent)
                expect(t, e1.fromId, server.server_id, "wrong fromId")
                expect(t, e1.term, 1, "Latest term was not sent")
                expect(t, e1.prevLogIndex, 3, "Wrong prevLogIndex received")
                expect(t, e1.prevLogTerm, 1, "Wrong prevLogTerm received")
                expect(t, e1.entries[0].index, 4, "Wrong log entry received")
                
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}


func TestAppendRequestResponse_commit_check(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().index+1
    }
    server.matchIndex[server.server_id] = server.getLastLog().index

    event   := appendRequestRespEvent{fromId:1, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:2, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:3, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:4, term:1, success:true, lastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:5, term:1, success:true, lastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:6, term:1, success:true, lastLogIndex:4}
    actions = server.processEvent(event)
    
    expect(t, server.commitIndex, 3, "Index 3 was not commited")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}



func TestAppendRequestResponse_commit_check1(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().index+1
    }
    server.matchIndex[server.server_id] = server.getLastLog().index

    event   := appendRequestRespEvent{fromId:1, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:2, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:3, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:4, term:1, success:true, lastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:5, term:1, success:true, lastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{fromId:6, term:1, success:true, lastLogIndex:4}
    actions = server.processEvent(event)
    
    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.commitIndex, 1, "Commit index should not change")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}


/********************************************************************************************
 *                                                                                          *
 *                                  Vote Request Testing                                    *
 *                                                                                          *
 ********************************************************************************************/

/********************************
 *  Positive test cases         *
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
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.voteGranted, true, "Vote was not granted")
                expect(t, e1.term, int(1), "Latest term was not sent")
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
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.voteGranted, true, "Vote was not granted")
                expect(t, e1.term, int(1), "Latest term was not sent")
            default:
                t.Errorf("Invalid event returned")
            }
        default:
            t.Errorf("Invalid action returned")
        }
    }
}


func TestVoteRequest_duplecate_request_from_already_voted_candidate(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    // Change current term of server by appending valid entries from term 2
    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{fromId:2, term:2, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{fromId:1, term:3, lastLogIndex:5, lastLogTerm:3}
    actions  = server.processEvent(event)
    event    = requestVoteEvent{fromId:1, term:3, lastLogIndex:5, lastLogTerm:3}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.voteGranted, true, "Vote was not granted")
                expect(t, e1.term, int(3), "Latest term was not sent")
                expect(t, server.currentTerm, int(3), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().index, "Server log length doesn't match with index of last log entry")
            default:
                t.Errorf("Invalid event returned")
            }
        default:
            t.Errorf("Invalid action returned")
        }
    }
}

/********************************
 *  Negative test cases         *
 ********************************/

func TestVoteRequest_request_from_old_candidate(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    // Change current term of server by appending valid entries from term 2
    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{fromId:2, term:2, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{fromId:1, term:1, lastLogIndex:0, lastLogTerm:0}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.voteGranted, false, "Vote was granted to old candidate")
                expect(t, e1.term, int(2), "Latest term was not sent")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().index, "Server log length doesn't match with index of last log entry")
            default:
                t.Errorf("Invalid event returned")
            }
        default:
            t.Errorf("Invalid action returned")
        }
    }
}


func TestVoteRequest_voted_for_different_candidate_for_same_term(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)

    // Change current term of server by appending valid entries from term 2
    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:2, index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{fromId:2, term:2, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{fromId:1, term:3, lastLogIndex:5, lastLogTerm:3}
    actions  = server.processEvent(event)
    event    = requestVoteEvent{fromId:3, term:3, lastLogIndex:9, lastLogTerm:3}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(3), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.voteGranted, false, "Vote was granted for both candidates")
                expect(t, e1.term, int(3), "Latest term was not sent")
                expect(t, server.currentTerm, int(3), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, server.votedFor, int(1), "Voted to different candidate, expected candidate:1")
                expect(t, int(len(server.log)-1), server.getLastLog().index, "Server log length doesn't match with index of last log entry")
            default:
                t.Errorf("Invalid event returned")
            }
        default:
            t.Errorf("Invalid action returned")
        }
    }
}




/********************************************************************************************
 *                                                                                          *
 *                            Vote Request Response Testing                                 *
 *                                                                                          *
 ********************************************************************************************/

func TestVoteRequestResponseBasic1(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions := server.processEvent(event1)


    // server should drop this event as follower with same term
    event   := requestVoteRespEvent{fromId:1, term:1, voteGranted:false}
    actions = server.processEvent(event)

    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, FOLLOWER, "State should remain follower")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }



    // Make candidate
    server.myState  = CANDIDATE
    server.votedFor = server.server_id
    server.receivedVote[server.server_id] = server.currentTerm  // voting to self

    // server should drop this event
    event   = requestVoteRespEvent{fromId:1, term:0, voteGranted:false}
    actions = server.processEvent(event)

    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, CANDIDATE, "State should remain candidate")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }


    event   = requestVoteRespEvent{fromId:1, term:4, voteGranted:false}
    actions = server.processEvent(event)
    
    expect(t, server.currentTerm, 4, "Current term expected to be 1")

    expect(t, server.myState, FOLLOWER, "State should change to follower")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}



func TestVoteRequestResponse_election_win(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions := server.processEvent(event1)

    // Make candidate
    server.myState  = CANDIDATE
    server.votedFor = server.server_id
    server.receivedVote[server.server_id] = server.currentTerm  // voting to self

    event   := requestVoteRespEvent{fromId:1, term:1, voteGranted:true}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:2, term:1, voteGranted:true}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:3, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:4, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:5, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:6, term:1, voteGranted:true}
    actions = server.processEvent(event)
    
    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, LEADER, "State should change to leader")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case appendRequestEvent:
                // valid empty heartbeat event
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}



func TestVoteRequestResponse_election_lose(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions := server.processEvent(event1)

    // Make candidate
    server.myState  = CANDIDATE
    server.votedFor = server.server_id
    server.receivedVote[server.server_id] = server.currentTerm  // voting to self

    event   := requestVoteRespEvent{fromId:1, term:1, voteGranted:true}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:2, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:3, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:4, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:5, term:1, voteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{fromId:6, term:1, voteGranted:true}
    actions = server.processEvent(event)
    
    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, FOLLOWER, "State should change to follower")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}




/********************************************************************************************
 *                                                                                          *
 *                                    Timeout Testing                                       *
 *                                                                                          *
 ********************************************************************************************/

func TestTimeout_leader(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    // Updating server term from 0 to 1 with valid append request
    event1   := timeoutEvent{}
    actions  := server.processEvent(event1)
    
    expect(t, server.currentTerm, 0, "Current term expected to be 0")

    expect(t, server.myState, LEADER, "State should remain leader")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case appendRequestEvent:
                // valid empty heartbeat event
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}

func TestTimeout_follower(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions  := server.processEvent(event1)

    event    := timeoutEvent{}
    actions   = server.processEvent(event)
    
    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, CANDIDATE, "State should change to candidate")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case requestVoteEvent:
                // valid vote request events
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}


func TestTimeout_candidate(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{term:0, index:1}
    logs    = append(logs, log)
    log     = LogEntry{term:0, index:2}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:3}
    logs    = append(logs, log)
    log     = LogEntry{term:1, index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{fromId:1, term:1, prevLogIndex:0, prevLogTerm:0, entries:logs, leaderCommit:1}
    actions  := server.processEvent(event1)

    // Make candidate
    server.myState  = CANDIDATE
    server.votedFor = server.server_id
    server.receivedVote[server.server_id] = server.currentTerm  // voting to self

    event    := timeoutEvent{}
    actions   = server.processEvent(event)
    
    expect(t, server.currentTerm, 1, "Current term expected to be 1")

    expect(t, server.myState, CANDIDATE, "State should remain candidate")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case requestVoteEvent:
                // valid vote request events
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}





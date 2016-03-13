package main

import (
    "testing"
    "reflect"
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

// Initialise previously empty logs of server with some entries
func (server * ServerState) initialiseLogs() []interface{} {
    logs    := make([]LogEntry, 0)
    log     := LogEntry{Term:0, Index:1}
    logs     = append(logs, log)
    log      = LogEntry{Term:0, Index:2}
    logs     = append(logs, log)
    log      = LogEntry{Term:1, Index:3}
    logs     = append(logs, log)
    log      = LogEntry{Term:1, Index:4}
    logs     = append(logs, log)
    event   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event)

    return actions
}

/********************************************************************************************
 *                                                                                          *
 *                                  Append Request Testing                                  *
 *                                                                                          *
 ********************************************************************************************/

func TestAppendRequestBasic(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 11)
    actions := server.initialiseLogs()

    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.Success, true, "Append request failed for valid request")
                expect(t, e1.Term, int(1), "Latest term was not sent")
            default:
                t.Errorf("Invalid event returned")
            }
        case alarmAction:
            alarmActionReceived = true
        case logStore:
            // valid log store actions
        default:
            t.Errorf("Invalid action returned")
        }
    }
    expect(t, alarmActionReceived, true, "Alarm action not received")
}


func TestAppendRequest_leader_with_old_term(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 11)
    server.initialiseLeader()

    actions := server.initialiseLogs()

    alarmActionReceived := false
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestRespEvent:
                e1 := action.event.(appendRequestRespEvent)
                expect(t, e1.Success, true, "Append request failed for valid request")
                expect(t, e1.Term, 1, "Latest term was not sent")
                expect(t, server.myState, FOLLOWER, "Server state didn't change from leader to follower")
            default:
                t.Errorf("Invalid event returned")
            }
        case alarmAction:
            alarmActionReceived = true
        case logStore:
            // valid log store actions
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:3}
    logs    = append(logs, log)

    event   := appendRequestEvent{FromId:1, Term:0, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}

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
                expect(t, e1.Success, false, "Append request success for invalid request")
                expect(t, e1.Term, int(-1), "As this is fatal error, -1 was expected")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event)

    // append request from 2nd node with term 0
    event   = appendRequestEvent{FromId:2, Term:0, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:1}
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
                expect(t, e1.Success, false, "Append request success for invalid request")
                expect(t, e1.Term, int(1), "Append request old leader has changed the currentTerm of server")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event)

    logs    = make([]LogEntry, 0)       // Creating new logs
    log     = LogEntry{Term:2, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:4}
    logs    = append(logs, log)

    // append request from 2nd node with term 2
    event   = appendRequestEvent{FromId:2, Term:2, PrevLogIndex:2, PrevLogTerm:0, Entries:logs, LeaderCommit:10}
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
                expect(t, e1.Success, true, "Append request failed for valid request")
                expect(t, e1.Term, int(2), "latest term was not returned")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().Index, "Server log length doesn't match with index of last log entry")
                expect(t, server.commitIndex, int(len(server.log)-1), "Commit index was not updated")
            default:
                t.Errorf("Invalid event returned")
            }
        case alarmAction:
            alarmActionReceived = true
        case logStore:
            // valid log store actions
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event)

    logs    = make([]LogEntry, 0)       // Creating new logs
    log     = LogEntry{Term:2, Index:11}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:12}
    logs    = append(logs, log)

    // append request from 2nd node with term 2
    event   = appendRequestEvent{FromId:2, Term:2, PrevLogIndex:10, PrevLogTerm:1, Entries:logs, LeaderCommit:3}
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
                expect(t, e1.Success, false, "Append request should have failed as there are logs missing from index 5 to 10")
                expect(t, e1.Term, int(2), "latest term was not returned")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().Index, "Server log length doesn't match with index of last log entry")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
    server.matchIndex[server.server_id] = server.getLastLog().Index

    event   := appendRequestRespEvent{FromId:1, Term:1, Success:true, LastLogIndex:2}

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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
    server.matchIndex[server.server_id] = server.getLastLog().Index

    event   := appendRequestRespEvent{FromId:1, Term:2, Success:false, LastLogIndex:2}

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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
    server.matchIndex[server.server_id] = server.getLastLog().Index

    event   := appendRequestRespEvent{FromId:1, Term:1, Success:false, LastLogIndex:-1}

    actions = server.processEvent(event)

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, 1, "Response sent to wrong node")
            switch action.event.(type) {
            case appendRequestEvent:
                e1 := action.event.(appendRequestEvent)
                expect(t, e1.FromId, server.server_id, "wrong fromId")
                expect(t, e1.Term, 1, "Latest term was not sent")
                expect(t, e1.PrevLogIndex, 3, "Wrong prevLogIndex received")
                expect(t, e1.PrevLogTerm, 1, "Wrong prevLogTerm received")
                expect(t, e1.Entries[0].Index, 4, "Wrong log entry received")
                
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:2}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
    server.matchIndex[server.server_id] = server.getLastLog().Index

    event   := appendRequestRespEvent{FromId:1, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:2, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:3, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:4, Term:1, Success:true, LastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:5, Term:1, Success:true, LastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:6, Term:1, Success:true, LastLogIndex:4}
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
        case commitAction:
            // valid commit actions
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}



func TestAppendRequestResponse_commit_check1(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    logs    := make([]LogEntry, 0)
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:1, Index:4}
    logs    = append(logs, log)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendRequestEvent{FromId:1, Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:1}
    actions := server.processEvent(event1)

    // Make leader
    server.myState  = LEADER
    server.votedFor = server.server_id
    for i:=0 ; i<server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
    server.matchIndex[server.server_id] = server.getLastLog().Index

    event   := appendRequestRespEvent{FromId:1, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:2, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:3, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:4, Term:1, Success:true, LastLogIndex:2}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:5, Term:1, Success:true, LastLogIndex:3}
    actions = server.processEvent(event)
    event   = appendRequestRespEvent{FromId:6, Term:1, Success:true, LastLogIndex:4}
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

    event := requestVoteEvent{FromId:1, Term:1, LastLogIndex:0, LastLogTerm:0}
    actions := server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.VoteGranted, true, "Vote was not granted")
                expect(t, e1.Term, int(1), "Latest term was not sent")
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

    event := requestVoteEvent{FromId:1, Term:1, LastLogIndex:0, LastLogTerm:0}
    actions := server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.VoteGranted, true, "Vote was not granted")
                expect(t, e1.Term, int(1), "Latest term was not sent")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{FromId:2, Term:2, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{FromId:1, Term:3, LastLogIndex:5, LastLogTerm:3}
    actions  = server.processEvent(event)
    event    = requestVoteEvent{FromId:1, Term:3, LastLogIndex:5, LastLogTerm:3}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.VoteGranted, true, "Vote was not granted")
                expect(t, e1.Term, int(3), "Latest term was not sent")
                expect(t, server.currentTerm, int(3), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().Index, "Server log length doesn't match with index of last log entry")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{FromId:2, Term:2, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{FromId:1, Term:1, LastLogIndex:0, LastLogTerm:0}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(1), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.VoteGranted, false, "Vote was granted to old candidate")
                expect(t, e1.Term, int(2), "Latest term was not sent")
                expect(t, server.currentTerm, int(2), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, int(len(server.log)-1), server.getLastLog().Index, "Server log length doesn't match with index of last log entry")
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
    log     := LogEntry{Term:0, Index:1}
    logs    = append(logs, log)
    log     = LogEntry{Term:0, Index:2}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:3}
    logs    = append(logs, log)
    log     = LogEntry{Term:2, Index:4}
    logs    = append(logs, log)
    event1   := appendRequestEvent{FromId:2, Term:2, PrevLogIndex:0, PrevLogTerm:0, Entries:logs, LeaderCommit:3}
    actions := server.processEvent(event1)


    event   := requestVoteEvent{FromId:1, Term:3, LastLogIndex:5, LastLogTerm:3}
    actions  = server.processEvent(event)
    event    = requestVoteEvent{FromId:3, Term:3, LastLogIndex:9, LastLogTerm:3}
    actions  = server.processEvent(event)
    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            expect(t, action.toId, int(3), "Response sent to wrong node")
            switch action.event.(type) {
            case requestVoteRespEvent:
                e1 := action.event.(requestVoteRespEvent)
                expect(t, e1.VoteGranted, false, "Vote was granted for both candidates")
                expect(t, e1.Term, int(3), "Latest term was not sent")
                expect(t, server.currentTerm, int(3), "currentTerm of server was not updated")
                expect(t, server.myState, FOLLOWER, "Server state changed, expected state:follower")
                expect(t, server.votedFor, int(1), "Voted to different candidate, expected candidate:1")
                expect(t, int(len(server.log)-1), server.getLastLog().Index, "Server log length doesn't match with index of last log entry")
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
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()

    // server should drop this event as follower with same term
    event   := requestVoteRespEvent{FromId:1, Term:1, VoteGranted:false}
    actions := server.processEvent(event)

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

    // make candidate
    server.processEvent ( timeoutEvent{} )  
    expect(t, server.currentTerm, 2, "Current term expected to be 2")


    // server should drop this event
    event   = requestVoteRespEvent{FromId:1, Term:0, VoteGranted:false}
    actions = server.processEvent(event)

    expect(t, server.currentTerm, 2, "Current term expected to be 2")
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


    event   = requestVoteRespEvent{FromId:1, Term:4, VoteGranted:false}
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
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()
    server.processEvent ( timeoutEvent{} )  // make candidate
    expect(t, server.currentTerm, 2, "Current term expected to be 2")

    event   := requestVoteRespEvent{FromId:1, Term:2, VoteGranted:true}
    actions := server.processEvent(event)
    event   = requestVoteRespEvent{FromId:2, Term:2, VoteGranted:true}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:3, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:4, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:5, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:6, Term:2, VoteGranted:true}
    actions = server.processEvent(event)
    
    expect(t, server.currentTerm, 2, "Current term expected to be 2")
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
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()
    server.processEvent ( timeoutEvent{} )  // make candidate
    expect(t, server.currentTerm, 2, "Current term expected to be 2")

    event   := requestVoteRespEvent{FromId:1, Term:2, VoteGranted:true}
    actions := server.processEvent(event)
    event   = requestVoteRespEvent{FromId:2, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:3, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:4, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:5, Term:2, VoteGranted:false}
    actions = server.processEvent(event)
    event   = requestVoteRespEvent{FromId:6, Term:2, VoteGranted:true}
    actions = server.processEvent(event)

    expect(t, server.myState,       FOLLOWER,   "State should change to follower")
    expect(t, server.currentTerm,   2,          "Current term expected to be 2")

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
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()
    server.initialiseLeader()

    actions  := server.processEvent( timeoutEvent{} )
    
    expect(t, server.currentTerm,   1,      "Current term expected to be 1")
    expect(t, server.myState,       LEADER, "State should remain leader")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case appendRequestEvent:
                expect(t, action.toId, -1,  "The append request should be broadcast")
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
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()

    actions := server.processEvent( timeoutEvent{} )    // make candidate by using timeout
    expect(t,   server.myState,     CANDIDATE,  "State should be candidate")
    expect(t,   server.currentTerm, 2,          "Current term expected to be 2")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case requestVoteEvent:
                expect(t, action.toId, -1,  "The vote request should be broadcast")
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        case alarmAction:
            // valid alarm action
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}


func TestTimeout_candidate(t *testing.T) {
    server := ServerState{}
    server.setupServer(FOLLOWER, 7)
    server.initialiseLogs()

    actions := server.processEvent( timeoutEvent{} )    // make candidate by using timeout
    expect(t,   server.myState,       CANDIDATE,  "State should be candidate")
    expect(t,   server.currentTerm,   2,          "Current term expected to be 2")
    actions  = server.processEvent( timeoutEvent{} )    // timeout candidate again
    expect(t,   server.myState,       CANDIDATE,  "State should remain candidate")
    expect(t,   server.currentTerm,   3,          "Current term expected to be 3")

    for _, action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)
            switch action.event.(type) {
            case requestVoteEvent:
                expect(t, action.toId, -1,  "The vote request should be broadcast")
            default:
                t.Errorf("Invalid event returned:%v", reflect.TypeOf(action.event).String() )
            }
        case alarmAction:
            // valid alarm action
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}



/********************************************************************************************
 *                                                                                          *
 *                              Append Client Testing                                       *
 *                                                                                          *
 ********************************************************************************************/

func TestAppendClient_leader(t *testing.T) {
    server := ServerState{}
    server.setupServer(LEADER, 7)

    // Updating server term from 0 to 1 with valid append request
    event1   := appendEvent{}
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
        case logStore:
            // valid log store action
        default:
            t.Errorf("Invalid action returned:%v", reflect.TypeOf(action).String() )
        }
    }
}


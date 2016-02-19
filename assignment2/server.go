package main

import (
    "sort"
)


var TIMEOUTTIME = int(1000);  // Timeout in ms

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
    term    int
    index   int
}

/********************************************************************
 *                                                                  *
 *                          Input events                            *
 *                                                                  *
 ********************************************************************/
type appendRequestEvent struct {
    fromId          int
    term            int
    //leaderId      int   // same as fromId
    prevLogIndex    int
    prevLogTerm     int
    entries         []LogEntry
    leaderCommit    int
}

type appendRequestRespEvent struct {
    fromId          int
    term            int
    success         bool
    lastLogIndex    int   // Helps in updating nextIndex & matchIndex
}

type requestVoteEvent struct {
    fromId          int
    term            int
    // candidateId      int   // same as fromId
    lastLogIndex    int
    lastLogTerm     int
}

type requestVoteRespEvent struct {
    fromId      int
    term        int
    voteGranted bool
}

type timeoutEvent struct {

}

type appendEvent struct {
    // ignoring data
}


/********************************************************************
 *                                                                  *
 *                          Output actions                          *
 *                                                                  *
 ********************************************************************/
type sendAction struct {
    toId    int       // for broadcast, set to -1
    event   interface{}
}

type commitAction struct {
    index   int       // for error, set to -1
    data    []byte
    err     string
}

type logStore struct {
    index   int
    data    []byte
}

type alarmAction struct {
    time    int
}


/********************************************************************
 *                                                                  *
 *                          Server status                           *
 *                                                                  *
 ********************************************************************/
type ServerState struct {
    // Persistent state
    server_id       int
    currentTerm     int
    votedFor        int     // -1: not voted
    numberOfNodes   int

    // log is initialised with single empty log, to make life easier in future checking
    // Index starts from 1, as first empty entry is present
    log         []LogEntry

    // Non-persistent state
    commitIndex     int         // initialised to -1
    // TODO:: lastApplied
    nextIndex       []int
    matchIndex      []int
    myState         int         // CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

    // maintain received votes from other nodes, 
    // if vote received, set corresponding value to term for which the vote has received
    // -ve value represents negative vote
    receivedVote    []int
}

func (server *ServerState) setupServer ( state int, numberOfNodes int ) {
    server.server_id    = 0
    server.currentTerm  = 0
    server.votedFor     = -1
    server.numberOfNodes= numberOfNodes
    server.log          = make([]LogEntry, 0)
    server.log          = append(server.log, LogEntry{term:0, index:0}) // Initialising log with single empty log, to make life easier in future checking

    server.commitIndex  = -1
    server.nextIndex    = make([]int, numberOfNodes)
    server.matchIndex   = make([]int, numberOfNodes)
    server.receivedVote = make([]int, numberOfNodes)
    server.myState      = state


    for i := int(0); i < numberOfNodes; i++ {
        server.nextIndex[i]     = 0
        server.matchIndex[i]    = 0
    }
}

//  Returns last log entry
func (server *ServerState) getLastLog () LogEntry {
    return server.log[len(server.log) - 1]
}

/********************************************************************
 *                                                                  *
 *                          Vote Request                            *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) voteRequest ( event requestVoteEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if ( event.term < server.currentTerm ) {
        // In any state, if old termed candidate request vote, tell it to be a follower
        voteResp    := requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:false}
        resp        := sendAction{toId:event.fromId , event:voteResp}
        actions = append(actions, resp)
        return actions
    } else if event.term > server.currentTerm {
        // Request from more up-to-date node, so lets update our state
        server.currentTerm  = event.term
        server.myState      = FOLLOWER
        server.votedFor     = -1
    }

    // requester_term >= server.current_term
    // If not voted for this term
    if server.votedFor == -1 {
        // votedFor will be -1 ONLY for follower state, in case of leader/candidate it will be set to self id
        if event.lastLogTerm > server.getLastLog().term || event.lastLogTerm == server.getLastLog().term && event.lastLogIndex >= server.getLastLog().index {
            server.votedFor = event.fromId

            voteResp    := requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:true}
            resp        := sendAction{toId:event.fromId , event:voteResp}
            actions = append(actions, resp)
            return actions
        }
    } else {
        // If voted for this term, check if request is from same candidate for which this node has voted
        if server.votedFor == event.fromId {
            // Vote again to same candidate
            voteResp    := requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:true}
            resp        := sendAction{toId:event.fromId , event:voteResp}
            actions = append(actions, resp)
            return actions
        }
    }

    // For already voted for same term to different candidate,
    // Or not voted but requester's logs are old,
    // reject all requests
    voteResp := requestVoteRespEvent{fromId:server.server_id , term:server.currentTerm, voteGranted:false}
    resp := sendAction{toId:event.fromId , event:voteResp}
    actions = append(actions, resp)
    return actions
}



/********************************************************************
 *                                                                  *
 *                      Vote Request Response                       *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) voteRequestResponse ( event requestVoteRespEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if server.currentTerm < event.term {
        // This server term is not so up-to-date, so update
        server.myState      = FOLLOWER
        server.currentTerm  = event.term
        server.votedFor     = -1
        return actions
    } else if server.currentTerm > event.term {
        // Simply drop the response
        return actions
    }

    switch(server.myState) {
        case LEADER, FOLLOWER:
            return actions

        case CANDIDATE:
            // Refer comments @ receivedVote declaration
            vote := server.receivedVote[event.fromId]
            if vote < 0 {
                vote = -vote
            }

            if vote < event.term {
                if event.voteGranted {
                    server.receivedVote[event.fromId] = event.term
                } else {
                    server.receivedVote[event.fromId] = -event.term
                }
                count   := 0
                ncount  := 0
                for _,vote := range server.receivedVote {
                    if vote == event.term {
                        count++
                    } else if vote == -event.term {
                        ncount++
                    }
                }
                //fmt.Printf("eventTerm:%v\n COUNTING : %v : %v : %v\n",event.term, count,ncount, server.receivedVote)

                if ncount > server.numberOfNodes/2 {
                    // majority of -ve votes, so change to follower
                    server.myState = FOLLOWER
                    return actions
                } else if count > server.numberOfNodes/2 {
                    // become leader
                    server.myState      = LEADER
                    server.matchIndex   = make([]int, server.numberOfNodes)
                    server.nextIndex    = make([]int, server.numberOfNodes)
                    server.matchIndex[server.server_id] = server.getLastLog().index

                    // Send empty appendRequests
                    for i:=0 ; i<server.numberOfNodes ; i++ {
                        server.nextIndex[i] = server.getLastLog().index+1

                        if i != server.server_id {
                            event1      := appendRequestEvent{
                                            fromId          : server.server_id, 
                                            term            : server.currentTerm, 
                                            prevLogIndex    : server.getLastLog().index, 
                                            prevLogTerm     : server.getLastLog().term, 
                                            entries         : []LogEntry{}, 
                                            leaderCommit    : server.commitIndex}
                            action      := sendAction {toId : i, event : event1 }
                            actions      = append(actions, action)
                        }
                    }
                }
            }
    }

    return actions
}

/********************************************************************
 *                                                                  *
 *                          Append Request                          *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendRequest ( event appendRequestEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if server.currentTerm > event.term {
        // Append request is not from latest leader
        // In all states applicable
        appendResp := appendRequestRespEvent{fromId:server.server_id , term:server.currentTerm, success:false, lastLogIndex:server.getLastLog().index}
        resp := sendAction{toId:event.fromId , event:appendResp}
        actions = append(actions, resp)
        return actions
    }

    switch(server.myState) {
        case LEADER:
            // mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
            if ( event.term == server.currentTerm ) {
                appendResp := appendRequestRespEvent{fromId:server.server_id, term:-1, success:false, lastLogIndex:server.getLastLog().index}
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

            if server.currentTerm < event.term {
                // This server term is not so up-to-date, so update
                server.currentTerm  = event.term
                server.votedFor     = -1
                //fmt.Printf("\nUPDATING\n\n")
            }

            if ( server.getLastLog().index < event.prevLogIndex || server.log[event.prevLogIndex].term != event.prevLogTerm ) {
                // Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
                appendResp := appendRequestRespEvent{fromId:server.server_id, term:server.currentTerm, success:false, lastLogIndex:server.getLastLog().index}
                resp := sendAction{toId:event.fromId , event:appendResp}
                actions = append(actions, resp)
                return actions
            }

            if( server.getLastLog().index > event.prevLogIndex ) {
                // There are entries from last leaders
                // Strip them up to the end
                server.log = server.log[:event.prevLogIndex+1]
            }

            if len(event.entries) == 0 {
                // Empty log entries for heartbeat
                return actions
            } else {
                // Update log if entries are not present
                server.log = append(server.log, event.entries...)
    
                for _, log := range event.entries {
                    action := logStore{ index: log.index, data:[]byte{}}
                    actions = append(actions,action)
                }
    
                if ( event.leaderCommit > server.commitIndex ) {
                    // If leader has commited entries, so should this server
                    if event.leaderCommit < int(len(server.log)-1) {
                        server.commitIndex = event.leaderCommit
                    } else {
                        server.commitIndex = int(len(server.log)-1)
                    }
                }
            }
    }

    appendResp := appendRequestRespEvent{fromId:server.server_id, term:server.currentTerm, success:true, lastLogIndex:server.getLastLog().index}
    resp := sendAction{toId:event.fromId , event:appendResp}
    actions = append(actions, resp)
    return actions
}




/********************************************************************
 *                                                                  *
 *                    Append Request Response                       *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendRequestResponse ( event appendRequestRespEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if server.currentTerm < event.term {
        // This server term is not so up-to-date, so update
        server.myState      = FOLLOWER
        server.currentTerm  = event.term
        server.votedFor     = -1
        return actions
    }

    switch(server.myState) {
        case LEADER:
            if ! event.success {
                // there are holes in follower's log
                server.nextIndex[event.fromId] = server.nextIndex[event.fromId] - 1

                // Resend all logs from the holes to the end
                prevLog     := server.log[server.nextIndex[event.fromId]-1]
                startIndex  := server.nextIndex[event.fromId]
                logs        := append([]LogEntry{}, server.log[ startIndex : ]...)  // copy server.log from startIndex to the end to "logs"
                event1      := appendRequestEvent{
                                fromId          : server.server_id, 
                                term            : server.currentTerm, 
                                prevLogIndex    : prevLog.index, 
                                prevLogTerm     : prevLog.term, 
                                entries         : logs, 
                                leaderCommit    : server.commitIndex}
                action      := sendAction {toId : event.fromId, event : event1 }
                actions     = append(actions, action)
                return actions
            } else if event.lastLogIndex > server.matchIndex[event.fromId] {
                server.matchIndex[event.fromId] = event.lastLogIndex
            }

            // lets sort
            sorted := append([]int{}, server.matchIndex...)
            //matchCopy = []int{4,3,7,9,1,6}
            sort.IntSlice(sorted).Sort() // sort in ascending order
            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N .
            //fmt.Printf("\n\nSorted matchindices : %v\n\n", sorted)
            //fmt.Printf("\n\nLOG ENTRIES ---------------------------------------\n : %v\n\n", server.log)
            for i := server.numberOfNodes/2; i >= 0 ; i-- {
                // First N/2+1 of sorted matchindex
                // slice all are less than or equal to a majority of
                // matchindices

                //fmt.Printf("\nsorted[i]: %v ::: currterm: %v", sorted[i], server.currentTerm)
                if sorted[i] > server.commitIndex && server.log[sorted[i]].term == server.currentTerm {
                    
                    // Commit all not committed eligible entries
                    for k:=server.commitIndex+1 ; k<=sorted[i] ; k++ {
                        action := commitAction { 
                                    index   : k,
                                    data    : []byte{},
                                    err     : "" }
                        actions = append(actions, action)
                    }

                    server.commitIndex = sorted[i]
                    break
                }
            }

            // continue flow to next case for server.currentTerm < event.term
            fallthrough
        case CANDIDATE:
            fallthrough
        case FOLLOWER:
    }

    return actions
}


/********************************************************************
 *                                                                  *
 *                          Timeout                                 *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) timeout ( event timeoutEvent ) []interface{} {

    actions := make([]interface{}, 0)

    switch(server.myState) {
        case LEADER:
            // Send empty appendRequests
            for i:=0 ; i<server.numberOfNodes ; i++ {

                if i != server.server_id {
                    event1      := appendRequestEvent{
                                    fromId          : server.server_id, 
                                    term            : server.currentTerm, 
                                    prevLogIndex    : server.getLastLog().index, 
                                    prevLogTerm     : server.getLastLog().term, 
                                    entries         : []LogEntry{}, 
                                    leaderCommit    : server.commitIndex}
                    action      := sendAction {toId : i, event : event1 }
                    actions      = append(actions, action)
                }
            }
        case CANDIDATE:
            // Restart election
            fallthrough
        case FOLLOWER:
            // Start election
            server.myState = CANDIDATE
            server.votedFor = server.server_id
            server.receivedVote[server.server_id] = server.currentTerm  // voting to self

            // Vote request to all
            for i:=0 ; i<server.numberOfNodes ; i++ {

                if i != server.server_id {
                    event1      := requestVoteEvent{
                                    fromId          : server.server_id, 
                                    term            : server.currentTerm, 
                                    lastLogIndex    : server.getLastLog().index, 
                                    lastLogTerm     : server.getLastLog().term}
                    action      := sendAction {toId : i, event : event1 }
                    actions      = append(actions, action)
                }
            }
    }
    return actions
}



/********************************************************************
 *                                                                  *
 *                     Append from client                           *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendClientRequest ( event appendEvent ) []interface{} {

    actions := make([]interface{}, 0)

    switch(server.myState) {
        case LEADER:
            // append to self
            log := LogEntry{index:server.getLastLog().index+1, term:server.currentTerm}
            server.log = append(server.log, log)

            action := logStore{ index: log.index, data:[]byte{}}
            actions = append(actions,action)

            logs := append([]LogEntry{}, log)
            // Send appendRequests to all
            for i:=0 ; i<server.numberOfNodes ; i++ {

                if i != server.server_id {
                    event1      := appendRequestEvent{
                                    fromId          : server.server_id, 
                                    term            : server.currentTerm, 
                                    prevLogIndex    : server.getLastLog().index, 
                                    prevLogTerm     : server.getLastLog().term, 
                                    entries         : logs, 
                                    leaderCommit    : server.commitIndex}
                    action      := sendAction {toId : i, event : event1 }
                    actions      = append(actions, action)
                }
            }
        case CANDIDATE:
        case FOLLOWER:
    }
    return actions
}





/********************************************************************
 *                                                                  *
 *                          Process event                           *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) processEvent ( event interface{} ) []interface{} {
    // Initialise the variables and timeout

    switch event.(type) {
        case appendRequestEvent:
            return server.appendRequest(event.(appendRequestEvent))
        case appendRequestRespEvent:
            return server.appendRequestResponse(event.(appendRequestRespEvent))
        case requestVoteEvent:
            return server.voteRequest(event.(requestVoteEvent))
        case requestVoteRespEvent:
            return server.voteRequestResponse(event.(requestVoteRespEvent))
        case timeoutEvent:
            return server.timeout(event.(timeoutEvent))
        case appendEvent:
            return server.appendClientRequest(event.(appendEvent))
        default:
            return nil
    }

    return make([]interface{},0)
}

/*
var server ServerState
func main () {
    server.setupServer(FOLLOWER,10)
}*/ 


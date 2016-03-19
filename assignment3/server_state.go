package main

import (
    "sort"
    "fmt"
    "strconv"
"math/rand"
)


// Debugging tools
func (server_state *ServerState) prnt(format string, args ...interface{}) {
    fmt.Printf(strconv.Itoa(server_state.currentTerm) + " [RSM\t: " + strconv.Itoa(server_state.server_id) + "] \t" + format + "\n", args...)
}

//var TIMEOUTTIME = int(100);  // Timeout in ms

const (
    CANDIDATE=3;
    FOLLOWER=2;
    LEADER=1;
    )

type LogEntry struct {
    Term  int
    Index int
    Data  interface{}
}

/********************************************************************
 *                                                                  *
 *                          Input events                            *
 *                                                                  *
 ********************************************************************/
type appendRequestEvent struct {
    FromId       int
    Term         int
    //leaderId      int   // same as fromId
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type appendRequestRespEvent struct {
    FromId       int
    Term         int
    Success      bool
    LastLogIndex int // Helps in updating nextIndex & matchIndex
}

type requestVoteEvent struct {
    FromId       int
    Term         int
    // candidateId      int   // same as fromId
    LastLogIndex int
    LastLogTerm  int
}

type requestVoteRespEvent struct {
    FromId      int
    Term        int
    VoteGranted bool
}

type timeoutEvent struct {

}

type appendEvent struct {
    data []byte
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

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type commitAction struct {
    index   int       // for error, set to -1
    data    LogEntry
    err     string
}

type logStore struct {
    index   int
    data    LogEntry      // Data is of LogEntry type
}

// Make node to store its state on persistent store
type stateStore struct {}

type alarmAction struct {
    time    int
}


/********************************************************************
 *                                                                  *
 *                          Server status                           *
 *                                                                  *
 ********************************************************************/
// {0 1 0 11 [{0 0} {0 1} {0 2} {1 3} {1 4}] 2 0 [5 0 5 5 5 5 5 5 5 5 5 1] [4 1 1 1 1 1 1 1 1 1 1 1] 2 [0 0 0 0 0 0 0 0 0 0 0 0] 0 0}
type ServerState struct {
    // Persistent state
    server_id       int
    currentTerm     int
    votedFor        int     // -1: not voted
    numberOfNodes   int

    // log is initialised with single dummy log, to make life easier in future checking
    // Index starts from 1, as first empty entry is present
    log         []LogEntry

    // Using first 0th dummy entry for all arrays
    // Non-persistent state
    commitIndex     int         // initialised to -1
    lastApplied     int
    nextIndex       []int       // Using first 0th dummy entry for all arrays
    matchIndex      []int       // Using first 0th dummy entry for all arrays
    myState         int         // CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

    // maintain received votes from other nodes, 
    // if vote received, set corresponding value to term for which the vote has received
    // -ve value represents negative vote
    receivedVote    []int       // Using first 0th dummy entry for all arrays

    // Timeouts in milliseconds
    // TODO:: DO we need to reset alarm after state conversion?
    electionTimeout     int
    heartbeatTimeout    int
}

func (server *ServerState) setupServer ( state int, numberOfNodes int ) {
    //server.server_id    = 0
    server.currentTerm  = 0
    server.votedFor     = -1
    server.numberOfNodes= numberOfNodes
    server.log          = make([]LogEntry, 0)
    server.log          = append(server.log, LogEntry{Term:0, Index:0, Data:[]byte("Dummy Log")}) // Initialising log with single empty log, to make life easier in future checking

    server.commitIndex  = 0
    server.nextIndex    = make([]int, numberOfNodes+1)
    server.matchIndex   = make([]int, numberOfNodes+1)
    server.receivedVote = make([]int, numberOfNodes+1)
    server.myState      = state


    for i := 0; i <= numberOfNodes; i++ {
        server.nextIndex[i]     = 1     // Set to index of next log to send
        server.matchIndex[i]    = 0     // Set to last log index on that server, increases monotonically
    }
}

//  Returns last log entry
func (server *ServerState) getLastLog () LogEntry {
    return server.log[len(server.log) - 1]
}

// Broadcast an event, returns array of actions
func (server *ServerState) broadcast ( event interface{}) []interface{} {
    actions     := make([]interface{}, 0)
    action      := sendAction {toId : -1, event : event }   // Sending to -1, -1 is for broadcast
    actions      = append(actions, action)
    return actions
}

// Initialise leader state
func (server *ServerState) initialiseLeader () {
    // become leader
    server.myState                      = LEADER
    server.matchIndex                   = make([]int, server.numberOfNodes+1)
    server.nextIndex                    = make([]int, server.numberOfNodes+1)
    server.matchIndex[server.server_id] = server.getLastLog().Index

    // initialise nextIndex
    for i:=0 ; i<=server.numberOfNodes ; i++ {
        server.nextIndex[i] = server.getLastLog().Index +1
    }
}



/********************************************************************
 *                                                                  *
 *                          Vote Request                            *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) voteRequest ( event requestVoteEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if ( event.Term < server.currentTerm ) {
        // In any state, if old termed candidate request vote, tell it to be a follower
        voteResp    := requestVoteRespEvent{FromId:server.server_id , Term:server.currentTerm, VoteGranted:false}
        resp        := sendAction{toId:event.FromId, event:voteResp}
        actions = append(actions, resp)
        return actions
    } else if event.Term > server.currentTerm {
        // Request from more up-to-date node, so lets update our state
        server.currentTerm  = event.Term
        server.myState      = FOLLOWER
        server.votedFor     = -1
    }

    // requester_term >= server.current_term
    // If not voted for this term
    if server.votedFor == -1 {
        // votedFor will be -1 ONLY for follower state, in case of leader/candidate it will be set to self id
        if event.LastLogTerm > server.getLastLog().Term || event.LastLogTerm == server.getLastLog().Term && event.LastLogIndex >= server.getLastLog().Index {
            server.votedFor = event.FromId

            voteResp    := requestVoteRespEvent{FromId:server.server_id , Term:server.currentTerm, VoteGranted:true}
            resp        := sendAction{toId:event.FromId, event:voteResp}
            actions = append(actions, resp)
            return actions
        }
    } else {
        // If voted for this term, check if request is from same candidate for which this node has voted
        if server.votedFor == event.FromId {
            // Vote again to same candidate
            voteResp    := requestVoteRespEvent{FromId:server.server_id , Term:server.currentTerm, VoteGranted:true}
            resp        := sendAction{toId:event.FromId, event:voteResp}
            actions = append(actions, resp)
            return actions
        }
    }

    // For already voted for same term to different candidate,
    // Or not voted but requester's logs are old,
    // reject all requests
    voteResp := requestVoteRespEvent{FromId:server.server_id , Term:server.currentTerm, VoteGranted:false}
    resp := sendAction{toId:event.FromId, event:voteResp}
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

    if server.currentTerm < event.Term {
        // This server term is not so up-to-date, so update
        server.myState      = FOLLOWER
        server.currentTerm  = event.Term
        server.votedFor     = -1

        alarm  := alarmAction{time:server.electionTimeout+rand.Intn(500)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    } else if server.currentTerm > event.Term {
        // Simply drop the response
        return actions
    }

    switch(server.myState) {
        case LEADER, FOLLOWER:
            return actions

        case CANDIDATE:
            // Refer comments @ receivedVote declaration
            // If vote received from a node, we are storing the term in receivedVote array for which the vote has received.
            // This way we don't need to reinitialise the voted for array every time new election starts
            vote := server.receivedVote[event.FromId]
            if vote < 0 {
                vote = -vote
            }

            if vote < event.Term {
                if event.VoteGranted {
                    server.receivedVote[event.FromId] = event.Term
                } else {
                    server.receivedVote[event.FromId] = -event.Term
                }
                count   := 0
                ncount  := 0
                for _,vote := range server.receivedVote {
                    if vote == event.Term {
                        count++
                    } else if vote == -event.Term {
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

                    server.prnt("Leader has been elected : %v", server.server_id)
                    server.initialiseLeader()

                    appendReq   := appendRequestEvent{
                                            FromId          : server.server_id,
                                            Term            : server.currentTerm,
                                            PrevLogIndex    : server.getLastLog().Index,
                                            PrevLogTerm     : server.getLastLog().Term,
                                            Entries         : []LogEntry{},
                                            LeaderCommit    : server.commitIndex}

                    alarm  := alarmAction{time:server.heartbeatTimeout}
                    actions = append(actions, alarm)
                    appendReqActions    := server.broadcast(appendReq)
                    actions              = append(actions, appendReqActions...)
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

    if server.currentTerm > event.Term {
        // Append request is not from latest leader
        // In all states applicable
        appendResp := appendRequestRespEvent{FromId:server.server_id , Term:server.currentTerm, Success:false, LastLogIndex:server.getLastLog().Index}
        resp := sendAction{toId:event.FromId, event:appendResp}
        actions = append(actions, resp)
        return actions
    }

    switch(server.myState) {
        case LEADER:
            // mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
            if ( event.Term == server.currentTerm ) {
                appendResp := appendRequestRespEvent{FromId:server.server_id, Term:-1, Success:false, LastLogIndex:server.getLastLog().Index}
                resp := sendAction{toId:event.FromId, event:appendResp}
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
            alarm := alarmAction{time:server.electionTimeout+rand.Intn(500)} // slightly greater time to receive heartbeat
            actions = append(actions, alarm)

            if server.currentTerm < event.Term {
                // This server term is not so up-to-date, so update
                server.currentTerm  = event.Term
                server.votedFor     = -1
                //fmt.Printf("\nUPDATING\n\n")
            }


            // HERTBEAT check disabled
            // It was preventing check for changed commitIndex,
            // resulting this server not committing new entries which are committed by leader
            /*
            // Not required to check the last log index for heartbeat event
            if len(event.Entries) == 0 {
                // Empty log entries for heartbeat
                appendResp := appendRequestRespEvent{FromId:server.server_id, Term:server.currentTerm, Success:true, LastLogIndex:server.getLastLog().Index}
                resp := sendAction{toId:event.FromId, event:appendResp}
                actions = append(actions, resp)
                return actions
            }*/

            if ( server.getLastLog().Index < event.PrevLogIndex || server.log[event.PrevLogIndex].Term != event.PrevLogTerm ) {
                // Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
                appendResp := appendRequestRespEvent{FromId:server.server_id, Term:server.currentTerm, Success:false, LastLogIndex:server.getLastLog().Index}
                resp := sendAction{toId:event.FromId, event:appendResp}
                actions = append(actions, resp)
                return actions
            }

            if( server.getLastLog().Index > event.PrevLogIndex ) {
                // There are entries from last leaders
                // truncate them up to the end
                truncatedLogs := server.log[event.PrevLogIndex + 1 : ]
                server.log     = server.log[ : event.PrevLogIndex] // TODO:: is it really prevIndex+1? it should be only prevIndex
                server.prnt("%+v",server)
                server.prnt("Extra logs found, PrevLogIndex was %v, trucating logs: %+v", event.PrevLogIndex, truncatedLogs)
                for _, log := range truncatedLogs {
                    action := commitAction{index:log.Index, data:log, err:"Log truncated"}
                    actions = append(actions,action)
                }
            }


            // Update log if entries are not present
            server.log = append(server.log, event.Entries...)

            for _, log := range event.Entries {
                action := logStore{ index: log.Index, data:log }
                actions = append(actions,action)
            }

            //server.prnt("$$$$$$$$$$$$ Checking commits Leader:%v server:%v", event.LeaderCommit, server.commitIndex)
            if ( event.LeaderCommit > server.commitIndex ) {
                var commitFrom, commitUpto int
                // If leader has commited entries, so should this server
                if event.LeaderCommit < int(len(server.log)-1) {
                    commitFrom = server.commitIndex+1
                    commitUpto = event.LeaderCommit
                } else {
                    commitFrom = server.commitIndex+1
                    commitUpto = int(len(server.log)-1)
                }

                // commit all logs from commitFrom to commitUpto
                for i:=commitFrom ; i<=commitUpto ; i++ {
                    action := commitAction{index:i, data:server.log[i], err:""}
                    actions = append(actions,action)
                    server.prnt("Commiting index %v, data:%v",i,server.log[i].Data)
                }
                server.commitIndex = commitUpto
            }

    }

    // If the append request is heartbeat then ignore responding to it
    // We are updating matchIndex and nextIndex on positive appendRequestResponse, so consume heartbeats
    if len(event.Entries) != 0 {
        appendResp := appendRequestRespEvent{FromId:server.server_id, Term:server.currentTerm, Success:true, LastLogIndex:server.getLastLog().Index}
        resp := sendAction{toId:event.FromId, event:appendResp}
        actions = append(actions, resp)
    }
    return actions
}




/********************************************************************
 *                                                                  *
 *                    Append Request Response                       *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendRequestResponse ( event appendRequestRespEvent ) []interface{} {

    actions := make([]interface{}, 0)

    if server.currentTerm < event.Term {
        // This server term is not so up-to-date, so update
        server.myState      = FOLLOWER
        server.currentTerm  = event.Term
        server.votedFor     = -1

        // reset alarm
        alarm := alarmAction{time:server.electionTimeout+rand.Intn(500)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    }

    switch(server.myState) {
        case LEADER:
            if ! event.Success {
                // there are holes in follower's log
                // TODO::check for  event.LastLogIndex + 1
                if event.LastLogIndex < server.nextIndex[event.FromId] {
                    server.nextIndex[event.FromId] = event.LastLogIndex + 1
                    //server.prnt("THIS is index nextIndex[]:%v   Event:%+v", server.nextIndex[event.FromId], event)
                //server.nextIndex[event.FromId] -= 1
                }

                // Resend all logs from the holes to the end
                prevLog     := server.log[server.nextIndex[event.FromId]-1]
                startIndex  := server.nextIndex[event.FromId]
                logs        := append([]LogEntry{}, server.log[ startIndex : ]...)  // copy server.log from startIndex to the end to "logs"
                event1      := appendRequestEvent{
                                FromId          : server.server_id,
                                Term            : server.currentTerm,
                                PrevLogIndex    : prevLog.Index,
                                PrevLogTerm     : prevLog.Term,
                                Entries         : logs,
                                LeaderCommit    : server.commitIndex}
                action      := sendAction {toId : event.FromId, event : event1 }
                actions     = append(actions, action)
                return actions
            } else if event.LastLogIndex > server.matchIndex[event.FromId] {
                server.matchIndex[event.FromId] = event.LastLogIndex
                server.nextIndex[event.FromId]  = event.LastLogIndex + 1

                // lets sort
                sorted := append([]int{}, server.matchIndex[1:]...)
                //matchCopy = []int{4,3,7,9,1,6}
                sort.IntSlice(sorted).Sort() // sort in ascending order
                // If there exists an N such that N > commitIndex, a majority
                // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                // set commitIndex = N
                //server.prnt("$$$$$$$$$$$$$$$$$$$ Sorted array: %v", sorted)
                for i := server.numberOfNodes/2; i >= 0 ; i-- {
                    if sorted[i] > server.commitIndex && server.log[sorted[i]].Term == server.currentTerm {
                        //server.prnt("$$$$$$$$$$$$$$$$$$$ Found eligible: %v", i)
                        // Commit all not committed eligible entries
                        for k:=server.commitIndex+1 ; k<=sorted[i] ; k++ {
                            action := commitAction {
                                        index   : k,
                                        data    : server.log[k],
                                        err     : "" }
                            actions = append(actions, action)
                        }

                        server.commitIndex = sorted[i]
                        break
                    }
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

            heartbeatEvent      := appendRequestEvent{
                                    FromId          : server.server_id,
                                    Term            : server.currentTerm,
                                    PrevLogIndex    : server.getLastLog().Index,
                                    PrevLogTerm     : server.getLastLog().Term,
                                    Entries         : []LogEntry{},
                                    LeaderCommit    : server.commitIndex}
            heartbeatActions    := server.broadcast(heartbeatEvent)        // broadcast request vote event
            actions              = append(actions, heartbeatActions...)
            actions              = append(actions, alarmAction{time:server.heartbeatTimeout} )
        case CANDIDATE:
            // Restart election
            fallthrough
        case FOLLOWER:
            // Start election
            server.myState      = CANDIDATE
            server.currentTerm  = server.currentTerm+1
            server.votedFor     = server.server_id
            actions             = append(actions, alarmAction{time:server.electionTimeout+rand.Intn(500)} )
            server.receivedVote[server.server_id] = server.currentTerm  // voting to self


            voteReq     := requestVoteEvent{
                            FromId          : server.server_id,
                            Term            : server.currentTerm,
                            LastLogIndex    : server.getLastLog().Index,
                            LastLogTerm     : server.getLastLog().Term}
            voteReqActions  := server.broadcast(voteReq)        // broadcast request vote event
            actions          = append(actions, voteReqActions...)
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
            log  := LogEntry{Index:server.getLastLog().Index +1, Term:server.currentTerm, Data:event.data}
            logs := append([]LogEntry{}, log)

            appendReq   := appendRequestEvent{
                FromId          : server.server_id,
                Term            : server.currentTerm,
                PrevLogIndex    : server.getLastLog().Index,
                PrevLogTerm     : server.getLastLog().Term,
                Entries         : logs,
                LeaderCommit    : server.commitIndex}


            server.log = append(server.log, log)        // Append to self log
            server.matchIndex[server.server_id] = server.getLastLog().Index  // Update self matchIndex

            actions = append(actions, logStore{ index:log.Index, data:log })
            actions = append(actions, server.broadcast(appendReq)...)
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

    //return make([]interface{},0)
}

/*
var server ServerState
func main () {
    server.setupServer(FOLLOWER,10)
}*/ 


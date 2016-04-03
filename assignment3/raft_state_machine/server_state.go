package raft_state_machine

import (
    "cs733/assignment3/logging"
    "encoding/json"
    "fmt"
    "github.com/cs733-iitb/cluster/mock"
    "github.com/cs733-iitb/log"
    "math/rand"
    "os"
    "sort"
    "strconv"
)

/**
 *
 *   Debug Tools
 */
func (server ServerState) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[SM:%v] %v ", strconv.Itoa(server.server_id), strconv.Itoa(server.CurrentTerm)) + format
    logging.Error(skip, format, args...)
}
func (server ServerState) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[SM:%v] %v ", strconv.Itoa(server.server_id), strconv.Itoa(server.CurrentTerm)) + format
    logging.Info(skip, format, args...)
}

type Config struct {
    Id               int    // this node's id. One of the cluster's entries should match.
    LogDir           string // Log file directory for this node
    ElectionTimeout  int
    HeartbeatTimeout int
    NumOfNodes       int
    MockServer       *mock.MockServer
}

const RaftStateFile = "serverState.json"


type RaftState uint

const (
    CANDIDATE RaftState = iota
    FOLLOWER
    LEADER
)
const RandomTimeout = 200

type LogEntry struct {
    Term  int
    Index int
    Data  string
}

/********************************************************************
 *                                                                  *
 *                          Input events                            *
 *                                                                  *
 ********************************************************************/
type AppendRequestEvent struct {
    FromId       int
    Term         int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendRequestRespEvent struct {
    FromId       int
    Term         int
    Success      bool
    LastLogIndex int // Helps in updating nextIndex & matchIndex
}

type RequestVoteEvent struct {
    FromId       int
    Term         int
    LastLogIndex int
    LastLogTerm  int
}

type RequestVoteRespEvent struct {
    FromId      int
    Term        int
    VoteGranted bool
}

type TimeoutEvent struct {
}

type AppendEvent struct {
    Data string
}

/********************************************************************
 *                                                                  *
 *                          Output actions                          *
 *                                                                  *
 ********************************************************************/
type SendAction struct {
    ToId  int // for broadcast, set to -1
    Event interface{}
}

// Data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type CommitAction struct {
    Index int // for error, set to -1
    Data  LogEntry
    Err   string
}

type LogStore LogEntry
/*
type LogStore struct {
    Index int
    Data  LogEntry // Data is of LogEntry type
}*/

// Store state machine on persistent store
type StateStore struct {
}

type AlarmAction struct {
    Time int
}

/********************************************************************
 *                                                                  *
 *                          Server status                           *
 *                                                                  *
 ********************************************************************/
type ServerState struct {
    // Persistent state
    CurrentTerm   int
    VotedFor      int // -1: not voted
    numberOfNodes int

    // log is initialised with single dummy log, to make life easier in future checking
    // Index starts from 1, as first empty entry is present
    logs            []LogEntry  // Using first 0th dummy entry for all arrays
    PersistentLog   *log.Log    // Persistent log, used to retrieve logs which are not in memory

    // Non-persistent state
    server_id   int
    commitIndex int       // initialised to -1
    lastApplied int
    nextIndex   []int     // Using first 0th dummy entry for all arrays
    matchIndex  []int     // Using first 0th dummy entry for all arrays
    myState     RaftState // CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

    // maintain received votes from other nodes,
    // if vote received, set corresponding value to term for which the vote has received
    // -ve value represents negative vote
    receivedVote []int // Using first 0th dummy entry for all arrays

    // Timeouts in milliseconds
    ElectionTimeout  int
    HeartbeatTimeout int
}

func fromServerStateFile(serverStateFile string) (serState *ServerState, err error) {
    var state ServerState
    var f *os.File

    if f, err = os.Open(serverStateFile); err != nil {
        state.log_error(3, "Unable to open state file : %v", err.Error())
        return nil, err
    }
    defer f.Close()

    dec := json.NewDecoder(f)
    if err = dec.Decode(&state); err != nil {
        state.log_error(3, "Unable to decode state file : %v", err.Error())
        return nil, err
    }
    return &state, nil
}

func (serState *ServerState) ToServerStateFile(serverStateFile string) (err error) {
    var f *os.File
    if f, err = os.Create(serverStateFile); err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    if err = enc.Encode(serState); err != nil {
        return err
    }
    return nil
}

func New(config *Config) (server *ServerState) {
    server = &ServerState{
        server_id       : config.Id,
        CurrentTerm     : 0,
        VotedFor        : -1,
        numberOfNodes   : config.NumOfNodes,
        logs            : []LogEntry{{Term: 0, Index: 0, Data: "Dummy Log"}}, // Initialising log with single empty log, to make life easier in future checking
        commitIndex     : 0,
        nextIndex       : make([]int, config.NumOfNodes+1),
        matchIndex      : make([]int, config.NumOfNodes+1),
        receivedVote    : make([]int, config.NumOfNodes+1),
        myState         : FOLLOWER,
        ElectionTimeout : config.ElectionTimeout,
        HeartbeatTimeout: config.HeartbeatTimeout}

    for i := 0; i <= config.NumOfNodes; i++ {
        server.nextIndex[i] = 1  // Set to index of next log to send
        server.matchIndex[i] = 0 // Set to last log index on that server, increases monotonically
    }

    return server
}

func Restore(config *Config) (server *ServerState) {

    // Restore from file
    restored_state, err := fromServerStateFile(config.LogDir + RaftStateFile)
    if err != nil {
        restored_state.log_error(3, "Unable to restore server state : %v", err.Error())
        return nil
    }

    // Copy persistent state variables yo newly initialized state
    new_state := New(config)
    new_state.CurrentTerm = restored_state.CurrentTerm
    new_state.VotedFor = restored_state.VotedFor
    new_state.logs = make([]LogEntry,0)

    // Restore logs of restored_state from persistent storage
    lg, err := log.Open(config.LogDir)
    if err != nil {
        new_state.log_error(3, "Unable to open log file : %v\n", err)
        return nil
    }
    defer lg.Close()
/*
    new_state.log_info("Log opened, last log index : %+v", lg)
    for i := int64(0); i <= lg.GetLastIndex(); i++ {
        data, err := lg.Get(i) // Read log at index i
        if err != nil {
            new_state.log_error("Error in reading log : %v", err.Error())
            return nil
        }
        new_state.log_info("Restoring log : %v", data.(LogEntry))
        logEntry := data.(LogEntry) // The data is of LogEntry type
        new_state.logs = append(new_state.logs, logEntry)
    }*/

    lastLogEntry, err := lg.Get(lg.GetLastIndex())
    if err != nil {
        new_state.log_error(3, "Error in reading log : %v", err.Error())
        return nil
    }
    new_state.logs = append(new_state.logs, lastLogEntry.(LogEntry))
    new_state.log_info(3, "Last log from persistent store restored")

    return new_state
}

//  Returns last log entry
func (server *ServerState) getLastLog() *LogEntry {
    // logs would never empty, at least one log is always ensured
    return &server.logs[len(server.logs)-1]
}
//  Return log of given index
func (server *ServerState)getLogOf(index int) *LogEntry {
    // If index is out of range
    if index > server.getLastLog().Index || index < 0 {
        server.log_error(3, "Index is out of range")
        return nil
    }

    // If log is not in memory, get it from persistent log
    if index < server.logs[0].Index {
        l, e := server.PersistentLog.Get(int64(index))
        if e!=nil {
            server.log_error(3, "Persistent log access error : %v", e.Error())
            return nil
        } else {
            j := l.(LogEntry)
            return &j
        }
    }

    // Assuming all log indices are increasing by by 1
    return &server.logs[index - server.logs[0].Index]
}
//  Return all logs from given index(including index) to the end
func (server *ServerState)getLogsFrom(index int) *[]LogEntry {
    // If index is out of range
    if index > server.getLastLog().Index || index < 0 {
        server.log_error(3, "Index is out of range")
        return nil
    }

    logs := []LogEntry{}

    // If logs are not in memory, get it from persistent log
    if index < server.logs[0].Index {
        // Get all logs WHICH are NOT in memory but in persistent store
        for ; index<server.logs[0].Index ; index++ {
            l, e := server.PersistentLog.Get(int64(index))
            if e!=nil {
                server.log_error(3, "Persistent log access error : %v", e.Error())
                return nil
            } else {
                logs = append(logs,l.(LogEntry))
            }
        }
    }

    logs = append(logs, server.logs[index - server.logs[0].Index:]...)
    // Assuming all log indices are incremental
    return &logs
}
//  Return all logs from given index(including index) to the end
//  and truncate them from in-memory logs and also from persistent logs
func (server *ServerState)truncateLogsFrom(index int) *[]LogEntry {
    logs := server.getLogsFrom(index)

    // If part of to be truncated logs is in persistent store
    if index < server.logs[0].Index {
        err := server.PersistentLog.TruncateToEnd(int64(index))
        if err != nil {
            server.log_error(3, "Error while truncating persistent logs : %v", err.Error())
        }
        // Clear in memory log
        server.logs = []LogEntry{}
    } else {
        server.logs = server.logs[:index - server.logs[0].Index] // s = s[include_start:exclude_end]
    }

    // Check if logs is empty
    if len(server.logs) == 0 {
        // We need to restore at least one log from persistent store, logs should never be empty
        // Append index-1 th log
        l, e := server.PersistentLog.Get(int64(index-1))
        if e != nil {
            server.log_error(3, "Error while getting log : %v", e.Error())
            return nil
        }
        server.logs = append(server.logs, l.(LogEntry))
    }

    return logs
}

//  Set commitIndex, load commitIndex onwards logs into memory from persistent store if not available
func (server *ServerState) setCommitIndex(commitIndex int) {
    // if logs after commit index are not loaded,
    if commitIndex>0 && commitIndex < server.logs[0].Index {
        // Prepend all logs from commitIndex to server.logs[0].Index
        server.log_info(3, "Restorig logs from persistent store from index : %v", commitIndex)
        server.logs = *server.getLogsFrom(commitIndex)
    }
    server.log_info(3, "Updating commit index to : %v", commitIndex)
    server.commitIndex = commitIndex
}

//  Returns current server state
func (server *ServerState) GetServerState() RaftState {
    return server.myState
}
//  Returns current term
func (server *ServerState) GetCurrentTerm() int {
    return server.CurrentTerm
}
func (server *ServerState) GetServerId() int {
    return server.server_id
}

// Broadcast an event, returns array of actions
func (server *ServerState) broadcast(event interface{}) []interface{} {
    actions := make([]interface{}, 0)
    action := SendAction{ToId: -1, Event: event} // Sending to -1, -1 is for broadcast
    actions = append(actions, action)
    return actions
}

// Initialise leader state
func (server *ServerState) initialiseLeader() {
    // become leader
    server.myState = LEADER
    server.matchIndex = make([]int, server.numberOfNodes+1)
    server.nextIndex = make([]int, server.numberOfNodes+1)
    server.matchIndex[server.server_id] = server.getLastLog().Index

    // initialise nextIndex
    for i := 0; i <= server.numberOfNodes; i++ {
        server.nextIndex[i] = server.getLastLog().Index + 1
    }
}

/********************************************************************
 *                                                                  *
 *                          Vote Request                            *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) voteRequest(event RequestVoteEvent) []interface{} {

    actions := make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append([]interface{}{StateStore{}}, actions...)
        }
    }()

    if event.Term < server.CurrentTerm {
        // In any state, if old termed candidate request vote, tell it to be a follower
        voteResp := RequestVoteRespEvent{FromId: server.server_id, Term: server.CurrentTerm, VoteGranted: false}
        resp := SendAction{ToId: event.FromId, Event: voteResp}
        actions = append(actions, resp)
        return actions
    } else if event.Term > server.CurrentTerm {
        // Request from more up-to-date node, so lets update our state
        server.CurrentTerm = event.Term
        server.myState = FOLLOWER
        server.VotedFor = -1
        state_changed_flag = true
    }

    // requester_term >= server.current_term
    // If not voted for this term
    if server.VotedFor == -1 {
        // votedFor will be -1 ONLY for follower state, in case of leader/candidate it will be set to self id
        if event.LastLogTerm > server.getLastLog().Term || event.LastLogTerm == server.getLastLog().Term && event.LastLogIndex >= server.getLastLog().Index {
            server.VotedFor = event.FromId
            server.CurrentTerm = event.Term
            state_changed_flag = true

            voteResp := RequestVoteRespEvent{FromId: server.server_id, Term: server.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    } else {
        // If voted for this term, check if request is from same candidate for which this node has voted
        if server.VotedFor == event.FromId {
            // Vote again to same candidate
            voteResp := RequestVoteRespEvent{FromId: server.server_id, Term: server.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    }

    // For already voted for same term to different candidate,
    // Or not voted but requester's logs are old,
    // reject all requests
    voteResp := RequestVoteRespEvent{FromId: server.server_id, Term: server.CurrentTerm, VoteGranted: false}
    resp := SendAction{ToId: event.FromId, Event: voteResp}
    actions = append(actions, resp)
    return actions
}

/********************************************************************
 *                                                                  *
 *                      Vote Request Response                       *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) voteRequestResponse(event RequestVoteRespEvent) []interface{} {

    actions := make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append([]interface{}{StateStore{}}, actions...)
        }
    }()

    if server.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        server.myState = FOLLOWER
        server.CurrentTerm = event.Term
        server.VotedFor = -1
        state_changed_flag = true

        alarm := AlarmAction{Time: server.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    } else if server.CurrentTerm > event.Term {
        // Simply drop the response
        return actions
    }

    switch server.myState {
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
            count := 0
            ncount := 0
            for _, vote := range server.receivedVote {
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

                server.log_info(3, "Leader has been elected : %v", server.server_id)
                server.initialiseLeader()

                appendReq := AppendRequestEvent{
                    FromId:       server.server_id,
                    Term:         server.CurrentTerm,
                    PrevLogIndex: server.getLastLog().Index,
                    PrevLogTerm:  server.getLastLog().Term,
                    Entries:      []LogEntry{},
                    LeaderCommit: server.commitIndex}

                alarm := AlarmAction{Time: server.HeartbeatTimeout}
                actions = append(actions, alarm)
                appendReqActions := server.broadcast(appendReq)
                actions = append(actions, appendReqActions...)
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
func (server *ServerState) appendRequest(event AppendRequestEvent) []interface{} {
    actions := make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append([]interface{}{StateStore{}}, actions...)
        }
    }()


    if server.CurrentTerm > event.Term {
        // Append request is not from latest leader
        // In all states applicable
        appendResp := AppendRequestRespEvent{FromId: server.server_id, Term: server.CurrentTerm, Success: false, LastLogIndex: server.getLastLog().Index}
        resp := SendAction{ToId: event.FromId, Event: appendResp}
        actions = append(actions, resp)
        return actions
    }

    switch server.myState {
    case LEADER:
        // mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
        if event.Term == server.CurrentTerm {
            appendResp := AppendRequestRespEvent{FromId: server.server_id, Term: -1, Success: false, LastLogIndex: server.getLastLog().Index}
            resp := SendAction{ToId: event.FromId, Event: appendResp}
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
        alarm := AlarmAction{Time: server.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)

        // Check term
        if server.CurrentTerm < event.Term {
            // This server term is not so up-to-date, so update
            server.CurrentTerm = event.Term
            server.VotedFor = -1
            state_changed_flag = true
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

        // Check if previous entries are missing TODO:: prevent direct access of logs
        if server.getLastLog().Index < event.PrevLogIndex ||
           server.getLogOf(event.PrevLogIndex).Term /*logs[event.PrevLogIndex].Term*/ != event.PrevLogTerm {
            // Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
            appendResp := AppendRequestRespEvent{FromId: server.server_id, Term: server.CurrentTerm, Success: false, LastLogIndex: server.getLastLog().Index}
            resp := SendAction{ToId: event.FromId, Event: appendResp}
            actions = append(actions, resp)
            return actions
        }

        // Check if we have outdated/garbage logs
        if server.getLastLog().Index > event.PrevLogIndex {
            // There are entries from last leaders
            // truncate them up to the end
            //truncatedLogs := server.getLogsFrom(event.PrevLogIndex+1)// logs[event.PrevLogIndex+1:] // TODO:: prevent direct access of log
            truncatedLogs := server.truncateLogsFrom(event.PrevLogIndex+1)// logs[:event.PrevLogIndex+1] // TODO:: prevent direct access of log
            server.log_info(3, "Extra logs found, PrevLogIndex was %v, trucating logs: %+v", event.PrevLogIndex, truncatedLogs)
            for _, log := range *truncatedLogs {
                action := CommitAction{Index: log.Index, Data: log, Err: "Log truncated"}
                actions = append(actions, action)
            }
        }

        // Update log if entries are not present
        server.logs = append(server.logs, event.Entries...) // TODO:: prevent direct access of log

        for _, log := range event.Entries {
            action := LogStore{Index: log.Index, Term: log.Term, Data: log.Data}
            actions = append(actions, action)
        }

        if event.LeaderCommit > server.commitIndex {
            var commitFrom, commitUpto int
            // If leader has commited entries, so should this server
            if event.LeaderCommit <= server.getLastLog().Index {
                commitFrom = server.commitIndex + 1
                commitUpto = event.LeaderCommit
            } else {
                commitFrom = server.commitIndex + 1
                commitUpto = server.getLastLog().Index
            }

            // Loads logs from persistent store from commitIndex to end if not in in-memory logs
            server.setCommitIndex(commitUpto)

            // Commit all logs from commitFrom to commitUpto
            for i := commitFrom; i <= commitUpto; i++ {
                action := CommitAction{Index: i, Data: *server.getLogOf(i), Err: ""}
                actions = append(actions, action)
                server.log_info(3, "Commiting index %v, data:%v", i, server.getLogOf(i).Data)
            }
        }

    }

    // If the append request is heartbeat then ignore responding to it
    // We are updating matchIndex and nextIndex on positive appendRequestResponse, so consume heartbeats
    if len(event.Entries) != 0 {
        appendResp := AppendRequestRespEvent{
            FromId      : server.server_id,
            Term        : server.CurrentTerm,
            Success     : true,
            LastLogIndex: server.getLastLog().Index }
        resp := SendAction{ToId: event.FromId, Event: appendResp}
        actions = append(actions, resp)
    }
    return actions
}

/********************************************************************
 *                                                                  *
 *                    Append Request Response                       *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendRequestResponse(event AppendRequestRespEvent) []interface{} {

    actions := make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append([]interface{}{StateStore{}}, actions...)
        }
    }()

    // Check term
    if server.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        server.myState = FOLLOWER
        server.CurrentTerm = event.Term
        server.VotedFor = -1
        state_changed_flag = true

        // reset alarm
        alarm := AlarmAction{Time: server.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    }

    switch server.myState {
    case LEADER:
        if !event.Success {
            // there are holes in follower's log
            if event.LastLogIndex < server.nextIndex[event.FromId] {
                server.nextIndex[event.FromId] = event.LastLogIndex + 1
            }

            // Resend all logs from the holes to the end
            prevLog := server.logs[server.nextIndex[event.FromId]-1] // TODO:: prevent direct access of log
            startIndex := server.nextIndex[event.FromId]
            logs := append([]LogEntry{}, server.logs[startIndex:]...) // copy server.log from startIndex to the end to "logs"
            event1 := AppendRequestEvent{
                FromId:       server.server_id,
                Term:         server.CurrentTerm,
                PrevLogIndex: prevLog.Index,
                PrevLogTerm:  prevLog.Term,
                Entries:      logs,
                LeaderCommit: server.commitIndex}
            action := SendAction{ToId: event.FromId, Event: event1}
            actions = append(actions, action)
            return actions
        } else if event.LastLogIndex > server.matchIndex[event.FromId] {
            server.matchIndex[event.FromId] = event.LastLogIndex
            server.nextIndex[event.FromId] = event.LastLogIndex + 1

            // lets sort
            sorted := append([]int{}, server.matchIndex[1:]...)
            //matchCopy = []int{4,3,7,9,1,6}
            sort.IntSlice(sorted).Sort() // sort in ascending order
            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N
            for i := server.numberOfNodes / 2; i >= 0; i-- {
                if sorted[i] > server.commitIndex && server.logs[sorted[i]].Term == server.CurrentTerm {
                    // Commit all not committed eligible entries
                    for k := server.commitIndex + 1; k <= sorted[i]; k++ {
                        action := CommitAction{
                            Index: k,
                            Data:  server.logs[k],
                            Err:   ""}
                        actions = append(actions, action)
                    }

                    //server.commitIndex = sorted[i]
                    server.setCommitIndex(sorted[i])
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
func (server *ServerState) timeout(event TimeoutEvent) []interface{} {

    actions := make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append([]interface{}{StateStore{}}, actions...)
        }
    }()

    switch server.myState {
    case LEADER:
        // Send empty appendRequests

        heartbeatEvent := AppendRequestEvent{
            FromId:       server.server_id,
            Term:         server.CurrentTerm,
            PrevLogIndex: server.getLastLog().Index,
            PrevLogTerm:  server.getLastLog().Term,
            Entries:      []LogEntry{},
            LeaderCommit: server.commitIndex}
        heartbeatActions := server.broadcast(heartbeatEvent) // broadcast request vote event
        actions = append(actions, heartbeatActions...)
        actions = append(actions, AlarmAction{Time: server.HeartbeatTimeout})
    case CANDIDATE:
        // Restart election
        fallthrough
    case FOLLOWER:
        // Start election
        server.myState = CANDIDATE
        server.CurrentTerm = server.CurrentTerm + 1
        server.VotedFor = server.server_id
        state_changed_flag = true
        actions = append(actions, AlarmAction{Time: server.ElectionTimeout + rand.Intn(RandomTimeout)})
        server.receivedVote[server.server_id] = server.CurrentTerm // voting to self

        voteReq := RequestVoteEvent{
            FromId:       server.server_id,
            Term:         server.CurrentTerm,
            LastLogIndex: server.getLastLog().Index,
            LastLogTerm:  server.getLastLog().Term}
        voteReqActions := server.broadcast(voteReq) // broadcast request vote event
        actions = append(actions, voteReqActions...)
    }
    return actions
}

/********************************************************************
 *                                                                  *
 *                     Append from client                           *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) appendClientRequest(event AppendEvent) []interface{} {

    actions := make([]interface{}, 0)

    switch server.myState {
    case LEADER:
        log := LogEntry{Index: server.getLastLog().Index + 1, Term: server.CurrentTerm, Data: event.Data}
        logs := append([]LogEntry{}, log)

        appendReq := AppendRequestEvent{
            FromId:       server.server_id,
            Term:         server.CurrentTerm,
            PrevLogIndex: server.getLastLog().Index,
            PrevLogTerm:  server.getLastLog().Term,
            Entries:      logs,
            LeaderCommit: server.commitIndex}

        server.logs = append(server.logs, log)                          // Append to self log
        server.matchIndex[server.server_id] = server.getLastLog().Index // Update self matchIndex

        actions = append(actions, LogStore{Index: log.Index, Term:log.Term, Data: log.Data})
        actions = append(actions, server.broadcast(appendReq)...)
    case CANDIDATE:
    case FOLLOWER:
    }
    return actions
}

func (server *ServerState) checkLogConsistency() {

    for i:=1 ; i<len(server.logs) ; i++ {
        if server.logs[i].Index != server.logs[i-1].Index+1 {
            server.log_error(3, "Log inconsistency found on server : \n%v", server)
            return
        }
    }
}

/********************************************************************
 *                                                                  *
 *                          Process event                           *
 *                                                                  *
 ********************************************************************/
func (server *ServerState) ProcessEvent(event interface{}) []interface{} {
    // Initialise the variables and timeout
    defer server.checkLogConsistency()

    switch event.(type) {
    case AppendRequestEvent:
        return server.appendRequest(event.(AppendRequestEvent))
    case AppendRequestRespEvent:
        return server.appendRequestResponse(event.(AppendRequestRespEvent))
    case RequestVoteEvent:
        return server.voteRequest(event.(RequestVoteEvent))
    case RequestVoteRespEvent:
        return server.voteRequestResponse(event.(RequestVoteRespEvent))
    case TimeoutEvent:
        return server.timeout(event.(TimeoutEvent))
    case AppendEvent:
        return server.appendClientRequest(event.(AppendEvent))
    default:
        return nil
    }
}

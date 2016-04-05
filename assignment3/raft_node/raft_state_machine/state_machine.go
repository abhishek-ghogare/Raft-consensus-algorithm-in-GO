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
func (state StateMachine) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[SM:%v] %v ", strconv.Itoa(state.server_id), strconv.Itoa(state.CurrentTerm)) + format
    logging.Error(skip, format, args...)
}
func (state StateMachine) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[SM:%v] %v ", strconv.Itoa(state.server_id), strconv.Itoa(state.CurrentTerm)) + format
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
    State StateMachine
}

type AlarmAction struct {
    Time int
}

/********************************************************************
 *                                                                  *
 *                          Server status                           *
 *                                                                  *
 ********************************************************************/
type StateMachine struct {
                             // Persistent state
    CurrentTerm   int
    VotedFor      int        // -1: not voted
    CommitIndex   int        // initialised to 0
    numberOfNodes int

                             // log is initialised with single dummy log, to make life easier in future checking
                             // Index starts from 1, as first empty entry is present
    logs          []LogEntry // Using first 0th dummy entry for all arrays
    PersistentLog *log.Log   // Persistent log, used to retrieve logs which are not in memory

                             // Non-persistent state
    server_id     int
                             // lastApplied int      // not used here, duplicate committing is detected and handled in upper layer of raft
    nextIndex   []int        // Using first 0th dummy entry for all arrays
    matchIndex  []int        // Using first 0th dummy entry for all arrays
    myState     RaftState    // CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}

                             // maintain received votes from other nodes,
                             // if vote received, set corresponding value to term for which the vote has received
                             // -ve value represents negative vote
    receivedVote []int       // Using first 0th dummy entry for all arrays

                             // Timeouts in milliseconds
    ElectionTimeout  int
    HeartbeatTimeout int

     /**
      *      Few assumptions and implementation according :
      *      1.  All the logs in memory and also on persistent store are has strictly increasing order of indices with
      *          the difference of 1.
      *      2.  There will always be at least one log in in-memory log at any time, which will be used to compare
      *          prevLogIndex and prevLogTerm while appending new entries.
      *      3.  Logs are not loaded into memory from persistent store when node becomes alive, but loads only last log,
      *          so that it can be used for future checking while append request comes.
      *      4.  Logs from commitIndex onwards are loaded into in-memory log from persistent store only when commitIndex
      *          is updated
      */
}

func fromServerStateFile(serverStateFile string) (serState *StateMachine, err error) {
    var state StateMachine
    var f *os.File

    if f, err = os.Open(serverStateFile); err != nil {
        state.log_error(4, "Unable to open state file : %v", err.Error())
        return nil, err
    }
    defer f.Close()

    dec := json.NewDecoder(f)
    if err = dec.Decode(&state); err != nil {
        state.log_error(4, "Unable to decode state file : %v", err.Error())
        return nil, err
    }
    return &state, nil
}

func (state *StateMachine) ToServerStateFile(serverStateFile string) (err error) {
    var f *os.File
    if f, err = os.Create(serverStateFile); err != nil {
        state.log_error(4, "Unable to create state file : %v", err.Error())
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    if err = enc.Encode(*state); err != nil {
        state.log_error(4, "Unable to encode state file : %v", err.Error())
        return err
    }
    return nil
}

func New(config *Config) (server *StateMachine) {
    server = &StateMachine{
        server_id       : config.Id,
        CurrentTerm     : 0,
        VotedFor        : -1,
        numberOfNodes   : config.NumOfNodes,
        logs            : []LogEntry{{Term: 0, Index: 0, Data: "Dummy Log"}}, // Initialising log with single empty log, to make life easier in future checking
        CommitIndex     : 0,
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

func Restore(config *Config) (state *StateMachine) {
    // Restore from file
    restored_state, err := fromServerStateFile(config.LogDir + RaftStateFile)
    if err != nil {
        restored_state.log_error(3, "Unable to restore server state : %v", err.Error())
        return nil
    }

    // Copy persistent state variables to newly initialized state
    new_state              := New(config)
    new_state.CurrentTerm   = restored_state.CurrentTerm
    new_state.VotedFor      = restored_state.VotedFor
    new_state.logs          = make([]LogEntry,0)


    // Restore last log of restored_state from persistent storage
    lg, err := log.Open(config.LogDir)
    if err != nil {
        new_state.log_error(3, "Unable to open log file : %v\n", err)
        return nil
    }

    lastLogEntry, err := lg.Get(lg.GetLastIndex())
    if err != nil {
        new_state.log_error(3, "Error in reading log : %v", err.Error())
        lg.Close()
        return nil
    }
    new_state.logs = append(new_state.logs, lastLogEntry.(LogEntry))
    new_state.log_info(3, "Last log from persistent store restored")

    lg.Close()


    new_state.setCommitIndex(restored_state.CommitIndex)
    return new_state
}

// Returns StateStore action structure embedding cloned state
func (state *StateMachine) getStateStoreAction() StateStore {
    server_copy         := StateMachine{
        CommitIndex         : state.CommitIndex,
        ElectionTimeout     : state.ElectionTimeout,
        HeartbeatTimeout    : state.HeartbeatTimeout,
        CurrentTerm         : state.CurrentTerm,
        VotedFor            : state.VotedFor }
    return StateStore{State:server_copy}
}

/****
 *      Log manipulation interface
 */
//  Returns last log entry
func (state *StateMachine) getLastLog() *LogEntry {
    // logs would never empty, at least one log is always ensured
    return &state.logs[len(state.logs)-1]
}
//  Return log of given index
func (state *StateMachine)getLogOf(index int) *LogEntry {
    // If index is out of range
    if index > state.getLastLog().Index || index < 0 {
        state.log_error(4, "Index is out of range")
        return nil
    }

    // If log is not in memory, get it from persistent log
    if index < state.logs[0].Index {
        l, e := state.PersistentLog.Get(int64(index))
        if e!=nil {
            state.log_error(4, "Persistent log access error : %v", e.Error())
            return nil
        } else {
            j := l.(LogEntry)
            return &j
        }
    }

    // Assuming all log indices are increasing by 1, which is ensured
    return &state.logs[index - state.logs[0].Index]
}
//  Return all logs from given index(including index) to the end
func (state *StateMachine)getLogsFrom(index int) *[]LogEntry {
    // If index is out of range
    if index > state.getLastLog().Index || index < 0 {
        state.log_error(4, "Index is out of range")
        return nil
    }

    logs := []LogEntry{}

    // If logs are not in memory, get it from persistent log
    if index < state.logs[0].Index {
        // Get all logs WHICH are NOT in memory but in persistent store
        for ; index< state.logs[0].Index ; index++ {
            l, e := state.PersistentLog.Get(int64(index))
            if e!=nil {
                state.log_error(4, "Persistent log access error : %v", e.Error())
                return nil
            } else {
                logs = append(logs,l.(LogEntry))
            }
        }
    }

    logs = append(logs, state.logs[index - state.logs[0].Index:]...)
    // Assuming all log indices are incremental
    return &logs
}
//  Return all logs from given index(including index) to the end
//  and truncate them from in-memory logs and also from persistent logs
func (state *StateMachine)truncateLogsFrom(index int) *[]LogEntry {
    logs := state.getLogsFrom(index)

    // If part of to be truncated logs is in persistent store
    if index < state.logs[0].Index {
        err := state.PersistentLog.TruncateToEnd(int64(index))
        if err != nil {
            state.log_error(4, "Error while truncating persistent logs : %v", err.Error())
        }
        // Clear in memory log
        state.logs = []LogEntry{}
    } else {
        state.logs = state.logs[:index - state.logs[0].Index] // s = s[include_start:exclude_end]
    }

    // Check if logs is empty
    if len(state.logs) == 0 {
        // We need to restore at least one log from persistent store, logs should never be empty
        // Append index-1 th log
        l, e := state.PersistentLog.Get(int64(index-1))
        if e != nil {
            state.log_error(4, "Error while getting log : %v", e.Error())
            return nil
        }
        state.logs = append(state.logs, l.(LogEntry))
    }

    return logs
}

//  Set commitIndex, load commitIndex onwards logs into memory from persistent store if not available
func (state *StateMachine) setCommitIndex(commitIndex int) {
    // if logs after commit index are not loaded,
    if commitIndex>0 && commitIndex < state.logs[0].Index {
        // Prepend all logs from commitIndex to server.logs[0].Index
        state.log_info(4, "Restorig logs from persistent store from index : %v", commitIndex)
        state.logs = *state.getLogsFrom(commitIndex)
    }
    state.log_info(4, "Updating commit index to : %v", commitIndex)
    state.CommitIndex = commitIndex
}

//  Returns current server state
func (state *StateMachine) GetServerState() RaftState {
    return state.myState
}
//  Returns current term
func (state *StateMachine) GetCurrentTerm() int {
    return state.CurrentTerm
}
func (state *StateMachine) GetServerId() int {
    return state.server_id
}

// Broadcast an event, returns array of actions
func (state *StateMachine) broadcast(event interface{}) (actions []interface{}) {
    actions = make([]interface{}, 0)
    action := SendAction{ToId: -1, Event: event} // Sending to -1, -1 is for broadcast
    actions = append(actions, action)
    return actions
}

// Initialise leader state
func (state *StateMachine) initialiseLeader() {
    // become leader
    state.myState = LEADER
    state.matchIndex = make([]int, state.numberOfNodes+1)
    state.nextIndex = make([]int, state.numberOfNodes+1)
    state.matchIndex[state.server_id] = state.getLastLog().Index

    // initialise nextIndex
    for i := 0; i <= state.numberOfNodes; i++ {
        state.nextIndex[i] = state.getLastLog().Index + 1
    }
}

/********************************************************************
 *                                                                  *
 *                          Vote Request                            *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) voteRequest(event RequestVoteEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.getStateStoreAction())
        }
    }()

    if event.Term < state.CurrentTerm {
        // In any state, if old termed candidate request vote, tell it to be a follower
        voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: false}
        resp := SendAction{ToId: event.FromId, Event: voteResp}
        actions = append(actions, resp)
        return actions
    } else if event.Term > state.CurrentTerm {
        // Request from more up-to-date node, so lets update our state
        state.CurrentTerm = event.Term
        state.myState = FOLLOWER
        state.VotedFor = -1
        state_changed_flag = true
    }

    // requester_term >= server.current_term
    // If not voted for this term
    if state.VotedFor == -1 {
        // votedFor will be -1 ONLY for follower state, in case of leader/candidate it will be set to self id
        if event.LastLogTerm > state.getLastLog().Term || event.LastLogTerm == state.getLastLog().Term && event.LastLogIndex >= state.getLastLog().Index {
            state.VotedFor = event.FromId
            state.CurrentTerm = event.Term
            state_changed_flag = true

            voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    } else {
        // If voted for this term, check if request is from same candidate for which this node has voted
        if state.VotedFor == event.FromId {
            // Vote again to same candidate
            voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: true}
            resp := SendAction{ToId: event.FromId, Event: voteResp}
            actions = append(actions, resp)
            return actions
        }
    }

    // For already voted for same term to different candidate,
    // Or not voted but requester's logs are old,
    // reject all requests
    voteResp := RequestVoteRespEvent{FromId: state.server_id, Term: state.CurrentTerm, VoteGranted: false}
    resp := SendAction{ToId: event.FromId, Event: voteResp}
    actions = append(actions, resp)
    return actions
}

/********************************************************************
 *                                                                  *
 *                      Vote Request Response                       *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) voteRequestResponse(event RequestVoteRespEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.getStateStoreAction())
        }
    }()

    if state.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        state.myState = FOLLOWER
        state.CurrentTerm = event.Term
        state.VotedFor = -1
        state_changed_flag = true

        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    } else if state.CurrentTerm > event.Term {
        // Simply drop the response
        return actions
    }

    switch state.myState {
    case LEADER, FOLLOWER:
        return actions

    case CANDIDATE:
        // Refer comments @ receivedVote declaration
        // If vote received from a node, we are storing the term in receivedVote array for which the vote has received.
        // This way we don't need to reinitialise the voted for array every time new election starts
        vote := state.receivedVote[event.FromId]
        if vote < 0 {
            vote = -vote
        }

        if vote < event.Term {
            if event.VoteGranted {
                state.receivedVote[event.FromId] = event.Term
            } else {
                state.receivedVote[event.FromId] = -event.Term
            }
            count := 0
            ncount := 0
            for _, vote := range state.receivedVote {
                if vote == event.Term {
                    count++
                } else if vote == -event.Term {
                    ncount++
                }
            }
            //fmt.Printf("eventTerm:%v\n COUNTING : %v : %v : %v\n",event.term, count,ncount, server.receivedVote)

            if ncount > state.numberOfNodes/2 {
                // majority of -ve votes, so change to follower
                state.myState = FOLLOWER
                return actions
            } else if count > state.numberOfNodes/2 {
                // become leader

                state.log_info(3, "Leader has been elected : %v", state.server_id)
                state.initialiseLeader()

                appendReq := AppendRequestEvent{
                    FromId:       state.server_id,
                    Term:         state.CurrentTerm,
                    PrevLogIndex: state.getLastLog().Index,
                    PrevLogTerm:  state.getLastLog().Term,
                    Entries:      []LogEntry{},
                    LeaderCommit: state.CommitIndex}

                alarm := AlarmAction{Time: state.HeartbeatTimeout}
                actions = append(actions, alarm)
                appendReqActions := state.broadcast(appendReq)
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
func (state *StateMachine) appendRequest(event AppendRequestEvent) (actions []interface{}) {
    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            state.log_info(3, "Appending state store action")
            actions = append(actions, state.getStateStoreAction())
        }
    }()


    if state.CurrentTerm > event.Term {
        // Append request is not from latest leader
        // In all states applicable
        appendResp := AppendRequestRespEvent{FromId: state.server_id, Term: state.CurrentTerm, Success: false, LastLogIndex: state.getLastLog().Index}
        resp := SendAction{ToId: event.FromId, Event: appendResp}
        actions = append(actions, resp)
        return actions
    }

    switch state.myState {
    case LEADER:
        // mystate == leader && term == currentTerm, this is impossible, as two leaders will never be elected at any term
        if event.Term == state.CurrentTerm {
            appendResp := AppendRequestRespEvent{FromId: state.server_id, Term: -1, Success: false, LastLogIndex: state.getLastLog().Index}
            resp := SendAction{ToId: event.FromId, Event: appendResp}
            actions = append(actions, resp)
            return actions
        }
        // continue flow to next case for server.currentTerm < event.term
        fallthrough
    case CANDIDATE:
        // Convert to follower if current state is candidate/leader
        state.myState = FOLLOWER
        // continue flow to next case
        fallthrough
    case FOLLOWER:
        // Reset heartbeat timeout
        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)

        // Check term
        if state.CurrentTerm < event.Term {
            // This server term is not so up-to-date, so update
            state.CurrentTerm = event.Term
            state.VotedFor = -1
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

        // Check if previous entries are missing
        if state.getLastLog().Index < event.PrevLogIndex ||
           state.getLogOf(event.PrevLogIndex).Term /*logs[event.PrevLogIndex].Term*/ != event.PrevLogTerm {
            // Prev msg index,term doesn't match, i.e. missing previous entries, force leader to send previous entries
            appendResp := AppendRequestRespEvent{FromId: state.server_id, Term: state.CurrentTerm, Success: false, LastLogIndex: state.getLastLog().Index}
            resp := SendAction{ToId: event.FromId, Event: appendResp}
            actions = append(actions, resp)
            return actions
        }

        // Check if we have outdated/garbage logs
        if state.getLastLog().Index > event.PrevLogIndex {
            // There are entries from last leaders
            // truncate them up to the end
            //truncatedLogs := server.getLogsFrom(event.PrevLogIndex+1)// logs[event.PrevLogIndex+1:]
            truncatedLogs := state.truncateLogsFrom(event.PrevLogIndex+1)// logs[:event.PrevLogIndex+1]
            state.log_info(3, "Extra logs found, PrevLogIndex was %v, trucating logs: %+v", event.PrevLogIndex, truncatedLogs)
            for _, log := range *truncatedLogs {
                action := CommitAction{Index: log.Index, Data: log, Err: "Log truncated"}
                actions = append(actions, action)
            }
        }

        // Update log if entries are not present
        state.logs = append(state.logs, event.Entries...)

        for _, log := range event.Entries {
            action := LogStore{Index: log.Index, Term: log.Term, Data: log.Data}
            actions = append(actions, action)
        }

        if event.LeaderCommit > state.CommitIndex {
            var commitFrom, commitUpto int
            // If leader has commited entries, so should this server
            if event.LeaderCommit <= state.getLastLog().Index {
                commitFrom = state.CommitIndex + 1
                commitUpto = event.LeaderCommit
            } else {
                commitFrom = state.CommitIndex + 1
                commitUpto = state.getLastLog().Index
            }

            // Loads logs from persistent store from commitIndex to end if not in in-memory logs
            state.setCommitIndex(commitUpto)
            state_changed_flag = true

            // Commit all logs from commitFrom to commitUpto
            for i := commitFrom; i <= commitUpto; i++ {
                action := CommitAction{Index: i, Data: *state.getLogOf(i), Err: ""}
                actions = append(actions, action)
                state.log_info(3, "Commiting index %v, data:%v", i, state.getLogOf(i).Data)
            }
        }

    }

    // If the append request is heartbeat then ignore responding to it
    // We are updating matchIndex and nextIndex on positive appendRequestResponse, so consume heartbeats
    if len(event.Entries) != 0 {
        appendResp := AppendRequestRespEvent{
            FromId      : state.server_id,
            Term        : state.CurrentTerm,
            Success     : true,
            LastLogIndex: state.getLastLog().Index }
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
func (state *StateMachine) appendRequestResponse(event AppendRequestRespEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.getStateStoreAction())
        }
    }()

    // Check term
    if state.CurrentTerm < event.Term {
        // This server term is not so up-to-date, so update
        state.myState = FOLLOWER
        state.CurrentTerm = event.Term
        state.VotedFor = -1
        state_changed_flag = true

        // reset alarm
        alarm := AlarmAction{Time: state.ElectionTimeout + rand.Intn(RandomTimeout)} // slightly greater time to receive heartbeat
        actions = append(actions, alarm)
        return actions
    }

    switch state.myState {
    case LEADER:
        if !event.Success {
            // there are holes in follower's log
            if event.LastLogIndex < state.nextIndex[event.FromId] {
                state.nextIndex[event.FromId] = event.LastLogIndex + 1
            }

            // Resend all logs from the holes to the end
            prevLog := state.logs[state.nextIndex[event.FromId]-1]
            startIndex := state.nextIndex[event.FromId]
            logs := append([]LogEntry{}, state.logs[startIndex:]...) // copy server.log from startIndex to the end to "logs"
            event1 := AppendRequestEvent{
                FromId:       state.server_id,
                Term:         state.CurrentTerm,
                PrevLogIndex: prevLog.Index,
                PrevLogTerm:  prevLog.Term,
                Entries:      logs,
                LeaderCommit: state.CommitIndex}
            action := SendAction{ToId: event.FromId, Event: event1}
            actions = append(actions, action)
            return actions
        } else if event.LastLogIndex > state.matchIndex[event.FromId] {
            state.matchIndex[event.FromId] = event.LastLogIndex
            state.nextIndex[event.FromId] = event.LastLogIndex + 1

            // lets sort
            sorted := append([]int{}, state.matchIndex[1:]...)
            //matchCopy = []int{4,3,7,9,1,6}
            sort.IntSlice(sorted).Sort() // sort in ascending order
            // If there exists an N such that N > commitIndex, a majority
            // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            // set commitIndex = N
            for i := state.numberOfNodes / 2; i >= 0; i-- {
                if sorted[i] > state.CommitIndex && state.logs[sorted[i]].Term == state.CurrentTerm {
                    // Commit all not committed eligible entries
                    for k := state.CommitIndex + 1; k <= sorted[i]; k++ {
                        action := CommitAction{
                            Index: k,
                            Data:  state.logs[k],
                            Err:   ""}
                        actions = append(actions, action)
                    }

                    //server.commitIndex = sorted[i]
                    state.setCommitIndex(sorted[i])
                    state_changed_flag = true
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
func (state *StateMachine) timeout(event TimeoutEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    // Track if persistent state of raft state machine changes
    state_changed_flag := false
    // Check and store state on persistent store
    defer func() {
        if state_changed_flag {
            // Prepend StateStore action
            actions = append(actions, state.getStateStoreAction())
        }
    }()

    switch state.myState {
    case LEADER:
        // Send empty appendRequests

        heartbeatEvent := AppendRequestEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            PrevLogIndex: state.getLastLog().Index,
            PrevLogTerm:  state.getLastLog().Term,
            Entries:      []LogEntry{},
            LeaderCommit: state.CommitIndex}
        heartbeatActions := state.broadcast(heartbeatEvent) // broadcast request vote event
        actions = append(actions, heartbeatActions...)
        actions = append(actions, AlarmAction{Time: state.HeartbeatTimeout})
    case CANDIDATE:
        // Restart election
        fallthrough
    case FOLLOWER:
        // Start election
        state.myState = CANDIDATE
        state.CurrentTerm = state.CurrentTerm + 1
        state.VotedFor = state.server_id
        state_changed_flag = true
        actions = append(actions, AlarmAction{Time: state.ElectionTimeout + rand.Intn(RandomTimeout)})
        state.receivedVote[state.server_id] = state.CurrentTerm // voting to self

        voteReq := RequestVoteEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            LastLogIndex: state.getLastLog().Index,
            LastLogTerm:  state.getLastLog().Term}
        voteReqActions := state.broadcast(voteReq) // broadcast request vote event
        actions = append(actions, voteReqActions...)
    }
    return actions
}

/********************************************************************
 *                                                                  *
 *                     Append from client                           *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) appendClientRequest(event AppendEvent) (actions []interface{}) {

    actions = make([]interface{}, 0)

    switch state.myState {
    case LEADER:
        log := LogEntry{Index: state.getLastLog().Index + 1, Term: state.CurrentTerm, Data: event.Data}
        logs := append([]LogEntry{}, log)

        appendReq := AppendRequestEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            PrevLogIndex: state.getLastLog().Index,
            PrevLogTerm:  state.getLastLog().Term,
            Entries:      logs,
            LeaderCommit: state.CommitIndex}

        state.logs = append(state.logs, log)                          // Append to self log
        state.matchIndex[state.server_id] = state.getLastLog().Index // Update self matchIndex

        actions = append(actions, LogStore{Index: log.Index, Term:log.Term, Data: log.Data})
        actions = append(actions, state.broadcast(appendReq)...)
    case CANDIDATE:
    case FOLLOWER:
    }
    return actions
}

func (state *StateMachine) checkLogConsistency() {

    for i:=1 ; i<len(state.logs) ; i++ {
        if state.logs[i].Index != state.logs[i-1].Index+1 {
            state.log_error(3, "Log inconsistency found on server : \n%v", state)
            return
        }
    }
}

/********************************************************************
 *                                                                  *
 *                          Process event                           *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) ProcessEvent(event interface{}) []interface{} {
    // Initialise the variables and timeout
    defer state.checkLogConsistency()   // TODO:: debug

    switch event.(type) {
    case AppendRequestEvent:
        return state.appendRequest(event.(AppendRequestEvent))
    case AppendRequestRespEvent:
        return state.appendRequestResponse(event.(AppendRequestRespEvent))
    case RequestVoteEvent:
        return state.voteRequest(event.(RequestVoteEvent))
    case RequestVoteRespEvent:
        return state.voteRequestResponse(event.(RequestVoteRespEvent))
    case TimeoutEvent:
        return state.timeout(event.(TimeoutEvent))
    case AppendEvent:
        return state.appendClientRequest(event.(AppendEvent))
    default:
        return nil
    }
}

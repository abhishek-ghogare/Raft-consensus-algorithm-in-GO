package raft_state_machine

import (
    "fmt"
    "github.com/cs733-iitb/log"
    "math/rand"
    "strconv"
    "reflect"
    "github.com/avg598/cs733/logging"
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
func (state StateMachine) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[SM:%v] %v ", strconv.Itoa(state.server_id), strconv.Itoa(state.CurrentTerm)) + format
    logging.Warning(skip, format, args...)
}

const RaftStateFile = "serverState.json"


type RaftState uint

const (
    CANDIDATE RaftState = iota
    FOLLOWER
    LEADER
)
//const RandomTimeout = 1000  // Random timeout extra to election timeout
const BATCHSIZE = 50        // Send logs to followers in batches

type LogEntry struct {
    Term  int
    Index int64
    Data  interface{}
}

/********************************************************************
 *                                                                  *
 *                          Input events                            *
 *                                                                  *
 ********************************************************************/
type AppendRequestEvent struct {
    FromId       int
    Term         int
    PrevLogIndex int64
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int64
}

type AppendRequestRespEvent struct {
    FromId       int
    Term         int
    Success      bool
    LastLogIndex int64 // Helps in updating nextIndex & matchIndex
}

type RequestVoteEvent struct {
    FromId       int
    Term         int
    LastLogIndex int64
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
    Data interface{}
}
type UpdateLastAppliedEvent struct {
    Index int64
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
    Index   int64
    Data    interface{}
    Err     error
}
type Error_Commit struct {}
func (err Error_Commit) Error() string {
    return "Unable to commit the data"
}
type Error_NotLeader struct {
    LeaderId int
}
func (err Error_NotLeader) Error() string{
    if err.LeaderId != 0 {
        return fmt.Sprintf("This server is not a leader, current leader : %v", err.LeaderId)
    } else {
        return "This server is not a leader, current leader is unknown"
    }
}

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
    LastApplied   int64      // Updated by client handler when the log is applied to its state machine
    numberOfNodes int

                             // log is initialised with single dummy log, to make life easier in future checking
                             // Index starts from 1, as first empty entry is present
    PersistentLog *log.Log   // Persistent log, used to retrieve logs which are not in memory

                             // Non-persistent state
    server_id     int
    commitIndex   int64      // initialised to 0
    nextIndex   []int64      // Using first 0th dummy entry for all arrays
    matchIndex  []int64      // Using first 0th dummy entry for all arrays
    myState       RaftState  // CANDIDATE/FOLLOWER/LEADER, this server state {candidate, follower, leader}
    currentLdr    int        // Id of the current leader, used to redirect client to the leader

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

// Returns StateStore action structure embedding cloned state
func (state *StateMachine) GetStateStoreAction() StateStore {
    server_copy         := StateMachine{
        LastApplied         : state.LastApplied,
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
    log := state.GetLogAt(state.PersistentLog.GetLastIndex())
    return log
}
func (state *StateMachine) GetLastLogIndex() int64 {
    return state.PersistentLog.GetLastIndex()
}
func (state *StateMachine) GetLastLogTerm() int {
    log, _ := state.PersistentLog.Get(state.PersistentLog.GetLastIndex())
    return log.(LogEntry).Term
}
//  Return log of given index
func (state *StateMachine)GetLogAt(index int64) *LogEntry {
    l, e := state.PersistentLog.Get(index)
    if e!=nil {
        state.log_error(5, "Persistent log access error : %v : last index:%v  | accessed index:%v", e.Error(), state.PersistentLog.GetLastIndex(), index)
        panic("PANNICING") // TODO:: for leveldb: not found error, because the key doesn't exist in leveldb
        return nil
    }

    j := l.(LogEntry)
    return &j
}
//  Return maximum batch size logs from given index(including index) to end
func (state *StateMachine)getLogsFrom(index int64) *[]LogEntry {
    logs := []LogEntry{}

    for ; index <= state.PersistentLog.GetLastIndex() ; index++ {
        //state.log_info(4, "Fetching %v th log from persistent store", index)
        l, e := state.PersistentLog.Get(int64(index))
        if e!=nil {
            state.log_error(4, "Persistent log access error : %v", e.Error())
            return nil
        } else {
            logs = append(logs,l.(LogEntry))
        }

        if len(logs) >= BATCHSIZE {
            break
        }
    }

    return &logs
}
//  Return all logs from given index(including index) to the end
//  and truncate them from persistent logs
func (state *StateMachine)truncateLogsFrom(index int64) *[]LogEntry {
    logs := state.getLogsFrom(index)

    err := state.PersistentLog.TruncateToEnd(index)
    if err != nil {
        state.log_error(4, "Error while truncating persistent logs : %v", err.Error())
    }

    return logs
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
func (state *StateMachine) GetNumberOfNodes() int {
    return state.numberOfNodes
}
func (state *StateMachine) GetCurrentLeader() int {
    return state.currentLdr
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
    state.matchIndex = make([]int64, state.numberOfNodes+1)
    state.nextIndex = make([]int64, state.numberOfNodes+1)
    state.matchIndex[state.server_id] = state.GetLastLogIndex()
    state.currentLdr = state.GetServerId()  // update current leader

    // initialise nextIndex
    for i := 0; i <= state.numberOfNodes; i++ {
        state.nextIndex[i] = state.GetLastLogIndex() + 1
    }
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
            actions = append(actions, state.GetStateStoreAction())
        }
    }()

    switch state.myState {
    case LEADER:
        // Send empty appendRequests

        heartbeatEvent := AppendRequestEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            PrevLogIndex: state.GetLastLogIndex(),
            PrevLogTerm:  state.GetLastLogTerm(),
            Entries:      []LogEntry{},
            LeaderCommit: state.commitIndex}
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
        actions = append(actions, AlarmAction{Time: state.ElectionTimeout + rand.Intn(state.ElectionTimeout)})
        state.receivedVote[state.server_id] = state.CurrentTerm // voting to self

        voteReq := RequestVoteEvent{
            FromId:       state.server_id,
            Term:         state.CurrentTerm,
            LastLogIndex: state.GetLastLogIndex(),
            LastLogTerm:  state.GetLastLogTerm()}
        voteReqActions := state.broadcast(voteReq) // broadcast request vote event
        actions = append(actions, voteReqActions...)
    }
    return actions
}

/********************************************************************
 *                                                                  *
 *                          Process event                           *
 *                                                                  *
 ********************************************************************/
func (state *StateMachine) ProcessEvent(event interface{}) []interface{} {
    // Initialise the variables and timeout

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
    case *[]AppendEvent:
        return state.appendClientRequest(event.(*[]AppendEvent))
    default:
        state.log_error(3, "Invalid event type %+v", reflect.TypeOf(event))
        return nil
    }
}

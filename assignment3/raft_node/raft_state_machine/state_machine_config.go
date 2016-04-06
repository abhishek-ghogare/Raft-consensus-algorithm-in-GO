package raft_state_machine

import (
    "os"
    "encoding/json"
    "github.com/cs733-iitb/log"
    "github.com/cs733-iitb/cluster/mock"
)

type Config struct {
    Id               int    // this node's id. One of the cluster's entries should match.
    LogDir           string // Log file directory for this node
    ElectionTimeout  int
    HeartbeatTimeout int
    NumOfNodes       int
    MockServer       *mock.MockServer
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

/*****
 *      Create and initialise state machine state
 *
 *
 */
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

/*****
 *      Restore state machine state from persistent store
 *
 *
 */
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
    defer lg.Close()
    new_state.PersistentLog = lg // temp storing lg

    lastLogEntry, err := lg.Get(lg.GetLastIndex())
    if err != nil {
        new_state.log_error(3, "Error in reading log : %v", err.Error())
        return nil
    }

    new_state.logs = append(new_state.logs, lastLogEntry.(LogEntry))
    new_state.log_info(3, "Last log from persistent store restored")

    new_state.setCommitIndex( restored_state.CommitIndex )
    new_state.PersistentLog = nil
    return new_state
}

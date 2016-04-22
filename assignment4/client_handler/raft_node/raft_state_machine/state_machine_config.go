package raft_state_machine

import (
    "os"
    "encoding/json"
    "github.com/cs733-iitb/log"
    "cs733/assignment4/raft_config"
    "fmt"
    "strconv"
    "path"
)

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
func New(Id int, config *raft_config.Config) (server *StateMachine) {

    server = &StateMachine{
        server_id       : Id,
        CurrentTerm     : 0,
        VotedFor        : -1,
        numberOfNodes   : config.NumOfNodes,
        PersistentLog   : nil,
        commitIndex     : 0,
        LastApplied     : 0,
        nextIndex       : make([]int64, config.NumOfNodes+1),
        matchIndex      : make([]int64, config.NumOfNodes+1),
        receivedVote    : make([]int, config.NumOfNodes+1),
        myState         : FOLLOWER,
        currentLdr      : Id,    // imposing that current leader is self
        ElectionTimeout : config.ElectionTimeout,
        HeartbeatTimeout: config.HeartbeatTimeout}

    // Open persistent log
    logPath := path.Clean(config.LogDir + "/raft_" + strconv.Itoa(Id) + "/")
    server.log_info(3, "Opening raft logs : %v", logPath + "/")
    lg, err := log.Open(logPath)
    if err != nil {
        server.log_error(3, "Unable to open raft logs : %v", err)
        return nil
    }

    lg.SetCacheSize(1000000)    // TODO:: out of cache logs are not accessible
    lg.RegisterSampleEntry(LogEntry{})      //  Problem might be this

    server.PersistentLog = lg
    server.PersistentLog.Append(LogEntry{Index:0, Term:0, Data:"Dummy Entry"})

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
func Restore(Id int, config *raft_config.Config) (state *StateMachine) {
    // Restore from file
    statePath := path.Clean(config.LogDir + "/raft_" + strconv.Itoa(Id) + "/" + RaftStateFile)
    restored_state, err := fromServerStateFile(statePath)
    if err != nil {
        fmt.Printf("Unable to restore server state : %v\n", err.Error())
        os.Exit(2)
    }

    // Copy persistent state variables to newly initialized state
    new_state              := New(Id, config)
    new_state.CurrentTerm   = restored_state.CurrentTerm
    new_state.VotedFor      = restored_state.VotedFor
    new_state.LastApplied   = restored_state.LastApplied
    new_state.commitIndex   = restored_state.LastApplied

    new_state.PersistentLog.TruncateToEnd(new_state.PersistentLog.GetLastIndex())   // removing dummy entry appended in New(config)
    return new_state
}

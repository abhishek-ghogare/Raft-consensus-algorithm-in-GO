package raft_node

import (
    "github.com/cs733-iitb/log"
    rsm "cs733/assignment3/raft_node/raft_state_machine"
)


// Returns a Node object
func NewRaftNode(config *rsm.Config) *RaftNode {
    (&RaftNode{}).log_info(3, "Opening log file : %v", config.LogDir)
    lg, err := log.Open(config.LogDir)
    if err != nil {
        (&RaftNode{}).log_error(3, "Unable to create log file : %v", err)
        r := RaftNode{}
        return &r
    }

    server_state := rsm.New(config)
    server_state.PersistentLog = lg     // Storing persistent log

    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : config.MockServer,
        logs                : lg,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 200),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true}

    raft.logs.Append(rsm.LogEntry{Index:0, Term:0, Data:"Dummy Entry"})

    // Storing server state
    raft.server_state.ToServerStateFile(config.LogDir + rsm.RaftStateFile)

    return &raft
}

func RestoreServerState(config *rsm.Config) *RaftNode {

    server_state := rsm.Restore(config) // TODO:: move this down, as we wont need to open the logs in server_state

    // Restore logs of server_state from persistent storage
    lg, err := log.Open(config.LogDir)
    if err != nil {
        (&RaftNode{}).log_error(3, "Unable to open log file : %v\n", err)
        return nil
    }

    server_state.PersistentLog = lg     // Storing persistent log

    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : config.MockServer,
        logs                : lg,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 200),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true}
    /*
        // Drain the Inbox channel of the cluster server
        for {
            select {
            case <-raft.clusterServer.Inbox():
            default:
                break
            }
        }*/
    raft.log_info(3, "Raft node restored and initialised")
    return &raft
}

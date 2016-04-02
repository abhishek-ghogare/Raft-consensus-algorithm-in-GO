package raft_node

import (
    "github.com/cs733-iitb/log"
    rsm "cs733/assignment3/raft_state_machine"
)


// Returns a Node object
func NewRaftNode(config *rsm.Config) *RaftNode {
    (&RaftNode{}).log_info("Opening log file : %v", config.LogDir)
    lg, err := log.Open(config.LogDir)
    if err != nil {
        (&RaftNode{}).log_error("Unable to create log file : %v", err)
        r := RaftNode{}
        return &r
    }

    server_state := rsm.New(config)

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

    server_state := rsm.Restore(config)

    // Restore logs of server_state from persistent storage
    lg, err := log.Open(config.LogDir)
    if err != nil {
        (&RaftNode{}).log_error("Unable to open log file : %v\n", err)
        return nil
    }

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
    raft.log_info("Raft node restored and initialised")
    return &raft
}

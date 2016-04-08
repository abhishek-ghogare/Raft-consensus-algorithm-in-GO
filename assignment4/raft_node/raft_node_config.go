package raft_node

import (
    rsm "cs733/assignment4/raft_node/raft_state_machine"
)


// Returns a Node object
func NewRaftNode(config *rsm.Config) *RaftNode {

    (&RaftNode{}).log_info(3, "Opening log file : %v", config.LogDir)

    server_state := rsm.New(config)
    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : config.MockServer,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 200),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true}

    // Storing server state TODO:: Store server state only on valid StateStore action
    raft.server_state.ToServerStateFile(config.LogDir + rsm.RaftStateFile)
    raft.log_info(3, "New raft node created and initialized")
    return &raft
}

func RestoreServerState(config *rsm.Config) *RaftNode {

    server_state := rsm.Restore(config)
    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : config.MockServer,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 200),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true}

    raft.log_info(3, "Raft node restored and initialised")
    return &raft
}

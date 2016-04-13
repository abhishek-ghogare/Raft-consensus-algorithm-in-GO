package raft_node

import (
    rsm "cs733/assignment4/client_handler/raft_node/raft_state_machine"
    "cs733/assignment4/raft_config"
    "github.com/cs733-iitb/cluster"
    "os"
    "cs733/assignment4/logging"
)


// Returns a Node object
func NewRaftNode(config *raft_config.Config) *RaftNode {
    // Remove all persistent store
    os.RemoveAll(config.LogDir)


    logging.Info(2, "Opening log file : %v", config.LogDir)

    clusterServer, err := cluster.New(config.Id,config.ClusterConfig)
    if err!=nil {
        (&RaftNode{}).log_error(3, "Unable to create cluster server : %v", err.Error())
    }

    server_state := rsm.New(config)
    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : clusterServer,
        //clusterServer       : config.MockServer,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 20000),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true,
        serverList          : config.ServerList}

    // Storing server state TODO:: Store server state only on valid StateStore action
    raft.server_state.ToServerStateFile(config.LogDir + rsm.RaftStateFile)
    raft.log_info(3, "New raft node created and initialized")
    return &raft
}

func RestoreServerState(config *raft_config.Config) *RaftNode {

    clusterServer, err := cluster.New(config.Id,config.ClusterConfig)
    if err!=nil {
        (&RaftNode{}).log_error(3, "Unable to create cluster server : %v", err.Error())
    }

    server_state := rsm.Restore(config)
    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : clusterServer,
        //clusterServer       : config.MockServer,
        eventCh             : make(chan interface{}),
        timeoutCh           : make(chan interface{}),
        CommitChannel       : make(chan rsm.CommitAction, 20000),
        ShutdownChannel     : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true,
        serverList          : config.ServerList}

    raft.log_info(3, "Raft node restored and initialised")
    return &raft
}

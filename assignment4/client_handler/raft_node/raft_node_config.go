package raft_node

import (
    rsm "cs733/assignment4/client_handler/raft_node/raft_state_machine"
    "cs733/assignment4/raft_config"
    "github.com/cs733-iitb/cluster"
    "os"
    "path"
    "strconv"
)


// Returns a Node object
func NewRaftNode(Id int, config *raft_config.Config) *RaftNode {
    // Remove all persistent store
    os.RemoveAll(config.LogDir + "/raft_" + strconv.Itoa(Id) + "/")

    clusterServer, err := cluster.New(Id,config.ClusterConfig)
    if err!=nil {
        (&RaftNode{}).log_error(3, "Unable to create cluster server : %v", err.Error())
    }

    server_state := rsm.New(Id, config)

    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : clusterServer,
        eventCh             : make(chan interface{}, 500),   // TODO:: change size to 500
        CommitChannel       : make(chan rsm.CommitAction, 20000),
        shutDownChan        : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true,
        ServerList          : config.ServerList}

    // Storing server state TODO:: Store server state only on valid StateStore action
    statePath := path.Clean(config.LogDir + "/raft_" + strconv.Itoa(Id) + "/" + rsm.RaftStateFile)
    err = raft.server_state.ToServerStateFile(statePath)
    raft.log_info(3, "New raft node created and initialized")
    return &raft
}

func RestoreServerState(Id int, config *raft_config.Config) *RaftNode {

    clusterServer, err := cluster.New(Id,config.ClusterConfig)
    if err!=nil {
        (&RaftNode{}).log_error(3, "Unable to create cluster server : %v", err.Error())
    }

    server_state := rsm.Restore(Id, config)
    raft := RaftNode{
        server_state        : server_state,
        clusterServer       : clusterServer,
        eventCh             : make(chan interface{}, 500),
        CommitChannel       : make(chan rsm.CommitAction, 20000),
        shutDownChan        : make(chan int),
        LogDir              : config.LogDir,
        isUp                : false,
        isInitialized       : true,
        ServerList          : config.ServerList}

    raft.log_info(3, "Raft node restored and initialised")
    return &raft
}

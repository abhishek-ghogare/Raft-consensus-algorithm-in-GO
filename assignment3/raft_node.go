package main

import (
    "fmt"
    "github.com/cs733-iitb/cluster"
)

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil
type AppendResp struct {
    Data  []byte
    Index int       // or int .. whatever you have in your code
    Err   error
    // Err can be errred
}

type NodeNetAddr struct {
    Id   int
    Host string
    Port int
}

// This is an example structure for Config .. change it to your convenience.
type Config struct {
    NodeNetAddrList     []NodeNetAddr     // Information about all servers, including this.
    Id                  int             // this node's id. One of the cluster's entries should match.
    LogDir              string          // Log file directory for this node
    ElectionTimeout     int
    HeartbeatTimeout    int
}

type RaftNode struct { // implements Node interface
    eventCh         chan interface{}
    timeoutCh       chan interface{}
    config          Config
    server_state    ServerState
    myServer        cluster.Server
    /*// Node's id
    func Id() int {
        return config.Id
    }*/
    // Id of leader. -1 if unknown
    LeaderId int 
    // A channel for client to listen on. What goes into Append must come out of here at some point.
    CommitChannel chan AppendResp
    // Last known committed index in the log.  This could be -1 until the system stabilizes.
    /*func CommittedIndex int {
        return server_state.commitIndex
    }*/
/*
    // Client's message to Raft node
    Append([]byte)
    // Returns the data at a log index, or an error.
    Get(index int) (err, []byte)
    // Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
    Shutdown()*/
}

// Returns a Node object
func NewRaftNode(config Config) RaftNode {
    var peers []cluster.PeerConfig
    for _,nodeNetAddr := range config.NodeNetAddrList {
        peers = append(peers, cluster.PeerConfig{Id: nodeNetAddr.Id, Address: fmt.Sprintf("%v:%v", nodeNetAddr.Host, nodeNetAddr.Port)})
    }

    config1 := cluster.Config { Peers: peers }
    server1, _ := cluster.New(config.Id, config1)

    var server ServerState
    server.setupServer ( FOLLOWER, len(config.NodeNetAddrList) )

    raft := RaftNode{config:config, server_state:server, myServer:server1}

    return raft
}

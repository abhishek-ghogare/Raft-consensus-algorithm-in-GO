package main

import (
"fmt"
	"github.com/cs733-iitb/log"
	"github.com/cs733-iitb/cluster/mock"
)

type NodeNetAddr struct {
	Id   int
	Host string
	Port int
}

// This is an example structure for Config .. change it to your convenience.
type Config struct {
	//NodeNetAddrList     []NodeNetAddr     // Information about all servers, including this.
	Id                  int             // this node's id. One of the cluster's entries should match.
	LogDir              string          // Log file directory for this node
	ElectionTimeout     int
	HeartbeatTimeout    int
	NumOfNodes	    int
	mockServer  	    *mock.MockServer
}

// Returns a Node object
func (config Config) NewRaftNode() *RaftNode {
	(&RaftNode{}).prnt("Opening log file : %v", config.LogDir)
	lg, err := log.Open(config.LogDir)
	if err != nil {
		fmt.Printf("Unable to create log file : %v\n", err)
		r := RaftNode{}
		return &r
	}


	var server_state ServerState
	server_state.setupServer ( FOLLOWER, config.NumOfNodes )
	server_state.electionTimeout     = config.ElectionTimeout
	server_state.heartbeatTimeout    = config.HeartbeatTimeout
	server_state.Server_id = config.Id

	raft := RaftNode{
		//config            : config,
		server_state        : server_state,
		//clusterServer     : config.getClusterServer(),
		clusterServer       : config.mockServer,
		logs                : lg,
		eventCh             : make(chan interface{}),
		timeoutCh           : make(chan interface{}),
		CommitChannel       : make(chan commitAction,200),
		ShutdownChannel     : make(chan int),
		LogDir              : config.LogDir }
	raft.isUp = false
	raft.isInitialized = true

	raft.logs.Append(LogEntry{Index:0,Term:0,Data:"Dummy Entry"})

	// Storing server state
	ToServerStateFile(config.LogDir+"/serverState.json",raft.server_state)

	return &raft
}


/*func (config Config) getClusterServer () *cluster.Server {
	//--------------------------------
	// Create cluster server from config
	//--------------------------------

	var peers []cluster.PeerConfig
	for _,nodeNetAddr := range config.NodeNetAddrList {
		peers = append(peers, cluster.PeerConfig{Id: nodeNetAddr.Id, Address: fmt.Sprintf("%v:%v", nodeNetAddr.Host, nodeNetAddr.Port)})
	}
	config1 := cluster.Config { Peers: peers }
	server, err := cluster.New(config.Id, config1)
	if err!=nil {
		(&RaftNode{}).prnt("Error in creting cluster server : %v", err.Error())
		return nil
	}


	(&RaftNode{}).prnt("Cluster server created : %v", config.Id)
	return &server
}*/

/*func (config Config) getMocCluster () * mock.MockCluster {
	cl ,_ := mock.NewCluster(nil)
	for _,nodeNetAddr := range config.NodeNetAddrList {
		cl.AddServer(nodeNetAddr.Id)
		//peers = append(peers, cluster.PeerConfig{Id: nodeNetAddr.Id, Address: fmt.Sprintf("%v:%v", nodeNetAddr.Host, nodeNetAddr.Port)})
	}
	//cl.Servers[0].
	return cl
}*/

func (config Config) RestoreServerState () *RaftNode {

	//--------------------------------
	// Restore server state from persistent storage
	//--------------------------------
	server_state, err := FromServerStateFile(config.LogDir+"/serverState.json")
	if err != nil {
		(&RaftNode{}).prnt("Unable to restore server state : %v", err.Error())
		return nil
	}

	server_state.CommitIndex = 0
	server_state.electionTimeout    = config.ElectionTimeout
	server_state.heartbeatTimeout   = config.HeartbeatTimeout
	server_state.numberOfNodes      = config.NumOfNodes
	server_state.nextIndex 		= make([]int, config.NumOfNodes+1)
	server_state.matchIndex 	= make([]int, config.NumOfNodes+1)
	server_state.receivedVote       = make([]int, config.NumOfNodes+1)
	server_state.myState = FOLLOWER
	server_state.logs = make([]LogEntry, 0)

	for i := 0; i <= config.NumOfNodes; i++ {
		server_state.nextIndex[i]     = 1     // Set to index of next log to send
		server_state.matchIndex[i]    = 0     // Set to last log index on that server, increases monotonically
	}

	// Restore logs of server_state from persistent storage
	lg, err := log.Open(config.LogDir)
	if err != nil {
		prnt("Unable to open log file : %v\n", err)
		return nil
	}

	prnt("Log opened, last log index : %+v", lg)
	for i:=int64(0) ; i<=lg.GetLastIndex() ; i++ {
		data, err := lg.Get(i)  // Read log at index i
		if err!=nil {
			server_state.prnt("Error in reading log : %v", err.Error())
			return nil
		}
		server_state.prnt("Restoring log : %v", data.(LogEntry))
		logEntry := data.(LogEntry) // The data is of LogEntry type
		server_state.logs = append(server_state.logs, logEntry)
	}

	//--------------------------------
	// Create RaftNode
	//--------------------------------
	raft := RaftNode{
		server_state        : *server_state,
		//clusterServer       : config.getClusterServer(),
		clusterServer       : config.mockServer,
		logs                : lg,
		eventCh             : make(chan interface{}),
		timeoutCh           : make(chan interface{}),
		CommitChannel       : make(chan commitAction,200),
		ShutdownChannel     : make(chan int),
		LogDir              : config.LogDir }

	raft.isUp = false
	raft.isInitialized = true

	return &raft
}

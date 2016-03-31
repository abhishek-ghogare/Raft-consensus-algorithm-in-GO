package raft_node

import (
	"github.com/cs733-iitb/log"
	rsm "cs733/assignment3/raft_state_machine"
	"os"
	"encoding/json"
)


func ToServerStateFile(serverStateFile string, serState *rsm.ServerState) (err error){
	var f *os.File
	if f, err = os.Create(serverStateFile); err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err = enc.Encode(serState); err != nil {
		return  err
	}
	return nil
}



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
		//config            : config,
		server_state        : server_state,
		//clusterServer     : config.getClusterServer(),
		clusterServer       : config.MockServer,
		logs                : lg,
		eventCh             : make(chan interface{}),
		timeoutCh           : make(chan interface{}),
		CommitChannel       : make(chan rsm.CommitAction,200),
		ShutdownChannel     : make(chan int),
		LogDir              : config.LogDir }
	raft.isUp = false
	raft.isInitialized = true

	raft.logs.Append(rsm.LogEntry{Index:0,Term:0,Data:"Dummy Entry"})

	// Storing server state
	ToServerStateFile(config.LogDir+"/serverState.json",raft.server_state)

	return &raft
}

func  RestoreServerState (config *rsm.Config) *RaftNode {

	server_state := rsm.Restore(config)

	// Restore logs of server_state from persistent storage
	lg, err := log.Open(config.LogDir)
	if err != nil {
		(&RaftNode{}).log_error("Unable to open log file : %v\n", err)
		return nil
	}
	//--------------------------------
	// Create RaftNode
	//--------------------------------
	raft := RaftNode{
		server_state        : server_state,
		clusterServer       : config.MockServer,
		logs                : lg,
		eventCh             : make(chan interface{}),
		timeoutCh           : make(chan interface{}),
		CommitChannel       : make(chan rsm.CommitAction,200),
		ShutdownChannel     : make(chan int),
		LogDir              : config.LogDir }

	raft.isUp = false
	raft.isInitialized = true
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

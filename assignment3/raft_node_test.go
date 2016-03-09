package main

import (
    "testing"
    "time"
)

func makeRafts() []RaftNode {
	var nodeNetAddrList []NodeNetAddr
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:1, Host:"localhost", Port:7001} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:2, Host:"localhost", Port:7002} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:3, Host:"localhost", Port:7003} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:4, Host:"localhost", Port:7004} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:5, Host:"localhost", Port:7005} )

	config := Config {
	    NodeNetAddrList 	:nodeNetAddrList,
	    Id                  :1,
	    LogDir              :"log file",          // Log file directory for this node
	    ElectionTimeout     :500,
	    HeartbeatTimeout    :500}

	var rafts []RaftNode
	for i:=1 ; i<=5 ; i++ {
		config.Id = i
		raft := NewRaftNode(config)
		rafts = append(rafts,raft)
	}

	return rafts
}

func TestBasic (t *testing.T) {
	rafts := makeRafts() // array of []RaftNode
	ldr := getLeader(rafts)
	ldr.Append("foo")
	time.Sleep(1*time.Second)
	for _, node:= range rafts {
		select {
		// to avoid blocking on channel.
		case ci := <- node.CommitChannel():
			if ci.err != nil {
				t.Fatal(ci.err)
			}
			if string(ci.data) != "foo" {
				t.Fatal("Got different data")
			}
		default: 
			t.Fatal("Expected message on all nodes")
		}
	}
}
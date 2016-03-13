package main

import (
    "testing"
    "time"
    "fmt"
)

// Debugging tools
func prnt(format string, args ...interface{}) {
  fmt.Printf("[TEST]   " + format + "\n", args...)
}

func makeRafts() []*RaftNode {
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

	var rafts []*RaftNode
	for i:=1 ; i<=5 ; i++ {
		config.Id = i
		raft := NewRaftNode(config)
		go raft.processEvents()
		rafts = append(rafts,raft)
	}

    //rafts[0].server_state.setupServer ( LEADER, len(config.NodeNetAddrList) )
	return rafts
}

func getLeader(rafts []*RaftNode) (*RaftNode, string) {
	ldrCount:=0
	var ldr *RaftNode
	for _, node := range rafts {
		if node.server_state.myState == LEADER {
			ldrCount+=1
			ldr = node
		}
	}

	if ldrCount!=1 {
		return &RaftNode{}, "Multiple/Zero leaders found"
	} else {
		return ldr, ""
	}
}

func TestBasic (t *testing.T) {

	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
	time.Sleep(10*time.Second)
	ldr, err := getLeader(rafts)
	if err!="" {
		t.Fatal(err)
	}

	ldr.Append([]byte("foo"))
	for _, node:= range rafts {
		select {
		// to avoid blocking on channel.
		case ci := <- node.CommitChannel:
			if ci.Err != nil {
				t.Fatal(ci.Err)
			}
			if string(ci.Data) != "foo" {
				t.Fatal("Got different data")
			}
		default: 
			t.Fatal("Expected message on all nodes")
		}
	}
}
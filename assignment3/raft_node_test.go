package main

import (
    "testing"
    "time"
    "fmt"
)

// Debugging tools
func prnt(format string, args ...interface{}) {
  fmt.Printf("[TEST] \t: " + format + "\n", args...)
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
	    HeartbeatTimeout    :200}

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
	time.Sleep(5*time.Second)
	ldr, err := getLeader(rafts)
	if err!="" {
		t.Fatal(err)
	}
	prnt("Leader found : %v", ldr.server_state.server_id)

	ldr.Append([]byte("foo"))
	//ldr.Append([]byte("bar"))
	time.Sleep(2*time.Second)
	//ldr.Append([]byte("foo1"))
	//time.Sleep(2*time.Second)
	for _, node:= range rafts {
		select {
		// to avoid blocking on channel.
		case ci := <- node.CommitChannel:
			if ci.err != "" {
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
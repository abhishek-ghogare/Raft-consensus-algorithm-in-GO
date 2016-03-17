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

func shutdownRafts (rafts []*RaftNode) {
	for _, r := range rafts {
		prnt("%+v", r)
		r.Shutdown()
	}
}

func getLeader(t *testing.T, rafts []*RaftNode) (*RaftNode) {
	ldrCount:=0
	var ldr *RaftNode
	for _, node := range rafts {
		//prnt("SERVER : %+v", node.server_state)
		if node.IsLeader() {
			ldrCount+=1
			ldr = node
		}
	}

	if ldrCount!=1 {
		t.Fatal("Multiple/Zero leaders found");
		return &RaftNode{}
	} else {
		prnt("Leader found : %v", ldr.server_state.server_id)
		return ldr
	}
}

func TestLeaderReelection (t *testing.T) {
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
	time.Sleep(2*time.Second)
	ldr := getLeader(t, rafts)
	ldr.Shutdown()
	time.Sleep(2*time.Second)
	ldr = getLeader(t, rafts)
	ldr.Append([]byte("foo"))

	time.Sleep(3*time.Second)
	ldr.Shutdown()
	for _, node:= range rafts {
		if !node.IsNodeUp() {
			continue	// Ignore if node is down
		}
		select {
		// to avoid blocking on channel.
		case ci := <- node.CommitChannel:
			if ci.err != "" {
				t.Error(ci.err)
			}
			if string(ci.data) != "foo" {
				t.Error("Got different data")
			}
		default:
			t.Error("Expected message on all nodes")
		}
	}
	shutdownRafts(rafts)
}

func TestBasic (t *testing.T) {
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
	time.Sleep(2*time.Second)
	ldr:= getLeader(t, rafts)
	ldr.Append([]byte("foo"))
	time.Sleep(1*time.Second)
	for _, node:= range rafts {
		select {
		// to avoid blocking on channel.
		case ci := <- node.CommitChannel:
			if ci.err != "" {
				t.Error(ci.err)
			}
			if string(ci.data) != "foo" {
				t.Error("Got different data")
			}
		default: 
			t.Error("Expected message on all nodes")
		}
	}
	shutdownRafts(rafts)
}


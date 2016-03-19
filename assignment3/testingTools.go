package main

import (
	"testing"
	"time"
	"strconv"
	"os"
	"github.com/cs733-iitb/cluster/mock"
)


type Rafts []*RaftNode
var mockCluster *mock.MockCluster


func expect(t *testing.T, found interface{}, expected interface{}, msg string) {
	//if a.(type) != b.(type) {
	//  t.Errorf("Type mismatch, %v & %v", a.(type), b.(type))
	//} else
	if found != expected {
		t.Errorf("Expected %v, found %v : %v", expected, found, msg) // t.Error is visible when running `go test -verbose`
	}
}

func makeConfigs() []*Config {/*
	var nodeNetAddrList []NodeNetAddr
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:1, Host:"localhost", Port:7001} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:2, Host:"localhost", Port:7002} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:3, Host:"localhost", Port:7003} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:4, Host:"localhost", Port:7004} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:5, Host:"localhost", Port:7005} )*/

	var err error
	mockCluster, err = mock.NewCluster(nil)
	if err != nil {
		prnt("Error in creating CLUSTER : %v", err.Error())
	}


	configBase := Config {
		//NodeNetAddrList 	:nodeNetAddrList,
		Id                  :1,
		LogDir              :"log file",          // Log file directory for this node
		ElectionTimeout     :700,
		HeartbeatTimeout    :200,
		NumOfNodes 	    :5}

	var configs []*Config
	for i:=1 ; i<=5 ; i++ {
		config := configBase // Copy config
		config.Id = i
		config.LogDir = "/tmp/raft/node"+strconv.Itoa(i)
		mockCluster.AddServer(i)
		config.mockServer = mockCluster.Servers[i]
		prnt("Creating Config %v", config.Id)
		configs = append(configs,&config)
	}
	return configs
}


func makeRafts() Rafts {
	var rafts Rafts
	for _, config := range makeConfigs() {
		raft := config.NewRaftNode()
		err := ToConfigFile(config.LogDir+"/config.json",*config)
		if err != nil {
			prnt("Error in storing config to file : %v", err.Error())
		}
		raft.Start()
		rafts = append(rafts,raft)
	}

	return rafts
}

func (rafts Rafts) shutdownRafts () {
	for _, r := range rafts {
		//prnt("%+v", r)
		r.Shutdown()
	}
}
func cleanupLogs(){
	os.RemoveAll("/tmp/raft/")
}

// Gets stable leader, i.e. Only ONE leader is present and all other nodes are in follower state of that term
// If no stable leader is elected after timeout, return the current leader
func (rafts Rafts) getLeader(t *testing.T) (*RaftNode) {
	time.Sleep(1*time.Second)
	var ldr *RaftNode

	// Set 10 sec time span to probe the stable leader
	abortCh := time.NewTimer(4*time.Second)
	for {
		select {
		case <-abortCh.C:	// listen on abort channel for abort timeout
			prnt("Stable leader NOT found : %v", ldr.server_state.Server_id)
			abortCh.Stop()
			return ldr
		default:
			ldr = rafts.getCurrentLeader(t)

			// Check if all others are followers of ldr
			areAllFollowers := true
			for _, node := range rafts {
				if node.GetId() != ldr.GetId() && node.IsNodeUp() {
					// Ignore the leader for this check
					// or if node is down
					if node.server_state.myState != FOLLOWER ||
					node.GetCurrentTerm() != ldr.GetCurrentTerm() {
						// If this node is not follower
						// or if this node is not follower of the leader
						areAllFollowers = false
						break
					}
				}
			}

			if areAllFollowers {
				prnt("Stable leader found : %v", ldr.server_state.Server_id)
				abortCh.Stop()
				return ldr
			}
		}
	}
	return ldr
}

func (rafts Rafts) getCurrentLeader(t *testing.T) (*RaftNode) {
	var ldr *RaftNode
	var ldrTerm int

	// Set 10 sec time span to probe the stable leader
	abortCh := time.NewTimer(4*time.Second)

	for {
		select {
		case <-abortCh.C:	// listen on abort channel for abort timeout
			rafts.shutdownRafts()
			t.Fatalf("No leader elected after timeout")
		default:
			ldrTerm = -1;
			ldr = nil
			// Get current latest leader
			for _, node := range rafts {
				if node.IsLeader() {
					if ldrTerm != 0 && node.GetCurrentTerm() == ldrTerm {
						t.Fatalf("Two leaders for same term found")
					} else if node.GetCurrentTerm() > ldrTerm {
						ldr = node
						ldrTerm = node.GetCurrentTerm()
					}
				}
			}
			// Leader not found, so continue
			if ldrTerm != -1 {
				//prnt("Leader found : %v", ldr.server_state.Server_id)
				abortCh.Stop()
				return ldr
			}
		}
	}
	return ldr
}

func (rafts Rafts) checkSingleCommit (t *testing.T, data string) {
	// Set 5 sec time span to probe the commit channels
	abortCh := time.NewTimer(5*time.Second)

	// Set flag when either commitChannel received or the node is down
	// This will help in reading only one commit msg from any given node
	checked := make([]bool, len(rafts))
	for checkedNum := 0 ; checkedNum < len(rafts) ; {
		// Check for all nodes
		select {
		case <-abortCh.C:
			rafts.shutdownRafts()
			t.Fatalf("Commit msg not received on all nodes after 5 seconds")
		default:
			//prnt("Checking if commit msg received")
			for i, node:= range rafts {
				if ! checked[i] {
					if ! node.IsNodeUp() { // if node is down then ignore it
						checked[i] = true
						checkedNum++
						prnt("Node is down, ignoring commit for this %v", node.GetId())
					} else {
						select {
						case ci := <-node.CommitChannel:
							if ci.err != "" {
								prnt("Unable to commit the log : %v", ci.err)
							} else {
								prnt("Commit received at commit channel of %v", node.GetId())
							}
							checked[i] = true // Ignore from future consideration
							checkedNum++
							if ci.data.Data != data {
								t.Fatalf("Got different data : expected %v , received : %v", data, ci.data.Data)
							}
						default:
						}
					}
				}
			}
		}
	}


}

func (rafts Rafts) restoreRaft(t *testing.T, node_id int) {
	node_index := node_id-1

	config, err := FromConfigFile("/tmp/raft/node" + strconv.Itoa(node_id) + "/config.json")
	prnt("Node config read : %+v", config)
	if err!=nil {
		t.Fatalf("Error reopening config : %v", err.Error())
	}
	config.mockServer = mockCluster.Servers[config.Id]
	//config.mockServer.Heal()
	rafts[node_index] = config.RestoreServerState()
	rafts[node_index].Start()

}

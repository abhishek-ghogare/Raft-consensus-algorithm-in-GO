package main

import (
	"testing"
	"time"
	"strconv"
	"os"
)


func makeConfigs() []*Config {
	var nodeNetAddrList []NodeNetAddr
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:1, Host:"localhost", Port:7001} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:2, Host:"localhost", Port:7002} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:3, Host:"localhost", Port:7003} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:4, Host:"localhost", Port:7004} )
	nodeNetAddrList = append(nodeNetAddrList, NodeNetAddr {Id:5, Host:"localhost", Port:7005} )

	configBase := Config {
		NodeNetAddrList 	:nodeNetAddrList,
		Id                  :1,
		LogDir              :"log file",          // Log file directory for this node
		ElectionTimeout     :500,
		HeartbeatTimeout    :200}

	var configs []*Config
	for i:=1 ; i<=5 ; i++ {
		config := configBase
		config.Id = i
		config.LogDir = "/tmp/raft/node"+strconv.Itoa(i)
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

func shutdownRafts (rafts Rafts) {
	for _, r := range rafts {
		//prnt("%+v", r)
		r.Shutdown()
	}
}
func cleanupLogs(){
	os.RemoveAll("/tmp/raft/")
}

// Gets stable leader, i.e. Only ONE leader is present and all other nodes are in follower state of that term
// If no stable leader is elected after 10 sec, error
func getLeader(t *testing.T, rafts Rafts) (*RaftNode) {
	var ldr *RaftNode
	var ldrTerm int

	// Set 10 sec time span to probe the stable leader
	abortCh := time.NewTimer(5*time.Second)

	for {
		select {
		case <-abortCh.C:	// listen on abort channel for abort timeout
			shutdownRafts(rafts)
			t.Fatalf("No stable leader elected after 5 seconds")
		default:
			ldrTerm = 0;
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
			if ldrTerm == 0 {
				continue
			}

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

func (rafts Rafts) checkSingleCommit (t *testing.T, data string) {
	// Set 5 sec time span to probe the commit channels
	abortCh := time.NewTimer(5*time.Second)

	// Set flag when either commitChannel received or the node is down
	// This will help in reading only one commit msg from any given node
	checked := make([]bool, len(rafts))
	for checkedNum := 0 ; checkedNum != len(rafts) ; {
		// Check for all nodes
		select {
		case <-abortCh.C:
			shutdownRafts(rafts)
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
								t.Fatalf(ci.err)
							}
							if string(ci.data.Data.([]byte)) != data {
								t.Fatalf("Got different data : expected %v , received : %v", data, string(ci.data.Data.([]byte)))
							}
							prnt("Commit received at commit channel of %v", node.GetId())
							checked[i] = true // Ignore from future consideration
							checkedNum++
						}
					}
				}
			}
		}
	}


}

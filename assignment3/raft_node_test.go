package main

import (
    "testing"
    "time"
    "fmt"
	"strconv"
	"os"
)

// Debugging tools
func prnt(format string, args ...interface{}) {
  fmt.Printf("[TEST] \t: " + format + "\n", args...)
}


type Rafts []*RaftNode

func makeRafts() Rafts {
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

	var rafts Rafts
	for i:=1 ; i<=5 ; i++ {
		config.Id = i
		config.LogDir = "/tmp/raft/node"+strconv.Itoa(i)
		prnt("Creating node")
		raft := NewRaftNode(config)
		raft.Start()
		//go raft.processEvents()
		rafts = append(rafts,raft)
	}

    //rafts[0].server_state.setupServer ( LEADER, len(config.NodeNetAddrList) )
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
				prnt("Stable leader found : %v", ldr.server_state.server_id)
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
							if string(ci.data) != data {
								t.Fatalf("Got different data")
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

func TestBasic (t *testing.T) {
	rafts := makeRafts() 		// array of []RaftNode
	prnt("Rafts created")
	ldr:= getLeader(t, rafts)	// Wait until a stable leader is elected
	ldr.Append([]byte("foo"))	// Append to leader
	ldr = getLeader(t, rafts)	// Again wait for stable leader
	rafts.checkSingleCommit(t,"foo")// Wait until next single append is commited on all nodes
	shutdownRafts(rafts)
	cleanupLogs()
}



func TestLeaderReelection (t *testing.T) {
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
	ldr := getLeader(t, rafts)
	ldr.Shutdown()
	ldr = getLeader(t, rafts)
	ldr.Append([]byte("foo"))
	rafts.checkSingleCommit(t,"foo")
	ldr.Shutdown()
	shutdownRafts(rafts)
	cleanupLogs()
}

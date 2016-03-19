package main

import (
    "testing"
	"strconv"
)



type Rafts []*RaftNode

func TestBasic (t *testing.T) {
	defer cleanupLogs()
	rafts := makeRafts() 		// array of []RaftNode
	prnt("Rafts created")
	ldr:= getLeader(t, rafts)	// Wait until a stable leader is elected
	ldr.Append([]byte("foo"))	// Append to leader
	ldr = getLeader(t, rafts)	// Again wait for stable leader
	rafts.checkSingleCommit(t,"foo")// Wait until next single append is commited on all nodes
	shutdownRafts(rafts)
}



func TestLeaderReelection (t *testing.T) {
	defer cleanupLogs()
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
	ldr := getLeader(t, rafts)
	ldr.Shutdown()
	ldr = getLeader(t, rafts)
	ldr.Append([]byte("foo"))
	rafts.checkSingleCommit(t,"foo")
	ldr.Shutdown()
	shutdownRafts(rafts)
}


func TestServerStateRestore (t *testing.T) {
	//defer cleanupLogs()
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")

	ldr := getLeader(t, rafts)
	ldr.Append([]byte("foo"))
	rafts.checkSingleCommit(t,"foo")

	ldr = getLeader(t, rafts)
	ldr.Append([]byte("bar"))
	rafts.checkSingleCommit(t,"bar")

	ldr = getLeader(t, rafts)
	ldr.Append([]byte("foo1"))
	rafts.checkSingleCommit(t,"foo1")

	ldr = getLeader(t, rafts)
	ldr.Append([]byte("bar1"))
	rafts.checkSingleCommit(t,"bar1")


	ldr = getLeader(t, rafts)
	ldr_id := ldr.GetId()
	ldr_index := ldr_id-1

	ldr.Shutdown()
	ldr = getLeader(t, rafts)

	config, err := FromConfigFile("/tmp/raft/node" + strconv.Itoa(ldr_id) + "/config.json")
	if err!=nil {
		t.Fatalf("Error reopening config : %v", err.Error())
	}
	rafts[ldr_index] = config.RestoreServerState()
	rafts[ldr_index].Start()


	ldr.Shutdown()
	shutdownRafts(rafts)
}

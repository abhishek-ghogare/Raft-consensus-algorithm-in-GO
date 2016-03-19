package main

import (
    "testing"
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

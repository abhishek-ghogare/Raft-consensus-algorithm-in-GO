package main

import (
    	"testing"
	"time"
	"math/rand"
)


func Test (t *testing.T) {
	rand.Seed(10)
	cleanupLogs()
	rafts := makeRafts() // array of []RaftNode


	ldr := rafts.getLeader(t)
	ldr.Append("foo")
	rafts.checkSingleCommit(t,"foo")

	ldr = rafts.getLeader(t)
	mockCluster.Partition([]int{1,2,3}, []int{4,5})
	time.Sleep(2*time.Second)
	mockCluster.Heal()
	ldr = rafts.getLeader(t)
	ldr.Append("bar")
	rafts.checkSingleCommit(t,"bar")

	rafts.shutdownRafts()
}


func TestNetworkPartition (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() // array of []RaftNode

	ldr := rafts.getLeader(t)
	ldr.Append("foo")
	rafts.checkSingleCommit(t,"foo")

	ldr = rafts.getLeader(t)
	mockCluster.Partition([]int{1,2,3}, []int{4,5})
	time.Sleep(2*time.Second)
	mockCluster.Heal()
	time.Sleep(2*time.Second)
	ldr = rafts.getLeader(t)
	ldr.Append("bar")

	rafts.shutdownRafts()
}

func TestBasic (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() 		// array of []RaftNode
	log_info("Rafts created")
	ldr:= rafts.getLeader(t)	// Wait until a stable leader is elected
	ldr.Append("foo")	// Append to leader
	ldr = rafts.getLeader(t)	// Again wait for stable leader
	rafts.checkSingleCommit(t,"foo")// Wait until next single append is commited on all nodes
	rafts.shutdownRafts()
}


func TestLeaderReelection (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() // array of []RaftNode

	ldr := rafts.getLeader(t)
	ldr.Shutdown()
	ldr = rafts.getLeader(t)
	ldr.Append("foo")
	rafts.checkSingleCommit(t,"foo")
	rafts.shutdownRafts()
}


func TestServerStateRestore (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() // array of []RaftNode

	ldr := rafts.getLeader(t)
	ldr.Append("foo")
	rafts.checkSingleCommit(t,"foo")


	ldr = rafts.getLeader(t)


	ldr.Append("bar")
	rafts.checkSingleCommit(t,"bar")

	ldr = rafts.getLeader(t)
	ldr.Append("foo1")
	rafts.checkSingleCommit(t,"foo1")

	ldr = rafts.getLeader(t)
	ldr.Append("bar1")
	rafts.checkSingleCommit(t,"bar1")


	ldr = rafts.getLeader(t)
	ldr_id := ldr.GetId()
	ldr_index := ldr_id-1

	ldr.Shutdown()
	ldr = rafts.getLeader(t)
	rafts.restoreRaft(t,ldr_id)

	expect(t, rafts[ldr_index].GetLogAt(1).Data , "foo", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].GetLogAt(2).Data , "bar", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].GetLogAt(3).Data , "foo1", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].GetLogAt(4).Data , "bar1", "Log mismatch after restarting server")

	ldr.Shutdown()
	rafts.shutdownRafts()
}



package main

import (
    "testing"
	"strconv"
)




func weTestBasic (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() 		// array of []RaftNode
	prnt("Rafts created")
	ldr:= rafts.getLeader(t)	// Wait until a stable leader is elected
	ldr.Append("foo")	// Append to leader
	ldr = rafts.getLeader(t)	// Again wait for stable leader
	rafts.checkSingleCommit(t,"foo")// Wait until next single append is commited on all nodes
	rafts.shutdownRafts()
}


func TestLeaderReelection (t *testing.T) {
	cleanupLogs()
	rafts := makeRafts() // array of []RaftNode
	prnt("Rafts created")
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
	prnt("Rafts created")

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

	config, err := FromConfigFile("/tmp/raft/node" + strconv.Itoa(ldr_id) + "/config.json")
	prnt("Node config read : %+v", config)
	if err!=nil {
		t.Fatalf("Error reopening config : %v", err.Error())
	}
	config.mockServer = mockCluster.Servers[config.Id]
	//config.mockServer.Heal()
	rafts[ldr_index] = config.RestoreServerState()
	rafts[ldr_index].Start()

	expect(t, rafts[ldr_index].server_state.logs[1].Data , "foo", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].server_state.logs[2].Data , "bar", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].server_state.logs[3].Data , "foo1", "Log mismatch after restarting server")
	expect(t, rafts[ldr_index].server_state.logs[4].Data , "bar1", "Log mismatch after restarting server")

	ldr.Shutdown()
	rafts.shutdownRafts()
}



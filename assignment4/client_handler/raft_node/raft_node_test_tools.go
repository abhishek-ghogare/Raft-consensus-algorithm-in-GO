package raft_node

import (
    "testing"
    "time"
    "strconv"
    "os"
    rsm "cs733/assignment4/client_handler/raft_node/raft_state_machine"
    "cs733/assignment4/logging"
    "cs733/assignment4/raft_config"
    "errors"
    "github.com/cs733-iitb/cluster"
)

func log_error(skip int, format string, args ...interface{}) {
    logging.Error(skip, "[TEST] : " + format, args...)
}
func log_info(skip int, format string, args ...interface{}) {
    logging.Info(skip, "[TEST] : " + format, args...)
}
func log_warning(skip int, format string, args ...interface{}) {
    logging.Warning(skip, "[TEST] : " + format, args...)
}

type Rafts []*RaftNode

//var mockCluster *mock.MockCluster

func expect(t *testing.T, found interface{}, expected interface{}, msg string) {
    if found != expected {
        t.Errorf("Expected %v, found %v : %v", expected, found, msg) // t.Error is visible when running `go test -verbose`
    }
}


func (rafts Rafts) restoreRaft(t *testing.T, node_id int) {
	node_index := node_id - 1

    // TODO:: make single config for all nodes
	config, err := raft_config.FromConfigFile("/tmp/raft/node" + strconv.Itoa(node_id) + "/config.json")
	log_info(3, "Node config read : %+v", config)
	if err != nil {
		t.Fatalf("Error reopening config : %v", err.Error())
	}

    //_, err = mockCluster.AddServer(config.Id)
    if err != nil {
        rafts.shutdownRafts()
        t.Fatalf("Unable to re-add server with id : %v : error:%v", node_id, err.Error())
    }
	//config.MockServer = mockCluster.Servers[config.Id]
	//config.mockServer.Heal()
	rafts[node_index] = RestoreServerState(node_id, config)
	rafts[node_index].Start()

}


func makeConfigs() []*raft_config.Config {
    var err error
    //mockCluster, err = mock.NewCluster(nil)
    if err != nil {
        log_error(3, "Error in creating CLUSTER : %v", err.Error())
    }

    configBase := raft_config.Config{
        //Id                  : 1,
        LogDir              : "log file", // Log file directory for this node
        ElectionTimeout     : 500,
        HeartbeatTimeout    : 100,
        NumOfNodes          : 5,
        ClusterConfig       : cluster.Config   {
                                                    Peers: []cluster.PeerConfig{
                                                        {Id: 1, Address: "localhost:7001"},
                                                        {Id: 2, Address: "localhost:7002"},
                                                        {Id: 3, Address: "localhost:7003"},
                                                        {Id: 4, Address: "localhost:7004"},
                                                        {Id: 5, Address: "localhost:7005"},
                                                    },
                                                },
    }

    var configs []*raft_config.Config
    for i := 1; i <= 5; i++ {
        config := configBase // Copy config
        //config.Id = i
        config.LogDir = "/tmp/raft/node" + strconv.Itoa(i) + "/"
        //mockCluster.AddServer(i)
        //config.MockServer = mockCluster.Servers[i]
        log_info(3, "Creating Config %v", i)
        configs = append(configs, &config)
    }
    return configs
}

func makeRafts() Rafts {
    var rafts Rafts
    for i, conf := range makeConfigs() {
        raft := NewRaftNode(i+1, conf)
        err := raft_config.ToConfigFile(conf.LogDir + "config.json", *conf)
        if err != nil {
            log_error(3, "Error in storing config to file : %v", err.Error())
        }
        raft.Start()
        rafts = append(rafts, raft)
    }

    return rafts
}

func (rafts Rafts) shutdownRafts() {
    log_info(3, "Shutting down all rafts")
    for _, r := range rafts {
        r.Shutdown()
    }
}
func cleanupLogs() {
    os.RemoveAll("/tmp/raft/")
}


/**
 *
 * Gets stable leader, i.e. Only ONE leader is present and all other nodes are in follower state of that term
 * If no stable leader is elected after timeout, return the current leader
 */
func (rafts Rafts) getLeader(t *testing.T) (*RaftNode) {
    var ldr *RaftNode

    // Set 10 sec time span to probe the stable leader
    abortCh := time.NewTimer(4 * time.Second)
    for {
        //time.Sleep(200*time.Millisecond)
        select {
        case <-abortCh.C:
        // listen on abort channel for abort timeout
            log_warning(3, "Stable leader NOT found : %v", ldr.GetId())
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
                    if node.GetServerState() != rsm.FOLLOWER ||
                    node.GetCurrentTerm() != ldr.GetCurrentTerm() {
                        // If this node is not follower
                        // or if this node is not follower of the leader
                        areAllFollowers = false
                        break
                    }
                }
            }

            if areAllFollowers {
                log_info(3, "Stable leader found : %v", ldr.GetId())
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
    abortCh := time.NewTimer(4 * time.Second)

    for {
        select {
        case <-abortCh.C:
            // listen on abort channel for abort timeout
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

func (rafts Rafts) checkSingleCommit(t *testing.T, data interface{}) error{
    // Set 5 sec time span to probe the commit channels
    abortCh := time.NewTimer(5 * time.Second)

    // Set flag when either commitChannel received or the node is down
    // This will help in reading only one commit msg from any given node
    checked := make([]bool, len(rafts))
    for checkedNum := 0; checkedNum < len(rafts); {
        // Check for all nodes
        select {
        case <-abortCh.C:
            return  errors.New("Commit msg not received on all nodes after 5 seconds")
        default:
            //prnt("Checking if commit msg received")
            for i, node := range rafts {
                if ! checked[i] {
                    if ! node.IsNodeUp() {
                        // if node is down then ignore it
                        checked[i] = true
                        checkedNum++
                        log_warning(3, "Node is down, ignoring commit for this %v", node.GetId())
                    } else {
                        select {
                        case ci := <-node.CommitChannel:
                            if ci.Err != nil {
                                log_warning(3, "Unable to commit the log : %v", ci.Err.Error())
                            } else {
                                log_info(3, "Commit received at commit channel of %v", node.GetId())
                            }
                            checked[i] = true // Ignore from future consideration
                            checkedNum++
                            if ci.Data != data {
                                rafts.shutdownRafts()
                                t.Fatalf("Got different data : expected %v , received : %v", data, ci.Data)
                            }
                        default:
                        }
                    }
                }
            }
        }
    }
    return nil
}

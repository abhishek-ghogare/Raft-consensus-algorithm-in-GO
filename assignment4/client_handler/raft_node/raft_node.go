package raft_node

import (
    "github.com/cs733-iitb/cluster"
    "math/rand"
    "reflect"
    "time"
    "sync"
    "strconv"
    rsm "cs733/assignment4/client_handler/raft_node/raft_state_machine"
    "cs733/assignment4/logging"
    "fmt"
    "path"
)

func (rn RaftNode) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[RN:%v] %v ", strconv.Itoa(rn.GetId()), strconv.Itoa(rn.GetCurrentTerm())) + format
    logging.Error(skip, format, args...)
}
func (rn RaftNode) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[RN:%v] %v ", strconv.Itoa(rn.GetId()), strconv.Itoa(rn.GetCurrentTerm())) + format
    logging.Info(skip, format, args...)
}
func (rn RaftNode) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[RN:%v] %v ", strconv.Itoa(rn.GetId()), strconv.Itoa(rn.GetCurrentTerm())) + format
    logging.Warning(skip, format, args...)
}


type RaftNode struct {
                             // implements Node interface
    eventCh         chan interface{}
    timeoutCh       chan interface{}
                             //config          Config
    LogDir          string   // Log file directory for this node
    server_state   *rsm.StateMachine
    clusterServer   cluster.Server
                             //clusterServer   *mock.MockServer
    timer           *time.Timer

                             // A channel for client to listen on. What goes into Append must come out of here at some point.
    CommitChannel   chan rsm.CommitAction
    ShutdownChannel chan int
    isUp            bool
    isInitialized   bool

                             // Wait in shutdown function until the processEvents go routine returns and all resources gets cleared
    waitShutdown    sync.WaitGroup
    ServerList      []string // List of addrs of other raft nodes, 0th addr is null
}


// Client's message to Raft node
func (rn *RaftNode) Append(data interface{}) {
    rn.eventCh <- rsm.AppendEvent{Data: data}
}
// Called from client handler, which will first apply a log to state machine then instructs here to update lastApplied
func (rn *RaftNode) UpdateLastApplied(index int64) {
    rn.eventCh <- rsm.UpdateLastAppliedEvent{Index: index}
}

func (rn *RaftNode) processEvents() {
    rn.log_info(3, "Process events started")
    if !rn.IsNodeInitialized() {
        rn.log_error(3, "Raft node not initialized")
        return
    }

    if rn.clusterServer.IsClosed() {
        rn.log_warning(3, "Cluster server is closed")
        return
    }

    rn.timer = time.NewTimer(time.Duration(rn.server_state.ElectionTimeout + rand.Intn(rsm.RandomTimeout)) * time.Millisecond)
    rn.isUp = true
    for {
        var ev interface{}
        select {
        case ev = <-rn.timer.C:
            actions := rn.server_state.ProcessEvent(rsm.TimeoutEvent{})
            rn.doActions(actions)
        case ev = <-rn.eventCh:

            // Get batch of max 500 requests
            appendEvents     := []rsm.AppendEvent{}
            lastAppliedEvent := rsm.UpdateLastAppliedEvent{}

        RequestFetcherLoop:
            for count:=0 ;  ; count++{
                // Serve first event fetched from event channel
                switch ev.(type) {
                case rsm.AppendEvent:
                    appendEvents = append(appendEvents, ev.(rsm.AppendEvent))
                case rsm.UpdateLastAppliedEvent:
                    if lastAppliedEvent.Index < ev.(rsm.UpdateLastAppliedEvent).Index {
                        lastAppliedEvent = ev.(rsm.UpdateLastAppliedEvent)
                    }
                }

                if count>500 {
                    break
                }

                // Fetch next event if available, or break
                select {
                case ev = <- rn.eventCh:
                    rn.log_info(3, "Fetching another event, append events : %v", len(appendEvents))
                default:
                    rn.log_info(3, "All events fetched : %v", len(appendEvents))
                    break RequestFetcherLoop
                }
            }

            actions := []interface{}{}
            if len(appendEvents) > 0 {
                rn.log_info(3, "Append request/s received of length %v", len(appendEvents))
                actions = rn.server_state.ProcessEvent(&appendEvents)
            }

            if lastAppliedEvent.Index > 0 {
                rn.server_state.LastApplied = lastAppliedEvent.Index
                rn.log_info(3, "Update lastApplied to %v", rn.server_state.LastApplied)
                stateStoreAction := rn.server_state.GetStateStoreAction()
                actions = []interface{}{stateStoreAction}
            }

            rn.doActions(actions)
        case ev = <- rn.clusterServer.Inbox():
            ev := ev.(*cluster.Envelope)

        // Debug logging
            switch ev.Msg.(type) {
            case rsm.AppendRequestEvent:
                //rn.log_info("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.GetId(), ev.Pid, ev.Msg)
            case rsm.AppendRequestRespEvent:
                //rn.log_info("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.GetId(), ev.Pid, ev.Msg)
            case rsm.RequestVoteEvent :
            //rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.server_id, ev.Pid, ev.Msg)
            case rsm.RequestVoteRespEvent :
            //rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.server_id, ev.Pid, ev.Msg)
            }

            event := ev.Msg.(interface{})
            actions := rn.server_state.ProcessEvent(event)
            rn.doActions(actions)
        case _, ok := <-rn.ShutdownChannel:
            if !ok {
                // If channel closed, return from function
                close(rn.CommitChannel)
                close(rn.eventCh)
                close(rn.timeoutCh)
                rn.server_state.PersistentLog.Close()
                rn.clusterServer.Close() // in restoring the node, restarting this cluster is not possible, so avoid closing
                rn.server_state = &rsm.StateMachine{}
                rn.waitShutdown.Done()
                return
            }
        //default:
        //fmt.Printf("Hello %v\n", rn.config.Id)
        //rn.eventCh <- timeoutEvent{}
        }
    }
}

func (rn *RaftNode) doActions(actions [] interface{}) {
    for _, action := range actions {
        switch action.(type) {

        /*
         *  Send Action
         */
        case rsm.SendAction :
            action := action.(rsm.SendAction)

            // Debug logging
            switch action.Event.(type) {
            case rsm.AppendRequestEvent:
                appendReqE := action.Event.(rsm.AppendRequestEvent)
                length := len(appendReqE.Entries)
                var start, end int64
                if length!=0 {
                    start = appendReqE.Entries[0].Index
                    end = appendReqE.Entries[length-1].Index
                    rn.log_info(3, "%25v %2v -->> %-14v from index %v to %v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, start, end)
                } else {
                    rn.log_info(3, "%25v %2v -->> %-14v Heartbeat %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
                }
            case rsm.AppendRequestRespEvent:
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            case rsm.RequestVoteEvent :
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            case rsm.RequestVoteRespEvent :
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            }

            if action.ToId == -1 {
                rn.clusterServer.Outbox() <- &cluster.Envelope{Pid:cluster.BROADCAST, Msg:action.Event}
            } else {
                rn.clusterServer.Outbox() <- &cluster.Envelope{Pid:action.ToId, Msg:action.Event}
            }

        /*
         *  Commit action
         */
        case rsm.CommitAction :
            action := action.(rsm.CommitAction)
            rn.log_info(3, "commitAction received  for index %v", action.Log.Index)
            rn.CommitChannel <- action

        /*
         *  Alarm action
         */
        case rsm.AlarmAction :
            action := action.(rsm.AlarmAction)
            rn.timer.Reset(time.Duration(action.Time) * time.Millisecond)
            rn.log_info(3, "Alarm action received of time %v", action.Time)

        /*
         *  State store action
         */
        case rsm.StateStore:
            stateStore := action.(rsm.StateStore)
            statePath := path.Clean(rn.LogDir + "/raft_" + strconv.Itoa(rn.GetId()) + "/" + rsm.RaftStateFile)
            stateStore.State.ToServerStateFile(statePath)
            //rn.log_info(3, "state store received")
        default:
            rn.log_error(3, "Unknown action received : %v", action)
        }
    }
}

func (rn *RaftNode) Start() {
    rn.log_info(4, "Starting raft node")
    go rn.processEvents()
}
// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown() {
    if !rn.IsNodeUp() {
        rn.log_warning(4, "Already down")
        return
    }

    rn.log_info(4, "Shutting down")
    rn.isUp = false
    rn.isInitialized = false
    rn.timer.Stop()
    rn.waitShutdown.Add(1)
    close(rn.ShutdownChannel)       // Closing this channel would trigger the go routine to terminate
    rn.waitShutdown.Wait()
}

func (rn *RaftNode) GetId() int {
    if rn.IsNodeInitialized() {
        return rn.server_state.GetServerId()
    } else {
        //log_warning(4, "Node not initialized")
        return 0;
    }
}

func (rn *RaftNode) GetLogAt(index int64) rsm.LogEntry { // TODO:: return nil on error
    if ! rn.IsNodeInitialized() {
        logging.Warning(3, "Node not initialized")
        return rsm.LogEntry{};
    }

    return *rn.server_state.GetLogOf(index)
}

func (rn *RaftNode) GetCurrentTerm() int {
    if rn.IsNodeInitialized() {
        return rn.server_state.GetCurrentTerm()
    } else {
        logging.Warning(3, "Node not initialized")
        return 0
    }
}

func (rn *RaftNode) GetServerState() rsm.RaftState {
    if rn.IsNodeInitialized() {
        return rn.server_state.GetServerState()
    } else {
        logging.Warning(3, "Node not initialized")
        return 0
    }
}

func (rn *RaftNode) IsNodeUp() bool {
    return rn.isUp
}
func (rn *RaftNode) IsNodeInitialized() bool {
    return rn.isInitialized
}
func (rn *RaftNode) IsLeader() bool {
    return rn.IsNodeUp() && (rn.server_state.GetServerState() == rsm.LEADER)
}
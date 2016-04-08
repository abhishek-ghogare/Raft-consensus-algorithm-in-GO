package raft_node

import (
    "encoding/gob"
    "github.com/cs733-iitb/cluster"
    "github.com/cs733-iitb/log"
    "math/rand"
    "reflect"
    "time"
    "sync"
    "strconv"
    "github.com/cs733-iitb/cluster/mock"
    rsm "cs733/assignment4/raft_node/raft_state_machine"
    "cs733/assignment4/logging"
    "fmt"
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
    LogDir          string // Log file directory for this node
    server_state    *rsm.StateMachine
    clusterServer   *mock.MockServer
    logs            *log.Log
    timer           *time.Timer

                           // A channel for client to listen on. What goes into Append must come out of here at some point.
    CommitChannel   chan rsm.CommitAction
    ShutdownChannel chan int
    isUp            bool
    isInitialized   bool

                           // Wait in shutdown function until the processEvents go routine returns and all resources gets cleared
    waitShutdown    sync.WaitGroup
}


// Client's message to Raft node
func (rn *RaftNode) Append(data interface{}) {
    rn.eventCh <- rsm.AppendEvent{Data: data}
}
func (rn *RaftNode) UpdateLastApplied(index int) {
    rn.eventCh <- rsm.UpdateLastAppliedEvent{Index: index}
}
// TODO:: Update lastapplied function here, sends event on eventCh

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

    RegisterEncoding()
    rn.timer = time.NewTimer(time.Duration(rn.server_state.ElectionTimeout + rand.Intn(rsm.RandomTimeout)) * time.Millisecond)
    rn.isUp = true
    for {
        var ev interface{}
        select {
        case ev = <-rn.timer.C:
            actions := rn.server_state.ProcessEvent(rsm.TimeoutEvent{})
            rn.doActions(actions)
        case ev = <-rn.eventCh:
            switch ev.(type) {
            case rsm.AppendEvent:
                rn.log_info(3, "Append request received")
                actions := rn.server_state.ProcessEvent(ev)
                rn.doActions(actions)
            case rsm.UpdateLastAppliedEvent:
                // TODO:: handle update lastApplied here
                rn.log_info(3, "Update last applied event received")
                rn.server_state.LastApplied = ev.(rsm.UpdateLastAppliedEvent).Index
                stateStoreAction := rn.server_state.GetStateStoreAction()
                actions := []interface{}{stateStoreAction}
                rn.doActions(actions)
            }
        case ev = <-(*rn.clusterServer).Inbox():
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
                (*rn.clusterServer).Close() // in restoring the node, restarting this cluster is not possible, so avoid closing
                rn.logs.Close() //TODO:: leveldb not found problem when restore testcase is executed first
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
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            case rsm.AppendRequestRespEvent:
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            case rsm.RequestVoteEvent :
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            case rsm.RequestVoteRespEvent :
                rn.log_info(3, "%25v %2v -->> %-14v %+v", reflect.TypeOf(action.Event).Name(), rn.GetId(), action.ToId, action.Event)
            }

            if action.ToId == -1 {
                (*rn.clusterServer).Outbox() <- &cluster.Envelope{Pid:cluster.BROADCAST, Msg:action.Event}
            } else {
                (*rn.clusterServer).Outbox() <- &cluster.Envelope{Pid:action.ToId, Msg:action.Event}
            }

        /*
         *  Commit action
         */
        case rsm.CommitAction :
            rn.log_info(3, "commitAction received %+v", action)
            action := action.(rsm.CommitAction)
            rn.CommitChannel <- action


        /*
         *  Log store action
         */
        case rsm.LogStore :
            rn.log_info(3, "logStore received")
            action := action.(rsm.LogStore)
            lastInd := int(rn.logs.GetLastIndex())
            if lastInd >= action.Index {
                rn.logs.TruncateToEnd(int64(action.Index)) // Truncate extra entries
            } else if lastInd < action.Index - 1 {
                rn.log_error(3, "Log inconsistency found")
            }
            rn.logs.Append(rsm.LogEntry{Index:action.Index, Term:action.Term, Data:action.Data})

        /*
         *  Alarm action
         */
        case rsm.AlarmAction :
            action := action.(rsm.AlarmAction)
            rn.timer.Reset(time.Duration(action.Time) * time.Millisecond)

        /*
         *  State store action
         */
        case rsm.StateStore:
            stateStore := action.(rsm.StateStore)
            rn.log_info(3, "state store received")
            stateStore.State.ToServerStateFile(rn.LogDir + rsm.RaftStateFile)
        default:
            rn.log_error(3, "Unknown action received : %v", action)
        }
    }
}

func RegisterEncoding() {
    gob.Register(rsm.AppendRequestEvent{})
    gob.Register(rsm.AppendRequestRespEvent{})
    gob.Register(rsm.RequestVoteEvent{})
    gob.Register(rsm.RequestVoteRespEvent{})
    //gob.Register(timeoutEvent{})
    gob.Register(rsm.AppendEvent{})
    gob.Register(rsm.LogEntry{})
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

func (rn *RaftNode) GetLogAt(index int) rsm.LogEntry { // TODO:: return nil on error
    if ! rn.IsNodeInitialized() {
        log_warning(4, "Node not initialized")
        return rsm.LogEntry{};
    }

    log, err := rn.logs.Get(int64(index))
    if err != nil {
        log_error(4, "Log access error : %v", err.Error())
        return rsm.LogEntry{};
    }

    return log.(rsm.LogEntry)
}

func (rn *RaftNode) GetCurrentTerm() int {
    if rn.IsNodeInitialized() {
        return rn.server_state.GetCurrentTerm()
    } else {
        log_warning(4, "Node not initialized")
        return 0
    }
}

func (rn *RaftNode) GetServerState() rsm.RaftState {
    if rn.IsNodeInitialized() {
        return rn.server_state.GetServerState()
    } else {
        log_warning(4, "Node not initialized")
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
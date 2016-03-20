package main

import (
    "encoding/gob"
    "github.com/cs733-iitb/cluster"
    "github.com/cs733-iitb/log"
    "math/rand"
    "reflect"
    "time"
    "sync"
    "os"
    "encoding/json"
    "errors"
    "strconv"
    "github.com/cs733-iitb/cluster/mock"
)

type RaftNode struct { // implements Node interface
    eventCh         chan interface{}
    timeoutCh       chan interface{}
    //config          Config
    LogDir          string          // Log file directory for this node
    server_state    ServerState
    clusterServer   *mock.MockServer
    logs            *log.Log
    timer           *time.Timer

    // A channel for client to listen on. What goes into Append must come out of here at some point.
    CommitChannel   chan commitAction
    ShutdownChannel chan int
    isUp            bool
    isInitialized   bool

    // Wait in shutdown function until the processEvents go routine returns and all resources gets cleared
    waitShutdown    sync.WaitGroup

}

// Last known committed index in the log.  This is 0 until the system stabilizes.
func (rn *RaftNode) CommittedIndex() int {
    if rn.IsNodeUp() {
        return rn.server_state.CommitIndex
    } else {
        return 0
    }
}

func ToConfigFile(configFile string, config Config) (err error){
    var f *os.File
    if f, err = os.Create(configFile); err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    if err = enc.Encode(config); err != nil {
        return  err
    }
    return nil
}

func FromConfigFile(configuration interface{}) (config *Config, err error){
    var cfg Config
    var ok bool
    var configFile string
    if configFile, ok = configuration.(string); ok {
        var f *os.File
        if f, err = os.Open(configFile); err != nil {
            return nil, err
        }
        defer f.Close()
        dec := json.NewDecoder(f)
        if err = dec.Decode(&cfg); err != nil {
            return nil, err
        }
    } else if cfg, ok = configuration.(Config); !ok {
        return nil, errors.New("Expected a configuration.json file or a Config structure")
    }
    return &cfg, nil
}


func ToServerStateFile(serverStateFile string, serState ServerState) (err error){
    var f *os.File
    if f, err = os.Create(serverStateFile); err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    if err = enc.Encode(serState); err != nil {
        return  err
    }
    return nil
}

func FromServerStateFile(serverState interface{}) ( serState *ServerState, err error){
    var state ServerState
    var ok bool
    var serverStateFile string
    if serverStateFile, ok = serverState.(string); ok {
        var f *os.File
        if f, err = os.Open(serverStateFile); err != nil {
            return nil, err
        }
        defer f.Close()
        dec := json.NewDecoder(f)
        if err = dec.Decode(&state); err != nil {
            return nil, err
        }
    } else if state, ok = serverState.(ServerState); !ok {
        return nil, errors.New("Expected a serverstate.json file or a ServerState structure")
    }
    return &state, nil
}


// Client's message to Raft node
func (rn *RaftNode) Append(data string) {
                //fmt.Println("channel in append ", &rn.eventCh)
    rn.eventCh <- appendEvent{data: data}
                //fmt.Printf("Hello\n")
}

func (rn *RaftNode) processEvents() {
    rn.prnt("Process events started")
    if !rn.IsNodeInitialized() {
        rn.prnt("Raft node not initialized")
        return
    }

    if rn.clusterServer.IsClosed() {
        rn.prnt("ERROR:: cluster server is closed\n\n\n")
    }

    RegisterEncoding()
    rn.timer = time.NewTimer(time.Duration(rn.server_state.electionTimeout +rand.Intn(400))*time.Millisecond)
    rn.isUp = true
    for {
        var ev interface{}
                //fmt.Println("channel in process events ",rn.config.Id, &rn.eventCh)
        select {
        case ev = <- rn.timer.C :
            //rn.prnt("Timeout event received")
            actions := rn.server_state.processEvent(timeoutEvent{})
            rn.doActions(actions)
        //ev = timeoutEvent{}
        case ev = <- rn.eventCh :
            rn.prnt("Append request received")
            actions := rn.server_state.processEvent(ev)
            rn.doActions(actions)
        case ev = <- (*rn.clusterServer).Inbox() :
            ev := ev.(*cluster.Envelope)

            // Debug logging
            switch ev.Msg.(type) {
            case appendRequestEvent:
                rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.Server_id, ev.Pid, ev.Msg)
            case appendRequestRespEvent:
                rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.Server_id, ev.Pid, ev.Msg)
            case requestVoteEvent :
                //rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.server_id, ev.Pid, ev.Msg)
            case requestVoteRespEvent :
                //rn.prnt("%25v %2v <<-- %-14v %+v", reflect.TypeOf(ev.Msg).Name(), rn.server_state.server_id, ev.Pid, ev.Msg)
            }

            //rn.prnt("InboxEvent  : from %v \"%v\" \t\t%v", ev.Pid, reflect.TypeOf(ev.Msg).Name(), ev)
            //reflect.TypeOf(ev.Msg).Name()
            event := ev.Msg.(interface{})
            actions := rn.server_state.processEvent(event)
            rn.doActions(actions)
        case _, ok := <- rn.ShutdownChannel:
            if !ok {    // If channel closed, return from function
                close(rn.CommitChannel)
                close(rn.eventCh)
                close(rn.timeoutCh)
                (*rn.clusterServer).Close()
                rn.logs.Close() //TODO:: leveldb not found problem
                rn.server_state = ServerState{}
                rn.waitShutdown.Done()
                return
            }
        default:
            //fmt.Printf("Hello %v\n", rn.config.Id)
            //rn.eventCh <- timeoutEvent{}
        }
    }
}

func (rn *RaftNode) doActions(actions [] interface{}) {

    //var timer *Timer

    for _,action := range actions {
        switch action.(type) {
        case sendAction :
            action := action.(sendAction)

            // Debug logging
            switch action.event.(type) {
            case appendRequestEvent:
                rn.prnt("%25v %2v -->> %-14v %+v", reflect.TypeOf(action.event).Name(), rn.server_state.Server_id, action.toId, action.event)
            case appendRequestRespEvent:
                rn.prnt("%25v %2v -->> %-14v %+v", reflect.TypeOf(action.event).Name(), rn.server_state.Server_id, action.toId, action.event)
            case requestVoteEvent :
                rn.prnt("%25v %2v -->> %-14v %+v", reflect.TypeOf(action.event).Name(), rn.server_state.Server_id, action.toId, action.event)
            case requestVoteRespEvent :
                rn.prnt("%25v %2v -->> %-14v %+v", reflect.TypeOf(action.event).Name(), rn.server_state.Server_id, action.toId, action.event)
            }
            //rn.prnt("OutboxEvent : to   %v \"%v\"\t\t%v", action.toId, reflect.TypeOf(action.event).Name(), action.event)


            if action.toId == -1 {
                (*rn.clusterServer).Outbox() <- &cluster.Envelope{Pid:cluster.BROADCAST, Msg:action.event}
            } else {
                (*rn.clusterServer).Outbox() <- &cluster.Envelope{Pid:action.toId, Msg:action.event}
            }
        case commitAction :
            rn.prnt("commitAction received %+v", action)
            action := action.(commitAction)
            rn.CommitChannel <- action
        case logStore :
            rn.prnt("logStore received")
            action := action.(logStore)
            lastInd := int(rn.logs.GetLastIndex())
            if lastInd >= action.index {
                rn.logs.TruncateToEnd(int64(action.index)) // Truncate extra entries
            } else if lastInd < action.index-1 {
                rn.prnt("Log inconsistency found")
            }
            rn.logs.Append(action.data)
        case alarmAction :
            //rn.prnt("==== %25v", "resetting alarm")
            action := action.(alarmAction)
            //rn.timer.Stop()
            //timer =
            rn.timer.Reset(time.Duration(action.time)*time.Millisecond)
        default:

        }
    }
}

func RegisterEncoding () {
    gob.Register(appendRequestEvent{})
    gob.Register(appendRequestRespEvent{})
    gob.Register(requestVoteEvent{})
    gob.Register(requestVoteRespEvent{})
    //gob.Register(timeoutEvent{})
    gob.Register(appendEvent{})
    gob.Register(LogEntry{})
}

func (rn *RaftNode) Start() {
    go rn.processEvents()
}
// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown() {
    err := ToServerStateFile("/tmp/raft/node" + strconv.Itoa(rn.GetId()) + "/serverState.json", rn.server_state) // TODO:: temp patch
    if err!=nil {
        prnt("Failed to store server state")
    } else {
        prnt("Server state stored on file")
    }
    if !rn.IsNodeUp() {
        rn.prnt("Already down")
        return
    }
    rn.prnt("Shutting down")
    rn.isUp = false
    rn.isInitialized = false
    rn.timer.Stop()
    rn.waitShutdown.Add(1)
    close(rn.ShutdownChannel)       // Closing this channel would trigger the go routine to terminate
    rn.waitShutdown.Wait()
}

func (rn *RaftNode) GetId() int {
    if rn.IsNodeUp() {
        return rn.server_state.Server_id
    } else {
        return 0;
    }
}

func (rn *RaftNode) GetCurrentTerm() int {
    if rn.IsNodeUp() {
        return rn.server_state.CurrentTerm
    } else {
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
    return rn.IsNodeUp() && (rn.server_state.myState ==LEADER)
}
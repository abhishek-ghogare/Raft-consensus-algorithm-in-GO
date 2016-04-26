package client_handler

import (
    "bufio"
    "fmt"
    "net"
    "os"
    "strconv"
    "cs733/assignment4/client_handler/raft_node"
    "cs733/assignment4/client_handler/filesystem/fs"
    "sync"
    rsm "cs733/assignment4/client_handler/raft_node/raft_state_machine"
    "cs733/assignment4/raft_config"
    "cs733/assignment4/logging"
    "time"
    "encoding/gob"
)

const CONNECTION_TIMEOUT = 10*time.Minute // in seconds

/*
 *  Debug tools
 */
func (chdlr *ClientHandler) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", chdlr.Raft.GetId()) + format
    logging.Error(skip, format, args...)
}
func (chdlr *ClientHandler) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", chdlr.Raft.GetId()) + format
    logging.Info(skip, format, args...)
}
func (chdlr *ClientHandler) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", chdlr.Raft.GetId()) + format
    logging.Warning(skip, format, args...)
}

var crlf = []byte{'\r', '\n'}

/*
 *  Request, containing msg from client, is replicated into raft nodes
 */
type Request struct {
    ServerId int    // Id of raft node on which the request has arrived
    ReqId    int    // Request id and wait channel, mapped into ActiveReq, used to send
                    // replicated msg to correct tcp serve thread which is handling this request
    Message  fs.Msg // Request from client
}

/*
 *  Client handler
 */
type ClientHandler struct {
    Raft             *raft_node.RaftNode
    ActiveReq        map[int]chan fs.Msg // Mapping of request id to channel on which serve thread
                                         // is waiting for the request to get replicated on raft nodes
    ActiveReqLock    sync.RWMutex        // Lock on active requests map
    NextReqId        int                 // Next request id available to be assigned to next request
    ClientPort       int                 // Port on which the client handler will listen for client requests
    WaitOnServerExit sync.WaitGroup
    shutDownChan     chan int            // This channel is closed in shutdown to force all threads to stop
}

/***
 *  Create client handler
 *  # Id        : Id of the raft node
 *  # config    : Raft node config
 *  # restore   : Whether to clean start or resume from last crash point
 */
func New(Id int, config *raft_config.Config, restore bool) (chd *ClientHandler) {

    // Register the structures to gob
    gob.Register(fs.Msg{})
    gob.Register(Request{})
    gob.Register(rsm.AppendRequestEvent{})
    gob.Register(rsm.AppendRequestRespEvent{})
    gob.Register(rsm.RequestVoteEvent{})
    gob.Register(rsm.RequestVoteRespEvent{})
    //gob.Register(rsm.TimeoutEvent{})          // Not sending timeout event, no need to register
    gob.Register(rsm.AppendEvent{})
    gob.Register(rsm.LogEntry{})

    // Create/restore raft node based on command line parameter
    var raft *raft_node.RaftNode
    if restore {
        raft = raft_node.RestoreServerState(Id, config)
    } else {
        raft = raft_node.NewRaftNode(Id, config)
    }

    // Create client handler
    chd = &ClientHandler{
        Raft        : raft,
        ActiveReq   : make(map[int]chan fs.Msg),
        NextReqId   : 0,
        ClientPort  : config.ClientPorts[Id],
        shutDownChan: make(chan int) }

    chd.WaitOnServerExit.Add(2) // Client handler and listener

    return chd
}

/***
 *  Asynchronously start client handler
 */
func (chd *ClientHandler) Start() {
    chd.Raft.Start()

    // Error checking function
    checkError := func (obj interface{}) {
        if obj != nil {
            chd.log_error(3, "Error occurred : %v", obj)
            os.Exit(1)
        }
    }

    tcp_addr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(chd.ClientPort))
    checkError(err)
    tcp_acceptor, err := net.ListenTCP("tcp", tcp_addr)
    checkError(err)

    chd.log_info(3, "Starting commit handler")
    go func () {
        HandlerLoop:
        for {

            select {
            case commitAction, ok := <-chd.Raft.CommitChannel:
                if ok {
                    chd.handleCommit(commitAction)
                } else {
                    // Raft node closed
                    break HandlerLoop
                }
            case <-chd.shutDownChan:
                // Wait on shutdown channel
                break HandlerLoop
            }
        }

        chd.log_info(3, "Raft node shutdown, exiting commit handler thread %+v", chd.WaitOnServerExit)
        // Make sync start function to wait on this thread
        chd.WaitOnServerExit.Done()
    }()

    chd.log_info(3, "Starting client listener")
    go func () {
        ListenerLoop:
        for {
            select {
            case <-chd.shutDownChan:
                chd.log_info(3, "Raft node shutdown, exiting client listener thread")
                tcp_acceptor.Close()
                break ListenerLoop
            default:
                tcp_acceptor.SetDeadline(time.Now().Add(time.Second*2))  // Listen on socket for 2 sec, then check if shutdown
                tcp_conn, err := tcp_acceptor.AcceptTCP()
                if nil != err {
                    if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
                        continue
                    }
                }
                checkError(err)

                chd.WaitOnServerExit.Add(1)     // Make sync start function to wait on every serve thread
                go chd.serveClient(tcp_conn)    // Start serve thread
            }
        }
        // Make sync start function to wait on this thread
        chd.WaitOnServerExit.Done()
        chd.log_info(3, "Exiting client listener %+v", chd.WaitOnServerExit)
    }()
}


/***
 *  Synchronously start client handler, wait for all threads to exit
 */
func (chd *ClientHandler) StartSync() {
    chd.log_info(3, "Esddscsadcasdcsadcsadcsadcasdcs sd sad sad sad sd asdasasd asd  das %+v", chd.WaitOnServerExit)
    chd.log_info(3, "Esddscsadcasdcsadcsadcsadcasdcs sd sad sad sad sd asdasasd asd  das %+v", chd.WaitOnServerExit)
    chd.log_info(3, "Esddscsadcasdcsadcsadcsadcasdcs sd sad sad sad sd asdasasd asd  das %+v", chd.WaitOnServerExit)
    chd.log_info(3, "Esddscsadcasdcsadcsadcsadcasdcs sd sad sad sad sd asdasasd asd  das %+v", chd.WaitOnServerExit)
    chd.log_info(3, "Esddscsadcasdcsadcsadcsadcasdcs sd sad sad sad sd asdasasd asd  das %+v", chd.WaitOnServerExit)
    chd.Start()
    chd.WaitOnServerExit.Wait()
}


/***
 *  Serve a client connection
 */
func (chd *ClientHandler) serveClient(conn *net.TCPConn) {
    defer chd.WaitOnServerExit.Done()

    reader := bufio.NewReader(conn)
    for {
        msg, msgerr, fatalerr := fs.GetMsg(reader)
        if fatalerr != nil {
            if (!chd.replyToClient(conn, &fs.Msg{Kind: 'M'})) {
                chd.log_error(3, "Reply to client was not sucessful : %v, %v", msgerr, fatalerr)
            }
            conn.Close()
            return
        }

        //Replicate msg and after receiving at commitChannel, ProcessMsg(msg)
        reqId, waitChan := chd.RegisterRequest()


        // Send request to replicate
        request := Request{ServerId:chd.Raft.GetId(), ReqId:reqId, Message:*msg}
        chd.Raft.Append(request)


        // Wait for replication to happen
        select {
        case response := <-waitChan:
            chd.DeregisterRequest(reqId)                // First deregister the request

            if !chd.replyToClient(conn, &response) {    // Reply to client with response
                chd.log_error(3, "Reply to client was not sucessful")
                conn.Close()
                return
            }
        case  <- time.After(CONNECTION_TIMEOUT) :
            chd.DeregisterRequest(reqId)

            chd.log_error(3, "Connection timed out, closing the connection")
            chd.replyToClient(conn, &fs.Msg{Kind:'I'})  // Reply with internal error
            conn.Close()
            return
        }
    }
}


/***
 *  Handle commit action received on commit channel of raft.
 *
 *  Applies commited request to file system, generates the response,
 *  sends the response to appropriate client serve thread only if
 *  the request was made to this server.
 */
func (chd *ClientHandler) handleCommit (commitAction rsm.CommitAction) {
    var response *fs.Msg

    request := commitAction.Data.(Request)

    if commitAction.Err == nil {                        // Check if replication was successful
        response = fs.ProcessMsg(&request.Message)      // Apply request to state machine, i.e. Filesystem
    } else {
        switch commitAction.Err.(type) {
        case rsm.Error_Commit:                          // Unable to commit, internal error
            response = &fs.Msg{Kind:'I'}
        case rsm.Error_NotLeader:                       // Not a leader, redirect error
            errorNotLeader := commitAction.Err.(rsm.Error_NotLeader)
            response = &fs.Msg{
                Kind            : 'R',
                RedirectAddr    : chd.Raft.ServerList[ errorNotLeader.LeaderId ] }
        default:
            chd.log_error(3, "Unknown error type : %v", commitAction.Err)
        }
    }

    chd.Raft.UpdateLastApplied(commitAction.Index)      // Update last applied

    // Reply only if the client has requested this server
    if request.ServerId == chd.Raft.GetId() {
        chd.SendToWaitCh(request.ReqId, *response)      // Send response to corresponding serve thread
    }
}


/***
 *  Request Reqistration and Deregistration
 *
 */

// Register client request and returns request id
func (chd *ClientHandler) RegisterRequest() (reqId int, waitChan chan fs.Msg) {
    waitChan = make(chan fs.Msg)
    chd.ActiveReqLock.Lock()
    chd.NextReqId++
    reqId = chd.NextReqId
    chd.ActiveReq [ reqId ] = waitChan
    chd.ActiveReqLock.Unlock()
    return reqId, waitChan
}

// Deregister client request corresponding to request id
func (chd *ClientHandler) DeregisterRequest(reqId int) {
    chd.ActiveReqLock.Lock()
    close(chd.ActiveReq[reqId])
    delete(chd.ActiveReq, reqId)
    chd.ActiveReqLock.Unlock()
}

// Send msg to the serve thread waiting for the request to get replicated
func (chd *ClientHandler) SendToWaitCh (reqId int, msg fs.Msg) {
    chd.ActiveReqLock.RLock()
    conn, ok := chd.ActiveReq[reqId]    // Extract wait channel from map
    if ok {                             // If request was not de-registered due to timeout
        conn <- msg
    } else {                            // Request was de-registered
        chd.log_error(4, "No connection found for reqId %v", reqId)
    }
    chd.ActiveReqLock.RUnlock()
}


/***
 *  Reply to client tcp connection with the given msg
 */
func (chd *ClientHandler) replyToClient(conn *net.TCPConn, msg *fs.Msg) bool {
    var err error
    write := func(data []byte) {
        if err != nil {
            return
        }
        _, err = conn.Write(data)
    }
    var resp string
    switch msg.Kind {
    case 'C': // read response
        resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
    case 'O':
        resp = "OK "
        if msg.Version > 0 {
            resp += strconv.Itoa(msg.Version)
        }
    case 'F':
        resp = "ERR_FILE_NOT_FOUND"
    case 'V':
        resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
    case 'M':
        resp = "ERR_CMD_ERR"
    case 'I':
        resp = "ERR_INTERNAL"
    case 'R': // redirect addr of leader
        resp = fmt.Sprintf("ERR_REDIRECT %v", msg.RedirectAddr)
    default:
        chd.log_error(3, "Unknown response kind '%c', of msg : %+v", msg.Kind, msg)
        return false
    }
    resp += "\r\n"
    write([]byte(resp))
    if msg.Kind == 'C' {
        write(msg.Contents)
        write(crlf)
    }
    return err == nil
}


/***
 *  Shutdown raft, commit handler thread and client listener thread
 */
func (chd *ClientHandler) Shutdown() {
    chd.log_info(3, "Client handler shuting down")
    close(chd.shutDownChan)     // Shutdown commit handler and client listener threads
    chd.WaitOnServerExit.Wait()
    chd.Raft.Shutdown()         // Shutdown raft node
}
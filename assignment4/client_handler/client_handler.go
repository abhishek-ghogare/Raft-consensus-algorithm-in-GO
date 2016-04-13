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

const CONNECTION_TIMEOUT = 1000000 // in seconds

func (rn *ClientHandler) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", strconv.Itoa(rn.ClientPort)) + format
    logging.Error(skip, format, args...)
}
func (rn *ClientHandler) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", strconv.Itoa(rn.ClientPort)) + format
    logging.Info(skip, format, args...)
}
func (rn *ClientHandler) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CH:%v] ", strconv.Itoa(rn.ClientPort)) + format
    logging.Warning(skip, format, args...)
}

var crlf = []byte{'\r', '\n'}

/*
 *  Request is replicated into raft nodes
 */
type Request struct {
    ServerId int // Id of raft node on which the request has arrived
    ReqId    int // Connection id, mapped into activeConn, from which request has arrived
    Message  fs.Msg
}

type ClientHandler struct {
    Raft            *raft_node.RaftNode
    ActiveReq       map[int]chan fs.Msg
    ActiveReqLock   sync.RWMutex
    NextConnId      int
    ClientPort      int     //Port on which the client handler will listen
    WaitOnServer    sync.WaitGroup
}

func New(config *raft_config.Config, restore bool) (ch *ClientHandler) {

    gob.Register(fs.Msg{})
    gob.Register(Request{})
    gob.Register(rsm.AppendRequestEvent{})
    gob.Register(rsm.AppendRequestRespEvent{})
    gob.Register(rsm.RequestVoteEvent{})
    gob.Register(rsm.RequestVoteRespEvent{})
    //gob.Register(timeoutEvent{})
    gob.Register(rsm.AppendEvent{})
    gob.Register(rsm.LogEntry{})

    ch = &ClientHandler{
        ActiveReq   : make(map[int]chan fs.Msg),
        NextConnId  : 0,
        ClientPort  : config.ClientPort }

    if restore {
        ch.Raft = raft_node.RestoreServerState(config)
    } else {
        ch.Raft = raft_node.NewRaftNode(config)
    }

    return ch
}

func (ch *ClientHandler) Start() {

    ch.Raft.Start()

    tcpaddr, err := net.ResolveTCPAddr("tcp", "localhost:"+strconv.Itoa(ch.ClientPort))
    ch.check(err)
    tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
    ch.check(err)

    ch.log_info(3, "Starting handle commits thread")
    go ch.handleCommits()

    ch.log_info(3, "Starting loop to handle tcp connections")
    go func () {
        for {
            tcp_conn, err := tcp_acceptor.AcceptTCP()
            ch.check(err)
            go ch.serve(tcp_conn)
        }
        ch.WaitOnServer.Done()
    }()
}

func (ch *ClientHandler) StartSync() {
    // Done will becalledtwice, once in handleCommits and once in Start
    ch.WaitOnServer.Add(2)
    ch.Start()
    ch.WaitOnServer.Wait()
}

// Add a connection to active request queue and returns request id
func (ch *ClientHandler) CreateRequest() (reqId int, waitChan chan fs.Msg) {
    waitChan = make(chan fs.Msg)
    ch.ActiveReqLock.Lock()
    ch.NextConnId++
    connId := ch.NextConnId
    ch.ActiveReq [ connId ] = waitChan
    ch.ActiveReqLock.Unlock()
    return connId, waitChan
}
// Remove request from active requests
func (ch *ClientHandler) DequeueRequest(connId int) {
    ch.ActiveReqLock.Lock()
    close(ch.ActiveReq[connId])
    delete(ch.ActiveReq, connId)
    ch.ActiveReqLock.Unlock()
}
func (ch *ClientHandler) SendToWaitCh (connId int, msg fs.Msg) {
    ch.ActiveReqLock.RLock()
    conn, ok := ch.ActiveReq[connId]
    if ok {
        conn <- msg
    } else {
        ch.log_error(4, "No connection found for reqId %v", connId)
    }
    ch.ActiveReqLock.RUnlock()
}


func (ch *ClientHandler) check(obj interface{}) {
    if obj != nil {
        ch.log_error(3, "Error occurred : %v", obj)
        fmt.Println(obj)
        os.Exit(1)
    }
}

func (ch *ClientHandler) reply(conn *net.TCPConn, msg *fs.Msg) bool {
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
        ch.log_error(3, "Unknown response kind '%c', of msg : %+v", msg.Kind, msg)
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

func (ch *ClientHandler) serve(conn *net.TCPConn) {
    reader := bufio.NewReader(conn)
    for {
        msg, msgerr, fatalerr := fs.GetMsg(reader)
        if fatalerr != nil {
            ch.log_error(3, "Error occured while getting a msg from client : %v, %v", msgerr, fatalerr)
            if (!ch.reply(conn, &fs.Msg{Kind: 'M'})) {
                ch.log_error(3, "Reply to client was not sucessful")
            }
            conn.Close()
            break
        }

        ch.log_info(3, "Request received from client : %+v", *msg)
        /***
         *      Replicate msg and after receiving at commitChannel, ProcessMsg(msg)
         */

        reqId, waitChan := ch.CreateRequest()

        request := Request{ServerId:ch.Raft.GetId(), ReqId:reqId, Message:*msg}
        // Send request to replicate
        ch.Raft.Append(request)


        // Wait for replication to happen
        select {
        case response := <-waitChan:
            // Reply to client with response
            if !ch.reply(conn, &response) {
                ch.log_error(3, "Reply to client was not sucessful")
                conn.Close()
            }
        case  <- time.After(CONNECTION_TIMEOUT*time.Second) :
            // Connection timed out
            ch.log_error(3, "Connection timed out, closing the connection")
            // TODO:: send proper msg
            conn.Close()
        }


        // Remove request from active requests
        ch.log_info(3, "Removing request from queue with reqId %v", reqId)
        ch.DequeueRequest(reqId)
    }
}

func (ch *ClientHandler) handleCommits() {
    for {
        commitAction, ok := <- ch.Raft.CommitChannel
        if ok {
            var response *fs.Msg

            request := commitAction.Log.Data.(Request)

            // Check if replication was successful
            if commitAction.Err == nil {
                // Apply request to state machine, i.e. Filesystem
                ch.log_info(3, "Applying request to file system : %+v", request)
                response = fs.ProcessMsg(&request.Message)          // TODO, this is global file system,
            } else {
                switch commitAction.Err.(type) {
                case rsm.Error_Commit:                  // unable to commit, internal error
                    response = &fs.Msg{Kind:'I'}
                case rsm.Error_NotLeader:               // not a leader, redirect error
                    errorNotLeader := commitAction.Err.(rsm.Error_NotLeader)
                    response = &fs.Msg{Kind:'R', RedirectAddr:errorNotLeader.LeaderAddr}
                default:
                    ch.log_error(3, "Unknown error type : %v", commitAction.Err)
                }
            }

            // update last applied
            ch.Raft.UpdateLastApplied(commitAction.Log.Index)

            // Reply only if the client has requested this server
            if request.ServerId == ch.Raft.GetId() {
                ch.SendToWaitCh(request.ReqId, *response)
            }
        } else {
            // Raft node closed
            ch.log_info(3, "Raft node shutdown, exiting handleCommits")
            ch.WaitOnServer.Done()
            return
        }
    }
}


func (ch *ClientHandler) Shutdown() {
    ch.log_info(3, "Client handler shuting down")
    ch.Raft.Shutdown()
}
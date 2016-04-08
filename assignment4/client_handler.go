package main

import (
    "bufio"
    "fmt"
    "net"
    "os"
    "strconv"
    "cs733/assignment4/raft_node"
    "cs733/assignment4/filesystem/fs"
    "sync"
    rsm "cs733/assignment4/raft_node/raft_state_machine"
)
var crlf = []byte{'\r', '\n'}

/****
 *      Variables used for replicating
 */

/*
 *  Request is replicated into raft nodes
 */
type Request struct {
    ServerId int // Id of raft node on which the request has arrived
    ReqId    int // Connection id, mapped into activeConn, from which request has arrived
    msg      fs.Msg
}
var raft        *raft_node.RaftNode
var activeReq  map[int]net.TCPConn
var nextConnId  int
var connLock    sync.RWMutex




func check(obj interface{}) {
    if obj != nil {
        fmt.Println(obj)
        os.Exit(1)
    }
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
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
        fmt.Printf("Unknown response kind '%c'", msg.Kind)
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

func serve(conn *net.TCPConn) {
    reader := bufio.NewReader(conn)
    for {
        msg, msgerr, fatalerr := fs.GetMsg(reader)
        if fatalerr != nil || msgerr != nil {
            reply(conn, &fs.Msg{Kind: 'M'})
            conn.Close()
            break
        }

        if msgerr != nil {
            if (!reply(conn, &fs.Msg{Kind: 'M'})) {
                conn.Close()
                break
            }
        }

        /***
         *      Replicate msg and after receiving at commitChannel, ProcessMsg(msg)
         */
        nextConnId++
        connLock.Lock()
        activeReq[nextConnId] = conn
        connLock.Unlock()
        request := Request{ServerId:raft.GetId(), ReqId:nextConnId, msg:msg}
        raft.Append(request)
    }
}

func handleCommits() {
    for {
        select {
        case commitAction, ok := <- raft.CommitChannel :
            if ok {
                var response *fs.Msg
                request := commitAction.Log.Data.(Request)


                if commitAction.Err != nil {
                    response = fs.ProcessMsg(request.msg)
                } else {
                    switch commitAction.Err.(type) {
                    case rsm.Error_Commit:                  // unable to commit, internal error
                        response = &fs.Msg{Kind:'I'}
                    case rsm.Error_NotLeader:               // not a leader, redirect error
                        errorNotLeader := commitAction.Err.(rsm.Error_NotLeader)
                        response = &fs.Msg{Kind:'R', RedirectAddr:errorNotLeader.LeaderAddr + ":" + strconv.Itoa(errorNotLeader.LeaderPort)}
                    }
                }

                // Reply only if the client has requested this server
                if request.ServerId == raft.GetId() {
                    connLock.RLock()
                    conn := activeReq[request.ReqId]
                    connLock.RUnlock()
                    if !reply(conn, response) {
                        conn.Close()
                    }
                    // Remove request from active requests
                    connLock.Lock()
                    delete(activeReq, request.ReqId)
                    connLock.Unlock()
                }

                // update last applied
                raft.UpdateLastApplied(commitAction.Log.Index)
            } else {
                // Raft node closed
                return
            }
        }
    }
}

func serverMain() {
    tcpaddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
    check(err)
    tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
    check(err)

    for {
        tcp_conn, err := tcp_acceptor.AcceptTCP()
        check(err)
        go serve(tcp_conn)
    }
}

func main() {
    serverMain()
}

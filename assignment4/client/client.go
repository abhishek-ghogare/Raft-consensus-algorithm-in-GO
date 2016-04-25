package client

import (
    "fmt"
    "net"
    "bufio"
    "strconv"
    "errors"
    "cs733/assignment4/logging"
    "cs733/assignment4/client_handler/filesystem/fs"
    "cs733/assignment4/raft_config"
    "strings"
    "sync"
)


/*
 *  Debug tools
 */
func (rn *Client) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%3v] ", strconv.Itoa(rn.Id)) + format
    logging.Error(skip, format, args...)
}
func (rn *Client) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%3v] ", strconv.Itoa(rn.Id)) + format
    logging.Info(skip, format, args...)
}
func (rn *Client) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%3v] ", strconv.Itoa(rn.Id)) + format
    logging.Warning(skip, format, args...)
}



var errNoConn = errors.New("Connection is closed")

type Client struct {
    Id               int            // Client id
    conn             *net.TCPConn   // TCP connection to server
    reader           *bufio.Reader  // A bufio Reader wrapper over conn
    ServerList       []string       // List of server addrs to which client can send requests. 0th server is null
    lock             sync.Mutex     // Mutex to access conn and reader
}


/***
 *  Create client and establish connection to any reachable server
 *  # config    : Config containing list of server addrs
 *  # id        : Client ID
 */
func New(config *raft_config.Config, id int) *Client {
    client := Client{
                            Id          : id,
                            ServerList  : config.ServerList,
                            conn        : nil,
                            reader      : nil}
    client.setupConnectionToServer()
    return &client
}

/***
 *  Establish connection to any reachable server
 *
 */
func (cl *Client) setupConnectionToServer() {

    for i:=1 ; i<len(cl.ServerList) ; i++ {

        raddr, err := net.ResolveTCPAddr("tcp", cl.ServerList[i])

        if err != nil {                         // If error
            continue                            // Try connecting another server
        }

        var conn *net.TCPConn
        conn, err = net.DialTCP("tcp", nil, raddr)
        if err == nil {                         // Connection success
            cl.lock.Lock()
            cl.conn = conn
            cl.reader = bufio.NewReader(conn)
            cl.lock.Unlock()
            return
        }
    }
    cl.log_error(3, "Unable to connect to any server, servers may be down")
}



/***
 *  Client operations
 *
 */
// Send msg to server
func (cl *Client) send(str string) error {
    cl.lock.Lock()
    defer cl.lock.Unlock()

    cl.log_info(3, "Sending : %v", strings.Replace(str,"\r\n", "", -1))

    _, err := cl.conn.Write([]byte(str))
    if err != nil {
        err = fmt.Errorf("Write error in SendRaw: %v", err)
        cl.log_error(3, "Socket write error : %v", err.Error())
        cl.Close()
    }
    return err
}

func (cl *Client) rcv() (msg *fs.Msg, err error) {
    cl.lock.Lock()
    defer cl.lock.Unlock()

    var fatalerr error

    line, err := cl.reader.ReadString('\n')         // Read line from socket
    if err != nil {
        cl.log_error(3, "Socket read error : %v", err.Error())
        cl.Close()
        return nil, err
    }

    cl.log_info(3, "Received : %v", strings.Replace(line,"\r\n", "", -1))
    msg, err, fatalerr = fs.PaserString(line)       // Parse msg from read line
    if err != nil {
        return nil, err
    } else if fatalerr !=nil {
        return nil, fatalerr
    }

    if msg.Kind == 'C' {                            // Read contents
        contents := make([]byte, msg.Numbytes)
        var c byte
        for i := 0; i < msg.Numbytes; i++ {
            if c, err = cl.reader.ReadByte(); err != nil {
                break
            }
            contents[i] = c
        }
        if err == nil {
            msg.Contents = contents
            cl.reader.ReadByte() // \r
            cl.reader.ReadByte() // \n
        }
    }
    return msg, err
}

// Send msg to server and receive a response; NO retry on error
func (cl *Client) sendRcvBasic(str string) (msg *fs.Msg, err error) {
    err = cl.send(str)
    if err == nil {
        msg, err = cl.rcv()
    }
    return msg, err
}

// Send msg to server and receive a response, RETRY on redirect error
// or internal error.
// On redirect error, close connection to existing server and establish
// new connection to redirected server
func (cl *Client) sendRcv(str string) (msg *fs.Msg, err error) {

    var m *fs.Msg

    for retries := 1 ; retries < 20 ; retries++ {
        // Check for redirect
        m, err = cl.sendRcvBasic(str)
        if err!=nil {
            cl.log_warning(3, "Unable to connect : %v", err.Error())
            cl.setupConnectionToServer()
            continue
        }


        if m.Kind == 'R' {              // If redirect error
            cl.log_warning(3, "Server replied with redirect error, redirecting to : %v", m.RedirectAddr)

            cl.Close()

            raddr, err1 := net.ResolveTCPAddr("tcp", m.RedirectAddr)
            if err1 == nil {
                var conn *net.TCPConn
                cl.log_info(3, "Creating tcp connection to %v", m.RedirectAddr)
                conn, err1 = net.DialTCP("tcp", nil, raddr)     // Connect to redirect addr
                if err1 == nil {
                    cl.lock.Lock()
                    cl.conn = conn
                    cl.reader = bufio.NewReader(conn)
                    cl.lock.Unlock()
                }
            }

            if err1 != nil {
                cl.log_warning(3, "Unable to connect to server %v : %v", m.RedirectAddr, err1.Error())
            }
        } else if m.Kind == 'I' {           // If internal error, i.e. unable to replicate or timeout
            cl.log_warning(3, "Server replied with internal error")
            cl.Close()
            cl.setupConnectionToServer()    // Again setup new connection and retry
            continue
        } else {
            // There is no error of either "not a leader" or "internal error"
            return m, err
        }
    }

    cl.log_error(4, "Unable to send msg after many retries: %v", m)
    return m, fmt.Errorf("Unable to send msg after many retries: %v", m)
}


/***
 *  File system operations
 *
 */
// Read file
func (cl *Client) Read(filename string) (*fs.Msg, error) {
    cmd := "read " + filename + "\r\n"
    return cl.sendRcv(cmd)
}

// Write to file
func (cl *Client) Write(filename string, contents string, exptime int) (*fs.Msg, error) {
    var cmd string
    if exptime == 0 {
        cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
    } else {
        cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
    }
    cmd += contents + "\r\n"
    return cl.sendRcv(cmd)
}

// CAS operation on file
func (cl *Client) Cas(filename string, version int, contents string, exptime int) (*fs.Msg, error) {
    var cmd string
    if exptime == 0 {
        cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
    } else {
        cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
    }
    cmd += contents + "\r\n"
    return cl.sendRcv(cmd)
}

// Delete file
func (cl *Client) Delete(filename string) (*fs.Msg, error) {
    cmd := "delete " + filename + "\r\n"
    return cl.sendRcv(cmd)
}


func (cl *Client) Close() {
    cl.lock.Lock()
    defer cl.lock.Unlock()

    if cl.conn != nil {
        cl.log_info(4, "Closing client : %+v", *cl)
        cl.conn.Close()
        cl.conn = nil
    }
}
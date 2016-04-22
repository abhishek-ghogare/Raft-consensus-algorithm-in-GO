package client

import (
    "fmt"
    "net"
    "bufio"
    "strconv"
    "errors"
    "cs733/assignment4/logging"
    "cs733/assignment4/client_handler/filesystem/fs"
)


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
    Id     int
    conn   *net.TCPConn
    reader *bufio.Reader // a bufio Reader wrapper over conn
}

func New(serverUrl string, id int) *Client {
    var client Client
    raddr, err := net.ResolveTCPAddr("tcp", serverUrl)
    if err == nil {
        var conn *net.TCPConn
        logging.Logger.Printf("Creating tcp connection to %v\n", serverUrl)
        conn, err = net.DialTCP("tcp", nil, raddr)
        if err == nil {
            client = Client{Id:id, conn: conn, reader: bufio.NewReader(conn)}
        }
    }

    if err != nil {
        client.log_error(3, "Unable to connect to server : %v", err.Error())
        return nil
    }
    return &client
}

func (cl *Client) Read(filename string) (*fs.Msg, error) {
    cmd := "read " + filename + "\r\n"
    return cl.SendRcv(cmd)
}

func (cl *Client) Write(filename string, contents string, exptime int) (*fs.Msg, error) {
    var cmd string
    if exptime == 0 {
        cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
    } else {
        cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
    }
    cmd += contents + "\r\n"
    return cl.SendRcv(cmd)
}

func (cl *Client) Cas(filename string, version int, contents string, exptime int) (*fs.Msg, error) {
    var cmd string
    if exptime == 0 {
        cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
    } else {
        cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
    }
    cmd += contents + "\r\n"
    return cl.SendRcv(cmd)
}

func (cl *Client) Delete(filename string) (*fs.Msg, error) {
    cmd := "delete " + filename + "\r\n"
    return cl.SendRcv(cmd)
}


func (cl *Client) Send(str string) error {
    cl.log_info(6, "Sending : %v", str)
    if cl.conn == nil {
        return errNoConn
    }
    _, err := cl.conn.Write([]byte(str))
    if err != nil {
        err = fmt.Errorf("Write error in SendRaw: %v", err)
        cl.conn.Close()
        cl.log_error(6, "Socket write error : %v", err.Error())
    }
    return err
}

func (cl *Client) SendRcvBasic(str string) (msg *fs.Msg, err error) {
    if cl.conn == nil {
        return nil, errNoConn
    }
    err = cl.Send(str)
    if err == nil {
        msg, err = cl.Rcv()
    }
    return msg, err
}

func (cl *Client) SendRcv(str string) (msg *fs.Msg, err error) {

    var m *fs.Msg

    for retries := 1 ; retries < 20 ; retries++ {
        // Check for redirect
        m, err = cl.SendRcvBasic(str)
        if err!=nil {
            cl.log_error(4, "Unable to connect : %v", err.Error())
            // TODO:: the server might be down, try different servers
            return nil, err
        }
        if m.Kind == 'R' {
            cl.log_warning(4, "Server replied with redirect error, redirecting to : %v", m.RedirectAddr)
            cl.Close()
            *cl = *New(m.RedirectAddr, cl.Id)
        } else if m.Kind != 'I' {
            // Error is not of either "not a leader" or "internal error"
            return m, err
        } else {
            cl.log_warning(4, "Server replied with internal error")
        }
    }

    cl.log_error(4, "Unable to send msg after many retries: %v", m)
    return m, fmt.Errorf("Unable to send msg after many retries: %v", m)
}

func (cl *Client) Rcv() (msg *fs.Msg, err error) {
    var fatalerr error
    // we will assume no errors in server side formatting
    line, err := cl.reader.ReadString('\n')
    if err == nil {
        cl.log_info(6, "Received : %v", line)
        msg, err, fatalerr = fs.PaserString(line)
        if err != nil {
            return nil, err
        } else if fatalerr !=nil {
            return nil, fatalerr
        }

        if msg.Kind == 'C' {
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
    }
    if err != nil {
        cl.log_error(6, "Socket read error : %v", err.Error())
        cl.Close()
    }
    return msg, err
}


func (cl *Client) Close() {
    if cl != nil && cl.conn != nil {/*
        cl.log_info(10, "Closing client")
        cl.log_info(9, "Closing client")
        cl.log_info(8, "Closing client")
        cl.log_info(7, "Closing client")
        cl.log_info(6, "Closing client")
        cl.log_info(5, "Closing client")*/
        cl.log_info(4, "Closing client")
        cl.conn.Close()
        cl.conn = nil
    }
}
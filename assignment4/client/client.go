package client

import (
    "fmt"
    "net"
    "bufio"
    "strconv"
    "errors"
    "cs733/assignment4/logging"
    "cs733/assignment4/filesystem/fs"
)


func (rn *Client) log_error(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%v] ", strconv.Itoa(rn.clientId)) + format
    logging.Error(skip, format, args...)
}
func (rn *Client) log_info(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%v] ", strconv.Itoa(rn.clientId)) + format
    logging.Info(skip, format, args...)
}
func (rn *Client) log_warning(skip int, format string, args ...interface{}) {
    format = fmt.Sprintf("[CL:%v] ", strconv.Itoa(rn.clientId)) + format
    logging.Warning(skip, format, args...)
}



var errNoConn = errors.New("Connection is closed")

/*
type Msg struct {
                 // Kind = the first character of the command. For errors, it
                 // is the first letter after "ERR_", ('V' for ERR_VERSION, for
                 // example), except for "ERR_CMD_ERR", for which the kind is 'M'
    Kind     byte
    Filename string
    Contents []byte
    Numbytes int
    Exptime  int // expiry time in seconds
    Version  int
}*/

type Client struct {
    clientId    int
    conn        *net.TCPConn
    reader      *bufio.Reader // a bufio Reader wrapper over conn
}

func New(serverUrl string, id int) *Client {
    var client Client
    raddr, err := net.ResolveTCPAddr("tcp", serverUrl)
    if err == nil {
        conn, err := net.DialTCP("tcp", nil, raddr)
        if err == nil {
            client = Client{clientId:id, conn: conn, reader: bufio.NewReader(conn)}
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
    if cl.conn == nil {
        return errNoConn
    }
    _, err := cl.conn.Write([]byte(str))
    if err != nil {
        err = fmt.Errorf("Write error in SendRaw: %v", err)
        cl.conn.Close()
        cl.conn = nil
    }
    return err
}

func (cl *Client) SendRcv(str string) (msg *fs.Msg, err error) {
    if cl.conn == nil {
        return nil, errNoConn
    }
    err = cl.Send(str)
    if err == nil {
        msg, err = cl.Rcv()
    }
    return msg, err
}

func (cl *Client) Close() {
    if cl != nil && cl.conn != nil {
        cl.conn.Close()
        cl.conn = nil
    }
}

func (cl *Client) Rcv() (msg *fs.Msg, err error) {
    var fatalerr error
    // we will assume no errors in server side formatting
    line, err := cl.reader.ReadString('\n')
    if err == nil {
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
        cl.Close()
    }
    return msg, err
}
/*

func parseFirst(line string) (msg *fs.Msg, err error) {
    fields := strings.Fields(line)
    msg = &fs.Msg{}

    // Utility function fieldNum to int
    toInt := func(fieldNum int) int {
        var i int
        if err == nil {
            if fieldNum >=  len(fields) {
                err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
                return 0
            }
            i, err = strconv.Atoi(fields[fieldNum])
        }
        return i
    }

    if len(fields) == 0 {
        return nil, errors.New("Empty line. The previous command is likely at fault")
    }
    switch fields[0] {
    case "OK": // OK [version]
        msg.Kind = 'O'
        if len(fields) > 1 {
            msg.Version = toInt(1)
        }
    case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
        msg.Kind = 'C'
        msg.Version = toInt(1)
        msg.Numbytes = toInt(2)
        msg.Exptime = toInt(3)
    case "ERR_VERSION":
        msg.Kind = 'V'
        msg.Version = toInt(1)
    case "ERR_FILE_NOT_FOUND":
        msg.Kind = 'F'
    case "ERR_CMD_ERR":
        msg.Kind = 'M'
    case "ERR_INTERNAL":
        msg.Kind = 'I'
    default:
        err = errors.New("Unknown response " + fields[0])
    }
    if err != nil {
        return nil, err
    } else {
        return msg, nil
    }
}
*/

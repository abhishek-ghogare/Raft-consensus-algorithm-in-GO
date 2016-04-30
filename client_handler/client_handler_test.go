package client_handler

import (
    "testing"
    "time"
    "fmt"
    "bytes"
    "sync"
    "strings"
    "errors"
    "os"
    "github.com/cs733-iitb/cluster"
    "github.com/avg598/cs733/raft_config"
    "github.com/avg598/cs733/client"
    "github.com/avg598/cs733/client_handler/filesystem/fs"
)

//var mockCluster *mock.MockCluster
var clientHandlers []*ClientHandler
var baseConfig *raft_config.Config

func TestCHDMain(t *testing.T) {
    var err error

    os.RemoveAll("/tmp/raft/")

    if err!=nil {
        t.Error("Unable to create cluster : ", err.Error())
    }

    baseConfig = &raft_config.Config{
        LogDir           : "/tmp/raft/",
        ElectionTimeout  : 500,
        HeartbeatTimeout : 2000,
        NumOfNodes       : 5,
        ClusterConfig    : cluster.Config   {
                                                Peers: []cluster.PeerConfig{
                                                    {Id: 1, Address: "localhost:7001"},
                                                    {Id: 2, Address: "localhost:7002"},
                                                    {Id: 3, Address: "localhost:7003"},
                                                    {Id: 4, Address: "localhost:7004"},
                                                    {Id: 5, Address: "localhost:7005"},
                                                },
                                                InboxSize:100000,
                                                OutboxSize:100000,
                                            },
        ClientPorts      : []int{ 0, 9001, 9002, 9003, 9004, 9005},
        ServerList       : []string{
            "",
            "localhost:9001",
            "localhost:9002",
            "localhost:9003",
            "localhost:9004",
            "localhost:9005",
        }}

    for i:=1 ; i<=5 ; i++ {
        config := raft_config.Config(*baseConfig)
        //config.Id+=i
        //config.ClientPort+=i
        config.ElectionTimeout += 90000*(i-1)

        clientHandlers = append(clientHandlers, New(i, &config,false))

        // Start client handler
        clientHandlers[i-1].Start()
    }
    time.Sleep(4 * time.Second)
}

func expect(t *testing.T, response *fs.Msg, expected *fs.Msg, errstr string, err error) {
    if err != nil {
        t.Fatal("Unexpected error: " + err.Error())
    }
    ok := true
    if response.Kind != expected.Kind {
        ok = false
        errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
    }
    if expected.Version > 0 && expected.Version != response.Version {
        ok = false
        errstr += " Version mismatch"
    }
    if response.Kind == 'C' {
        if expected.Contents != nil &&
        bytes.Compare(response.Contents, expected.Contents) != 0 {
            ok = false
        }
    }
    if !ok {
        t.Fatal("Expected " + errstr)
    }
}


func TestCHD_Binary(t *testing.T) {
    cl := client.New(baseConfig, 1)
    if cl==nil {
        t.Fatal("Client unable to connect.")
    }
    //cl := client.New("127.0.0.1:" + strconv.Itoa(clientHandlers[0].ClientPort), 1)
    defer cl.Close()

    // Write binary contents
    data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
    m, err := cl.Write("binfile", data, 0)
    expect(t, m, &fs.Msg{Kind: 'O'}, "write success", err)

    // Expect to read it back
    m, err = cl.Read("binfile")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

}


func TestCHD_BasicSequential(t *testing.T) {
    cl := client.New(baseConfig, 1)
    if cl==nil {
        t.Fatal("Client unable to connect.")
    }
    defer cl.Close()

    // Read non-existent file cs733net
    m, err := cl.Read("cs733net")
    expect(t, m, &fs.Msg{Kind: 'F'}, "file not found", err)

    // Read non-existent file cs733net
    m, err = cl.Delete("cs733net")
    expect(t, m, &fs.Msg{Kind: 'F'}, "file not found", err)

    // Write file cs733net
    data := "Cloud fun"
    m, err = cl.Write("cs733net", data, 0)
    expect(t, m, &fs.Msg{Kind: 'O'}, "write success", err)

    // Expect to read it back
    m, err = cl.Read("cs733net")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

    // CAS in new value
    version1 := m.Version
    data2 := "Cloud fun 2"
    // Cas new value
    m, err = cl.Cas("cs733net", version1, data2, 0)
    expect(t, m, &fs.Msg{Kind: 'O'}, "cas success", err)

    // Expect to read it back
    m, err = cl.Read("cs733net")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)

    // Expect Cas to fail with old version
    m, err = cl.Cas("cs733net", version1, data, 0)
    expect(t, m, &fs.Msg{Kind: 'V'}, "cas version mismatch", err)

    // Expect a failed cas to not have succeeded. Read should return data2.
    m, err = cl.Read("cs733net")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)

    // delete
    m, err = cl.Delete("cs733net")
    expect(t, m, &fs.Msg{Kind: 'O'}, "delete success", err)

    // Expect to not find the file
    m, err = cl.Read("cs733net")
    expect(t, m, &fs.Msg{Kind: 'F'}, "file not found", err)
}

func TestCHD_BasicTimer(t *testing.T) {
    cl := client.New(baseConfig, 1)
    if cl==nil {
        t.Fatal("Client unable to connect.")
    }
    defer cl.Close()

    // Write file cs733, with expiry time of 2 seconds
    str := "Cloud fun"
    m, err := cl.Write("cs733", str, 2)
    expect(t, m, &fs.Msg{Kind: 'O'}, "write success", err)

    // Expect to read it back immediately.
    m, err = cl.Read("cs733")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

    time.Sleep(3 * time.Second)

    // Expect to not find the file after expiry
    m, err = cl.Read("cs733")
    expect(t, m, &fs.Msg{Kind: 'F'}, "file not found", err)

    // Recreate the file with expiry time of 1 second
    m, err = cl.Write("cs733", str, 1)
    expect(t, m, &fs.Msg{Kind: 'O'}, "file recreated", err)

    // Overwrite the file with expiry time of 4. This should be the new time.
    m, err = cl.Write("cs733", str, 3)
    expect(t, m, &fs.Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

    // The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
    time.Sleep(2 * time.Second)

    // Expect the file to not have expired.
    m, err = cl.Read("cs733")
    expect(t, m, &fs.Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

    time.Sleep(3 * time.Second)
    // 5 seconds since the last write. Expect the file to have expired
    m, err = cl.Read("cs733")
    expect(t, m, &fs.Msg{Kind: 'F'}, "file not found after 4 sec", err)

    // Create the file with an expiry time of 1 sec. We're going to delete it
    // then immediately create it. The new file better not get deleted.
    m, err = cl.Write("cs733", str, 1)
    expect(t, m, &fs.Msg{Kind: 'O'}, "file created for delete", err)

    m, err = cl.Delete("cs733")
    expect(t, m, &fs.Msg{Kind: 'O'}, "deleted ok", err)

    m, err = cl.Write("cs733", str, 0) // No expiry
    expect(t, m, &fs.Msg{Kind: 'O'}, "file recreated", err)

    time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
    m, err = cl.Read("cs733")
    expect(t, m, &fs.Msg{Kind: 'C'}, "file should not be deleted", err)

}

func TestCHD_RestartAll(t *testing.T) {
    TestCHDEnd(t)
    //time.Sleep(time.Second*5)
    clientHandlers = []*ClientHandler{}
    TestCHDMain(t)
}

// nclients write to the same file. At the end the file should be
// any one clients' last write

func TestCHD_ConcurrentWrites(t *testing.T) {
    time.Sleep(1 * time.Second)
    nclients := 500
    niters := 10
    clients := make([]*client.Client, nclients)
    for i := 0 ; i < nclients ; i++ {
        cl := client.New(baseConfig, i)
        for cl==nil {
            cl = client.New(baseConfig, i)
        }
        defer cl.Close()
        clients[i] = cl
    }

    errCh := make(chan error, nclients)
    var sem sync.WaitGroup // Used as a semaphore to coordinate go-routines to begin concurrently
    sem.Add(1)
    ch := make(chan *fs.Msg, nclients*niters) // channel for all replies
    for i := 0; i < nclients; i++ {
        go func(i int, cl *client.Client) {
            sem.Wait()
            for j := 0; j < niters; j++ {
                str := fmt.Sprintf("cl %d %d", i, j)
                m, err := cl.Write("concWrite", str, 0)
                if err != nil {
                    errCh <- err
                    break
                } else {
                    ch <- m
                }
            }
        }(i, clients[i])
    }
    time.Sleep(100 * time.Millisecond) // give goroutines a chance
    sem.Done()                         // Go!

    // There should be no errors
    for i := 0; i < nclients*niters; i++ {
        select {
        case m := <-ch:
            if m.Kind != 'O' {
                t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
            }
        case err := <- errCh:
            t.Fatal(err)
        }
    }
    m, _ := clients[0].Read("concWrite")
    // Ensure the contents are of the form "cl <i> 9"
    // The last write of any client ends with " 9"
    if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 9")) {
        t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
    }
}

// nclients cas to the same file. At the end the file should be any one clients' last write.
// The only difference between this test and the ConcurrentWrite test above is that each
// client loops around until each CAS succeeds. The number of concurrent clients has been
// reduced to keep the testing time within limits.
func TestCHD_ConcurrentCas(t *testing.T) {
    nclients := 100
    niters := 10

    clients := make([]*client.Client, nclients)
    for i := 0; i < nclients; i++ {
        cl := client.New(baseConfig, i)
        for cl==nil {
            cl = client.New(baseConfig, i)
        }
        defer cl.Close()
        clients[i] = cl
    }

    var sem sync.WaitGroup // Used as a semaphore to coordinate go-routines to *begin* concurrently
    sem.Add(1)

    m, _ := clients[0].Write("concCas", "first", 0)
    ver := m.Version
    if m.Kind != 'O' || ver == 0 {
        t.Fatalf("Expected write to succeed and return version")
    }

    var wg sync.WaitGroup
    wg.Add(nclients)

    errorCh := make(chan error, nclients)

    for i := 0; i < nclients; i++ {
        go func(i int, ver int, cl *client.Client) {
            sem.Wait()
            defer wg.Done()
            for j := 0; j < niters; j++ {
                str := fmt.Sprintf("cl %d %d", i, j)
                for {
                    m, err := cl.Cas("concCas", ver, str, 0)
                    if err != nil {
                        errorCh <- err
                        return
                    } else if m.Kind == 'O' {
                        break
                    } else if m.Kind != 'V' {
                        errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
                        return
                    }
                    ver = m.Version // retry with latest version
                }
            }
        }(i, ver, clients[i])
    }

    time.Sleep(100 * time.Millisecond) // give goroutines a chance
    sem.Done()                         // Start goroutines
    wg.Wait()                          // Wait for them to finish
    select {
    case e := <- errorCh:
        t.Fatalf("Error received while doing cas: %v", e)
    default: // no errors
    }
    m, _ = clients[0].Read("concCas")
    if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 9")) {
        t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
    }
}




func TestCHDEnd(t *testing.T) {
    for i:=0 ; i < 5 ; i++ {
        // Start client handler
        clientHandlers[i].Shutdown()
    }
}
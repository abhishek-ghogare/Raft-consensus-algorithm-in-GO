package raft_node

import (
    "testing"
    "time"
    "math/rand"
    "strconv"
    "encoding/gob"
)
 type TestStruct struct {
     Num int
     Str string
//     Arr []int
 }



func TestBasic1(t *testing.T) {
    gob.Register(TestStruct{})
    cleanupLogs()
    rafts := makeRafts()        // array of []RaftNode
    log_info(3, "Rafts created")
    ldr := rafts.getLeader(t)    // Wait until a stable leader is elected
    ts := TestStruct{Num:1, Str:"hello"/*, Arr:[]int{1,2,3,4,5}*/}
    ldr.Append(ts)    // Append to leader
    ldr = rafts.getLeader(t)    // Again wait for stable leader
    rafts.checkSingleCommit(t, ts)// Wait until next single append is commited on all nodes
    rafts.shutdownRafts()
}


func TestServerStateRestore(t *testing.T) {
    rand.Seed(10)
    cleanupLogs()
    rafts := makeRafts() // array of []RaftNode


    ldr := rafts.getLeader(t)


    retries:=0

    for i:=1 ; i<=10 ;{
        ldr = rafts.getLeader(t)
        ldr.Append(strconv.Itoa(i))
        err := rafts.checkSingleCommit(t, strconv.Itoa(i))
        if err != nil {
            log_warning(3, "Committing msg : %v failed", strconv.Itoa(i))
            retries++
            if retries>10 {
                rafts.shutdownRafts()
                t.Fatalf("Failed to commit a msg, %v, after 10 retries", strconv.Itoa(i))
            }
            continue
        }
        i++
    }

    ldr = rafts.getLeader(t)
    ldr_id := ldr.GetId()
    ldr_index := ldr_id - 1

    ldr.Shutdown()

    retries=0
    for i:=11; i<=20 ;  {
        ldr = rafts.getLeader(t)
        ldr.Append(strconv.Itoa(i))
        err := rafts.checkSingleCommit(t, strconv.Itoa(i))
        if err != nil {
            log_warning(3, "Committing msg : %v failed", strconv.Itoa(i))
            retries++
            if retries>10 {
                rafts.shutdownRafts()
                t.Fatalf("Failed to commit a msg, %v, after 10 retries", strconv.Itoa(i))
            }
            continue
        }
        i++
    }

    rafts.restoreRaft(t, ldr_id)
    time.Sleep(3*time.Second)


    expect(t, rafts[ldr_index].GetLogAt(17).Data, "17", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(18).Data, "18", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(19).Data, "19", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(20).Data, "20", "Log mismatch after restarting server")

    ldr.Shutdown()
    rafts.shutdownRafts()
}


func TestServerStateRestore2(t *testing.T) {
    rand.Seed(10)
    cleanupLogs()
    rafts := makeRafts() // array of []RaftNode


    ldr := rafts.getLeader(t)


    for i, retries := 1,1 ; i<=10 ;{
        ldr = rafts.getLeader(t)
        ldr.Append(strconv.Itoa(i))
        err := rafts.checkSingleCommit(t, strconv.Itoa(i))
        if err != nil {
            log_warning(3, "Committing msg : %v failed", strconv.Itoa(i))
            retries++
            if retries>10 {
                rafts.shutdownRafts()
                t.Fatalf("Failed to commit a msg, %v, after 10 retries", strconv.Itoa(i))
            }
            continue
        }
        i++
    }

    ldr = rafts.getLeader(t)
    ldr_id := ldr.GetId()
    ldr_index := ldr_id - 1

    ldr.Shutdown()

    for i, retries:=11,1; i<=20 ;  {
        ldr = rafts.getLeader(t)
        ldr.Append(strconv.Itoa(i))
        err := rafts.checkSingleCommit(t, strconv.Itoa(i))
        if err != nil {
            log_warning(3, "Committing msg : %v failed", strconv.Itoa(i))
            retries++
            if retries>10 {
                rafts.shutdownRafts()
                t.Fatalf("Failed to commit a msg, %v, after 10 retries", strconv.Itoa(i))
            }
            continue
        }
        i++
    }

    rafts.restoreRaft(t, ldr_id)
    time.Sleep(3*time.Second)


    expect(t, rafts[ldr_index].GetLogAt(17).Data, "17", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(18).Data, "18", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(19).Data, "19", "Log mismatch after restarting server")
    expect(t, rafts[ldr_index].GetLogAt(20).Data, "20", "Log mismatch after restarting server")

    ldr.Shutdown()
    rafts.shutdownRafts()
}


func TestBasic(t *testing.T) {
    cleanupLogs()
    rafts := makeRafts()        // array of []RaftNode
    log_info(3, "Rafts created")
    ldr := rafts.getLeader(t)    // Wait until a stable leader is elected
    ldr.Append("foo")    // Append to leader
    ldr = rafts.getLeader(t)    // Again wait for stable leader
    rafts.checkSingleCommit(t, "foo")// Wait until next single append is commited on all nodes
    rafts.shutdownRafts()
}

func TestLeaderReelection(t *testing.T) {
    cleanupLogs()
    rafts := makeRafts() // array of []RaftNode

    ldr := rafts.getLeader(t)
    ldr.Shutdown()
    ldr = rafts.getLeader(t)
    ldr.Append("foo")
    rafts.checkSingleCommit(t, "foo")
    rafts.shutdownRafts()
}




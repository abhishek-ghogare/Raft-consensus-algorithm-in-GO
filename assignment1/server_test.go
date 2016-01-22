package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
	//"reflect"
    "sync"
)
var (
	server_running = false
)

// Basic write and read
func TestWrite(t *testing.T) {
	if server_running==false {
		go serverMain()
		server_running = true
		time.Sleep(time.Second)
	}
	name := "hi.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost"+PORT)
	if err != nil {
		t.Fatalf("[TEST]: Cannot connect:" + err.Error()) // report error through testing framework
		return
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v  \r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Fields(resp) // split into OK and <version>
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))     
	scanner.Scan()
	expect(t, scanner.Text(), contents)
	conn.Close()
}

func TestDelete(t *testing.T) {
	//go serverMain()
	name := "hi1.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	//ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	//version := int64(ver)

	fmt.Fprintf(conn, "delete %v\r\n", name) // try a delete now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "OK")
}

func TestCas(t *testing.T) {
//go serverMain()
	name := "hi2.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, version, len(contents), exptime, contents)

	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "OK")
}

func ConcurrentWrites(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()

	name := "hi.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost"+PORT)
	if err != nil {
		t.Fatalf("[TEST]: Cannot connect:" + err.Error()) // report error through testing framework
		return
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v  \r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Fields(resp) // split into OK and <version>
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))     
	scanner.Scan()
	expect(t, scanner.Text(), contents)
	conn.Close()

}
func ConcurrentDelete(t *testing.T, wg *sync.WaitGroup) {

	TestDelete(t)

	wg.Done()
}
func ConcurrentCas(t *testing.T, wg *sync.WaitGroup) {

	TestCas(t)

	wg.Done()
}

func TestConcurrentWrite(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1000)

	for i:=1 ; i<=1000 ; i++ {
		//fmt.Printf("Adding write thread %v\n", i)
		go ConcurrentWrites(t, &wg)
	}

	 wg.Wait()
}

// 
func eTestConcurrentDelete(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(10)

	for i:=1 ; i<=10 ; i++ {
		//fmt.Printf("Adding write thread %v\n", i)
		go ConcurrentDelete(t, &wg)
	}

	 wg.Wait()
}


func eTestConcurrentCas(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(10)

	for i:=1 ; i<=10 ; i++ {
		//fmt.Printf("Adding write thread %v\n", i)
		go ConcurrentCas(t, &wg)
	}

	 wg.Wait()
}
// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Errorf("Expected %v, found %v", b, a) // t.Error is visible when running `go test -verbose`
	}
}



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
	conn, err := net.Dial("tcp", "localhost"+PORT)
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
	conn, err := net.Dial("tcp", "localhost"+PORT)
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
	//ver, err := strconv.Atoi(arr[1]) // parse version as number
	//if err != nil {
	//	t.Error("Non-numeric version found")
	//}
	//version := int64(ver)

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	// expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))     
	scanner.Scan()
	expect(t, scanner.Text(), contents)
	conn.Close()

}
func ConcurrentDelete(t *testing.T, wg *sync.WaitGroup) {

	TestDelete(t)

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



/******************************************Actual concurrency test********************/
/* Check if the server handles the multiple cas commands with appropriate locking
*/

/*
 *	Employ multiple threads to do CAS on same file repeatedly until each thread completes the CAS successfully.
 *	This way, if any two threads updates on same file version, this is the error.
 *	This states that on server side, there is concurrency handled correctly
 */
func TestConcurrentCas(t *testing.T) {
	var wg sync.WaitGroup

	version_chan := make(chan int64)
	ver_list := [] int64{}

	wg.Add(100)

	for i:=0 ; i<100 ; i++ {
		//fmt.Printf("Adding write thread %v\n", i)
		go ConcurrentCas(t,version_chan,&wg)
	}

	for i:=0 ; i<100 ; i++ {
		var v int64 = 0
		v = <-version_chan
  		//fmt.Printf("Ver%v : %v\n", i, v)
  		for j:=0 ; j<len(ver_list) ; j++ {
  			if ver_list[j] == v {
  				t.Error("Two different CAS happened on same old version number:\nculprit version ", v, "\nmatched index:",j, "\ncurrent index:",len(ver_list))
  				break
  			}
  		}
  		ver_list = append(ver_list, v)
	}

	wg.Wait()
}

func ConcurrentCas(t *testing.T, version_chan chan int64, wg *sync.WaitGroup) {

	write_file(t, "hi.txt", "hello")

	old_ver, _ := force_cas(t, "hi.txt", "hello1")
	version_chan <- old_ver


	wg.Done()
}


/****************************************************************************
 *																			*
 *	Utility functions														*
 *																			*
 **************************************************************************/
// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Errorf("Expected %v, found %v", b, a) // t.Error is visible when running `go test -verbose`
	}
}

/*
 *	Forcefully do a CAS until the CAS is successfull.
 *	Update latest version and try to CAS it until it is successfull.
 *	Returns : old_version, new_version
 *			- old_version : old version on which CAS was done successfully
 *			- new_version : new latest version
 *
 *	Command : cas <filename> <version> <numbytes> [<exptime>]\r\n<content bytes>\r\n
 */
func force_cas(t *testing.T, filename string, data string) (int64, int64) {

	// read a file
	old_ver, data := read_file(t, filename)
	new_ver := int64(0)

	for {
		ver := cas_file(t, filename, old_ver, data)
		if ver!=-1 {
			new_ver = ver
			break	
		} else {
			// read updated file
			old_ver, data = read_file(t, filename)
		}
	}

	return old_ver, new_ver
}


/*
 *	Read a file
 *	Returns : version, data
 *
 *	TCP specs
 *	Command : read <filename>\r\n
 *	Reply	: CONTENTS <version> <numbytes> <exptime>\r\n<content bytes>\r\n
 */
func read_file(t *testing.T, filename string) (int64, string) {

	conn, err := net.Dial("tcp", "localhost"+PORT)
	defer conn.Close()

	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	reader := bufio.NewReader(conn)

	fmt.Fprintf(conn, "read %v\r\n", filename)

	line, _, err := reader.ReadLine()	// Read until \n or \r\n
	reply := strings.Fields(string(line))		// Split on blank spaces

	if len(reply) == 1 {	// if error in reading
		return -1, ""
	}

	ver, err := strconv.ParseInt(reply[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	line, _, err = reader.ReadLine()	// Read until \n or \r\n
	return version, strings.TrimSpace(string(line))
}


/*
 *	Write to file
 *	Returns : version
 *
 *	TCP specs
*	Command : write <filename> <numbytes> [<exptime>]\r\n<content bytes>\r\n
*	Returns : OK <version>\r\n
 */
func write_file(t *testing.T, filename string, data string) (int64) {

	conn, err := net.Dial("tcp", "localhost"+PORT)
	defer conn.Close()

	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	reader := bufio.NewReader(conn)

	fmt.Fprintf(conn, "write %v %v %v  \r\n%v\r\n", filename, len(data), int64(30000000), data)

	line, _, err := reader.ReadLine()	// Read until \n or \r\n
	reply := strings.Fields(string(line))		// Split on blank spaces

	if len(reply) != 2 {	// if error in writing
		return -1
	}

	ver, err := strconv.ParseInt(reply[1], 10, 64)
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	return version
}


/*
 *	CAS to file
 *	Returns : version
 *
 *	TCP specs
*	Command : cas <filename> <version> <numbytes> [<exptime>]\r\n<content bytes>\r\n
*	Returns : OK <version>\r\n
 */
func cas_file(t *testing.T, filename string, version int64, data string) (int64) {

	conn, err := net.Dial("tcp", "localhost"+PORT)
	defer conn.Close()

	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	reader := bufio.NewReader(conn)

	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", filename, version, len(data), int64(30000000), data)

	line, _, err := reader.ReadLine()	// Read until \n or \r\n
	reply := strings.Fields(string(line))		// Split on blank spaces

	if strings.TrimSpace(reply[0]) != "OK" {	// if error in writing
		return -1
	}

	ver, err := strconv.ParseInt(reply[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	return int64(ver)
}
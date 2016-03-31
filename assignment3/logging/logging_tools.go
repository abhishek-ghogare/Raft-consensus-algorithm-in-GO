package logging

import (
	"log"
	"os"
	"runtime"
	"fmt"
	"strings"
)

// Debugging tools

const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_FMT = "\x1b[3%v;1m"
const CLR_END = "\x1b[0m"
/*

func (rn *RaftNode) prnt(format string, args ...interface{}) {
	fmt.Printf( fmt.Sprintf(CLR_FMT, rn.server_state.myState) +
	strconv.Itoa(rn.server_state.CurrentTerm) +
	" [NODE\t: " + strconv.Itoa(rn.GetId()) + "] \t" + format + "\n" + CLR_END, args...)
}


// Debugging tools
func prnt(format string, args ...interface{}) {
	fmt.Printf("[TEST] \t: " + format + "\n", args...)
}


// Debugging tools
func (server_state *ServerState) prnt(format string, args ...interface{}) {
    fmt.Printf(strconv.Itoa(server_state.CurrentTerm) + " [RSM\t: " + strconv.Itoa(server_state.Server_id) + "] \t" + format + "\n", args...)
}

*/

/***********************************************************************************************************************/
type LogLevel uint

const (
	FLAG_INF LogLevel = 1<<iota	// info
	FLAG_WAR 			// warning
	FLAG_ERR			// error
	FLAG_CRI			// critical
)
var logLevel = (FLAG_ERR | FLAG_CRI | FLAG_WAR | FLAG_INF)
// Enable a log level
func SetLogFlag (bit LogLevel) {
	logLevel = logLevel | bit
}

// Set log level
func SetLogLevel (mask LogLevel) {
	logLevel = mask
}




var logger *log.Logger = log.New(os.Stdout,"",log.Ldate|log.Lmicroseconds)
func logMsg (logType LogLevel, format string, args ...interface{} ) {
	if logLevel & logType != 0 {
		pc, fileName, line, _ := runtime.Caller(3)

		arr := strings.Split(fileName,"/")
		fileName = arr[len(arr)-1]

		funcName := runtime.FuncForPC(pc).Name()
		arr = strings.Split(funcName, ".")
		funcName = arr[len(arr)-1]

		format = fmt.Sprintf( "%25v:%-3v : %-25v : ", fileName, line, funcName) + format
		log.Printf( format,  args...)
	}
}

func Error(format string, args ...interface{}) {
	logMsg( FLAG_ERR, "[ERR] : " + format , args...)
}
func Info(format string, args ...interface{}) {
	logMsg( FLAG_INF, "[INF] : " + format , args...)
}
func Warning(format string, args ...interface{}) {
	logMsg( FLAG_WAR, "[War] : " + format , args...)
}
func Critical(format string, args ...interface{}) {
	logMsg( FLAG_CRI, "[CRI] : " + format , args...)
}

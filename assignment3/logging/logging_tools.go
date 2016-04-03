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

type LogLevel uint

const (
    FLAG_INF LogLevel = 1 << iota    // info
    FLAG_WAR            // warning
    FLAG_ERR            // error
    FLAG_CRI            // critical
)

var logLevel = (FLAG_ERR | FLAG_CRI | FLAG_WAR | FLAG_INF )
// Enable a log level
func SetLogFlag(bit LogLevel) {
    logLevel = logLevel | bit
}

// Set log level
func SetLogLevel(mask LogLevel) {
    logLevel = mask
}

var logger *log.Logger = log.New(os.Stdout, "", log.Ldate | log.Lmicroseconds)

// The argument skip is the number of stack frames
// to ascend, with 0 identifying the caller of Caller.  (For historical reasons the
// meaning of skip differs between Caller and Callers.)
func logMsg(skip int, logType LogLevel, format string, args ...interface{}) {
    if logLevel & logType != 0 {
        pc, fileName, line, _ := runtime.Caller(skip)

        arr := strings.Split(fileName, "/")
        fileName = arr[len(arr) - 1]

        funcName := runtime.FuncForPC(pc).Name()
        arr = strings.Split(funcName, ".")
        funcName = arr[len(arr) - 1]

        format = fmt.Sprintf("%25v:%-3v : %-25v : ", fileName, line, funcName) + format
        log.Printf(format, args...)
    }
}

func Error(skip int, format string, args ...interface{}) {
    logMsg(skip, FLAG_ERR, "[ERR] : " + format, args...)
}
func Info(skip int,format string, args ...interface{}) {
    logMsg(skip, FLAG_INF, "[INF] : " + format, args...)
}
func Warning(skip int,format string, args ...interface{}) {
    logMsg(skip, FLAG_WAR, "[War] : " + format, args...)
}
func Critical(skip int,format string, args ...interface{}) {
    logMsg(skip, FLAG_CRI, "[CRI] : " + format, args...)
}


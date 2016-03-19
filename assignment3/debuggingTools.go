package main

import (
	"fmt"
	"strconv"
)

// Debugging tools

const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_FMT = "\x1b[3%v;1m"
const CLR_END = "\x1b[0m"

func (rn *RaftNode) prnt(format string, args ...interface{}) {
	fmt.Printf( fmt.Sprintf(CLR_FMT, rn.server_state.myState) +
	strconv.Itoa(rn.server_state.currentTerm) +
	" [NODE\t: " + strconv.Itoa(rn.GetId()) + "] \t" + format + "\n" + CLR_END, args...)
}


// Debugging tools
func prnt(format string, args ...interface{}) {
	fmt.Printf("[TEST] \t: " + format + "\n", args...)
}
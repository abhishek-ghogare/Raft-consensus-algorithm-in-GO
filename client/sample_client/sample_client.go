package main

import (
    "github.com/avg598/cs733/raft_config"
    "fmt"
    "os"
    "github.com/avg598/cs733/client"
)

func usage () {
    fmt.Println("Usage : [read|write|cas|delete]")
    fmt.Println("      : read   <filename>")
    fmt.Println("      : write  <filename> <content>")
    fmt.Println("      : cas    <filename> <version> <content>")
    fmt.Println("      : delete <filename>")
}
func main() {
    config, err := raft_config.FromConfigFile("config.json")
    if err != nil {
        fmt.Printf("Error : %v\n", err.Error())
        os.Exit(2)
    }

    cl := client.New(config, 1)

    if cl == nil {
        fmt.Println("Unable to connect to raft servers")
        os.Exit(2)
    }


    expectArgs := func (n int) {
        if len(os.Args) < n {
            usage()
            os.Exit(1)
        }
    }

    expectArgs(2)

    switch os.Args[1] {
    case "read" :
        expectArgs(3)
        msg, err := cl.Read(os.Args[2])
        fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
    case "write" :
        expectArgs(4)
        msg, err := cl.Write(os.Args[2], os.Args[3], 0)
        fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
    default:
        fmt.Println("Invalid operation")
        usage()
    }
}

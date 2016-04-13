package main

import (
    "os"
    "fmt"
    "cs733/assignment4/raft_config"
    "cs733/assignment4/client_handler"
    "flag"
    "cs733/assignment4/logging"
    "log"
    "path"
)

func main() {
    configFilePath := flag.String("config", "config.json", "Path to config file")
    cleanStart := flag.Bool("clean_start", false, "Whether to start raft in clean state. WARNING:This would delete all previous state and logs")
    flag.Parse()

    config, err := raft_config.FromConfigFile(*configFilePath)
    if err != nil {
        fmt.Printf("Error : %v\n", err.Error())
        os.Exit(2)
    }


    f, err := os.OpenFile(path.Clean(config.LogDir + "../debug.log"), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
    logging.Logger = log.New(f, "", log.Ldate | log.Lmicroseconds)
    logging.Logger.Println("Logger initialised")

    server := client_handler.New(config, !*cleanStart)

    server.StartSync()
}

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
    "os/signal"
    "syscall"
)

func main() {
    serverId := flag.Int("id", -1, "mandatory Id of this server")
    configFilePath := flag.String("config", "config.json", "Path to config file")
    cleanStart := flag.Bool("clean_start", false, "Whether to start raft in clean state. WARNING:This would delete all previous state and logs")
    flag.Parse()

    if *serverId==-1 {
        fmt.Printf("Error : Please provide server id\n")
        flag.Usage()
        os.Exit(1)
    }

    config, err := raft_config.FromConfigFile(*configFilePath)
    if err != nil {
        fmt.Printf("Error : %v\n", err.Error())
        os.Exit(2)
    }

    logPath := path.Clean(config.LogDir + "/debug.log")
    f, err := os.OpenFile(logPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
    logging.Logger = log.New(f, "", log.Ldate | log.Lmicroseconds)
    logging.Logger.Println("Logger initialised")


    server := client_handler.New(*serverId, config, !*cleanStart)


    // Listen to kill signals
    c := make(chan os.Signal)
    signal.Notify(c, os.Interrupt)
    signal.Notify(c, syscall.SIGTERM)
    signal.Notify(c, syscall.SIGKILL)
    signal.Notify(c, syscall.SIGHUP)
    go func(server *client_handler.ClientHandler) {
        <-c
        fmt.Println("\n\n\nSHUTTING DOWN")
        server.Shutdown()
        os.Exit(1)
    }(server)


    server.StartSync()
}

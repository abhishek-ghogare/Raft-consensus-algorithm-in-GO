package raft_config

import (
    "os"
    "encoding/json"
    "github.com/cs733-iitb/cluster/mock"
)

type Config struct {
    Id               int    // this node's id. One of the cluster's entries should match.
    LogDir           string // Log file directory for this node
    ElectionTimeout  int
    HeartbeatTimeout int
    NumOfNodes       int
    MockServer       *mock.MockServer

    // Client side config
    ClientPort      int
}


func ToConfigFile(configFile string, config Config) (err error) {
    var f *os.File
    if f, err = os.Create(configFile); err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    if err = enc.Encode(config); err != nil {
        return err
    }
    return nil
}

func FromConfigFile(configFile string) (config *Config, err error) {
    var cfg Config
    var f *os.File
    if f, err = os.Open(configFile); err != nil {
        return nil, err
    }
    defer f.Close()
    dec := json.NewDecoder(f)
    if err = dec.Decode(&cfg); err != nil {
        return nil, err
    }
    return &cfg, nil
}


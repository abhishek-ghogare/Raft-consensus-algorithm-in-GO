Implementation of [Raft consensus algorithm](https://raft.github.io/) in GO
============================================================================
## Overview
#### What is Raft?
Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems. We hope Raft will make consensus available to a wider audience, and that this wider audience will be able to develop a variety of higher quality consensus-based systems than are available today.
[Ref : [The Raft Consensus Algorithm](https://raft.github.io/)]

This package implements the Raft mechanism in GO language. Leveldb is used for underlying persistent log store requirement (github.com/cs733-iitb/log/). There are slight customizations from original [Raft: In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) specification. The read requests from clients are not redirected to a leader and they are not replicated. Any raft node can serve read requests regardless of their state.

## Setup
```
# Requires libzmq3-dev library
go get github.com/cs733-iitb/log/...
go build raft_main.go
```
## Usage
```
Usage of raft_main:
  -clean_start
    	Whether to start raft in clean state. WARNING:This would delete all previous state and logs
  -config string
    	Path to config file (default "config.json")
  -id int
    	Id of this server (default -1)

```
Use `sample_client` program to communicate with the raft cluster.

## Code Structure
The package is divided into different sub-packages.

#### Client Handler
Defines client handler class. Responsible for listening to client requests, replicate on raft nodes, apply replicated client requests to the file system and reply to client with response.

#### Raft Node
Raft node class. Responsible for inter-raftnode communication, serve client handler's replication requests, set raft timeouts, etc.

#### Raft State Machine
State machine class. Implements actual raft mechanism. Manages persistent replicated log, fullfills raft services according to raft state (leader, follower, candidate), generates actions for events, e.g. vote request action for timeout events, append request to followers for requests received from client, etc.

#### File System (fs) - A simple network file server
**fs** is a simple network file server. Access to the server is via a simple telnet compatible API. Each file has a version number, and the server keeps the latest version. There are four commands, to read, write, compare-and-swap and delete the file.

Refer `assignment4>client_handler>filesystem>README.md` for more information.

#### Logging mechanism
Raft logs are diveided into 4 levels, **critical, error, warning** and **info**.

#### Raft Config Structure
A univarsal config structure used by client handler, raft node, raft state machine and the client. 
```go
type Config struct {
    LogDir           string // Log file directory for this node
    ElectionTimeout  int
    HeartbeatTimeout int
    NumOfNodes       int
    ClusterConfig    cluster.Config
    ClientPorts      []int
    ServerList       []string
}
```
#### Sample config.json file
```
{
	"LogDir"            : "/home/raft/",
	"ElectionTimeout"   : 15000,    		# In msec
	"HeartbeatTimeout"  : 1000,     		# In msec
	"NumOfNodes"        : 5,
	"ClusterConfig"     : {
	                            "Peers"         : 
	                                   [{"Id":1,"Address":<IP:PORT of node 1>},
	                                    {"Id":2,"Address":<IP:PORT of node 2>},
                                    	{"Id":3,"Address":<IP:PORT of node 3>},
                                    	{"Id":4,"Address":<IP:PORT of node 4>},
                                    	{"Id":5,"Address":<IP:PORT of node 5>}],
	                            "InboxSize"     : 0,
	                            "OutboxSize"    : 0
	                        },
	                        
	# Port on which client handler will listen client requests
	"ClientPorts"       : [  0,
	                        <CLIENT_PORT of node 1>,
	                        <CLIENT_PORT of node 2>,
	                        <CLIENT_PORT of node 3>,
	                        <CLIENT_PORT of node 4>,
	                        <CLIENT_PORT of node 5>], 
	"ServerList"        : [
	                        "",
                        	<IP:CLIENT_PORT of node 1>,
                        	<IP:CLIENT_PORT of node 2>,
                        	<IP:CLIENT_PORT of node 3>,
                        	<IP:CLIENT_PORT of node 4>,
                        	<IP:CLIENT_PORT of node 5>,
                            ]
}
```


#### Client package
Client package for encoding and decoding file system requests response sent over TCP connection. A simple command line client program `sample_client.go` uses this Client class to communicate to server.
`sample_client.go` can be used for testing purpose.

## Version
1.0.0

## Contact
  - Developer : [Abhishek Ghogare](https://github.com/avg598)

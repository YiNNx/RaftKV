package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/BurntSushi/toml"

	"raftkv/internal/common"
)

type Args struct {
	Str string
}

type Reply struct {
	Str string
}

type Raft int

func (t *Raft) Echo(args *Args, reply *Reply) error {
	log.Println(args.Str)
	reply.Str = args.Str
	return nil
}

func heartbeat(me string, peers []string) {
	for {
		select {
		case <-time.After(1 * time.Second):
			for _, peer := range peers {
				args := Args{me}
				var reply Reply
				client, err := rpc.DialHTTP("tcp", peer)
				if err != nil {
					continue
				}
				client.Call("Raft.Echo", &args, &reply)
			}
		}
	}
}

func main() {
	port := flag.String("p", ":8080", "rpc server port")
	configPath := flag.String("c", "config.toml", "config file path")
	flag.Parse()
	log.Printf(*port)
	var config common.Config
	if _, err := toml.DecodeFile(*configPath, &config); err != nil {
		panic(err)
	}

	go heartbeat(*port, config.RPC.Cluster)

	arith := new(Raft)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	http.Serve(l, nil)
}

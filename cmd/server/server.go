package main

import (
	"errors"
	"flag"
	"log"

	"github.com/BurntSushi/toml"

	"raftkv/internal/common"
	"raftkv/internal/server"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

func StartServer(id int, nodes []common.Node, restart bool, persisterConf common.Persister) error {
	var port string
	rpcEnds := make(map[int]*rpc.ClientEnd, len(nodes))
	for _, node := range nodes {
		rpcEnds[node.ID] = rpc.MakeClientEnd(node.Host + node.Port)
		if node.ID == id {
			port = node.Port
		}
	}
	if len(port) == 0 {
		return errors.New("invalid node id")
	}
	rpcServer := rpc.NewServer(port)
	server.StartKVServer(
		rpcServer,
		rpcEnds,
		id,
		persister.MakePersister(id, restart,persisterConf.State, persisterConf.Snapshot),
	)
	return rpcServer.Run()
}

func main() {
	id := flag.Int("index", -1, "specify current node id in cluster config")
	restart := flag.Bool("restart", false, "recover from last crash")
	configPath := flag.String("c", "config.toml", "config file")
	flag.Parse()
	var config common.Config
	if _, err := toml.DecodeFile(*configPath, &config); err != nil {
		log.Fatal(err)
	}
	log.Print("[config] ", config)

	log.Fatal(StartServer(*id, config.Cluster.Nodes, *restart, config.Persister))
}

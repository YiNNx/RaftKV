package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	"raftkv/internal/shardkv/server"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

func StartServer(id int, nodes []string, restart bool) error {
	var me string
	rpcEnds := make(map[int]*rpc.ClientEnd, len(nodes))
	for nodeID, node := range nodes {
		rpcEnds[nodeID] = rpc.MakeClientEnd(node)
		if nodeID == id {
			me = node
		}
	}
	if len(me) == 0 {
		return errors.New("invalid node id")
	}
	rpcServer := rpc.NewServer(me)
	server.StartKVServer(
		rpcServer,
		rpcEnds,
		id,
		persister.MakePersister(id, restart, "/tmp/kvraft/state", "/tmp/kvraft/snapshot"),
	)
	return rpcServer.Run()
}

func main() {
	id := flag.Int("id", -1, "specify current node id in cluster config")
	nodes := flag.String("nodes", "", "node address list")
	restart := flag.Bool("recover", false, "recover from last crash")
	flag.Parse()

	log.Fatal(StartServer(*id, strings.Split(*nodes, ","), *restart))
}

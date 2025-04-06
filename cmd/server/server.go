package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	"raftkv/internal/kvserver"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

func StartServers(id int, peerAddrs []string, restart bool) error {
	var me string
	peers := make(map[int]*rpc.ClientEnd, len(peerAddrs))
	for nodeID, node := range peerAddrs {
		peers[nodeID] = rpc.MakeClientEnd(node)
		if nodeID == id {
			me = node
		}
	}
	if len(me) == 0 {
		return errors.New("invalid node id")
	}
	rpcServer := rpc.NewServer(me)
	kvserver.StartKVServer(
		rpcServer,
		peers,
		id,
		persister.MakePersister(id, restart, "/tmp/kvraft/state", "/tmp/kvraft/snapshot"),
	)
	return rpcServer.Run()
}

func main() {
	id := flag.Int("id", -1, "specify current node id in the group")
	nodes := flag.String("nodes", "", "node address list")
	restart := flag.Bool("recover", false, "recover from last crash")
	flag.Parse()

	log.Fatal(StartServers(*id, strings.Split(*nodes, ","), *restart))
}

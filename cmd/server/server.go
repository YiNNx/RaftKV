package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	"raftkv/internal/kvraft"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

func StartServers(id string, peerAddrs []string, restart bool) error {
	var me string
	peers := make(map[string]*rpc.ClientEnd, len(peerAddrs))
	for _, node := range peerAddrs {
		peers[node] = rpc.MakeClientEnd(node)
		if node == id {
			me = node
		}
	}
	backupPeers := make(map[int]*rpc.ClientEnd, len(peerAddrs))
	for nodeID, node := range []string{"localhost:8083"} {
		backupPeers[nodeID] = rpc.MakeClientEnd(node)
	}
	if len(me) == 0 {
		return errors.New("invalid node id")
	}
	rpcServer := rpc.NewServer(me)
	kvraft.StartKVServer(
		rpcServer,
		peers,
		backupPeers,
		id,
		persister.MakePersister(id, restart, "/tmp/kvraft/state", "/tmp/kvraft/snapshot"),
	)
	return rpcServer.Run()
}

func main() {
	id := flag.String("id", "", "specify current node id in the group")
	nodes := flag.String("nodes", "", "node address list")
	restart := flag.Bool("recover", false, "recover from last crash")
	flag.Parse()

	log.Fatal(StartServers(*id, strings.Split(*nodes, ","), *restart))
}

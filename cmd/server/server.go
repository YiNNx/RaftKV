package main

import (
	"errors"
	"flag"
	"log"
	"strings"

	shardctl_server "raftkv/internal/shardctl/server"
	shardkv_server "raftkv/internal/shardkv/server"
	"raftkv/pkg/persister"
	"raftkv/pkg/rpc"
)

func StartShardCtrlers(id int, peerAddrs []string, restart bool) error {
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
	shardctl_server.StartServer(
		rpcServer,
		peers,
		id,
		persister.MakePersister(id, restart, "/tmp/kvraft/state", "/tmp/kvraft/snapshot"),
	)
	return rpcServer.Run()
}

func StartServers(id int, gid int, peerAddrs []string, ctrlerAddrs []string, restart bool) error {
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
	ctrlers := make([]*rpc.ClientEnd, len(ctrlerAddrs))
	for nodeID, node := range ctrlerAddrs {
		ctrlers[nodeID] = rpc.MakeClientEnd(node)
	}
	rpcServer := rpc.NewServer(me)
	shardkv_server.StartKVServer(
		rpcServer,
		peers,
		id,
		persister.MakePersister(id, restart, "/tmp/kvraft/state", "/tmp/kvraft/snapshot"),
		gid,
		ctrlers,
	)
	return rpcServer.Run()
}

func main() {
	id := flag.Int("id", -1, "specify current node id in the group")
	gid := flag.Int("gid", -1, "specify group id in the cluster")
	nodes := flag.String("nodes", "", "node address list")
	ctrlers := flag.String("ctrlers", "", "ctrlers address list")
	shardctrl := flag.Bool("shardctrl", false, "")
	restart := flag.Bool("recover", false, "recover from last crash")
	flag.Parse()

	if *shardctrl {
		log.Fatal(StartShardCtrlers(*id, strings.Split(*nodes, ","), *restart))
	} else {
		log.Fatal(StartServers(*id, *gid, strings.Split(*nodes, ","), strings.Split(*ctrlers, ","), *restart))
	}
}

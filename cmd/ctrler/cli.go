package main

import (
	"encoding/gob"
	"flag"
	"fmt"

	"raftkv/internal/raft"
	"raftkv/pkg/rpc"
)

func main() {
	addr := flag.String("addr", "", "leader addr")
	add := flag.String("add", "", "add server")
	remove := flag.String("remove", "", "add server")
	flag.Parse()

	rpcEnd := rpc.MakeClientEnd(*addr)
	gob.Register(raft.AddServerArgs{})
	gob.Register(raft.AddServerReply{})
	gob.Register(raft.RemoveServerArgs{})
	gob.Register(raft.RemoveServerReply{})

	if len(*add) != 0 {
		args := raft.AddServerArgs{
			NewServer: *add,
		}
		var reply raft.AddServerReply
		ok := rpcEnd.Call("Raft.AddServer", &args, &reply)
		fmt.Printf("%+v %+v\n", ok, reply)
	}

	if len(*remove) != 0 {
		args := raft.RemoveServerArgs{
			OldServer: *remove,
		}
		var reply raft.RemoveServerReply
		ok := rpcEnd.Call("Raft.RemoveServer", &args, &reply)
		fmt.Printf("%+v %+v\n", ok, reply)
	}
}

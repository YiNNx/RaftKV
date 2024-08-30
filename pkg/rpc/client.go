package rpc

import "net/rpc"

type ClientEnd struct {
	addr string
}

func (c *ClientEnd) Call(serviceMethod string, args any, reply any) bool {
	client, err := rpc.DialHTTP("tcp", c.addr)
	if err != nil {
		return false
	}
	defer client.Close()
	err = client.Call("Raft.Echo", &args, &reply)
	if err != nil {
		return false
	}
	return true
}

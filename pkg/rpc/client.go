package rpc

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type ClientEnd struct {
	addr string
}

func MakeClientEnd(addr string) *ClientEnd {
	return &ClientEnd{addr}
}

func (c *ClientEnd) Call(serviceMethod string, args any, reply any) bool {
	client, err := rpc.DialHTTP("tcp", c.addr)
	if err != nil {
		return false
	}
	defer client.Close()
	err = client.Call(serviceMethod, args, reply)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

type Server struct {
	port string
}

func NewServer(port string) *Server {
	return &Server{port}
}

func (s *Server) Register(rcvr any) error {
	return rpc.Register(rcvr)
}

func (s *Server) Run() error {
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", s.port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	return http.Serve(l, nil)
}

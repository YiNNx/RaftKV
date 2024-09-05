package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"raftkv/internal/shardkv/client"
	"raftkv/pkg/rpc"
)

var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrInvalidFormat  = errors.New("invalid format")
)

type Cli struct {
	clerk *client.Clerk
}

func NewCli(addrList []string) *Cli {
	rpcEnds := make([]*rpc.ClientEnd, len(addrList))
	for i, addr := range addrList {
		rpcEnds[i] = rpc.MakeClientEnd(addr)
	}
	return &Cli{
		clerk: client.MakeClerk(rpcEnds),
	}
}

func (cli *Cli) HandleCommand(command string, args []string) (output string, err error) {
	switch command {
	case "GET":
		if len(args) != 1 {
			return "", ErrInvalidFormat
		}
		output = cli.clerk.Get(args[0])
	case "PUT":
		if len(args) != 2 {
			return "", ErrInvalidFormat
		}
		cli.clerk.Put(args[0], args[1])
		output = "ok"
	case "APPEND":
		if len(args) != 2 {
			return "", ErrInvalidFormat
		}
		cli.clerk.Append(args[0], args[1])
		output = "ok"
	case "EXIT":
		return "", io.EOF
	default:
		return "", ErrInvalidCommand
	}
	return
}

func (cli *Cli) Run() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("raftkv cli > ")
		text, _ := reader.ReadString('\n')
		fields := strings.Fields(text)
		command := strings.ToUpper(fields[0])
		args := fields[1:]
		output, err := cli.HandleCommand(command, args)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(output)
	}
}

func main() {
	addr := flag.String("addr", "", "kv server address list, split by ',' ")
	flag.Parse()
	if len(*addr) == 0 {
		fmt.Print("arg -addr missing\n")
		return
	}

	cli := NewCli(strings.Split(*addr, ","))
	cli.Run()
}

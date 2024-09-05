package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"raftkv/internal/shardctl/client"
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

func (cli *Cli) HandleCommand(command string, args []string) (output interface{}, err error) {
	switch command {
	case "QUERY":
		if len(args) != 0 {
			return "", ErrInvalidFormat
		}
		output = cli.clerk.Query(-1)
	case "JOIN":
		if len(args) != 2 {
			return "", ErrInvalidFormat
		}
		gid, err := strconv.Atoi(args[0])
		if err != nil {
			return "", ErrInvalidFormat
		}
		cli.clerk.Join(map[int][]string{gid: strings.Split(args[1], ",")})
	case "LEAVE":
		if len(args) != 1 {
			return "", ErrInvalidFormat
		}
		num, err := strconv.Atoi(args[0])
		if err != nil {
			return "", ErrInvalidFormat
		}
		cli.clerk.Leave([]int{num})
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
		fmt.Print("shardctl cli > ")
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
	addr := flag.String("ctrlers", "", "ctrlers address list, split by ',' ")
	flag.Parse()
	if len(*addr) == 0 {
		fmt.Print("arg -addr missing\n")
		return
	}

	cli := NewCli(strings.Split(*addr, ","))
	cli.Run()
}

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"raftkv/internal/client"
	"raftkv/internal/common"
	"raftkv/pkg/rpc"
)

var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrInvalidFormat  = errors.New("invalid format")
)

type Cli struct {
	clerk *client.Clerk
}

func NewCli(nodes []common.Node) *Cli {
	rpcEnds := make([]*rpc.ClientEnd, len(nodes))
	for _, node := range nodes {
		rpcEnds[node.ID] = rpc.MakeClientEnd(node.Host + node.Port)
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
		return
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
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(output)
	}
}

func main() {
	configPath := flag.String("c", "config.toml", "config file")
	flag.Parse()
	var config common.Config
	if _, err := toml.DecodeFile(*configPath, &config); err != nil {
		log.Fatal(err)
	}

	cli := NewCli(config.Cluster.Nodes)
	cli.Run()
}

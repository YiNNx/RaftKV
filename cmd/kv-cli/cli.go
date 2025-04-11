package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"raftkv/internal/kvserver"
	"raftkv/pkg/rpc"
)

var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrInvalidFormat  = errors.New("invalid format")
)

// Transaction state for the CLI
type TxnState struct {
	TxnID  string
	Active bool
}

type Cli struct {
	clerk *kvserver.Clerk
	txn   TxnState
}

func NewCli(addrList []string) *Cli {
	rpcEnds := make([]*rpc.ClientEnd, len(addrList))
	for i, addr := range addrList {
		rpcEnds[i] = rpc.MakeClientEnd(addr)
	}
	return &Cli{
		clerk: kvserver.MakeClerk(rpcEnds),
		txn: TxnState{
			Active: false,
		},
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
	case "BEGIN":
		if cli.txn.Active {
			return "Transaction already in progress", nil
		}
		cli.txn.TxnID = cli.clerk.BeginTransaction()
		cli.txn.Active = true
		output = fmt.Sprintf("Transaction started with ID: %s", cli.txn.TxnID)
	case "TGET":
		if !cli.txn.Active {
			return "No active transaction. Use BEGIN first.", nil
		}
		if len(args) != 1 {
			return "", ErrInvalidFormat
		}
		output = cli.clerk.TxnGet(cli.txn.TxnID, args[0])
	case "TPUT":
		if !cli.txn.Active {
			return "No active transaction. Use BEGIN first.", nil
		}
		if len(args) != 2 {
			return "", ErrInvalidFormat
		}
		cli.clerk.TxnPut(cli.txn.TxnID, args[0], args[1])
		output = "ok"
	case "COMMIT":
		if !cli.txn.Active {
			return "No active transaction. Use BEGIN first.", nil
		}
		success := cli.clerk.CommitTransaction(cli.txn.TxnID)
		if success {
			output = "Transaction committed successfully"
		} else {
			output = "Failed to commit transaction"
		}
		cli.txn.Active = false
	case "ABORT":
		if !cli.txn.Active {
			return "No active transaction. Use BEGIN first.", nil
		}
		success := cli.clerk.AbortTransaction(cli.txn.TxnID)
		if success {
			output = "Transaction aborted successfully"
		} else {
			output = "Failed to abort transaction"
		}
		cli.txn.Active = false
	case "EXIT":
		return "", io.EOF
	default:
		return "", ErrInvalidCommand
	}
	return
}

func (cli *Cli) Run() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("RaftKV CLI with Transaction Support")
	fmt.Println("Commands:")
	fmt.Println("  GET <key>                - Get a value")
	fmt.Println("  PUT <key> <value>        - Put a value")
	fmt.Println("  APPEND <key> <value>     - Append to a value")
	fmt.Println("  BEGIN                    - Begin a transaction")
	fmt.Println("  TGET <key>               - Get a value within a transaction")
	fmt.Println("  TPUT <key> <value>       - Put a value within a transaction")
	fmt.Println("  COMMIT                   - Commit the current transaction")
	fmt.Println("  ABORT                    - Abort the current transaction")
	fmt.Println("  EXIT                     - Exit the CLI")

	for {
		fmt.Print("raftkv cli > ")
		text, _ := reader.ReadString('\n')
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
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
	addr := flag.String("nodes", "", "nodes address list, split by ',' ")
	flag.Parse()
	if len(*addr) == 0 {
		fmt.Print("arg -addr missing\n")
		return
	}

	cli := NewCli(strings.Split(*addr, ","))
	cli.Run()
}

package persister

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
)

type Persister struct {
	mu            sync.Mutex
	id            int
	raftStatePath string
	snapshotPath  string
	raftstate     []byte
	snapshot      []byte
}

func MakePersister(id int, restart bool, raftStatePath string, snapshotPath string) *Persister {
	err := os.MkdirAll(raftStatePath, 0755)
	if err != nil {
		log.Fatal(err)
	}
	os.MkdirAll(snapshotPath, 0755)
	if err != nil {
		log.Fatal(err)
	}
	ps := &Persister{
		mu:            sync.Mutex{},
		id:            id,
		raftStatePath: path.Join(raftStatePath, strconv.Itoa(id)),
		snapshotPath:  path.Join(snapshotPath, strconv.Itoa(id)),
	}
	if !restart {
		ps.Save(nil, nil)
	}
	return ps
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	file, err := os.OpenFile(ps.raftStatePath, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	_, err = file.Read(buffer)
	if err != nil && err != io.EOF {
		log.Println("Error reading from file:", err)
		return nil
	}
	return buffer
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	stateFile, err := os.OpenFile(ps.raftStatePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer stateFile.Close()

	_, err = stateFile.Write(raftstate)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	snapshotFile, err := os.OpenFile(ps.snapshotPath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer snapshotFile.Close()

	_, err = snapshotFile.Write(snapshot)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	file, err := os.OpenFile(ps.snapshotPath, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	_, err = file.Read(buffer)
	if err != nil && err != io.EOF {
		log.Println("Error reading from file:", err)
		return nil
	}
	return buffer
}

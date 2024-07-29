package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToIntermediateFile(mapID int, reduceID int, kvs []KeyValue) error {
	filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
	content, err := json.Marshal(kvs)
	if err != nil {
		return err
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(string(content))
	return err
}

func readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var kvs []KeyValue
	err = json.Unmarshal(content, &kvs)
	return kvs, err
}

type worker struct {
	workerID int
	nReduce  int
}

func (w *worker) assignMapTask() (int, *MapTask, error) {
	return CallAssignMapTask(w.workerID)
}

func (w *worker) processMap(mapID int, task MapTask, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(task.Filename)
	if err != nil {
		return err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	kvs := mapf(task.Filename, string(content))
	// map shuffle
	intermediate := make([][]KeyValue, w.nReduce)
	for _, kv := range kvs {
		reduceID := ihash(kv.Key) % w.nReduce
		intermediate[reduceID] = append(intermediate[reduceID], kv)
	}
	for reduceID, kvs := range intermediate {
		writeToIntermediateFile(mapID, reduceID, kvs)
	}
	// fmt.Printf("finish task: %s\n", task.Filename)
	time.Sleep(time.Second)
	return nil
}

func (w *worker) finishMap(taskID int) error {
	return CallFinishMapTask(taskID, w.workerID)
}

func (w *worker) assignReduceTask() (int, *ReduceTask, error) {
	return CallAssignReduceTask(w.workerID)
}

func (w *worker) processReduce(reduceID int, task ReduceTask, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for _, intermidiateFile := range task.IntermediateFiles {
		kvs, err := readIntermediateFile(intermidiateFile)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kvs...)
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return nil
}

func (w *worker) finishReduce(taskID int) error {
	return CallFinishReduceTask(taskID, w.workerID)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID, nReduce, err := CallRegisterWorker()
	if err != nil {
		log.Fatal("failed to RegisterWorker:", err)
	}
	worker := worker{
		workerID: workerID,
		nReduce:  nReduce,
	}
	// fmt.Printf("worker %+v\n", worker)
	// map phrase
	for {
		taskID, task, err := worker.assignMapTask()
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Printf("receive map task %d %+v\n", taskID, task)
		if taskID == -1 {
			break
		}
		err = worker.processMap(taskID, *task, mapf)
		if err != nil {
			log.Fatal(err)
		}
		err = worker.finishMap(taskID)
		if err != nil {
			log.Fatal(err)
		}
	}
	// reduce phrase
	for {
		taskID, task, err := worker.assignReduceTask()
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Printf("receive reduce task %d %+v\n", taskID, task)
		if taskID == -1 {
			break
		}
		err = worker.processReduce(taskID, *task, reducef)
		if err != nil {
			log.Fatal(err)
		}
		err = worker.finishReduce(taskID)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func CallFinishReduceTask(taskID int, workerID int) (err error) {
	args := ArgsFinishReduceTask{TaskID: taskID, WorkerID: workerID}
	reply := ReplyFinishReduceTask{}
	err = call("Coordinator.FinishReduceTask", &args, &reply)
	return err
}

func CallAssignMapTask(workerID int) (int, *MapTask, error) {
	args := ArgsAssignMapTask{WorkerID: workerID}
	reply := ReplyAssignMapTask{}
	err := call("Coordinator.AssignMapTask", &args, &reply)
	return reply.TaskID, reply.Task, err
}

func CallAssignReduceTask(workerID int) (int, *ReduceTask, error) {
	args := ArgsAssignReduceTask{WorkerID: workerID}
	reply := ReplyAssignReduceTask{}
	err := call("Coordinator.AssignReduceTask", &args, &reply)
	return reply.TaskID, reply.Task, err
}

func CallFinishMapTask(taskID int, workerID int) (err error) {
	args := ArgsFinishMapTask{TaskID: taskID, WorkerID: workerID}
	reply := ReplyFinishMapTask{}
	err = call("Coordinator.FinishMapTask", &args, &reply)
	return err
}

func CallRegisterWorker() (workerID int, nReduce int, err error) {
	args := ArgsRegisterWorker{}
	reply := ReplyRegisterWorker{}
	err = call("Coordinator.RegisterWorker", &args, &reply)
	return reply.WorkerID, reply.NReduce, err
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}

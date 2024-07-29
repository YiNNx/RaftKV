package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type MapTask struct {
	Filename string
}

type ReduceTask struct {
	IntermediateFiles []string
}

type Coordinator struct {
	nReduce       int
	workers       WorkerManager
	mapTaskMng    TaskManager[MapTask]
	reduceTaskMng TaskManager[ReduceTask]
}

func (c *Coordinator) FinishMapTask(args *ArgsFinishMapTask, reply *ReplyFinishMapTask) error {
	if !c.workers.getWorkerStatus(args.WorkerID) {
		return fmt.Errorf("task timeout")
	}
	c.mapTaskMng.finishTask(args.TaskID)
	return nil
}

func (c *Coordinator) FinishReduceTask(args *ArgsFinishReduceTask, reply *ReplyFinishReduceTask) error {
	if !c.workers.getWorkerStatus(args.WorkerID) {
		return fmt.Errorf("task timeout")
	}
	c.reduceTaskMng.finishTask(args.TaskID)
	return nil
}

func (c *Coordinator) assignMapTask(workerID int) (int, *MapTask, error) {
	if !c.workers.getWorkerStatus(workerID) {
		return -1, nil, fmt.Errorf("worker %d is offline", workerID)
	}
	taskID, task := c.mapTaskMng.assignTask()
	if taskID == -1 {
		return -1, nil, nil
	}
	go func() {
		ok := c.mapTaskMng.superviseTask(taskID)
		if !ok {
			c.workers.setWorkerStatus(workerID, false)
		}
	}()
	// fmt.Printf("assign map task %d %+v\n", taskID, task)
	return taskID, task, nil
}

func (c *Coordinator) AssignMapTask(args *ArgsAssignMapTask, reply *ReplyAssignMapTask) (err error) {
	reply.TaskID, reply.Task, err = c.assignMapTask(args.WorkerID)
	return
}

func (c *Coordinator) assignReduceTask(workerID int) (int, *ReduceTask, error) {
	if !c.workers.getWorkerStatus(workerID) {
		return -1, nil, fmt.Errorf("worker %d is offline", workerID)
	}
	taskID, task := c.reduceTaskMng.assignTask()
	if taskID == -1 {
		return -1, nil, nil
	}
	go func() {
		ok := c.reduceTaskMng.superviseTask(taskID)
		if !ok {
			c.workers.setWorkerStatus(workerID, false)
		}
	}()
	// fmt.Printf("assign reduce task %d %+v\n", taskID, task)
	return taskID, task, nil
}

func (c *Coordinator) AssignReduceTask(args *ArgsAssignReduceTask, reply *ReplyAssignReduceTask) (err error) {
	reply.TaskID, reply.Task, err = c.assignReduceTask(args.WorkerID)
	return
}

func (c *Coordinator) RegisterWorker(args *ArgsRegisterWorker, reply *ReplyRegisterWorker) error {
	reply.NReduce = c.nReduce
	reply.WorkerID = c.workers.addWorker()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceTaskMng.prepared && !c.reduceTaskMng.hasUnfinishedTask()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:       nReduce,
		workers:       WorkerManager{idCounter: atomic.Uint32{}, workers: sync.Map{}},
		mapTaskMng:    NewTaskManager[MapTask](),
		reduceTaskMng: NewTaskManager[ReduceTask](),
	}
	for _, filename := range files {
		c.mapTaskMng.addTaskToProcess(MapTask{
			Filename: filename,
		}, nil)
	}
	c.mapTaskMng.setPrepared()

	c.server()

	go func() {
		for c.mapTaskMng.hasUnfinishedTask() {
			time.Sleep(time.Second)
		}

		intermediateFiles := make([][]string, nReduce)
		for reduceID := 0; reduceID < c.nReduce; reduceID++ {
			for _, mapID := range c.mapTaskMng.FinishedList {
				filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
				intermediateFiles[reduceID] = append(intermediateFiles[reduceID], filename)
			}
		}
		for reduceID, files := range intermediateFiles {
			c.reduceTaskMng.addTaskToProcess(ReduceTask{
				IntermediateFiles: files,
			}, &reduceID)
		}
		c.reduceTaskMng.setPrepared()
	}()

	return &c
}

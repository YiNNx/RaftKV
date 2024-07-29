package mr

import (
	"sync"
	"sync/atomic"
	"time"
)

type TaskManager[T any] struct {
	Counter *atomic.Int32

	ProcessMapMut *sync.RWMutex
	ProcessMap    map[int]struct {
		Processing bool
		Task       T
	}

	FinishedListMut *sync.RWMutex
	FinishedList    []int

	prepared bool
}

func NewTaskManager[T any]() TaskManager[T] {
	t := TaskManager[T]{
		Counter:       &atomic.Int32{},
		ProcessMapMut: &sync.RWMutex{},
		ProcessMap: make(map[int]struct {
			Processing bool
			Task       T
		}),
		FinishedListMut: &sync.RWMutex{},
		FinishedList:    []int{},
	}
	t.Counter.Store(-1)
	return t
}

func (t *TaskManager[T]) setPrepared() {
	t.prepared = true
}

func (t *TaskManager[T]) genTaskID() int {
	return int(t.Counter.Add(1))
}

func (t *TaskManager[T]) addTaskToProcess(task T, taskID *int) {
	if taskID == nil {
		id := t.genTaskID()
		taskID = &id
	}

	t.ProcessMapMut.Lock()
	defer t.ProcessMapMut.Unlock()

	t.ProcessMap[*taskID] = struct {
		Processing bool
		Task       T
	}{
		Processing: false,
		Task:       task,
	}
}

func (t *TaskManager[T]) hasUnfinishedTask() bool {
	t.ProcessMapMut.RLock()
	defer t.ProcessMapMut.RUnlock()
	return len(t.ProcessMap) != 0
}

func (t *TaskManager[T]) assignPendingTask() (taskID int, task *T) {
	t.ProcessMapMut.Lock()
	defer t.ProcessMapMut.Unlock()

	taskID = -1
	for id, p := range t.ProcessMap {
		if !p.Processing {
			taskID = id
			break
		}
	}
	if taskID != -1 {
		p := t.ProcessMap[taskID]
		p.Processing = true
		t.ProcessMap[taskID] = p
		return taskID, &p.Task
	}
	return taskID, nil
}

func (t *TaskManager[T]) assignTask() (taskID int, task *T) {
	for !t.prepared {
		time.Sleep(time.Second * 1)
	}
	taskID, task = t.assignPendingTask()
	for taskID == -1 && t.hasUnfinishedTask() {
		time.Sleep(time.Second * 1)
		taskID, task = t.assignPendingTask()
	}
	if taskID == -1 {
		return -1, nil
	}
	return taskID, task
}

func (t *TaskManager[T]) superviseTask(taskID int) (ok bool) {
	time.Sleep(10 * time.Second)

	t.ProcessMapMut.Lock()
	defer t.ProcessMapMut.Unlock()
	if p, processing := t.ProcessMap[taskID]; processing {
		p.Processing = false
		t.ProcessMap[taskID] = p
		return false
	}
	return true
}

func (t *TaskManager[T]) finishTask(taskID int) {
	t.ProcessMapMut.Lock()
	delete(t.ProcessMap, taskID)
	t.ProcessMapMut.Unlock()

	t.FinishedListMut.Lock()
	t.FinishedList = append(t.FinishedList, taskID)
	t.FinishedListMut.Unlock()
}

type WorkerManager struct {
	idCounter atomic.Uint32
	workers   sync.Map // Map[string]bool workerID:workerStatus
}

func (w *WorkerManager) genWorkerID() int {
	return int(w.idCounter.Add(1))
}

func (w *WorkerManager) getWorkerStatus(workerID int) bool {
	status, ok := w.workers.Load(workerID)
	if !ok {
		return false
	}
	return status.(bool)
}

func (w *WorkerManager) addWorker() (workerID int) {
	workerID = w.genWorkerID()
	w.workers.Store(workerID, true)
	return workerID
}

func (w *WorkerManager) setWorkerStatus(workerID int, status bool) {
	w.workers.Store(workerID, status)
}

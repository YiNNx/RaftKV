package kvraft

import "sync"

type KVRepositery struct {
	dataMu *sync.RWMutex
	data   map[string]string
}

func NewKVRepositories() *KVRepositery {
	return &KVRepositery{
		dataMu: &sync.RWMutex{},
		data:   map[string]string{},
	}
}

func (repo *KVRepositery) Get(key string) string {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	return repo.data[key]
}

func (repo *KVRepositery) Put(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	repo.data[key] = val
}

func (repo *KVRepositery) Append(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	oldVal := repo.data[key]
	repo.data[key] = oldVal + val
}

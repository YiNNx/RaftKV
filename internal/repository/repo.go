package repository

import "sync"

type KVRepository struct {
	dataMu *sync.RWMutex
	data   map[string]string
}

func NewKVRepositories() *KVRepository {
	return &KVRepository{
		dataMu: &sync.RWMutex{},
		data:   map[string]string{},
	}
}

func (repo *KVRepository) Get(key string) string {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	return repo.data[key]
}

func (repo *KVRepository) Put(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	repo.data[key] = val
}

func (repo *KVRepository) Append(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	oldVal := repo.data[key]
	repo.data[key] = oldVal + val
}

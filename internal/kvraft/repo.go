package kvraft

import "sync"

// KVRepositery stores key-value data with support for multiple column families
type KVRepositery struct {
	dataMu *sync.RWMutex
	// Map of column family -> map of key -> value
	data map[string]map[string]string
	// Transaction and lock managers
	txnManager *TransactionManager
	lockManager *LockManager
}

func NewKVRepositories() *KVRepositery {
	repo := &KVRepositery{
		dataMu: &sync.RWMutex{},
		data: map[string]map[string]string{
			DefaultCF: make(map[string]string),
			LockCF:    make(map[string]string),
			WriteCF:   make(map[string]string),
		},
		txnManager:  NewTransactionManager(),
		lockManager: NewLockManager(),
	}
	return repo
}

// Get retrieves a value from the default column family
func (repo *KVRepositery) Get(key string) string {
	return repo.GetFromCF(DefaultCF, key)
}

// GetFromCF retrieves a value from a specific column family
func (repo *KVRepositery) GetFromCF(cf string, key string) string {
	repo.dataMu.RLock()
	defer repo.dataMu.RUnlock()
	
	if cfData, ok := repo.data[cf]; ok {
		return cfData[key]
	}
	return ""
}

// Put stores a value in the default column family
func (repo *KVRepositery) Put(key string, val string) {
	repo.PutToCF(DefaultCF, key, val)
}

// PutToCF stores a value in a specific column family
func (repo *KVRepositery) PutToCF(cf string, key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	
	if _, ok := repo.data[cf]; !ok {
		repo.data[cf] = make(map[string]string)
	}
	
	repo.data[cf][key] = val
}

// Append appends a value to an existing value in the default column family
func (repo *KVRepositery) Append(key string, val string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	
	oldVal := repo.data[DefaultCF][key]
	repo.data[DefaultCF][key] = oldVal + val
}

// DeleteFromCF deletes a key from a specific column family
func (repo *KVRepositery) DeleteFromCF(cf string, key string) {
	repo.dataMu.Lock()
	defer repo.dataMu.Unlock()
	
	if cfData, ok := repo.data[cf]; ok {
		delete(cfData, key)
	}
}

// TxnGet gets a value within a transaction
func (repo *KVRepositery) TxnGet(txnID string, key string) (string, Err) {
	// Check if transaction exists
	txn, exists := repo.txnManager.GetTransaction(txnID)
	if !exists {
		return "", ErrTxnNotFound
	}
	
	// Check if key is in transaction's write set
	if val, ok := txn.WriteSet[key]; ok {
		return val, OK
	}
	
	// Check if key is locked by another transaction
	if locked, lockTxnID := repo.lockManager.IsLocked(key); locked && lockTxnID != txnID {
		return "", ErrKeyLocked
	}
	
	// Get from storage
	val := repo.Get(key)
	
	// Add to read set
	txn.ReadSet[key] = val
	
	return val, OK
}

// TxnPut puts a value within a transaction
func (repo *KVRepositery) TxnPut(txnID string, key string, value string) Err {
	// Check if transaction exists
	txn, exists := repo.txnManager.GetTransaction(txnID)
	if !exists {
		return ErrTxnNotFound
	}
	
	// Try to acquire lock
	if !repo.lockManager.TryLock(key, txnID, txn.Timestamp) {
		return ErrKeyLocked
	}
	
	// Add to write set
	txn.WriteSet[key] = value
	
	return OK
}

// TxnCommit commits a transaction using the Percolator protocol
func (repo *KVRepositery) TxnCommit(txnID string) Err {
	// Check if transaction exists
	txn, exists := repo.txnManager.GetTransaction(txnID)
	if !exists {
		return ErrTxnNotFound
	}
	
	// Phase 1: Prewrite - Write locks and data
	for key, value := range txn.WriteSet {
		// Write to lock column family
		lockKey := key
		lockValue := txnID
		repo.PutToCF(LockCF, lockKey, lockValue)
		
		// Write data with timestamp
		writeKey := key
		writeValue := value
		repo.PutToCF(WriteCF, writeKey, writeValue)
	}
	
	// Phase 2: Commit - Write commit timestamp and clean up locks
	for key := range txn.WriteSet {
		// Remove lock
		repo.lockManager.Unlock(key, txnID)
		repo.DeleteFromCF(LockCF, key)
		
		// Apply changes to default column family
		repo.Put(key, txn.WriteSet[key])
	}
	
	// Update transaction status
	repo.txnManager.UpdateTransactionStatus(txnID, TxnStatusCommitted)
	
	return OK
}

// TxnAbort aborts a transaction
func (repo *KVRepositery) TxnAbort(txnID string) Err {
	// Check if transaction exists
	txn, exists := repo.txnManager.GetTransaction(txnID)
	if !exists {
		return ErrTxnNotFound
	}
	
	// Clean up locks
	for key := range txn.WriteSet {
		repo.lockManager.Unlock(key, txnID)
		repo.DeleteFromCF(LockCF, key)
	}
	
	// Update transaction status
	repo.txnManager.UpdateTransactionStatus(txnID, TxnStatusAborted)
	
	return OK
}

package kvserver

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// Transaction status
const (
	TxnStatusUnknown   = "unknown"
	TxnStatusPending   = "pending"
	TxnStatusCommitted = "committed"
	TxnStatusAborted   = "aborted"
)

// Transaction error types
const (
	ErrTxnNotFound      = "ErrTxnNotFound"
	ErrTxnConflict      = "ErrTxnConflict"
	ErrTxnAborted       = "ErrTxnAborted"
	ErrKeyLocked        = "ErrKeyLocked"
	ErrTxnTimeout       = "ErrTxnTimeout"
	ErrInvalidTxnStatus = "ErrInvalidTxnStatus"
)

// Column families for Percolator model
const (
	DefaultCF = ""
	LockCF    = "lock"
	WriteCF   = "write"
)

// Transaction represents a transaction in the system
type Transaction struct {
	ID        string
	Status    string
	StartTime time.Time
	Timestamp uint64
	ReadSet   map[string]string
	WriteSet  map[string]string
}

// NewTransaction creates a new transaction
func NewTransaction() *Transaction {
	return &Transaction{
		ID:        uuid.New().String(),
		Status:    TxnStatusPending,
		StartTime: time.Now(),
		Timestamp: uint64(time.Now().UnixNano()),
		ReadSet:   make(map[string]string),
		WriteSet:  make(map[string]string),
	}
}

// TransactionManager manages all transactions in the system
type TransactionManager struct {
	mu           sync.Mutex
	transactions map[string]*Transaction
	timestamps   map[string]uint64 // key -> timestamp
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		timestamps:   make(map[string]uint64),
	}
}

// CreateTransaction creates a new transaction and returns its ID
func (tm *TransactionManager) CreateTransaction() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn := NewTransaction()
	tm.transactions[txn.ID] = txn
	return txn.ID
}

// GetTransaction returns a transaction by ID
func (tm *TransactionManager) GetTransaction(txnID string) (*Transaction, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, ok := tm.transactions[txnID]
	return txn, ok
}

// UpdateTransactionStatus updates the status of a transaction
func (tm *TransactionManager) UpdateTransactionStatus(txnID string, status string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if txn, ok := tm.transactions[txnID]; ok {
		txn.Status = status
		return true
	}
	return false
}

// Lock represents a lock on a key
type Lock struct {
	TxnID     string
	Timestamp uint64
}

// LockManager manages locks for the Percolator model
type LockManager struct {
	mu    sync.Mutex
	locks map[string]*Lock // key -> lock
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*Lock),
	}
}

// TryLock attempts to acquire a lock on a key
func (lm *LockManager) TryLock(key string, txnID string, timestamp uint64) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[key]; exists {
		// Key is already locked by another transaction
		return lock.TxnID == txnID
	}

	// Key is not locked, acquire the lock
	lm.locks[key] = &Lock{
		TxnID:     txnID,
		Timestamp: timestamp,
	}
	return true
}

// Unlock releases a lock on a key
func (lm *LockManager) Unlock(key string, txnID string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[key]; exists && lock.TxnID == txnID {
		delete(lm.locks, key)
		return true
	}
	return false
}

// IsLocked checks if a key is locked
func (lm *LockManager) IsLocked(key string) (bool, string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lock, exists := lm.locks[key]; exists {
		return true, lock.TxnID
	}
	return false, ""
}

// TransactionArgs represents arguments for transaction operations
type TransactionArgs struct {
	TxnID string
	Key   string
	Value string
}

// TransactionReply represents replies for transaction operations
type TransactionReply struct {
	Value string
	TxnID string
}

// CommitArgs represents arguments for commit operation
type CommitArgs struct {
	TxnID string
}

// CommitReply represents replies for commit operation
type CommitReply struct {
	Success bool
}

// AbortArgs represents arguments for abort operation
type AbortArgs struct {
	TxnID string
}

// AbortReply represents replies for abort operation
type AbortReply struct {
	Success bool
}

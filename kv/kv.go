// Package kv implements the replicated key-value state machine that sits on top
// of the Raft log. Commands are serialised, replicated through Raft, and
// applied here in log order, which gives every replica the same state. Reads
// are served through Raft's read-index path so they are linearizable.
package kv

import (
	"encoding/json"
	"sync"
)

// OpKind enumerates the supported operations.
type OpKind string

const (
	OpGet    OpKind = "get"
	OpPut    OpKind = "put"
	OpDelete OpKind = "delete"
)

// Command is the unit of replication. Get commands are not replicated through
// the log; they go through the read-index path instead, but the type is shared
// for symmetry and history recording.
type Command struct {
	Kind  OpKind `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// EncodeCommand serialises a command for the Raft log.
func EncodeCommand(c Command) ([]byte, error) { return json.Marshal(c) }

// DecodeCommand deserialises a command from the Raft log.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Store is the in-memory key-value state machine. It is concurrency-safe so the
// apply goroutine and read path can touch it at once.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{data: map[string]string{}}
}

// Apply executes a committed command against the state machine and returns the
// resulting value for the affected key, which is what a writer can echo back.
func (s *Store) Apply(c Command) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Kind {
	case OpPut:
		s.data[c.Key] = c.Value
		return c.Value
	case OpDelete:
		delete(s.data, c.Key)
		return ""
	case OpGet:
		return s.data[c.Key]
	}
	return ""
}

// Get reads a key directly. Callers must have confirmed linearizability through
// the Raft read-index path before calling this; the cluster client does so.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Snapshot serialises the entire store for log compaction.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, _ := json.Marshal(s.data)
	return b
}

// Restore replaces the store's contents from a snapshot produced by Snapshot.
func (s *Store) Restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]string{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	s.data = m
}

// Len returns the number of keys, used by tests and metrics.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

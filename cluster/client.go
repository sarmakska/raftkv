package cluster

import (
	"errors"
	"time"

	"github.com/sarmakska/raftkv/kv"
	"github.com/sarmakska/raftkv/linz"
	"github.com/sarmakska/raftkv/raft"
)

// ErrNoLeader is returned when the client cannot find a leader within its retry
// budget.
var ErrNoLeader = errors.New("client: no leader available")

// Client is a leader-aware key-value client. It discovers the leader, retries
// writes that land on a follower, and serves reads through the Raft read-index
// path so they are linearizable. Every operation is optionally recorded into a
// History for the linearizability checker.
type Client struct {
	c       *Cluster
	timeout time.Duration
	history *linz.History
}

// NewClient returns a client bound to the cluster with the given per-operation
// timeout. Pass a non-nil history to record operations.
func NewClient(c *Cluster, timeout time.Duration, history *linz.History) *Client {
	return &Client{c: c, timeout: timeout, history: history}
}

// Put replicates a write and waits for it to commit and apply on the leader.
func (cl *Client) Put(key, value string) error {
	var callID int
	if cl.history != nil {
		callID = cl.history.Invoke(linz.Op{Kind: linz.OpPut, Key: key, Value: value})
	}
	err := cl.propose(kv.Command{Kind: kv.OpPut, Key: key, Value: value})
	if cl.history != nil {
		cl.history.Return(callID, value, err == nil)
	}
	return err
}

// Delete replicates a delete and waits for it to commit and apply.
func (cl *Client) Delete(key string) error {
	var callID int
	if cl.history != nil {
		callID = cl.history.Invoke(linz.Op{Kind: linz.OpDelete, Key: key})
	}
	err := cl.propose(kv.Command{Kind: kv.OpDelete, Key: key})
	if cl.history != nil {
		cl.history.Return(callID, "", err == nil)
	}
	return err
}

// Get performs a linearizable read. It confirms leadership via the read-index
// path, waits for the state machine to catch up, then reads.
func (cl *Client) Get(key string) (string, bool, error) {
	var callID int
	if cl.history != nil {
		callID = cl.history.Invoke(linz.Op{Kind: linz.OpGet, Key: key})
	}
	val, ok, err := cl.read(key)
	if cl.history != nil {
		// A read that errored is recorded as a failed (indeterminate-free) op
		// the checker can ignore; a successful read records the observed value.
		recorded := val
		if !ok {
			recorded = linz.Nil
		}
		cl.history.Return(callID, recorded, err == nil)
	}
	return val, ok, err
}

func (cl *Client) propose(cmd kv.Command) error {
	data, err := kv.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(cl.timeout)
	for time.Now().Before(deadline) {
		leader := cl.c.Leader()
		if leader < 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		rn := cl.c.raftOf(leader)
		if rn == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		idx, term, isLeader := rn.Propose(data)
		if !isLeader {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if cl.waitCommitted(rn, idx, term) {
			return nil
		}
	}
	return ErrNoLeader
}

// waitCommitted waits until the leader has committed and applied the entry at
// idx in term. If leadership changes under it (the entry might be lost) it
// reports failure so the client retries.
func (cl *Client) waitCommitted(rn *raft.Node, idx, term uint64) bool {
	deadline := time.Now().Add(cl.timeout)
	for time.Now().Before(deadline) {
		role, curTerm, _ := rn.State()
		if role != raft.Leader || curTerm != term {
			return false
		}
		if rn.CommitIndex() >= idx {
			// Wait for the state machine to actually apply the entry so a
			// subsequent read on this leader observes it.
			cl.waitStoreApplied(rn.ID(), idx)
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// waitStoreApplied blocks until the named node's state machine has applied at
// least idx, bounded by the client timeout.
func (cl *Client) waitStoreApplied(id int, idx uint64) bool {
	deadline := time.Now().Add(cl.timeout)
	for time.Now().Before(deadline) {
		if cl.c.appliedOf(id) >= idx {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cl.c.appliedOf(id) >= idx
}

func (cl *Client) read(key string) (string, bool, error) {
	deadline := time.Now().Add(cl.timeout)
	for time.Now().Before(deadline) {
		leader := cl.c.Leader()
		if leader < 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		rn := cl.c.raftOf(leader)
		store := cl.c.storeOf(leader)
		if rn == nil || store == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		readIdx, err := rn.ReadIndex(cl.timeout)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		// Wait until this node's state machine has applied the read index, so
		// the value we return is at least as fresh as the linearization point.
		if !cl.waitStoreApplied(leader, readIdx) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		v, ok := store.Get(key)
		return v, ok, nil
	}
	return "", false, ErrNoLeader
}

package raft

import (
	"errors"
	"time"
)

// ErrNotLeader is returned by leader-only operations when this node is not the
// current leader.
var ErrNotLeader = errors.New("raft: not leader")

// ErrTimeout is returned when a linearizable read cannot confirm leadership in
// time.
var ErrTimeout = errors.New("raft: read confirmation timed out")

// State reports the node's current role, term and known leader. It is safe to
// call concurrently.
func (n *Node) State() (role Role, term uint64, leader int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm, n.leaderID
}

// IsLeader reports whether the node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// ID returns the node's id.
func (n *Node) ID() int { return n.id }

// ReadIndex implements linearizable reads. It combines two safety mechanisms
// from the Raft paper, section 6.4:
//
//  1. Leader lease. If a majority of followers acknowledged a heartbeat within
//     the last election-timeout minimum, the leader holds a lease and can serve
//     the read immediately. The lease is shorter than the minimum election
//     timeout, so no other node can have been elected in the meantime.
//  2. Read index. The leader records its current commit index as the read
//     index, then waits until its state machine has applied at least that
//     index before allowing the read. Because a no-op entry is committed at the
//     start of each term, the commit index reflects the latest term.
//
// The returned index is the point in the log at which the read is linearized;
// the caller must wait for the state machine to reach it (the cluster harness
// does this) before reading.
func (n *Node) ReadIndex(timeout time.Duration) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIdx := n.commitIndex
	leaseOK := time.Now().Before(n.leaderLeaseUntil)
	n.mu.Unlock()

	if leaseOK {
		return n.waitApplied(readIdx, timeout)
	}

	// No valid lease: confirm leadership with a round of heartbeats before
	// honouring the read.
	n.mu.Lock()
	n.broadcastAppendLocked(true)
	n.mu.Unlock()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		ok := n.role == Leader && time.Now().Before(n.leaderLeaseUntil)
		idx := n.commitIndex
		n.mu.Unlock()
		if ok {
			if idx > readIdx {
				readIdx = idx
			}
			return n.waitApplied(readIdx, timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return 0, ErrTimeout
}

func (n *Node) waitApplied(idx uint64, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		applied := n.lastApplied
		stillLeader := n.role == Leader
		n.mu.Unlock()
		if !stillLeader {
			return 0, ErrNotLeader
		}
		if applied >= idx {
			return idx, nil
		}
		time.Sleep(time.Millisecond)
	}
	return 0, ErrTimeout
}

// Package cluster wires the Raft core, the key-value state machine, an
// in-memory network and a leader-aware client into a single-process cluster.
// It is the substrate the fault-injection harness drives: nodes can be crashed
// and restarted with their on-disk state intact, and every client operation is
// recorded into a history for the linearizability checker.
package cluster

import (
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sarmakska/raftkv/kv"
	"github.com/sarmakska/raftkv/raft"
)

// Options configures a cluster.
type Options struct {
	N                  int    // number of nodes
	Dir                string // directory for per-node persistent state
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	SnapshotThreshold  uint64
}

// DefaultOptions returns options tuned for fast in-process tests.
func DefaultOptions(n int, dir string) Options {
	return Options{
		N:                  n,
		Dir:                dir,
		HeartbeatInterval:  40 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		SnapshotThreshold:  0,
	}
}

// node bundles a Raft node with its state machine and apply pump.
type node struct {
	id        int
	raft      *raft.Node
	store     *kv.Store
	applyCh   chan raft.ApplyMsg
	storage   *raft.Storage
	stopApply chan struct{}
	wg        sync.WaitGroup
	// applied is the highest log index whose command has been applied to the
	// state machine by this node's pump. The read path waits on it so a
	// linearizable Get never observes state behind its read index.
	applied atomic.Uint64
}

// Cluster is a running set of Raft nodes connected by an in-memory network.
type Cluster struct {
	mu    sync.Mutex
	opts  Options
	net   *Network
	nodes map[int]*node
	peers []int
}

// New constructs and starts a cluster of opts.N nodes.
func New(opts Options) (*Cluster, error) {
	c := &Cluster{opts: opts, net: NewNetwork(), nodes: map[int]*node{}}
	for i := 0; i < opts.N; i++ {
		c.peers = append(c.peers, i)
	}
	for i := 0; i < opts.N; i++ {
		if err := c.startNode(i); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Network exposes the in-memory fabric so the fault harness can install a
// filter.
func (c *Cluster) Network() *Network { return c.net }

// Peers returns the node ids in the cluster.
func (c *Cluster) Peers() []int { return c.peers }

func (c *Cluster) startNode(id int) error {
	dir := filepath.Join(c.opts.Dir, "node-"+strconv.Itoa(id))
	storage, err := raft.NewFileStorage(dir)
	if err != nil {
		return err
	}
	store := kv.NewStore()
	// Restore application state from the persisted snapshot, if any, so a
	// restarted node does not replay the compacted prefix.
	if snap, ok, err := storage.LoadSnapshot(); err == nil && ok {
		store.Restore(snap.Data)
	}
	applyCh := make(chan raft.ApplyMsg, 256)
	cfg := raft.Config{
		ID:                 id,
		Peers:              c.peers,
		Storage:            storage,
		Transport:          c.net.EndpointFor(id),
		HeartbeatInterval:  c.opts.HeartbeatInterval,
		ElectionTimeoutMin: c.opts.ElectionTimeoutMin,
		ElectionTimeoutMax: c.opts.ElectionTimeoutMax,
		SnapshotThreshold:  c.opts.SnapshotThreshold,
		ApplyCh:            applyCh,
	}
	rn, err := raft.NewNode(cfg)
	if err != nil {
		return err
	}
	rn.SetSnapshotter(func() []byte { return store.Snapshot() })
	var sptr raft.Storage = storage
	n := &node{id: id, raft: rn, store: store, applyCh: applyCh, storage: &sptr, stopApply: make(chan struct{})}
	c.net.Register(id, rn)

	// Apply pump: drains committed entries into the state machine.
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-n.stopApply:
				return
			case msg, ok := <-applyCh:
				if !ok {
					return
				}
				if msg.SnapshotValid {
					n.store.Restore(msg.Snapshot)
					if msg.SnapshotIndex > n.applied.Load() {
						n.applied.Store(msg.SnapshotIndex)
					}
					continue
				}
				if msg.CommandValid {
					// A nil command is a Raft no-op marker; advance the
					// watermark but do not touch the state machine.
					if msg.Command != nil {
						if cmd, err := kv.DecodeCommand(msg.Command); err == nil {
							n.store.Apply(cmd)
						}
					}
					n.applied.Store(msg.Index)
				}
			}
		}
	}()

	rn.Start()
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	return nil
}

// Crash stops a node's goroutines and removes it from the network without
// touching its on-disk state, simulating a power loss.
func (c *Cluster) Crash(id int) {
	c.mu.Lock()
	n, ok := c.nodes[id]
	if ok {
		delete(c.nodes, id)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	n.raft.Stop()
	close(n.stopApply)
	n.wg.Wait()
	(*n.storage).Close()
}

// Restart brings a previously crashed node back, replaying its persisted state
// and log from disk. This is the path the crash-recovery test verifies.
func (c *Cluster) Restart(id int) error {
	return c.startNode(id)
}

// Leader returns the id of a node that currently believes it is leader, or -1.
// When several nodes briefly believe they are leader during an election, the
// one with the highest term wins the tie so callers see a consistent answer.
func (c *Cluster) Leader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	best, bestTerm := -1, uint64(0)
	for id, n := range c.nodes {
		if n.raft.IsLeader() {
			_, term, _ := n.raft.State()
			if term >= bestTerm {
				best, bestTerm = id, term
			}
		}
	}
	return best
}

// raftOf returns the Raft node for an id, or nil if crashed.
func (c *Cluster) raftOf(id int) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[id]; ok {
		return n.raft
	}
	return nil
}

// Store returns the live state machine for a node, or nil if it is crashed.
// It is exposed for tests and tooling that need to inspect replica state
// directly; production clients should read through a Client for
// linearizability.
func (c *Cluster) Store(id int) *kv.Store { return c.storeOf(id) }

// storeOf returns the state machine for an id, or nil if crashed.
func (c *Cluster) storeOf(id int) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[id]; ok {
		return n.store
	}
	return nil
}

// appliedOf returns the highest index applied to a node's state machine, or 0
// if the node is crashed.
func (c *Cluster) appliedOf(id int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[id]; ok {
		return n.applied.Load()
	}
	return 0
}

// Stop tears the whole cluster down.
func (c *Cluster) Stop() {
	c.mu.Lock()
	ids := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.Crash(id)
	}
}

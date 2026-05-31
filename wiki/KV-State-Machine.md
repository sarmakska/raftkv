# KV State Machine

Raft replicates an opaque log; it does not know or care what the entries mean. The meaning lives in the state machine, and in raftkv that is `kv.Store` in `kv/kv.go`: a replicated map from string keys to string values, where each key behaves as an independent register. This is the simplest state machine that still makes linearizability interesting, which is the point.

## The register model

Every key is a single-copy register. It is absent until its first write, holds the last written value after that, and is absent again after a delete. The [[Linearizability-Checker]] checks each key as an independent register, so the state machine and the checker share exactly the same model. There are no cross-key invariants and no transactions; a `Put("a")` and a `Put("b")` are unrelated. That restriction is what keeps the checker tractable (it can partition the history by key) and is called out as a non-goal on the [[Roadmap]].

## Commands

```go
type Command struct {
    Kind  OpKind // OpGet | OpPut | OpDelete
    Key   string
    Value string
}
```

`EncodeCommand` and `DecodeCommand` are thin wrappers over `json.Marshal`/`Unmarshal`. A `Put` or `Delete` is encoded and handed to `Node.Propose`, which replicates the bytes through the log. A `Get` is never replicated; it goes through the read-index path (see [[Read-Index-and-Leases]]) and reuses the `Command`/`Op` types only so the client can record it in a history.

## Apply

The cluster runs an apply pump per node (in `cluster/cluster.go`) that drains committed entries from the Raft `ApplyMsg` channel and feeds them to the store:

```go
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
```

`Apply` is the only mutation path, and the apply pump calls it in strict log order, so every replica applies the same commands in the same sequence and reaches the same state. That determinism is the whole reason consensus is worth the trouble.

### No-op marker entries

A new leader appends a `nil`-command entry at the start of its term (see [[Raft-Walkthrough]]). When the apply pump sees a committed entry with a `nil` command it advances its applied watermark but does not call `Apply`:

```go
if msg.Command != nil {
    if cmd, err := kv.DecodeCommand(msg.Command); err == nil {
        n.store.Apply(cmd)
    }
}
n.applied.Store(msg.Index)
```

Advancing the watermark even for a no-op matters: the read-index path waits for the state machine to reach a given index, and if a no-op did not move the watermark a read could block on it forever. This is a small detail that is easy to get wrong and is exercised by every read in the chaos test.

## Concurrency

`Store` uses a `sync.RWMutex`. `Apply` takes the write lock; `Get`, `Snapshot` and `Len` take the read lock. The apply pump (one goroutine per node) and the read path can touch the store at once, so the lock is real, not decorative. The store is the one place in the system where the application's own concurrency meets the consensus machinery, and keeping it behind an `RWMutex` keeps that boundary clean.

## Linearizable reads

`Store.Get` reads the map directly and does no consistency work of its own. Linearizability is the caller's responsibility, and the `cluster.Client` discharges it: it confirms leadership and the read index through `Node.ReadIndex`, waits until this node's apply watermark has reached that index, and only then calls `Store.Get`. So the value returned is at least as fresh as the moment the read was linearized. Reading a replica's store directly through `Cluster.Store(id)` bypasses all of this and is documented as a tooling-only path. See [[Read-Index-and-Leases]] and [[Client-API]].

## Snapshot and restore

For log compaction the store serialises itself whole:

```go
func (s *Store) Snapshot() []byte {
    s.mu.RLock(); defer s.mu.RUnlock()
    b, _ := json.Marshal(s.data)
    return b
}

func (s *Store) Restore(data []byte) {
    s.mu.Lock(); defer s.mu.Unlock()
    m := map[string]string{}
    if len(data) > 0 { json.Unmarshal(data, &m) }
    s.data = m
}
```

`Snapshot` is registered with the Raft node via `SetSnapshotter`, so when the node decides to compact (see [[Snapshots-and-Compaction]]) it captures the store's bytes. `Restore` runs in two places: when a node boots and finds a persisted snapshot (so it does not replay the compacted prefix), and when it receives an `InstallSnapshot` from the leader. `TestSnapshotRestore` in `kv/kv_test.go` pins the round trip.

Serialising the whole map on every snapshot is the simple choice and is fine at the scale this project targets. A production state machine would snapshot incrementally; that is out of scope here.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

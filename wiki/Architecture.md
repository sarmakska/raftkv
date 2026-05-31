# Architecture

raftkv is layered so that the consensus core knows nothing about the network it runs on. That single decision is what makes the fault-injection harness possible: the harness sits in the seam between nodes and the consensus code never has to be aware of it.

## Package map

| Package | Responsibility |
| --- | --- |
| `transport` | Message types (RequestVote, AppendEntries, InstallSnapshot) and the `Transport` and `Handler` interfaces that decouple consensus from the wire. |
| `raft` | The consensus core and the crash-safe `FileStorage`. |
| `kv` | The replicated key-value state machine, with snapshot and restore. |
| `cluster` | An in-process cluster, the in-memory network with its fault seam, and the leader-aware client. |
| `fault` | The fault injector and the nemesis scheduler. |
| `linz` | The history recorder and the linearizability checker. |
| `cmd/raftkvd` | A demo binary that wires it all together. |

## The dataflow

```mermaid
flowchart LR
    app["Client.Put / Get / Delete"] --> propose["Node.Propose / Node.ReadIndex"]
    propose --> log["Raft log (FileStorage)"]
    log --> repl["AppendEntries to peers"]
    repl --> commit["commitIndex advances on majority"]
    commit --> apply["applyLoop emits ApplyMsg"]
    apply --> sm["kv.Store.Apply"]
    sm --> read["Client.Get reads through read-index"]
```

A write becomes a `kv.Command`, is serialised, and is handed to `Node.Propose`. The leader appends it to its log, replicates it with `AppendEntries`, and advances `commitIndex` once a majority has it. The apply loop then delivers committed entries in order to the state machine through an `ApplyMsg` channel. Reads do not go through the log; they go through `Node.ReadIndex`, which confirms leadership and waits for the state machine to catch up before the client reads the store.

## The transport seam

Every node reaches its peers only through `transport.Transport`:

```go
type Transport interface {
    SendRequestVote(target int, args *RequestVoteArgs) (*RequestVoteReply, error)
    SendAppendEntries(target int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
    SendInstallSnapshot(target int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
```

In this repository the implementation is `cluster.Endpoint`, which routes every call through a shared `cluster.Network`. The network consults a `Filter` before delivering any message:

```go
type Filter interface {
    Allow(m Message) (deliver bool, delay time.Duration)
}
```

The fault harness is nothing more than a `Filter` plus a scheduler. Because the seam is an interface, swapping the in-memory transport for a real gRPC or TCP transport later is a contained change that touches no consensus logic.

## Concurrency model

Each Raft node guards all of its mutable state with a single `sync.Mutex`. The public methods take the lock, the internal helpers are suffixed `Locked` to make the contract obvious, and the long-running work (sending RPCs) happens on goroutines that re-acquire the lock only to apply the reply. The election and heartbeat logic runs from one ticker goroutine; a separate apply goroutine, woken by a `sync.Cond`, delivers committed entries. This keeps the core close to the paper's pseudo-code instead of turning it into a lock-ordering puzzle.

## Persistence

`raft.FileStorage` writes three things per node:

- `state.json` holds `currentTerm` and `votedFor`, written atomically with write-to-temp-then-rename.
- `log.bin` is an append-only file of length-prefixed, CRC-checked records. On open it is replayed; a short or bad-checksum trailing record is treated as a torn write from a crash and truncated away.
- `snapshot.json` holds the most recent snapshot and is the anchor for log compaction.

The invariant the persistence layer upholds is that anything acknowledged to a peer or a client has already been flushed to disk, which is what lets committed entries survive a crash. See [[Raft-Walkthrough]] for how the core uses these.

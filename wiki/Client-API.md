# Client API

This page documents how to drive a raftkv cluster from Go. The two types you use are `cluster.Cluster` and `cluster.Client`.

## Starting a cluster

```go
import "github.com/sarmakska/raftkv/cluster"

opts := cluster.DefaultOptions(3, "/var/lib/raftkv") // 3 nodes, state under this dir
c, err := cluster.New(opts)
if err != nil {
    log.Fatal(err)
}
defer c.Stop()
```

`DefaultOptions` returns timeouts tuned for fast in-process use. The fields you will most often change are:

| Field | Meaning |
| --- | --- |
| `N` | Number of nodes. |
| `Dir` | Directory under which each node keeps `node-<id>/` with its state, log and snapshot. |
| `HeartbeatInterval` | How often the leader sends heartbeats. |
| `ElectionTimeoutMin` / `ElectionTimeoutMax` | The randomised election timeout window. Keep these several times the heartbeat interval. |
| `SnapshotThreshold` | Applied entries past the last snapshot before the node compacts. Zero disables automatic snapshotting. |

## The client

```go
client := cluster.NewClient(c, 5*time.Second, nil) // nil = do not record a history
```

The third argument is an optional `*linz.History`. Pass one to record every operation for the [[Linearizability-Checker]]; pass `nil` in normal use.

### Writes

```go
if err := client.Put("user:1", "alice"); err != nil {
    // no leader was reachable within the timeout
}
if err := client.Delete("user:1"); err != nil {
    // ...
}
```

`Put` and `Delete` find the current leader, propose the command, and block until it is committed and applied. If the operation lands on a follower, or leadership changes underneath it, the client retries against the new leader until the timeout. A returned error means no leader was reachable in time, not that the write half-applied.

### Linearizable reads

```go
value, found, err := client.Get("user:1")
```

`Get` is linearizable. Internally it calls `Node.ReadIndex`, which confirms the node is still leader and records the current commit index as the read index. It then waits until the node's state machine has applied at least that index before reading the store, so the value returned is at least as fresh as the moment the read was linearized. A read never returns a value older than any write that completed before the read began.

## How leader discovery works

`Cluster.Leader()` returns the id of a node that currently believes it is leader, breaking ties by the highest term so callers get a consistent answer during an election. The client polls it on each attempt, so you do not manage leader addresses yourself.

## Inspecting state directly

For tests and tooling you can reach a node's state machine without going through consensus:

```go
store := c.Store(nodeID) // nil if the node is crashed
v, ok := store.Get("user:1")
```

This bypasses linearizability and reads whatever that replica has applied so far. Production reads should always go through `Client.Get`.

## Crash and restart

```go
c.Crash(nodeID)         // stop the node, leave its disk state intact
err := c.Restart(nodeID) // bring it back, replaying state and log from disk
```

`Crash` simulates a power loss: the goroutines stop and the node leaves the network, but `node-<id>/` on disk is untouched. `Restart` re-opens that directory, so a restarted node rejoins with all of its committed entries. This is the pair the fault harness uses to inject node failures.

## Tuning timeouts

If you see frequent "no leader" errors under load, the most common cause is an election timeout that is too close to the heartbeat interval, which lets transient delays trigger spurious elections. Keep `ElectionTimeoutMin` at roughly three to five times `HeartbeatInterval`. See [[Troubleshooting]] for more.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

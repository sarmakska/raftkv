# Examples and Recipes

Copy-paste starting points for the things people actually do with raftkv. Each recipe is a complete, runnable fragment built from the real API in `cluster/`, `fault/` and `linz/`. For field-by-field reference see [[Client-API]] and [[Configuration-and-Tuning]].

## A three-node cluster, write and read

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/sarmakska/raftkv/cluster"
)

func main() {
    c, err := cluster.New(cluster.DefaultOptions(3, "/tmp/raftkv-demo"))
    if err != nil { log.Fatal(err) }
    defer c.Stop()

    // Wait for an election to settle.
    for c.Leader() < 0 { time.Sleep(10 * time.Millisecond) }

    cl := cluster.NewClient(c, 3*time.Second, nil)
    if err := cl.Put("user:1", "alice"); err != nil { log.Fatal(err) }

    v, ok, err := cl.Get("user:1")
    fmt.Printf("get user:1 -> %q ok=%v err=%v\n", v, ok, err)
}
```

`DefaultOptions` gives sensible timers; `nil` as the third client argument means do not record a history. The leader-wait loop matters: a cluster needs one election round before it can serve a write.

## Record a workload and check it

The recipe that gives raftkv its point: run a workload, record it, prove it linearizable.

```go
import "github.com/sarmakska/raftkv/linz"

history := linz.NewHistory()
cl := cluster.NewClient(c, 5*time.Second, history) // pass a non-nil history

for i := 0; i < 100; i++ {
    k := []string{"x", "y", "z"}[i%3]
    if i%3 == 2 {
        cl.Get(k)
    } else {
        cl.Put(k, fmt.Sprintf("v%d", i))
    }
}

res := linz.Check(history)
if res.Linearizable {
    fmt.Println("history is linearizable")
} else {
    fmt.Printf("VIOLATION on key %q: %s\n", res.Key, res.Reason)
}
```

The client records the observed value on every operation automatically. Spreading writes across three keys keeps the per-key checker fast (see [[Linearizability-Checker]]).

## Run the built-in nemesis

```go
import "github.com/sarmakska/raftkv/fault"

nm := fault.NewNemesis(c, 42) // 42 is the seed; reuse it to replay the schedule
nm.Run(500 * time.Millisecond)
// ... drive the workload above while this runs ...
nm.Stop()                     // heals the cluster and restarts any crashed node
```

The nemesis picks a random fault every period (isolate a node, split into two groups keeping a majority, a lossy slow link, or crash a node), holds it for half the period, then recovers. Always `Stop` before checking, so the history is judged against a healed, fully-connected cluster.

## Inject a specific partition by hand

When you want a deterministic fault rather than the nemesis schedule, drive the `Injector` directly:

```go
in := fault.NewInjector(1)
c.Network().SetFilter(in)

// Cut node 2 off from everyone.
in.Isolate(2, c.Peers())
// ... observe the surviving majority elect a new leader ...

// Or split into a majority and a minority.
in.Partition([]int{0, 1, 2}, []int{3, 4})

// Or model a lossy, slow link.
in.SetDropRate(0.2)
in.SetDelay(5*time.Millisecond, 25*time.Millisecond)

in.Heal() // clear every fault at once
```

This is exactly what `TestElectionAfterPartition` and `TestLogConvergesAfterHeal` do. See [[Fault-Injection-Harness]].

## Crash and restart a node

```go
c.Crash(2)            // stop node 2, leave its disk state intact (power loss)
// ... cluster keeps serving from the remaining majority ...
err := c.Restart(2)   // re-open node 2's directory, replay state and log
```

`Crash` removes the node from the network without touching `node-2/` on disk; `Restart` re-reads that directory so the node rejoins with all its committed entries. This is the pair `TestRecoveryOfCommittedEntriesFromDisk` exercises.

## Force a snapshot install

To provoke the `InstallSnapshot` path, compact aggressively and keep one follower behind long enough that the entries it needs are discarded:

```go
opts := cluster.DefaultOptions(3, "/tmp/raftkv-snap")
opts.SnapshotThreshold = 20 // compact every 20 applied entries
c, _ := cluster.New(opts)
// ... wait for a leader ...

in := fault.NewInjector(3)
c.Network().SetFilter(in)
in.Isolate(follower, c.Peers())   // hold a follower back

for i := 0; i < 60; i++ {          // generate enough writes to compact past it
    cl.Put(fmt.Sprintf("s%d", i), fmt.Sprintf("v%d", i))
}
in.Heal()                          // the follower catches up via InstallSnapshot
```

This is `TestSnapshotInstall` condensed. See [[Snapshots-and-Compaction]].

## Run the demo binary

No code needed; the `raftkvd` command wires all of the above together.

```bash
go run ./cmd/raftkvd -nodes 5 -ops 200 -seed 42
# raftkv: started 5-node cluster in /tmp/...
# raftkv: leader elected: node 1
# raftkv: nemesis running (partitions, delays, crashes)
# raftkv: ran 200 operations
# raftkv: history is LINEARIZABLE
```

Flags: `-nodes` (default 5), `-ops` (default 200), `-chaos` (default true), `-seed` (default the wall clock). Reuse a seed to replay a fault schedule.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

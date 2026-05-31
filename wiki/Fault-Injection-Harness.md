# Fault-Injection Harness

The harness in `fault/` is a small Jepsen-style framework I wrote to attack the cluster while a workload runs, then hand the recorded history to the [[Linearizability-Checker]]. It has two parts: an `Injector` that decides the fate of every message, and a `Nemesis` that schedules faults over time.

## The injector

`Injector` implements the `cluster.Filter` interface, so the in-memory network calls it for every RPC:

```go
func (in *Injector) Allow(m cluster.Message) (bool, time.Duration)
```

It applies three classes of fault, in order:

1. **Partition.** Nodes are assigned to groups. Two nodes can exchange messages only if they share a group, so a message that crosses groups is dropped. `Partition(groups ...[]int)` sets the groups and `Isolate(id, all)` cuts a single node off.
2. **Drop.** `SetDropRate(p)` drops each surviving message with probability `p`, modelling a lossy link.
3. **Delay and reorder.** `SetDelay(min, max)` holds each delivered message for a uniform random time in the window. Because each RPC runs on its own goroutine, independent delays naturally reorder messages, which is exactly the adversary Raft must tolerate.

`Heal()` clears every fault at once.

### Why this is the right seam

The injector never touches Raft state. It only decides whether a message is delivered and when. That means any behaviour it provokes (a leader stepping down, a follower backtracking, a snapshot install) is the genuine consensus code reacting to a hostile network, not a mock. If a test fails, the bug is real.

## The nemesis

`Nemesis` drives a schedule against a running cluster:

```go
nm := fault.NewNemesis(cluster, seed)
nm.Run(500 * time.Millisecond)
// ... run the workload ...
nm.Stop()
```

`NewNemesis` installs the injector as the cluster's filter. `Run` starts a goroutine that, every period, picks a random fault, holds it for half the period, then recovers:

- isolate one node (a minority partition),
- split the cluster into two groups keeping a majority on one side,
- a lossy, slow network (20 percent drop, 5 to 25 ms delay),
- crash a single node, which `recover` later restarts from disk.

The faults are chosen so that a majority can always make progress between disruptions, which is the regime in which linearizability must hold. `Stop` heals the cluster and restarts any crashed node, so the history can be checked against a quiescent, fully-connected cluster.

## Determinism

Both the injector and the nemesis take a seed, so a chaos run is reproducible. When a run flags a violation you can replay the exact same fault schedule to debug it. Raft's own election timers add real-time jitter, so a seed reproduces the fault schedule rather than a bit-exact run, which is the right trade-off for a timing-driven protocol.

## Worked example

This is the flagship test, condensed:

```go
c, _ := cluster.New(fastOpts(5, dir))
history := linz.NewHistory()
client := cluster.NewClient(c, 5*time.Second, history)

nm := fault.NewNemesis(c, 42)
nm.Run(500 * time.Millisecond)

for i := 0; i < 120; i++ {
    k := keys[i%len(keys)]
    if i%3 == 2 {
        client.Get(k)
    } else {
        client.Put(k, fmt.Sprintf("v%d", i))
    }
}
nm.Stop()

if res := linz.Check(history); !res.Linearizable {
    t.Fatalf("violation on key %q: %s", res.Key, res.Reason)
}
```

Every `Put` and `Get` records an invoke and a return into `history`. While that runs the nemesis is partitioning, dropping, delaying and crashing. The client transparently retries against whichever node is leader. At the end the checker proves no client ever observed a value that a correct single-copy register could not have produced.

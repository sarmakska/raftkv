# Performance and Benchmarks

The numbers here are real, measured on the machine named below, and I explain what dominates each one rather than quoting a headline. raftkv is correctness-first, so these figures exist to be honest about cost, not to win a leaderboard (see the [[Roadmap]] on why I will not chase one).

## The machine and how to reproduce

- Apple M3 Pro, 12 logical CPUs.
- Go 1.26.3, `darwin/arm64`.
- Fast test timeouts: 30 ms heartbeat, 120 to 250 ms election window (`fastOpts` in `cluster/cluster_test.go`).

```bash
go test ./cluster/ -bench Benchmark -run '^$' -benchmem
```

The two benchmarks are `BenchmarkPutThroughput` and `BenchmarkLinearizableGet` in `cluster/bench_test.go`.

## Results

Measured across three `-count=3` runs on the machine above:

| Workload | Result | Allocations |
| --- | --- | --- |
| Linearizable read under the leader lease | 296 to 435 ns/op | 0 B/op, 0 allocs/op |
| Committed write, 3 nodes | 12.2 to 14.7 ms/op | ~3.8 to 4.1 KB/op, ~69 to 71 allocs/op |
| 200-op workload under the nemesis, 5 nodes | linearizable, ~3.2 s wall | checked every run |

A representative raw line:

```
BenchmarkPutThroughput-12      99   12191562 ns/op   3832 B/op   69 allocs/op
BenchmarkLinearizableGet-12    3830896   295.6 ns/op    0 B/op    0 allocs/op
```

## What dominates the write number

A committed write takes on the order of 12 milliseconds in this configuration, which works out to roughly 70 to 80 writes per second through one client. That is not the cost of consensus arithmetic; it is the cost of waiting for the heartbeat cycle to carry the entry to a majority.

```mermaid
flowchart LR
    propose["Propose: append + fsync"] --> wait["wait for next heartbeat"]
    wait --> replicate["AppendEntries to majority"]
    replicate --> ack["majority fsync + ack"]
    ack --> commit["commitIndex advances"]
    commit --> apply["apply pump, watermark moves"]

    classDef c fill:#0d1117,stroke:#38bdf8,color:#f5f7fa
    class propose,wait,replicate,ack,commit,apply c
```

The two costs that set the floor:

1. **Heartbeat latency.** A write proposed just after a heartbeat waits up to one heartbeat interval (30 ms here, ticked at half that) before it rides out to followers. The benchmark drives one write at a time and waits for each to commit, so it pays a fraction of a heartbeat per op. Lower the heartbeat or batch entries and this drops sharply.
2. **fsync.** `AppendLog` ends with `fs.logFile.Sync()` (see [[Storage-Engine]]), and a majority of nodes must fsync before the entry commits. On this SSD that is cheap relative to the heartbeat wait, but it is the durability cost that cannot be optimised away without weakening the crash guarantee.

The benchmark is deliberately a single-client, one-write-at-a-time loop, so it measures latency, not peak throughput. Real throughput would come from batching many commands into one `AppendEntries` and from pipelining, neither of which this teaching-grade implementation does. The honest statement is: this is the latency of a synchronous, fsync-durable, heartbeat-paced write, and the figure is dominated by the timers, not the algorithm.

## What dominates the read number

A linearizable read under a valid lease is sub-microsecond and allocates nothing. That is because, on the hot path, it never leaves the leader: `ReadIndex` finds a valid lease, reads the commit index, confirms the state machine has already applied it, and reads the map (see [[Read-Index-and-Leases]]). No RPC, no fsync, no allocation. This is the payoff of the lease being an optimisation layered on the read index: when the lease is warm, reads are essentially free; when it is cold, a read pays one heartbeat round to reconfirm leadership, which the benchmark does not measure because it runs against a healthy, lease-warm leader.

## Why the absolute numbers do not transfer

The transport is in-process (see [[Transport-and-Network]]). A `SendAppendEntries` is a function call through a mutex, not a packet on a wire. So the read latency reflects goroutine scheduling and the write latency reflects the heartbeat timer and local fsync, not network round trips. On a real network the write latency would be dominated by the slowest majority round trip plus fsync, and the read latency under a cold lease by one round trip. The shape of the costs carries over; the milliseconds do not. This is the single biggest reason a real transport is the headline item on the [[Roadmap]].

## Measuring your own configuration

To see how the ratios move the numbers, change `fastOpts` (or pass your own `Options`) and rerun the bench. Halving the heartbeat roughly halves the per-write wait until fsync becomes the floor. Disabling the lease (forcing the cold path) would add one heartbeat round to every read. The benchmarks reset the timer after leader election (`b.ResetTimer()` follows the leader-wait loop), so startup is excluded and you are measuring steady state.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

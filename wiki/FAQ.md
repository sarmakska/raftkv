# FAQ

The questions a serious reader asks first, answered straight.

## Is this production-ready?

No, and it is not trying to be. The transport is in-process, membership is static, and the on-disk format favours clarity over speed. raftkv is a correctness-first, teaching-grade implementation whose value is that you can read it against the Raft paper and that every consistency claim is backed by a history a checker accepted. For production, use etcd or `hashicorp/raft` (see [[Comparisons]]). The honest limitations are on the [[Roadmap]].

## What does it actually guarantee?

Linearizable writes and reads while a majority of nodes are reachable, and durability of committed entries across crashes. No stale or invented value is ever returned, which the [[Linearizability-Checker]] verifies on every chaos run. It does not guarantee progress when a majority is unreachable; the cluster correctly stalls and the client times out with `ErrNoLeader`.

## Why build your own Raft instead of using a library?

Because the point is the proof, not the datastore. I had implemented Raft before, watched the happy-path tests go green, and could not honestly say it was correct under a partition because I had never put it under one. This time I wrote the checker and the fault harness first and built the core to satisfy them. Outsourcing the Raft or the checker would have defeated the exercise. The full reasoning is in [[Design-Decisions]].

## How do I know the linearizability proof is real and not theatre?

Three ways. The checker is the classic Wing and Gong search, small enough to read in `linz/checker.go`. It is pinned by a runnable example (`ExampleCheck_staleRead`) whose expected output is in the source, so a regression turns the test red. And it runs over every history the chaos suite produces, including the flagship `TestLinearizableUnderChaos`. If the consensus code ever served a stale value, the checker would catch it. See [[Linearizability-Checker]] and [[Testing-Strategy]].

## Why are reads sub-microsecond but writes milliseconds?

A read under a valid leader lease never leaves the leader: it confirms the lease, checks the state machine has applied the read index, and reads the map. No RPC, no fsync. A write must reach a majority and be fsynced before it commits, and in the test configuration it waits for the next heartbeat to carry it, so it is paced by the 30 ms heartbeat timer. The numbers and what dominates them are on [[Performance-and-Benchmarks]].

## Does correctness depend on synchronised clocks?

No. The leader lease uses a wall-clock timer, but it is only an optimisation: it decides whether a read is answered immediately or after a reconfirming heartbeat round. Correctness rests on the read index and the per-term no-op anchor, which do not involve clocks. A clock that drifts or jumps can cost a heartbeat round; it cannot produce a stale read. This is one of the decisions on [[Design-Decisions]] and [[Read-Index-and-Leases]].

## What happens to a write the client could not confirm?

It may or may not have applied. There are no client sessions or idempotency keys, so a retried unconfirmed write can apply twice. The checker models this honestly: an unconfirmed operation is treated as possibly-applied with an unbounded return time, which keeps the check sound. The store is not exactly-once; this is a stated non-goal on the [[Roadmap]].

## Can it lose committed data?

Not while the failure model holds. Term, vote and log are fsynced before being acknowledged, and a committed entry is on a majority of disks. A crash discards only unconfirmed work; a torn trailing record is truncated on restart (see [[Storage-Engine]]). The honest limit is a corruption in the middle of the log, which is outside the model and unrecoverable; such a node should be rebuilt from a peer.

## How many nodes should I run?

An odd number. Three tolerates one failure; five tolerates two. A majority (`N/2 + 1`) must be reachable for the cluster to make progress. See [[Configuration-and-Tuning]].

## Why JSON on disk if it is slow?

So the durable state is readable with `cat` and `jq`, which makes the recovery code reviewable and a corruption obvious. The log record framing is binary (length plus CRC) because that is what makes torn-write detection exact; only the payload inside it is JSON. A production format would swap the payload for a compact binary encoding without touching the framing. See [[Wire-Formats-and-Data-Layout]].

## Can I run it across multiple machines?

Not yet. Every node talks through a `Transport` interface, but the only implementation is in-memory. A real gRPC or TCP transport slots in behind the same seam with no consensus change; [[Writing-a-Transport]] documents the contract, and it is the headline item on the [[Roadmap]].

## Does the checker scale?

For the history sizes a test produces, yes: it finishes in milliseconds because it partitions by key and memoises. It is not built for million-operation histories; deciding linearizability is NP-complete in general, and a single key with many overlapping operations is the worst case. Spread the workload across keys. Porcupine is the right tool for very large histories (see [[Comparisons]]). The slow-checker symptom is in [[Troubleshooting]].

## How do I reproduce a violation the nemesis found?

The nemesis is seeded, so rerun with the same seed to reproduce the fault schedule. Raft's own election timers add real-time jitter, so a seed reproduces the schedule rather than a bit-exact run; that is the right granularity for a timing-driven protocol. A fully bit-reproducible recorded run is on the [[Roadmap]]. See [[Fault-Injection-Harness]].

## What is the licence?

MIT. See `LICENSE` in the repo.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

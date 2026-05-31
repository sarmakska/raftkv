# Design Decisions

A real project is defined as much by what it refused to do as by what it shipped. These are the choices in raftkv that went against the obvious option, each with the alternative I considered and rejected and the reason. They are scattered through the other pages in context; this page collects them so the rationale is in one place.

## Read index over a leader lease alone

**The obvious option.** A pure leader lease: trust a wall-clock timer, skip the read index, answer reads locally. Every read is a map lookup, no heartbeat, no waiting.

**Why I rejected it as the sole mechanism.** It ties correctness to bounded clock drift between machines. A paused VM, a long GC pause, or an NTP step can make a leader believe its lease is still valid after a new leader has been elected on the other side of a partition. It would then serve a stale read, and no deterministic test could have caught it, because the bug only appears under clock pathology.

**What I did instead.** The lease (`raft/read.go`) is layered on top of the read index, never a replacement. Correctness rests on the read index and the per-term no-op anchor, which do not involve clocks. The lease only decides whether the read index is honoured immediately or after a reconfirming heartbeat round. Speed from the lease, correctness from the read index. Full mechanism on [[Read-Index-and-Leases]].

## A single mutex per node over a channel-per-actor design

**The obvious option.** The idiomatic Go approach: model each node as a goroutine that owns its state and communicates with the rest of the system over channels, no shared memory.

**Why I rejected it.** I tried it. The code drifted away from the Raft paper, because the paper's invariants are stated over shared state ("if `commitIndex > lastApplied`, apply ..."), and re-expressing them as a message-ordering protocol added a second hard problem on top of the one I was trying to get right. Reviewing it meant reasoning about channel interleavings as well as Raft.

**What I did instead.** One `sync.Mutex` per `Node`, with internal helpers suffixed `Locked` so the lock contract is visible at every call site (`raft/raft.go`). The election and heartbeat logic runs from one ticker goroutine; the apply loop runs from another, woken by a `sync.Cond`. The cost is that I cannot fan out RPCs while holding the lock, so each `Send*` runs on its own goroutine that re-acquires the lock only to apply the reply. The payoff is that the core reads next to the paper's pseudocode. See [[Architecture]].

## A custom append-only log over an embedded database

**The obvious option.** Persist the log and state to SQLite or bbolt. Both give durability, atomicity and crash recovery for free.

**Why I rejected it.** Torn-write recovery from a crash mid-append is part of what I set out to demonstrate. Burying it inside a database would have hidden the one mechanism a reviewer most wants to see, and it would have added a dependency to a project whose selling point is that it is pure standard library.

**What I did instead.** A length-prefixed, CRC-checked append-only file (`raft/storage.go`), with state and snapshots as atomically-renamed JSON. On open the log is replayed and a torn trailing record is truncated. `TestTornTrailingRecordDiscarded` simulates a crash mid-write and asserts the torn record is dropped while the good entries survive. The honest cost is that the format is not tuned for throughput and a mid-log corruption is unrecoverable; both are stated plainly on [[Storage-Engine]].

## A custom Wing and Gong checker over Porcupine or Knossos

**The obvious option.** Pull in an existing linearizability checker. Porcupine (Go) and Knossos (Clojure) are excellent and battle-tested.

**Why I rejected it.** The proof is the product. If the checker is someone else's code, the project demonstrates that I can wire up a library, not that I understand the property. And a million-line history checker is overkill for the history sizes a test here produces.

**What I did instead.** The classic Wing and Gong backtracking search, written here (`linz/checker.go`), partitioned per key with memoisation on (linearized set, model state). It is small enough to read in one sitting and fast enough for these histories. The trade-off is scale: Porcupine is the right answer for million-operation histories, which is explicitly not what this is. See [[Linearizability-Checker]] and [[Comparisons]].

## Pre-vote on by default over plain elections

**The obvious option.** Plain Raft elections: a node that times out increments its term and solicits votes.

**Why I rejected leaving it out.** A node isolated by a partition keeps incrementing its term as it fails to win elections. When it rejoins, its term can be far ahead of the cluster's, and a plain `RequestVote` would force the healthy leader to step down and trigger a needless election, even though the rejoining node has a stale log and could never lead. This is the classic disruptive-rejoin bug.

**What I did instead.** A pre-vote phase (`runPreVoteLocked`, paper section 9.6): before bumping its term, a candidate asks peers "would you vote for me?" without anyone mutating persistent state. A node with a stale log loses the pre-vote and never disrupts the cluster. The cost is one extra round trip per election; the benefit is that a flapping partition cannot churn leadership. See [[Raft-Walkthrough]].

## A no-op entry per term over committing prior-term entries directly

**The obvious option.** Let a new leader commit entries from previous terms as soon as they reach a majority.

**Why I rejected it.** That is the exact bug the Raft paper warns about in section 5.4.2: an entry replicated to a majority under an old term can still be overwritten, so committing it on majority-count alone is unsafe.

**What I did instead.** A new leader appends a `nil`-command no-op for its own term (`becomeLeaderLocked`) and only advances the commit index for entries of the current term (`advanceCommitLocked`). Committing the no-op drags the safe prior-term entries with it. The no-op doubles as the read-index anchor. The cost is one extra entry per term and a small apply-pump special case for `nil` commands; the benefit is the commitment rule is correct. See [[Raft-Walkthrough]] and [[KV-State-Machine]].

## In-memory transport now, real network later

**The obvious option.** Build on gRPC or TCP from day one so the numbers are real.

**Why I deferred it.** The property under test, linearizability under faults, is best exercised by a transport I can attack deterministically. An in-memory `Filter` lets the nemesis partition, drop, delay and reorder with a seed, which a real socket makes far harder to control. Getting the harness and the checker right first was the priority.

**What I did instead.** Every node talks only through the `Transport` interface (`transport/transport.go`), with the in-memory `Network` as the one implementation. A real transport slots in behind the same seam with no consensus change. This is the headline item on the [[Roadmap]]; [[Writing-a-Transport]] documents the contract.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

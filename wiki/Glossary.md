# Glossary

Every term of art used across this wiki, defined once. Where a term maps to a symbol in the code, the file is named.

**AppendEntries.** The RPC a leader uses to replicate log entries to followers; doubles as the heartbeat when it carries no entries. `transport.AppendEntriesArgs`/`Reply`, handled by `HandleAppendEntries`. See [[Raft-Walkthrough]].

**Apply / apply loop.** Delivering committed entries to the state machine in log order. The core's `applyLoop` emits `ApplyMsg` values; the cluster's apply pump turns them into `kv.Store.Apply` calls. See [[KV-State-Machine]].

**Backtracking (fast).** The optimisation where a follower returns a conflict hint (`ConflictIndex`, `ConflictTerm`) so the leader jumps `nextIndex` to the right place in one round trip instead of decrementing per entry. `backtrackLocked`. See [[Wire-Formats-and-Data-Layout]].

**Candidate.** A node soliciting votes to become leader. One of the three `raft.Role` values.

**Commit index.** The highest log index known to be replicated on a majority. Entries up to it are safe to apply. `advanceCommitLocked`.

**Commitment rule.** A leader advances the commit index only for entries from its current term (Raft section 5.4.2), which is why a new leader appends a no-op.

**Filter.** The one-method interface the in-memory network consults for every message: deliver or not, and an optional delay. The fault injector is a `Filter`. `cluster.Filter`. See [[Transport-and-Network]].

**fsync.** Forcing buffered writes to durable storage. `AppendLog` ends with `Sync()`; this is the durability point. See [[Storage-Engine]].

**Follower.** A passive node that replicates the leader's log and votes. The default `raft.Role`.

**Heartbeat.** A periodic empty `AppendEntries` the leader sends to assert leadership and renew its lease. Interval is `HeartbeatInterval`.

**History.** A recorded sequence of operation invoke/return events that the checker judges. `linz.History`. See [[Linearizability-Checker]].

**InstallSnapshot.** The RPC that ships a snapshot to a follower too far behind to be caught up with entries. `HandleInstallSnapshot`. See [[Snapshots-and-Compaction]].

**Injector.** The fault `Filter` that partitions, drops, delays and reorders messages. `fault.Injector`. See [[Fault-Injection-Harness]].

**Leader.** The single node that accepts writes and drives replication in a term. One of the three `raft.Role` values.

**Leader lease.** A wall-clock window, renewed on a majority heartbeat ack, during which the leader can answer reads without a fresh heartbeat round. Length equals `ElectionTimeoutMin`. An optimisation on top of the read index, never a substitute. `leaderLeaseUntil`. See [[Read-Index-and-Leases]].

**Linearizability.** The consistency model raftkv provides: every operation appears to take effect instantaneously at one point inside its invoke/return window, consistent with real-time order, legal for a single-copy register. See [[Linearizability-Checker]].

**Majority.** `N/2 + 1` nodes. A cluster makes progress only while a majority is mutually reachable.

**Memoisation.** Caching the checker's verdict for a (linearized set, model state) pair so the search never re-explores a frontier. `memoKey`.

**Nemesis.** The scheduler that applies random faults to a running cluster on a timer, then heals. `fault.Nemesis`. See [[Fault-Injection-Harness]].

**No-op entry.** A `nil`-command log entry a new leader appends to commit prior-term entries safely and to anchor the read index. `appendCommandLocked(nil)`. See [[Design-Decisions]].

**nextIndex / matchIndex.** Leader bookkeeping: the next index to send each follower and the highest index known replicated to it. Drive replication and commitment.

**Partition.** A network split where two groups of nodes cannot exchange messages. Modelled by the injector's `Partition`/`Isolate`.

**Pre-vote.** A probe round before a real election in which a candidate asks "would you vote for me?" without mutating persistent state, preventing a partitioned node from disrupting a healthy cluster on rejoin. `runPreVoteLocked`. See [[Raft-Walkthrough]].

**Read index.** The commit index a leader records at the start of a linearizable read; the read waits until the state machine has applied it. `ReadIndex`. See [[Read-Index-and-Leases]].

**Register.** A single-value cell. Each key in the kv store is an independent register, which is also the checker's unit of analysis.

**RequestVote.** The election RPC. `transport.RequestVoteArgs`/`Reply`, handled by `HandleRequestVote`.

**Role.** A node's state: `Follower`, `Candidate` or `Leader`. `raft.Role`.

**Snapshot.** A serialisation of the state machine at a log position, replacing the prefix it covers. `raft.Snapshot`, captured via `SetSnapshotter`. See [[Snapshots-and-Compaction]].

**SnapshotThreshold.** Applied entries past the last snapshot before the node compacts. Zero disables it. See [[Configuration-and-Tuning]].

**State machine.** The deterministic application of committed commands; here the `kv.Store` register map. See [[KV-State-Machine]].

**Storage.** The durability contract the core relies on. `raft.Storage`, implemented by `FileStorage`. See [[Storage-Engine]].

**Term.** A monotonically increasing election epoch. At most one leader per term. Persisted as `currentTerm`.

**Torn write.** A partially written record left by a crash mid-append, detected by a short read or bad CRC on replay and truncated away. See [[Storage-Engine]].

**Transport.** The interface a node uses to reach peers, the seam that decouples consensus from the wire. `transport.Transport`. See [[Transport-and-Network]] and [[Writing-a-Transport]].

**Wing and Gong search.** The classic backtracking linearizability algorithm the checker implements: repeatedly pick a minimal operation, tentatively linearize it, check the model, recurse, backtrack on failure. `checkRegister`. See [[Linearizability-Checker]].

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

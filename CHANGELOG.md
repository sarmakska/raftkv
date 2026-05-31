# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Raft consensus core: leader election with randomised timeouts and a pre-vote phase, log replication with the fast-backtracking optimisation, term and commit-index commitment rules, and the responder side of RequestVote, AppendEntries and InstallSnapshot.
- Crash-safe persistence: an append-only, CRC-checked log that discards torn trailing records on recovery, plus atomic state and snapshot files.
- Log compaction through snapshots, including InstallSnapshot for followers that have fallen behind the compacted prefix.
- Replicated key-value state machine supporting Get, Put and Delete, with snapshot and restore.
- Linearizable reads through the read-index path, combining a leader lease with a read-index wait.
- In-process multi-node cluster harness, an in-memory network with a fault seam, and a leader-aware client that retries against the leader and records operation histories.
- Fault-injection harness: an injector for network partitions, message drop, delay and reorder, and a nemesis that schedules faults against a running cluster, including node crash and restart.
- Linearizability checker using the Wing and Gong backtracking search, partitioned per key, with memoisation.
- `raftkvd` demo binary that runs a cluster under chaos and reports the linearizability verdict.
- Test suite covering election after partition, log convergence after heal, leadership change, crash recovery from disk, snapshot install, torn-write recovery, and the checker accepting valid and rejecting corrupted histories.
- Benchmarks for write throughput and linearizable-read latency.
- Continuous integration that builds, vets and tests on push and pull request.

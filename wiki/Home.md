# raftkv

raftkv is a from-scratch Raft consensus key-value store in Go, paired with a fault-injection harness that proves the cluster stays linearizable while the network is partitioned, messages are dropped and delayed, and nodes crash and restart. I built it to demonstrate that I can implement a distributed consensus protocol correctly and, just as importantly, that I can prove it correct rather than assert it.

This wiki is the documentation an adopter or a reviewer would actually rely on. It explains how the system is put together, walks through the Raft protocol as it is implemented here, and documents the two pieces I am most proud of: the fault-injection harness and the linearizability checker.

## Pages

- [[Architecture]] How the packages fit together and why the transport seam matters.
- [[Raft-Walkthrough]] Leader election, log replication, persistence, and snapshots, tied to the code.
- [[Fault-Injection-Harness]] The Jepsen-style nemesis: partitions, drops, delays, crashes.
- [[Linearizability-Checker]] The Wing and Gong search that judges recorded histories.
- [[Client-API]] How to drive the cluster from Go, with examples.
- [[Troubleshooting]] Symptoms, causes and fixes for the issues you are most likely to hit.

## What raftkv guarantees

- Linearizable writes and reads while a majority of nodes are reachable.
- Committed entries survive crashes, because term, vote and log are persisted to disk before being acknowledged.
- A node that has fallen behind the compacted log prefix is caught up with a snapshot.
- The cluster never returns a stale or invented value, which the linearizability checker verifies on every chaos run.

## What raftkv is not

It is not a production datastore yet. The transport is in-process, membership is static, and the on-disk format favours clarity over speed. Those are deliberate choices for a correctness-first, teaching-grade implementation. See "When to use this, and when not to" in the README.

## Quickstart

```bash
git clone https://github.com/sarmakska/raftkv && cd raftkv
go build ./...
go test ./...
go run ./cmd/raftkvd -nodes 5 -ops 200
```

The demo boots a five-node cluster, runs the nemesis, drives a workload, and prints whether the recorded history was linearizable.

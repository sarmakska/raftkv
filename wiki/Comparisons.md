# Comparisons

raftkv sits next to several well-known projects, and the honest thing is to say where it overlaps with each and where it deliberately does not. This is not a feature scoreboard; it is a map of intent. raftkv is a correctness-first teaching-grade implementation whose centre of gravity is the proof, not the datastore (see [[Design-Decisions]]).

## Against production Raft stores: etcd and hashicorp/raft

etcd (with its own Raft library) and `hashicorp/raft` are production consensus libraries that run real clusters serving real traffic.

| Dimension | raftkv | etcd / hashicorp/raft |
| --- | --- | --- |
| Transport | In-process only, behind a `Transport` seam | Real gRPC / TCP, battle-tested |
| Membership | Static, fixed at startup | Dynamic reconfiguration (joint or single-server) |
| On-disk format | JSON state and snapshots, hand-rolled CRC log | Tuned binary WAL, mmap'd backends |
| Throughput | Latency-bound, single-client benchmark | Batching, pipelining, thousands of ops/sec |
| Built-in correctness proof | A linearizability checker that runs on every chaos history | External (etcd uses functional tests and external Jepsen analyses) |

The point is not that raftkv competes with these; it cannot and does not try to. If you need a consensus store in production, use one of them. raftkv's distinguishing feature is the opposite of theirs: it bundles the proof. The checker (`linz/`) and the nemesis (`fault/`) ship in the same repo as the Raft core and run on every test, so the consistency claim is demonstrated, not asserted. A production library leaves that to external tooling. See [[Linearizability-Checker]] and [[Fault-Injection-Harness]].

## Against linearizability checkers: Porcupine and Knossos

Porcupine (Go) and Knossos (Clojure, part of Jepsen) are the serious linearizability checkers.

raftkv's `linz` checker is the classic Wing and Gong backtracking search, partitioned per key with memoisation on (linearized set, model state). It is intentionally small enough to read in one sitting.

- **Where they win.** Porcupine and Knossos handle million-operation histories, richer data models, and have years of optimisation behind them. If you are checking large histories from a real system, use Porcupine.
- **Where raftkv's choice is right for the project.** Pulling in Porcupine would have meant the proof was outsourced, and the whole point is that I implemented the property myself. For the history sizes a test here produces (a few hundred operations, spread across keys), the in-house checker finishes in milliseconds. The trade-off is explicit and lives on [[Design-Decisions]].

The model is also narrower on purpose: raftkv checks each key as an independent single-copy register. It does not reason about multi-object or transactional histories, which Knossos's model can. That is a non-goal, stated on the [[Roadmap]].

## Against Jepsen itself

Jepsen is the gold standard for distributed-systems testing: it runs a real cluster, partitions a real network with `iptables`, drives a real workload, and checks the result with Knossos.

raftkv's `fault` package is a Jepsen-style harness in miniature. The structural similarity is real: a nemesis schedules faults against a running cluster while clients drive a workload, then a checker judges the recorded history.

The difference is the layer of injection. Jepsen attacks a real network and real processes; raftkv attacks an in-memory `Filter` seam (see [[Transport-and-Network]]). That makes raftkv's faults deterministic and seedable but means it is testing the consensus logic, not the operating system, the network stack, or the deployment. raftkv catches algorithm bugs; Jepsen catches those plus everything below them. When a real transport lands, a genuine Jepsen run becomes possible and is a natural next step.

## What raftkv is, in one line

A from-scratch Raft you can read against the paper, sitting under a fault harness and a linearizability checker you can also read, so that every consistency claim is backed by a history a checker has accepted. The production stores are for running; the checkers and Jepsen are for proving; raftkv is for understanding and demonstrating both halves in one small, dependency-free codebase. The [[FAQ]] answers the "should I use this in production?" question directly (no), and the [[Roadmap]] says what would have to change.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

# Roadmap

raftkv is a correctness-first implementation, so the roadmap is shaped by that: I will add things that deepen the proof or remove a deliberate limitation, and I will not chase features that turn it into a different kind of project.

## Limitations today

These are real and known, not oversights.

- **In-process transport.** Nodes talk through a `Transport` interface, but the only implementation is in-memory (`cluster.Endpoint`). There is no real network, so the latency numbers in the README reflect goroutine scheduling, not a wire.
- **Static membership.** The node set is fixed at startup. No joint consensus, no add or remove node at runtime.
- **Clarity-first on-disk format.** State and snapshots are JSON; the log is a hand-rolled length-prefixed binary file. Easy to read and to recover, not tuned for throughput.
- **No client sessions.** There are no idempotency keys, so a client that retries an unconfirmed write can apply it twice. The checker models this honestly (an unconfirmed write may or may not have taken effect), which keeps it sound, but the store is not exactly-once.
- **Single-register model in the checker.** Each key is checked as an independent register. Multi-key transactions are out of scope, so the checker does not reason about cross-key invariants.

## What I will add

- **A real network transport** behind the existing `Transport` seam, most likely gRPC, so the harness can attack genuine sockets. This is the change that makes the latency numbers meaningful.
- **A checked-in chaos recording.** A seed plus a recorded history committed to the repo, so that if a violation ever surfaced it would be replayable bit for bit rather than only reproducible to the granularity of the fault schedule.
- **Membership changes** via single-server joint consensus, with a test that adds and removes a node under load and confirms the history stays linearizable.

## What I will not add

- A SQL layer or a query language. This is a key-value register store on purpose.
- A wire-compatible clone of an existing system (etcd, Consul). The point is to show the protocol, not to drop into someone else's ecosystem.
- A throughput leaderboard. I will report honest numbers from this machine and explain what dominates them; I will not optimise the format for a benchmark at the cost of the readability that makes the recovery code reviewable.

## How decisions get made here

If a change makes the consensus core harder to read against the paper, or weakens what the checker can prove, it has to earn its place. The bias is toward keeping the system small enough that a reviewer can hold all of it in their head and toward keeping every consistency claim backed by a recorded history a checker has accepted.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

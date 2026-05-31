# Troubleshooting

This page collects the issues you are most likely to hit, with the cause and the fix. The symptoms are described in terms of the API and the tests.

## The cluster never elects a leader

**Symptom.** `Cluster.Leader()` stays `-1`; the demo prints "no leader elected within timeout".

**Causes and fixes.**

- A majority of nodes is not reachable. Raft needs `N/2 + 1` nodes that can talk to each other. Check that any partition you set with the injector still leaves a majority connected, and that you have not crashed too many nodes.
- The election timeout is too small relative to the heartbeat, so candidates keep timing out before a vote round completes. Keep `ElectionTimeoutMin` at three to five times `HeartbeatInterval`.
- The pre-vote round is failing because the would-be candidate's log is stale. This is correct behaviour when a node was partitioned away; once it catches up via `AppendEntries` it can win a pre-vote again.

## Frequent "no leader" errors from the client under load

**Symptom.** `Put` or `Get` returns `ErrNoLeader` intermittently even though the cluster is healthy.

**Cause.** Spurious elections. If the election timeout is too tight, ordinary scheduling delays look like a dead leader and trigger a re-election, which briefly leaves the cluster without a leader.

**Fix.** Widen the election timeout window and make sure `ElectionTimeoutMax` is comfortably above `ElectionTimeoutMin` so nodes do not time out in lockstep. Raise the client timeout so it rides out a single election.

## A restarted node has lost data

**Symptom.** After `Crash` then `Restart`, a read returns `found == false` for a key you wrote.

**Causes and fixes.**

- You read too soon. After a restart the node must elect a leader and commit a fresh no-op before old entries are reapplied to the state machine. Wait for `Cluster.Leader()` to be non-negative and give the apply loop a moment, or simply read through `Client.Get`, which waits for the state machine to reach the read index.
- The write was never committed. A `Put` that returned an error did not commit. Only writes that returned `nil` are guaranteed durable.
- The on-disk directory was not preserved. `Crash` leaves `node-<id>/` intact, but if you wiped the directory or pointed `Restart` at a fresh path, the node starts empty by design.

## The linearizability checker reports a violation

**Symptom.** `linz.Check` returns `Linearizable == false` with a key and reason.

**What it means.** The recorded history admits no legal ordering. If this happens on the real cluster it is a genuine consistency bug, which is exactly what the checker exists to catch. Reproduce it: the nemesis is seeded, so rerun with the same seed and narrow the workload until you have the smallest history that still fails, then inspect `History.Events()` for the offending key.

**Common non-bug causes when wiring up your own workload.**

- Recording the wrong value on return. A `Put` must record the value it wrote and a `Get` the value it observed. Recording the intended rather than the observed value will produce phantom violations.
- Calling `Return` with `ok == true` for an operation that actually failed or timed out. An operation whose result you did not confirm must be recorded with `ok == false` so the checker treats it as possibly-applied.

## The checker is slow on a large history

**Symptom.** `linz.Check` takes a long time.

**Cause.** Linearizability checking is NP-complete in general, and a history with many overlapping operations on a single key has a large search space.

**Fixes.**

- Spread the workload across more keys. The checker partitions by key, so independent keys are checked separately and cheaply.
- Reduce concurrency on any single key, or add small gaps between operations on the same key so the real-time bound prunes the search.

## A test is flaky

**Symptom.** A cluster test passes most of the time but occasionally fails on timing.

**Cause.** The integration tests drive real timers, so an unusually slow scheduler can push an election past a test deadline.

**Fix.** The default test timeouts leave generous headroom, but on a heavily loaded machine you can widen the `waitLeader` deadlines or the `fastOpts` timeouts. The consensus logic itself does not depend on the absolute timer values, only on the election window being larger than the heartbeat.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

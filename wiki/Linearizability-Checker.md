# Linearizability Checker

The checker in `linz/` is the evidence that the consensus core delivers the guarantee it claims. Recording a history is easy; deciding whether that history is linearizable is the hard, interesting part, and it is done here for real.

## What linearizability means here

Each key is an independent register. An operation is two events: an *invoke* at the moment the client sent the request and a *return* at the moment it observed the result. The window between them is when the operation could have taken effect.

A history is linearizable if there exists a total order of the operations such that:

1. each operation appears to take effect instantaneously at a single point inside its invoke-return window, and
2. that order is consistent with the real-time order: if operation A returned before operation B was invoked, A comes before B, and
3. the order is legal for a single-copy register: every read returns the value of the most recent preceding write (or the empty value before any write).

## Recording a history

`History` is a thread-safe recorder. Client goroutines call `Invoke` when they send a request and `Return` when they get a result:

```go
h := linz.NewHistory()
id := h.Invoke(linz.Op{Kind: linz.OpPut, Key: "k", Value: "1"})
// ... perform the operation ...
h.Return(id, "1", true)
```

The `cluster.Client` does this automatically when constructed with a non-nil history. A `Return` with `ok == false` marks an operation whose result the client never confirmed; the checker treats it as one that may or may not have taken effect, with an unbounded return time. This is important under faults, where a client can time out without knowing whether its write committed.

## The algorithm

`Check` partitions the history by key and checks each register independently; the whole history is linearizable if and only if every register is. For a single register `checkRegister` runs the Wing and Gong backtracking search:

1. Maintain the model state (the current register value, starting at the empty sentinel `Nil`).
2. Among the operations not yet linearized, find the earliest return time. Any operation whose invoke time is at or before that bound is a legal next choice, because its real-time window overlaps the frontier.
3. For each candidate, tentatively apply it: a write sets the state, a confirmed read must match the current state or it is rejected.
4. Recurse on the remaining operations. If the recursion succeeds, the history is linearizable. If not, backtrack and try the next candidate.

```go
search = func(state string, remaining int) bool {
    if remaining == 0 {
        return true
    }
    // ... compute minRet over not-yet-linearized ops ...
    for each not-yet-linearized op i {
        if ops[i].invoke > minRet {
            continue // would violate real-time order
        }
        if read && observed && ops[i].value != state {
            continue // illegal read
        }
        mark i done
        if search(newState, remaining-1) {
            return true
        }
        unmark i
    }
    return false
}
```

### Why it terminates quickly enough

The search is exponential in the worst case, which is inherent: deciding linearizability is NP-complete in general. Two things keep it practical for the histories a test produces. First, the real-time bound prunes hard: once operations are separated in time the branching factor collapses to one. Second, the search memoises on the pair (set of already-linearized operations, model state), so it never explores the same frontier twice. For the chaos workloads here, partitioned per key, the checker finishes in milliseconds.

## What it catches

The test suite pins the behaviour:

- `TestStaleReadIsRejected` a read that returns a value overwritten before it was invoked.
- `TestReadOfUnwrittenValueRejected` a read of a value that was never written.
- `TestCorruptedHistoryRejected` a valid history with one read corrupted to a stale value.
- `TestConcurrentValidHistory` overlapping writes where a later read sees either one, which is legal.
- `TestUnconfirmedWriteIsFlexible` an unconfirmed write that may or may not have applied, consistent with a later read seeing the old or the new value.

When `Check` returns a violation it names the key (`Result.Key`) and the reason, which is enough to start debugging from the recorded history.

## A violation, end to end

`linz/example_test.go` records a register history by hand, spaced out in real time so no operations overlap: a write of `1` completes, a write of `2` completes, then a read returns `1`. No single-copy register can do that. The expected output is pinned in the example, so `go test ./linz/` fails if the checker ever stops catching it.

```go
h := linz.NewHistory()
id := h.Invoke(linz.Op{Kind: linz.OpPut, Key: "x", Value: "1"}); h.Return(id, "1", true)
id = h.Invoke(linz.Op{Kind: linz.OpPut, Key: "x", Value: "2"}); h.Return(id, "2", true)
id = h.Invoke(linz.Op{Kind: linz.OpGet, Key: "x"});            h.Return(id, "1", true) // stale
fmt.Println(linz.Check(h))
```

```
linearizable=false key="x"
no sequential ordering consistent with real-time order
```

This is the same `Check` the chaos suite runs over every history. On the real cluster a `linearizable=false` line is a genuine consistency bug, which is exactly the signal the checker exists to produce.

## Failure modes worth knowing

- A history that times the operations too closely together makes their windows overlap, which gives the search more freedom and can mask a real violation. Recording with the wall clock and a busy machine is the usual cause. The example above sleeps between operations on purpose so the windows are clearly separated.
- A single key with many overlapping operations is the worst case, and the search can blow up. The fix is to spread the workload across keys, since the checker partitions per key. See [[Troubleshooting]].
- Recording the intended value rather than the observed value on a read produces phantom violations that are bugs in the harness, not the cluster. The `cluster.Client` records the observed value, which is the correct contract.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

package linz

import (
	"sort"
)

// Result is the outcome of a linearizability check. When Linearizable is false,
// Key names the register whose history could not be linearized, which is enough
// to start debugging.
type Result struct {
	Linearizable bool
	Key          string
	Reason       string
}

// Check decides whether the whole history is linearizable. Operations on
// distinct keys are independent (each key is its own register), so the history
// is partitioned by key and each partition is checked separately. The history
// is linearizable if and only if every partition is.
func Check(h *History) Result {
	byKey := map[string][]Event{}
	for _, e := range h.Events() {
		byKey[e.Op.Key] = append(byKey[e.Op.Key], e)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkRegister(byKey[k]) {
			return Result{Linearizable: false, Key: k, Reason: "no sequential ordering consistent with real-time order"}
		}
	}
	return Result{Linearizable: true}
}

// regOp is an operation reduced to the register model: a write that sets the
// register to a value (delete sets it to Nil), or a read that observed a value.
type regOp struct {
	id        int
	isWrite   bool
	value     string // for writes, the value written; for reads, the value observed
	observed  bool   // a read whose result the client confirmed
	invoke    int64
	ret       int64
	completed bool
}

// checkRegister runs the Wing and Gong linearizability search for a single
// register. It is the classic backtracking algorithm: repeatedly pick a
// minimal operation (one whose invocation precedes the return of every other
// pending operation), tentatively linearize it, check the model, recurse, and
// backtrack if the recursion fails. Memoisation on the set of already-chosen
// operations together with the model state keeps the worst case manageable for
// the history sizes a test produces.
func checkRegister(events []Event) bool {
	ops := make([]regOp, 0, len(events))
	for _, e := range events {
		o := regOp{
			id:        e.ID,
			invoke:    e.Invoke.UnixNano(),
			completed: e.OK && !e.Return.IsZero(),
		}
		switch e.Op.Kind {
		case OpPut:
			o.isWrite = true
			o.value = e.Op.Value
		case OpDelete:
			o.isWrite = true
			o.value = Nil
		case OpGet:
			o.value = e.Output
			o.observed = e.OK
		}
		if o.completed {
			o.ret = e.Return.UnixNano()
		} else {
			// An operation the client never confirmed may take effect at any
			// time, so its return is unbounded.
			o.ret = int64(1) << 62
		}
		ops = append(ops, o)
	}

	n := len(ops)
	if n == 0 {
		return true
	}
	done := make([]bool, n)
	// The register starts empty (Nil) because every key is absent before its
	// first write.
	memo := map[string]bool{}
	var search func(state string, remaining int) bool
	search = func(state string, remaining int) bool {
		if remaining == 0 {
			return true
		}
		key := memoKey(done, state)
		if v, ok := memo[key]; ok {
			return v
		}
		// Find the earliest return time among not-yet-linearized ops; any op
		// whose invoke is at or before that bound is a valid next choice
		// (its real-time window overlaps the frontier).
		const maxInt64 = int64(^uint64(0) >> 1)
		minRet := maxInt64
		for i := 0; i < n; i++ {
			if !done[i] && ops[i].ret < minRet {
				minRet = ops[i].ret
			}
		}
		for i := 0; i < n; i++ {
			if done[i] {
				continue
			}
			if ops[i].invoke > minRet {
				continue // would violate real-time order
			}
			next := state
			ok := true
			if ops[i].isWrite {
				next = ops[i].value
			} else if ops[i].observed {
				// A confirmed read must match the current register value.
				ok = ops[i].value == state
			}
			if !ok {
				continue
			}
			done[i] = true
			if search(next, remaining-1) {
				done[i] = false
				return true
			}
			done[i] = false
		}
		memo[key] = false
		return false
	}
	return search(Nil, n)
}

func memoKey(done []bool, state string) string {
	b := make([]byte, 0, len(done)+1+len(state))
	for _, d := range done {
		if d {
			b = append(b, '1')
		} else {
			b = append(b, '0')
		}
	}
	b = append(b, '|')
	b = append(b, state...)
	return string(b)
}

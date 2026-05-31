package linz

import (
	"testing"
	"time"
)

// at builds an event at fixed nanosecond offsets so the tests describe precise
// real-time orderings.
func at(id int, kind OpKind, key, val, out string, ok bool, inv, ret int64) Event {
	return Event{
		ID:     id,
		Op:     Op{Kind: kind, Key: key, Value: val},
		Output: out,
		OK:     ok,
		Invoke: time.Unix(0, inv),
		Return: time.Unix(0, ret),
	}
}

func histFrom(events []Event) *History {
	h := NewHistory()
	for _, e := range events {
		h.events[e.ID] = &e
		h.order = append(h.order, e.ID)
	}
	return h
}

func TestSequentialValidHistory(t *testing.T) {
	// put x=1, get x -> 1, put x=2, get x -> 2, all non-overlapping.
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 20),
		at(1, OpGet, "x", "", "1", true, 30, 40),
		at(2, OpPut, "x", "2", "2", true, 50, 60),
		at(3, OpGet, "x", "", "2", true, 70, 80),
	})
	if r := Check(h); !r.Linearizable {
		t.Fatalf("expected linearizable, got violation on %q: %s", r.Key, r.Reason)
	}
}

func TestConcurrentValidHistory(t *testing.T) {
	// Two concurrent writes overlap; a later read sees one of them. There
	// exists an ordering (put=2 then put=1 ... no, read saw 1) that works:
	// linearize put x=1 last before the read.
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 50),
		at(1, OpPut, "x", "2", "2", true, 20, 60),
		at(2, OpGet, "x", "", "1", true, 70, 80),
	})
	if r := Check(h); !r.Linearizable {
		t.Fatalf("expected linearizable concurrent history, got violation: %s", r.Reason)
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	// put x=1 completes, then put x=2 completes, then a read returns 1. This is
	// a classic stale read and must be flagged: no ordering respecting the
	// real-time order can produce it.
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 20),
		at(1, OpPut, "x", "2", "2", true, 30, 40),
		at(2, OpGet, "x", "", "1", true, 50, 60),
	})
	if r := Check(h); r.Linearizable {
		t.Fatalf("expected violation for stale read, but checker accepted it")
	}
}

func TestReadOfUnwrittenValueRejected(t *testing.T) {
	// A read returns a value that was never written.
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 20),
		at(1, OpGet, "x", "", "99", true, 30, 40),
	})
	if r := Check(h); r.Linearizable {
		t.Fatalf("expected violation for phantom read")
	}
}

func TestDeleteThenGetNil(t *testing.T) {
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 20),
		at(1, OpDelete, "x", "", "", true, 30, 40),
		at(2, OpGet, "x", "", Nil, true, 50, 60),
	})
	if r := Check(h); !r.Linearizable {
		t.Fatalf("expected linearizable delete-then-nil, got %s", r.Reason)
	}
}

func TestUnconfirmedWriteIsFlexible(t *testing.T) {
	// A write whose result the client never confirmed (OK=false) may or may
	// not have taken effect. A later read sees its value, which is consistent.
	h := histFrom([]Event{
		at(0, OpPut, "x", "7", "7", false, 10, 0),
		at(1, OpGet, "x", "", "7", true, 30, 40),
	})
	if r := Check(h); !r.Linearizable {
		t.Fatalf("expected linearizable with unconfirmed write, got %s", r.Reason)
	}
	// And a read that sees Nil is also consistent because the write may not
	// have applied.
	h2 := histFrom([]Event{
		at(0, OpPut, "x", "7", "7", false, 10, 0),
		at(1, OpGet, "x", "", Nil, true, 30, 40),
	})
	if r := Check(h2); !r.Linearizable {
		t.Fatalf("expected linearizable when unconfirmed write did not apply, got %s", r.Reason)
	}
}

func TestPerKeyIndependence(t *testing.T) {
	// A violation on key y must be reported even when key x is fine.
	h := histFrom([]Event{
		at(0, OpPut, "x", "1", "1", true, 10, 20),
		at(1, OpGet, "x", "", "1", true, 30, 40),
		at(2, OpPut, "y", "1", "1", true, 10, 20),
		at(3, OpPut, "y", "2", "2", true, 30, 40),
		at(4, OpGet, "y", "", "1", true, 50, 60),
	})
	r := Check(h)
	if r.Linearizable {
		t.Fatalf("expected violation on key y")
	}
	if r.Key != "y" {
		t.Fatalf("expected violation reported on key y, got %q", r.Key)
	}
}

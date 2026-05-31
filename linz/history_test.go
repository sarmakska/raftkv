package linz

import (
	"testing"
	"time"
)

// TestRecordedHistoryAccepted records a valid sequential history through the
// live Invoke/Return API and confirms the checker accepts it.
func TestRecordedHistoryAccepted(t *testing.T) {
	h := NewHistory()
	id := h.Invoke(Op{Kind: OpPut, Key: "k", Value: "a"})
	h.Return(id, "a", true)
	id = h.Invoke(Op{Kind: OpGet, Key: "k"})
	h.Return(id, "a", true)
	id = h.Invoke(Op{Kind: OpPut, Key: "k", Value: "b"})
	h.Return(id, "b", true)
	id = h.Invoke(Op{Kind: OpGet, Key: "k"})
	h.Return(id, "b", true)

	if r := Check(h); !r.Linearizable {
		t.Fatalf("expected recorded history to be linearizable, got %s", r.Reason)
	}
}

// TestCorruptedHistoryRejected takes a valid history and corrupts a single
// observed read so it returns a value that was overwritten, then confirms the
// checker rejects it. This is the deliberate-violation case from the spec.
func TestCorruptedHistoryRejected(t *testing.T) {
	h := NewHistory()
	id := h.Invoke(Op{Kind: OpPut, Key: "k", Value: "a"})
	h.Return(id, "a", true)
	time.Sleep(2 * time.Millisecond)
	id = h.Invoke(Op{Kind: OpPut, Key: "k", Value: "b"})
	h.Return(id, "b", true)
	// A short gap so the read is unambiguously after put=b returned; without
	// real-time separation the stale read could be linearized concurrently
	// and would be legal.
	time.Sleep(2 * time.Millisecond)
	// Corrupt: this read claims to observe the stale value "a" after "b"
	// committed, which no linearizable register could produce.
	id = h.Invoke(Op{Kind: OpGet, Key: "k"})
	h.Return(id, "a", true)

	if r := Check(h); r.Linearizable {
		t.Fatal("expected corrupted history to be rejected")
	}
}

func TestEmptyHistoryIsLinearizable(t *testing.T) {
	if r := Check(NewHistory()); !r.Linearizable {
		t.Fatal("empty history should be linearizable")
	}
}

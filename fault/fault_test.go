package fault

import (
	"testing"
	"time"

	"github.com/sarmakska/raftkv/cluster"
)

// msg is a small helper for building the cluster.Message the Injector judges.
func msg(from, to int) cluster.Message {
	return cluster.Message{From: from, To: to, Kind: cluster.KindAppendEntries}
}

// A fresh Injector has no faults: every message is delivered with no delay,
// including a node talking to itself.
func TestInjectorNoFaults(t *testing.T) {
	in := NewInjector(1)
	for _, m := range []cluster.Message{msg(0, 1), msg(1, 2), msg(2, 0), msg(0, 0)} {
		deliver, delay := in.Allow(m)
		if !deliver {
			t.Fatalf("message %v dropped by an injector with no faults", m)
		}
		if delay != 0 {
			t.Fatalf("message %v delayed by %v with no faults", m, delay)
		}
	}
}

// Partition is a hard cut: messages that cross groups never deliver, messages
// within a group always do, and it holds symmetrically in both directions.
func TestInjectorPartition(t *testing.T) {
	in := NewInjector(1)
	in.Partition([]int{0, 1}, []int{2})

	within := []cluster.Message{msg(0, 1), msg(1, 0)}
	for _, m := range within {
		if deliver, _ := in.Allow(m); !deliver {
			t.Fatalf("message %v within a group was dropped", m)
		}
	}

	crossing := []cluster.Message{msg(0, 2), msg(2, 0), msg(1, 2), msg(2, 1)}
	for _, m := range crossing {
		if deliver, _ := in.Allow(m); deliver {
			t.Fatalf("message %v crossing a partition was delivered", m)
		}
	}
}

// A node not named in any partition group stays fully connected to everyone.
func TestInjectorPartitionUnlistedNodeStaysConnected(t *testing.T) {
	in := NewInjector(1)
	// Nodes 0 and 1 are split; node 2 is unlisted and so reaches both sides.
	in.Partition([]int{0}, []int{1})
	for _, m := range []cluster.Message{msg(2, 0), msg(0, 2), msg(2, 1), msg(1, 2)} {
		if deliver, _ := in.Allow(m); !deliver {
			t.Fatalf("unlisted node message %v was dropped", m)
		}
	}
	// The two listed nodes remain cut from each other.
	if deliver, _ := in.Allow(msg(0, 1)); deliver {
		t.Fatal("listed nodes in different groups should not communicate")
	}
}

// Isolate cuts exactly one node off and leaves the rest fully connected.
func TestInjectorIsolate(t *testing.T) {
	in := NewInjector(1)
	all := []int{0, 1, 2, 3, 4}
	in.Isolate(2, all)

	// The isolated node cannot reach or be reached by anyone.
	for _, other := range []int{0, 1, 3, 4} {
		if deliver, _ := in.Allow(msg(2, other)); deliver {
			t.Fatalf("isolated node reached %d", other)
		}
		if deliver, _ := in.Allow(msg(other, 2)); deliver {
			t.Fatalf("node %d reached the isolated node", other)
		}
	}
	// The remaining majority still talks among itself.
	if deliver, _ := in.Allow(msg(0, 4)); !deliver {
		t.Fatal("the surviving majority lost connectivity")
	}
}

// Heal removes every fault so the cluster is whole again.
func TestInjectorHeal(t *testing.T) {
	in := NewInjector(1)
	in.Partition([]int{0}, []int{1, 2})
	in.SetDropRate(1.0)
	in.SetDelay(10*time.Millisecond, 20*time.Millisecond)

	in.Heal()

	deliver, delay := in.Allow(msg(0, 1))
	if !deliver {
		t.Fatal("Heal left a message undeliverable")
	}
	if delay != 0 {
		t.Fatalf("Heal left a delay of %v", delay)
	}
}

// A drop rate of 1.0 drops everything; 0.0 drops nothing. These bounds are
// deterministic regardless of the RNG seed.
func TestInjectorDropRateBounds(t *testing.T) {
	in := NewInjector(42)
	in.SetDropRate(1.0)
	for i := 0; i < 100; i++ {
		if deliver, _ := in.Allow(msg(0, 1)); deliver {
			t.Fatal("drop rate 1.0 delivered a message")
		}
	}
	in.SetDropRate(0.0)
	for i := 0; i < 100; i++ {
		if deliver, _ := in.Allow(msg(0, 1)); !deliver {
			t.Fatal("drop rate 0.0 dropped a message")
		}
	}
}

// A partial drop rate is honoured statistically and, being seeded, is exactly
// reproducible: two injectors with the same seed drop the same messages.
func TestInjectorDropRateIsSeededAndReproducible(t *testing.T) {
	const n = 2000
	count := func(seed int64) int {
		in := NewInjector(seed)
		in.SetDropRate(0.3)
		dropped := 0
		for i := 0; i < n; i++ {
			if deliver, _ := in.Allow(msg(0, 1)); !deliver {
				dropped++
			}
		}
		return dropped
	}
	a := count(7)
	b := count(7)
	if a != b {
		t.Fatalf("same seed produced different drop counts: %d vs %d", a, b)
	}
	// Roughly 30% should drop; allow a generous band so the test is not flaky.
	if a < n*15/100 || a > n*45/100 {
		t.Fatalf("drop count %d/%d far from the expected ~30%%", a, n)
	}
}

// The delay window is respected: every delivered message is delayed within
// [min, max].
func TestInjectorDelayWindow(t *testing.T) {
	in := NewInjector(3)
	min, max := 5*time.Millisecond, 25*time.Millisecond
	in.SetDelay(min, max)
	for i := 0; i < 500; i++ {
		deliver, delay := in.Allow(msg(0, 1))
		if !deliver {
			t.Fatal("delay should not drop a message")
		}
		if delay < min || delay > max {
			t.Fatalf("delay %v outside the window [%v, %v]", delay, min, max)
		}
	}
}

// A fixed delay (min == max) is applied exactly.
func TestInjectorFixedDelay(t *testing.T) {
	in := NewInjector(3)
	in.SetDelay(8*time.Millisecond, 8*time.Millisecond)
	for i := 0; i < 50; i++ {
		_, delay := in.Allow(msg(0, 1))
		if delay != 8*time.Millisecond {
			t.Fatalf("fixed delay produced %v, want 8ms", delay)
		}
	}
}

// Drop is evaluated before delay: a dropped message carries no delay, and the
// partition cut takes precedence over both.
func TestInjectorCheckOrdering(t *testing.T) {
	in := NewInjector(1)
	in.Partition([]int{0}, []int{1})
	in.SetDropRate(0.0)
	in.SetDelay(10*time.Millisecond, 10*time.Millisecond)
	// Even with a delay configured, a partitioned message is cut with no delay.
	deliver, delay := in.Allow(msg(0, 1))
	if deliver {
		t.Fatal("partitioned message delivered")
	}
	if delay != 0 {
		t.Fatalf("dropped message carried a delay of %v", delay)
	}
}

package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sarmakska/raftkv/cluster"
	"github.com/sarmakska/raftkv/fault"
	"github.com/sarmakska/raftkv/linz"
)

func fastOpts(n int, dir string) cluster.Options {
	o := cluster.DefaultOptions(n, dir)
	o.HeartbeatInterval = 30 * time.Millisecond
	o.ElectionTimeoutMin = 120 * time.Millisecond
	o.ElectionTimeoutMax = 250 * time.Millisecond
	return o
}

func waitLeader(t *testing.T, c *cluster.Cluster, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 {
			return l
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", within)
	return -1
}

func TestElectsLeader(t *testing.T) {
	c, err := cluster.New(fastOpts(3, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	waitLeader(t, c, 3*time.Second)
}

func TestBasicReadWrite(t *testing.T) {
	c, err := cluster.New(fastOpts(3, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	waitLeader(t, c, 3*time.Second)

	cl := cluster.NewClient(c, 3*time.Second, nil)
	if err := cl.Put("k", "hello"); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, ok, err := cl.Get("k")
	if err != nil || !ok || v != "hello" {
		t.Fatalf("get: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := cl.Delete("k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, ok, err = cl.Get("k")
	if err != nil || ok {
		t.Fatalf("expected key gone, ok=%v err=%v", ok, err)
	}
}

func TestElectionAfterPartition(t *testing.T) {
	c, err := cluster.New(fastOpts(5, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	leader := waitLeader(t, c, 3*time.Second)

	// Isolate the current leader. The remaining majority must elect a new one.
	in := fault.NewInjector(1)
	c.Network().SetFilter(in)
	in.Isolate(leader, c.Peers())

	deadline := time.Now().Add(4 * time.Second)
	var newLeader int = -1
	for time.Now().Before(deadline) {
		l := c.Leader()
		if l >= 0 && l != leader {
			newLeader = l
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader < 0 {
		t.Fatal("no new leader elected after isolating the old one")
	}
}

func TestLogConvergesAfterHeal(t *testing.T) {
	c, err := cluster.New(fastOpts(5, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	waitLeader(t, c, 3*time.Second)

	in := fault.NewInjector(2)
	c.Network().SetFilter(in)

	// Partition into a majority {0,1,2} and a minority {3,4}.
	in.Partition([]int{0, 1, 2}, []int{3, 4})
	time.Sleep(400 * time.Millisecond)

	cl := cluster.NewClient(c, 3*time.Second, nil)
	for i := 0; i < 10; i++ {
		if err := cl.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)); err != nil {
			t.Fatalf("put under partition: %v", err)
		}
	}

	// Heal and let the minority catch up.
	in.Heal()
	time.Sleep(1500 * time.Millisecond)

	// Every live node's state machine must hold all writes.
	for _, id := range c.Peers() {
		store := c.Store(id)
		if store == nil {
			continue
		}
		for i := 0; i < 10; i++ {
			if v, ok := store.Get(fmt.Sprintf("key%d", i)); !ok || v != fmt.Sprintf("val%d", i) {
				t.Fatalf("node %d missing key%d: v=%q ok=%v", id, i, v, ok)
			}
		}
	}
}

func TestLeadershipChange(t *testing.T) {
	c, err := cluster.New(fastOpts(3, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	leader := waitLeader(t, c, 3*time.Second)

	cl := cluster.NewClient(c, 3*time.Second, nil)
	if err := cl.Put("before", "1"); err != nil {
		t.Fatal(err)
	}

	// Crash the leader; a new one must take over and accept writes.
	c.Crash(leader)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 && l != leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if l := c.Leader(); l < 0 || l == leader {
		t.Fatalf("no new leader after crash, got %d", l)
	}
	if err := cl.Put("after", "2"); err != nil {
		t.Fatalf("write after leadership change: %v", err)
	}
	v, ok, err := cl.Get("before")
	if err != nil || !ok || v != "1" {
		t.Fatalf("committed entry lost across leadership change: v=%q ok=%v err=%v", v, ok, err)
	}
}

func TestRecoveryOfCommittedEntriesFromDisk(t *testing.T) {
	dir := t.TempDir()
	c, err := cluster.New(fastOpts(3, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	waitLeader(t, c, 3*time.Second)

	cl := cluster.NewClient(c, 3*time.Second, nil)
	for i := 0; i < 5; i++ {
		if err := cl.Put(fmt.Sprintf("d%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Crash and restart every node, then confirm the committed data is intact.
	for _, id := range c.Peers() {
		c.Crash(id)
	}
	for _, id := range c.Peers() {
		if err := c.Restart(id); err != nil {
			t.Fatalf("restart node %d: %v", id, err)
		}
	}
	waitLeader(t, c, 4*time.Second)

	for i := 0; i < 5; i++ {
		v, ok, err := cl.Get(fmt.Sprintf("d%d", i))
		if err != nil || !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("after restart d%d: v=%q ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestSnapshotInstall(t *testing.T) {
	opts := fastOpts(3, t.TempDir())
	opts.SnapshotThreshold = 20 // compact aggressively
	c, err := cluster.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	leader := waitLeader(t, c, 3*time.Second)

	cl := cluster.NewClient(c, 5*time.Second, nil)

	// Isolate one follower so it falls far behind while we generate enough
	// writes that the leader compacts the log it would need.
	follower := -1
	for _, id := range c.Peers() {
		if id != leader {
			follower = id
			break
		}
	}
	in := fault.NewInjector(3)
	c.Network().SetFilter(in)
	in.Isolate(follower, c.Peers())

	for i := 0; i < 60; i++ {
		if err := cl.Put(fmt.Sprintf("s%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// Heal: the lagging follower must be caught up via InstallSnapshot.
	in.Heal()
	deadline := time.Now().Add(5 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		store := c.Store(follower)
		if store != nil {
			if v, ok := store.Get("s59"); ok && v == "v59" {
				caught = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !caught {
		t.Fatal("lagging follower was not caught up via snapshot install")
	}
}

// TestLinearizableUnderChaos is the flagship end-to-end test: a workload runs
// against the cluster while a nemesis injects partitions, delays and crashes,
// and the recorded history must check out as linearizable.
func TestLinearizableUnderChaos(t *testing.T) {
	c, err := cluster.New(fastOpts(5, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	waitLeader(t, c, 3*time.Second)

	history := linz.NewHistory()
	cl := cluster.NewClient(c, 5*time.Second, history)

	nm := fault.NewNemesis(c, 42)
	nm.Run(500 * time.Millisecond)

	keys := []string{"x", "y", "z"}
	for i := 0; i < 120; i++ {
		k := keys[i%len(keys)]
		switch i % 3 {
		case 0, 1:
			_ = cl.Put(k, fmt.Sprintf("v%d", i))
		default:
			_, _, _ = cl.Get(k)
		}
	}
	nm.Stop()

	res := linz.Check(history)
	if !res.Linearizable {
		t.Fatalf("history not linearizable on key %q: %s", res.Key, res.Reason)
	}
	if len(history.Events()) == 0 {
		t.Fatal("no operations recorded")
	}
}

package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sarmakska/raftkv/cluster"
)

// BenchmarkPutThroughput measures committed writes per second through the
// leader-aware client on a healthy three-node cluster. It is the number quoted
// in the README results section.
func BenchmarkPutThroughput(b *testing.B) {
	c, err := cluster.New(fastOpts(3, b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for c.Leader() < 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cl := cluster.NewClient(c, 5*time.Second, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cl.Put("k", fmt.Sprintf("v%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLinearizableGet measures linearizable reads per second through the
// read-index path under the leader lease.
func BenchmarkLinearizableGet(b *testing.B) {
	c, err := cluster.New(fastOpts(3, b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for c.Leader() < 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cl := cluster.NewClient(c, 5*time.Second, nil)
	_ = cl.Put("k", "v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := cl.Get("k"); err != nil {
			b.Fatal(err)
		}
	}
}

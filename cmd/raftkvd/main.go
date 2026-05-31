// Command raftkvd runs a self-contained demonstration of raftkv: it boots an
// in-process cluster, drives a workload through the leader-aware client while a
// nemesis injects partitions, delays and crashes, then checks the recorded
// history for linearizability and prints the verdict. It is the fastest way to
// see the whole system working end to end.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sarmakska/raftkv/cluster"
	"github.com/sarmakska/raftkv/fault"
	"github.com/sarmakska/raftkv/linz"
)

func main() {
	nodes := flag.Int("nodes", 5, "number of nodes in the cluster")
	ops := flag.Int("ops", 200, "number of client operations to run")
	chaos := flag.Bool("chaos", true, "run the nemesis (partitions, delays, crashes)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed for the nemesis")
	flag.Parse()

	dir, err := os.MkdirTemp("", "raftkvd")
	if err != nil {
		fail(err)
	}
	defer os.RemoveAll(dir)

	c, err := cluster.New(cluster.DefaultOptions(*nodes, dir))
	if err != nil {
		fail(err)
	}
	defer c.Stop()

	fmt.Printf("raftkv: started %d-node cluster in %s\n", *nodes, dir)
	waitForLeader(c)

	var nm *fault.Nemesis
	if *chaos {
		nm = fault.NewNemesis(c, *seed)
		nm.Run(600 * time.Millisecond)
		fmt.Println("raftkv: nemesis running (partitions, delays, crashes)")
	}

	history := linz.NewHistory()
	cl := cluster.NewClient(c, 5*time.Second, history)

	keys := []string{"a", "b", "c"}
	for i := 0; i < *ops; i++ {
		k := keys[i%len(keys)]
		switch i % 3 {
		case 0, 1:
			_ = cl.Put(k, fmt.Sprintf("v%d", i))
		case 2:
			_, _, _ = cl.Get(k)
		}
	}

	if nm != nil {
		nm.Stop()
	}

	res := linz.Check(history)
	fmt.Printf("raftkv: ran %d operations\n", len(history.Events()))
	if res.Linearizable {
		fmt.Println("raftkv: history is LINEARIZABLE")
		return
	}
	fmt.Printf("raftkv: LINEARIZABILITY VIOLATION on key %q: %s\n", res.Key, res.Reason)
	os.Exit(1)
}

func waitForLeader(c *cluster.Cluster) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 {
			fmt.Printf("raftkv: leader elected: node %d\n", l)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	fail(fmt.Errorf("no leader elected within timeout"))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "raftkv:", err)
	os.Exit(1)
}

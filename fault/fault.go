// Package fault is the Jepsen-style fault-injection harness. It implements the
// cluster network Filter, so it can partition nodes, drop, reorder and delay
// messages, and it drives crash and restart through the cluster. A nemesis
// schedule applies faults on a timer while clients run, then heals the cluster
// so the linearizability checker can judge the recorded history.
package fault

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sarmakska/raftkv/cluster"
)

// Injector is the network Filter the harness installs on a cluster. It is
// concurrency-safe: the nemesis goroutine mutates the active faults while the
// transport queries them on every message.
type Injector struct {
	mu sync.RWMutex

	// partitions maps a node id to the partition group it belongs to. Two
	// nodes can exchange messages only if they share a group. An empty map
	// means a single fully-connected group.
	partition map[int]int

	// dropRate is the probability in [0,1] that any single message is dropped
	// even within a partition group, modelling lossy links.
	dropRate float64

	// delayMin and delayMax bound a uniform random delay applied to delivered
	// messages, modelling latency and reordering.
	delayMin time.Duration
	delayMax time.Duration

	rng *rand.Rand
}

// NewInjector returns an Injector with no active faults.
func NewInjector(seed int64) *Injector {
	return &Injector{
		partition: map[int]int{},
		rng:       rand.New(rand.NewSource(seed)),
	}
}

// Allow implements cluster.Filter. It is called for every message; the order of
// checks is partition first (a hard cut), then random drop, then delay.
func (in *Injector) Allow(m cluster.Message) (bool, time.Duration) {
	in.mu.RLock()
	defer in.mu.RUnlock()
	if len(in.partition) > 0 {
		gf, okF := in.partition[m.From]
		gt, okT := in.partition[m.To]
		if okF && okT && gf != gt {
			return false, 0
		}
	}
	if in.dropRate > 0 && in.rng.Float64() < in.dropRate {
		return false, 0
	}
	if in.delayMax > 0 {
		span := in.delayMax - in.delayMin
		d := in.delayMin
		if span > 0 {
			d += time.Duration(in.rng.Int63n(int64(span) + 1))
		}
		return true, d
	}
	return true, 0
}

// Partition splits the cluster into the given groups. Each inner slice is the
// set of node ids that can still talk to one another. Nodes not listed remain
// fully connected to everyone.
func (in *Injector) Partition(groups ...[]int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.partition = map[int]int{}
	for g, ids := range groups {
		for _, id := range ids {
			in.partition[id] = g + 1
		}
	}
}

// Isolate cuts a single node off from the rest of the cluster.
func (in *Injector) Isolate(id int, all []int) {
	rest := make([]int, 0, len(all))
	for _, x := range all {
		if x != id {
			rest = append(rest, x)
		}
	}
	in.Partition([]int{id}, rest)
}

// Heal removes all partitions, drops and delays.
func (in *Injector) Heal() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.partition = map[int]int{}
	in.dropRate = 0
	in.delayMin = 0
	in.delayMax = 0
}

// SetDropRate sets the probability that a message is dropped.
func (in *Injector) SetDropRate(p float64) {
	in.mu.Lock()
	in.dropRate = p
	in.mu.Unlock()
}

// SetDelay sets the latency window applied to delivered messages.
func (in *Injector) SetDelay(min, max time.Duration) {
	in.mu.Lock()
	in.delayMin = min
	in.delayMax = max
	in.mu.Unlock()
}

// Nemesis drives a schedule of faults against a cluster on a background
// goroutine. It is the orchestration layer: clients run normally while the
// nemesis partitions, crashes and heals, exactly as a chaos test would.
type Nemesis struct {
	c   *cluster.Cluster
	in  *Injector
	rng *rand.Rand

	stop chan struct{}
	wg   sync.WaitGroup

	crashed  int
	didCrash bool
}

// NewNemesis attaches a nemesis to a cluster, installing the injector as the
// network filter.
func NewNemesis(c *cluster.Cluster, seed int64) *Nemesis {
	in := NewInjector(seed)
	c.Network().SetFilter(in)
	return &Nemesis{c: c, in: in, rng: rand.New(rand.NewSource(seed + 1)), stop: make(chan struct{})}
}

// Injector exposes the underlying injector for manual fault control.
func (nm *Nemesis) Injector() *Injector { return nm.in }

// Run starts the nemesis loop. Every period it applies a random fault, holds it
// briefly, then heals. It runs until Stop is called. The faults are chosen so
// that a majority can always make progress between disruptions, which is the
// regime in which linearizability must hold.
func (nm *Nemesis) Run(period time.Duration) {
	nm.wg.Add(1)
	go func() {
		defer nm.wg.Done()
		peers := nm.c.Peers()
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-nm.stop:
				nm.in.Heal()
				return
			case <-ticker.C:
				nm.applyRandomFault(peers)
				select {
				case <-nm.stop:
					nm.in.Heal()
					return
				case <-time.After(period / 2):
				}
				nm.recover(peers)
			}
		}
	}()
}

func (nm *Nemesis) applyRandomFault(peers []int) {
	switch nm.rng.Intn(4) {
	case 0:
		// Minority partition: isolate one node.
		nm.in.Isolate(peers[nm.rng.Intn(len(peers))], peers)
	case 1:
		// Split the cluster into two groups, keeping a majority on one side.
		half := len(peers)/2 + 1
		shuffled := append([]int(nil), peers...)
		nm.rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		nm.in.Partition(shuffled[:half], shuffled[half:])
	case 2:
		// Lossy, slow network.
		nm.in.SetDropRate(0.2)
		nm.in.SetDelay(5*time.Millisecond, 25*time.Millisecond)
	case 3:
		// Crash a single node; recover will restart it.
		nm.crashed = peers[nm.rng.Intn(len(peers))]
		nm.c.Crash(nm.crashed)
		nm.didCrash = true
	}
}

func (nm *Nemesis) recover(peers []int) {
	nm.in.Heal()
	if nm.didCrash {
		_ = nm.c.Restart(nm.crashed)
		nm.didCrash = false
	}
}

// Stop halts the nemesis and heals the cluster.
func (nm *Nemesis) Stop() {
	close(nm.stop)
	nm.wg.Wait()
}

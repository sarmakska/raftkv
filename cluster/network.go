package cluster

import (
	"errors"
	"sync"
	"time"

	"github.com/sarmakska/raftkv/transport"
)

// ErrUnreachable is returned by the in-memory transport when the network filter
// decides a message should be dropped, which a real network would surface as a
// timeout.
var ErrUnreachable = errors.New("cluster: peer unreachable")

// MessageKind tags an in-flight RPC so the fault filter can reason about it.
type MessageKind int

const (
	KindRequestVote MessageKind = iota
	KindAppendEntries
	KindInstallSnapshot
)

// Message describes an RPC about to cross the network. The Filter inspects it
// and decides whether to deliver it, drop it, or hold it back by a delay.
type Message struct {
	From int
	To   int
	Kind MessageKind
}

// Filter is the hook the fault-injection harness implements. Allow returns
// whether the message may be delivered and an optional delay to apply before
// delivery. A nil Filter on the Network means everything is delivered with no
// delay.
type Filter interface {
	Allow(m Message) (deliver bool, delay time.Duration)
}

// Network is the in-memory message fabric connecting nodes in a single process.
// It owns the routing table from node id to handler and applies a Filter to
// every message, which is the single seam the fault harness uses to inject
// partitions, drops and delays.
type Network struct {
	mu       sync.RWMutex
	handlers map[int]transport.Handler
	filter   Filter
}

// NewNetwork returns an empty Network.
func NewNetwork() *Network {
	return &Network{handlers: map[int]transport.Handler{}}
}

// Register wires a node id to its RPC handler.
func (nw *Network) Register(id int, h transport.Handler) {
	nw.mu.Lock()
	nw.handlers[id] = h
	nw.mu.Unlock()
}

// SetFilter installs (or clears, with nil) the fault filter.
func (nw *Network) SetFilter(f Filter) {
	nw.mu.Lock()
	nw.filter = f
	nw.mu.Unlock()
}

func (nw *Network) gate(m Message) (transport.Handler, error) {
	nw.mu.RLock()
	h, ok := nw.handlers[m.To]
	f := nw.filter
	nw.mu.RUnlock()
	if !ok {
		return nil, ErrUnreachable
	}
	if f != nil {
		deliver, delay := f.Allow(m)
		if !deliver {
			return nil, ErrUnreachable
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	return h, nil
}

// Endpoint is the transport.Transport a single node uses to reach its peers
// through the shared Network.
type Endpoint struct {
	from int
	nw   *Network
}

// EndpointFor returns the transport endpoint for node id.
func (nw *Network) EndpointFor(id int) *Endpoint { return &Endpoint{from: id, nw: nw} }

// SendRequestVote routes a RequestVote RPC through the network filter.
func (e *Endpoint) SendRequestVote(target int, args *transport.RequestVoteArgs) (*transport.RequestVoteReply, error) {
	h, err := e.nw.gate(Message{From: e.from, To: target, Kind: KindRequestVote})
	if err != nil {
		return nil, err
	}
	return h.HandleRequestVote(args), nil
}

// SendAppendEntries routes an AppendEntries RPC through the network filter.
func (e *Endpoint) SendAppendEntries(target int, args *transport.AppendEntriesArgs) (*transport.AppendEntriesReply, error) {
	h, err := e.nw.gate(Message{From: e.from, To: target, Kind: KindAppendEntries})
	if err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(args), nil
}

// SendInstallSnapshot routes an InstallSnapshot RPC through the network filter.
func (e *Endpoint) SendInstallSnapshot(target int, args *transport.InstallSnapshotArgs) (*transport.InstallSnapshotReply, error) {
	h, err := e.nw.gate(Message{From: e.from, To: target, Kind: KindInstallSnapshot})
	if err != nil {
		return nil, err
	}
	return h.HandleInstallSnapshot(args), nil
}

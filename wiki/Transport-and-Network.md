# Transport and Network

The single decision that makes the whole fault harness possible is that a Raft node never touches the network directly. It reaches its peers only through an interface, and everything below that interface, including the seam the nemesis attacks, lives outside the consensus core. This page covers `transport/transport.go` and the in-memory implementation in `cluster/network.go`.

## The two interfaces

```go
type Transport interface {
    SendRequestVote(target int, args *RequestVoteArgs) (*RequestVoteReply, error)
    SendAppendEntries(target int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
    SendInstallSnapshot(target int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

type Handler interface {
    HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
    HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
    HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}
```

`Transport` is what a node calls to reach a peer. `Handler` is what a node implements so a transport can deliver an inbound RPC to it; `raft.Node` satisfies it. Every `Transport` method is synchronous from the caller's point of view and returns an error if the peer is unreachable. The contract the core relies on is deliberately weak: a transport may drop, delay or reorder messages freely. The consensus core copes by design, which is exactly the property the harness exercises.

## The in-memory Network

In this repository the only `Transport` is `cluster.Endpoint`, and it routes every call through a shared `cluster.Network`. The network owns a routing table from node id to `Handler` and a single optional `Filter`.

```mermaid
flowchart LR
    n0["Node 0<br/>(Handler)"] -->|EndpointFor(0)| e0["Endpoint 0"]
    e0 -->|gate(msg)| f["Filter.Allow"]
    f -->|deliver| route["handlers[To]"]
    route --> n1["Node 1.HandleAppendEntries"]
    f -.drop.-> drop["ErrUnreachable"]

    classDef c fill:#0d1117,stroke:#38bdf8,color:#f5f7fa
    classDef a fill:#0d1117,stroke:#34d399,color:#f5f7fa
    class n0,e0,route,n1 c
    class f,drop a
```

The path of one RPC:

1. The core calls `endpoint.SendAppendEntries(target, args)`.
2. The endpoint builds a `Message{From, To, Kind}` and calls `network.gate(msg)`.
3. `gate` looks up the target handler. If there is none (a crashed node), it returns `ErrUnreachable`.
4. If a `Filter` is installed, `gate` calls `filter.Allow(msg)`. A `false` return drops the message (returns `ErrUnreachable`); a non-zero delay sleeps before delivery.
5. On delivery, the endpoint calls the handler method directly and returns its reply.

```go
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
```

## The Filter seam

```go
type Filter interface {
    Allow(m Message) (deliver bool, delay time.Duration)
}
```

This is the entire surface the fault harness needs. A `nil` filter means everything is delivered with no delay, which is the healthy cluster. The fault `Injector` (see [[Fault-Injection-Harness]]) is a `Filter` and nothing more; the nemesis schedules calls that change its internal partition map and drop rate over time. Because the seam is a one-method interface that returns only "deliver?" and "how long to wait?", it can model partitions, drops, latency and reordering without ever knowing what Raft is, and Raft never knows it is there.

`MessageKind` (`KindRequestVote`, `KindAppendEntries`, `KindInstallSnapshot`) tags each message so a filter could treat RPC types differently. The current injector does not, but the hook is there.

## Why a synchronous call models a real wire

A real RPC is a request and a response over a connection that can fail. The in-memory transport models the same shape: a synchronous call that either returns a reply or an error. Delay is modelled by `time.Sleep` inside `gate`, and because each `Send*` call runs on its own goroutine in the core (the core never blocks the node lock on a send), independent delays naturally reorder concurrent messages. A drop is modelled as `ErrUnreachable`, which is what a real client sees as a timeout. The one thing the in-memory transport does not model is partial delivery of a single message, which a real network can do; that is handled at the storage layer instead (torn-write recovery, see [[Storage-Engine]]).

## Concurrency

`Network` guards its handler table and filter with a `sync.RWMutex`. `gate` takes the read lock, copies the handler and filter pointers, and releases it before sleeping or calling the handler, so a slow delivery never blocks a `Register` or `SetFilter`. The injector inside the filter has its own lock; the nemesis mutating faults and the transport querying them run concurrently and safely (see [[Fault-Injection-Harness]]).

## Swapping in a real transport

Because the seam is an interface, putting raftkv on a real network is a contained change: implement `transport.Transport` over gRPC or TCP, serialise the argument and reply types from [[Wire-Formats-and-Data-Layout]], and hand each node its endpoint instead of `cluster.EndpointFor`. No consensus logic changes. [[Writing-a-Transport]] walks through the contract a real transport must honour, including the gotchas the in-memory one hides.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

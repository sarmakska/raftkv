# Wire Formats and Data Layout

This page is the byte-and-field reference: the on-disk formats, the RPC argument and reply types, and the command encoding. If you are writing a real transport (see [[Writing-a-Transport]]) or a tool that reads a node's directory, this is the page you keep open. Everything here is defined in `transport/transport.go`, `raft/storage.go` and `kv/kv.go`.

## On-disk: the log record

`log.bin` is a flat sequence of records with no file header. Each record is:

```
+--------+--------+--------+----------------------+
| length |  crc32 | reserv |   payload (length)   |
| 4 bytes| 4 bytes| 4 bytes|   JSON LogEntry      |
+--------+--------+--------+----------------------+
   BE       BE      zero
```

- `length` is the payload length in bytes, big-endian `uint32`.
- `crc32` is `crc32.ChecksumIEEE` of the payload, big-endian `uint32`. This is what catches a torn or corrupted trailing record on replay.
- `reserved` is `hdr[8:12]`, currently always zero. It exists so a future version or alignment marker can be added without changing the header size.
- `payload` is `json.Marshal` of a `transport.LogEntry`.

The header is the fixed 12 bytes; the payload length varies with the command. Records are written back to back; replay walks them in order. See [[Storage-Engine]] for the read and recovery logic.

### LogEntry

```go
type LogEntry struct {
    Term    uint64
    Index   uint64
    Command []byte
}
```

`Command` is the application payload, opaque to the core. For the kv state machine it is a JSON `kv.Command` (below). A `nil` `Command` is the Raft no-op marker a new leader appends; it is replicated and committed like any entry but applies to nothing.

## On-disk: state.json

```json
{ "term": 7, "voted_for": 3 }
```

`voted_for` is `-1` when the node has not voted in the current term. Written by `SaveState` through `writeJSONAtomic` (temp file plus rename). The Go shape is `persistentState{Term uint64, VotedFor int}`.

## On-disk: snapshot.json

```json
{ "LastIncludedIndex": 40, "LastIncludedTerm": 3, "Data": "<base64 bytes>" }
```

`Data` is the application snapshot, base64-encoded by Go's JSON marshaller because it is a `[]byte`. For the kv store it is `json.Marshal` of the `map[string]string` (see [[KV-State-Machine]]). The Go shape is:

```go
type Snapshot struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    Data              []byte
}
```

`LastIncludedIndex` and `LastIncludedTerm` identify the log position the snapshot replaces, so the core can answer `termAt` and consistency checks for the snapshot boundary without the entries themselves.

## RPC: RequestVote

```go
type RequestVoteArgs struct {
    Term         uint64
    CandidateID  int
    LastLogIndex uint64
    LastLogTerm  uint64
    PreVote      bool
}

type RequestVoteReply struct {
    Term        uint64
    VoteGranted bool
}
```

`PreVote` is the raftkv-relevant field. When set, the responder answers "would you vote for me at this term?" without mutating its persistent `currentTerm` or `votedFor`. This gates a partitioned node from disrupting a healthy cluster on rejoin. The real vote leaves `PreVote` false. See [[Raft-Walkthrough]].

`LastLogIndex` and `LastLogTerm` carry the candidate's log tip so the responder can apply the up-to-date check (last term wins; on a tie, longer log wins).

## RPC: AppendEntries

```go
type AppendEntriesArgs struct {
    Term         uint64
    LeaderID     int
    PrevLogIndex uint64
    PrevLogTerm  uint64
    Entries      []LogEntry
    LeaderCommit uint64
}

type AppendEntriesReply struct {
    Term          uint64
    Success       bool
    ConflictIndex uint64
    ConflictTerm  uint64
}
```

`AppendEntries` doubles as the heartbeat when `Entries` is empty. `PrevLogIndex`/`PrevLogTerm` are the consistency check: the follower accepts the entries only if it has a matching entry at that index and term. `LeaderCommit` is how a follower learns the commit index.

`ConflictIndex` and `ConflictTerm` are the fast-backtracking hint. On a rejected `AppendEntries` the follower fills them so the leader can jump `nextIndex` to the right place in one round trip instead of decrementing one entry per round trip:

- If the follower's log is shorter than `PrevLogIndex`, it returns `ConflictTerm = 0` and `ConflictIndex = lastIndex + 1`.
- If it has an entry at `PrevLogIndex` but in the wrong term, it returns that term as `ConflictTerm` and the first index of that term as `ConflictIndex`.
- If `PrevLogIndex` falls inside the compacted prefix, it returns `ConflictIndex = lastSnapIndex + 1`.

The leader's `backtrackLocked` reads these and computes the next `nextIndex`.

## RPC: InstallSnapshot

```go
type InstallSnapshotArgs struct {
    Term              uint64
    LeaderID          int
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    Data              []byte
}

type InstallSnapshotReply struct {
    Term uint64
}
```

Sent when a follower needs entries the leader has already compacted. `Data` is the full application snapshot. This implementation sends the snapshot in a single message rather than chunking it, which is fine for the in-memory transport; a real transport over a wire would likely chunk large snapshots (noted on the [[Roadmap]]). See [[Snapshots-and-Compaction]].

## Application command: kv.Command

```go
type Command struct {
    Kind  OpKind `json:"kind"`  // "get" | "put" | "delete"
    Key   string `json:"key"`
    Value string `json:"value"`
}
```

`EncodeCommand` is `json.Marshal`; `DecodeCommand` is `json.Unmarshal`. A `Put` and `Delete` are replicated through the log as the `Command` bytes inside a `LogEntry`. A `Get` is never logged; it goes through the read-index path (see [[Read-Index-and-Leases]]) and the `Command` type is reused only for history recording.

## Why JSON for entries and state

JSON is not the fast choice, and that is a deliberate trade for clarity (see [[Design-Decisions]]). A node's entire durable state is human-readable with `cat` and `jq`, which makes the recovery code reviewable and a corrupted record obvious. The log record framing (length plus CRC) is binary because that is what makes torn-write detection exact; the payload inside it is JSON because that is what makes it inspectable. A production format would replace the JSON payload with a compact binary encoding without changing the framing.

---
SarmaLinux . sarmalinux.com . [raftkv on GitHub](https://github.com/sarmakska/raftkv)

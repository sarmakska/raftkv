// Package transport defines the message types exchanged between Raft peers
// and the Transport interface that decouples the consensus core from the
// wire. The in-process cluster and the fault-injection harness both implement
// Transport, which is how I can inject partitions, delays and drops without
// changing a single line of the Raft state machine.
package transport

// RequestVoteArgs is the argument for the RequestVote RPC described in the
// Raft paper, section 5.2.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
	// PreVote marks a vote request sent during the pre-vote phase. Pre-vote
	// requests never mutate the responder's persistent state; they only ask
	// "would you vote for me?". This prevents a partitioned node from
	// disrupting a healthy cluster by repeatedly bumping its term.
	PreVote bool
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// LogEntry is a single replicated command together with the term in which the
// leader created it.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// AppendEntriesArgs is the argument for the AppendEntries RPC. It doubles as
// the heartbeat when Entries is empty.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC. ConflictIndex
// and ConflictTerm implement the fast log-backtracking optimisation from the
// Raft paper so a lagging follower converges in O(1) round trips per term
// rather than one entry at a time.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs is the argument for the InstallSnapshot RPC used when a
// follower has fallen so far behind that the leader has already compacted the
// log entries it would need.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64
}

// Transport is the bidirectional channel a Raft node uses to talk to its
// peers. Every method is synchronous from the caller's point of view and
// returns an error if the peer is unreachable. Implementations are free to
// drop, delay or reorder messages; the consensus core copes by design.
type Transport interface {
	SendRequestVote(target int, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(target int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(target int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is implemented by a Raft node so a transport can deliver inbound
// RPCs to it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

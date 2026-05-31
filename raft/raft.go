// Package raft implements the Raft consensus algorithm: leader election with
// randomised timeouts, log replication with the fast-backtracking
// optimisation, persistence of term/vote/log to disk, snapshotting for log
// compaction, and a pre-vote phase to keep partitioned nodes from disrupting a
// healthy cluster. The state machine is driven by a single goroutine; all
// mutable Raft state is guarded by one mutex so the logic reads like the
// paper's pseudo-code rather than a lock-ordering puzzle.
package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sarmakska/raftkv/transport"
)

// Role is the Raft role of a node at a point in time.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// ApplyMsg is delivered to the application for each committed log entry and for
// each snapshot the node installs. Exactly one of CommandValid or
// SnapshotValid is true.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	Index        uint64
	Term         uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Config configures a Raft node. Timeouts are deliberately exposed so tests can
// shrink them and run a cluster's worth of elections in milliseconds.
type Config struct {
	ID                 int
	Peers              []int // all node ids including this one
	Storage            Storage
	Transport          transport.Transport
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// SnapshotThreshold is the number of applied entries past the last
	// snapshot at which the node asks the application for a fresh snapshot. A
	// zero value disables automatic snapshotting.
	SnapshotThreshold uint64
	ApplyCh           chan ApplyMsg
}

// Node is a single Raft server.
type Node struct {
	mu sync.Mutex

	id      int
	peers   []int
	storage Storage
	trans   transport.Transport
	applyCh chan ApplyMsg

	heartbeat     time.Duration
	electMin      time.Duration
	electMax      time.Duration
	snapThreshold uint64

	// Persistent state.
	currentTerm uint64
	votedFor    int // -1 for none

	// Snapshot bookkeeping mirrored from storage for O(1) access under lock.
	lastSnapIndex uint64
	lastSnapTerm  uint64
	snapData      []byte

	// Volatile state.
	role        Role
	commitIndex uint64
	lastApplied uint64
	leaderID    int

	// Leader volatile state.
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	// leaderLeaseUntil bounds how long the current leader may answer
	// leader-lease reads without a fresh round of heartbeats. It is extended
	// whenever a majority of followers acknowledge a heartbeat.
	leaderLeaseUntil time.Time

	electionDeadline time.Time
	rng              *rand.Rand

	stopCh    chan struct{}
	stopped   bool
	applyCond *sync.Cond

	// snapProvider is the application callback used to capture state for
	// automatic log compaction.
	snapProvider func() []byte
}

// NewNode constructs a node, loading persisted state and snapshot from storage.
func NewNode(cfg Config) (*Node, error) {
	term, votedFor, err := cfg.Storage.LoadState()
	if err != nil {
		return nil, err
	}
	n := &Node{
		id:            cfg.ID,
		peers:         cfg.Peers,
		storage:       cfg.Storage,
		trans:         cfg.Transport,
		applyCh:       cfg.ApplyCh,
		heartbeat:     cfg.HeartbeatInterval,
		electMin:      cfg.ElectionTimeoutMin,
		electMax:      cfg.ElectionTimeoutMax,
		snapThreshold: cfg.SnapshotThreshold,
		currentTerm:   term,
		votedFor:      votedFor,
		role:          Follower,
		leaderID:      -1,
		nextIndex:     map[int]uint64{},
		matchIndex:    map[int]uint64{},
		rng:           rand.New(rand.NewSource(int64(cfg.ID)*2654435761 + time.Now().UnixNano())),
		stopCh:        make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	if snap, ok, err := cfg.Storage.LoadSnapshot(); err != nil {
		return nil, err
	} else if ok {
		n.lastSnapIndex = snap.LastIncludedIndex
		n.lastSnapTerm = snap.LastIncludedTerm
		n.snapData = snap.Data
		n.commitIndex = snap.LastIncludedIndex
		n.lastApplied = snap.LastIncludedIndex
	}
	return n, nil
}

// Start launches the background goroutines: the election/heartbeat ticker and
// the apply loop.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
	go n.run()
	go n.applyLoop()
}

// Stop halts the node's goroutines. It is idempotent.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.mu.Unlock()
	n.applyCond.Broadcast()
}

func (n *Node) run() {
	ticker := time.NewTicker(n.heartbeat / 2)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.role {
	case Leader:
		n.broadcastAppendLocked(false)
	case Follower, Candidate:
		if time.Now().After(n.electionDeadline) {
			n.startElectionLocked()
		}
	}
}

func (n *Node) resetElectionTimer() {
	d := n.electMin + time.Duration(n.rng.Int63n(int64(n.electMax-n.electMin+1)))
	n.electionDeadline = time.Now().Add(d)
}

// lastLogIndexLocked and lastLogTermLocked account for the snapshot prefix.
func (n *Node) lastLogIndexLocked() uint64 {
	li, _ := n.storage.LastIndex()
	if li < n.lastSnapIndex {
		return n.lastSnapIndex
	}
	return li
}

func (n *Node) lastLogTermLocked() uint64 {
	li, _ := n.storage.LastIndex()
	if li == 0 || li == n.lastSnapIndex {
		return n.lastSnapTerm
	}
	es, err := n.storage.Entries(li, li+1)
	if err != nil || len(es) == 0 {
		return n.lastSnapTerm
	}
	return es[0].Term
}

func (n *Node) termAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == n.lastSnapIndex {
		return n.lastSnapTerm, true
	}
	if index < n.lastSnapIndex {
		return 0, false // compacted away
	}
	es, err := n.storage.Entries(index, index+1)
	if err != nil || len(es) == 0 {
		return 0, false
	}
	return es[0].Term, true
}

func (n *Node) persistStateLocked() {
	_ = n.storage.SaveState(n.currentTerm, n.votedFor)
}

// becomeFollowerLocked steps down to follower at the given term.
func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
		n.persistStateLocked()
	}
	n.role = Follower
}

func (n *Node) startElectionLocked() {
	// Pre-vote: probe whether a majority would grant a vote before bumping our
	// term. This is what stops a node isolated by a partition from forcing
	// repeated re-elections when it rejoins.
	if !n.runPreVoteLocked() {
		n.resetElectionTimer()
		return
	}
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.persistStateLocked()
	n.resetElectionTimer()
	term := n.currentTerm
	lastIdx := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()

	votes := 1
	majority := len(n.peers)/2 + 1
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		args := &transport.RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
		peer := p
		go func() {
			reply, err := n.trans.SendRequestVote(peer, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeaderLocked()
				}
			}
		}()
	}
}

// runPreVoteLocked synchronously polls peers with PreVote requests. It runs
// without releasing the lock for long by issuing the calls in parallel and
// waiting on a small channel; the transport calls themselves do not take the
// node lock so there is no deadlock.
func (n *Node) runPreVoteLocked() bool {
	term := n.currentTerm + 1
	lastIdx := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()
	majority := len(n.peers)/2 + 1
	if majority == 1 {
		return true
	}
	results := make(chan bool, len(n.peers))
	count := 0
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		count++
		peer := p
		args := &transport.RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm, PreVote: true}
		go func() {
			reply, err := n.trans.SendRequestVote(peer, args)
			results <- err == nil && reply.VoteGranted
		}()
	}
	n.mu.Unlock()
	granted := 1
	for i := 0; i < count; i++ {
		if <-results {
			granted++
		}
	}
	n.mu.Lock()
	return granted >= majority
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	last := n.lastLogIndexLocked()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = last
	// Append a no-op entry for the new term so the leader can commit entries
	// from prior terms safely (Raft paper, section 5.4.2) and so leader-lease
	// reads have a committed anchor.
	n.appendCommandLocked(nil)
	n.broadcastAppendLocked(true)
}

// appendCommandLocked appends a command to the local log and returns its index.
// A nil command is a no-op marker entry.
func (n *Node) appendCommandLocked(cmd []byte) uint64 {
	idx := n.lastLogIndexLocked() + 1
	e := transport.LogEntry{Term: n.currentTerm, Index: idx, Command: cmd}
	_ = n.storage.AppendLog([]transport.LogEntry{e})
	n.matchIndex[n.id] = idx
	return idx
}

// Propose submits a command to be replicated. It returns the index the command
// will occupy if committed, the current term, and whether this node believes it
// is the leader. A false return means the caller should retry against another
// node.
func (n *Node) Propose(cmd []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, n.currentTerm, false
	}
	idx := n.appendCommandLocked(cmd)
	n.broadcastAppendLocked(false)
	return idx, n.currentTerm, true
}

func (n *Node) broadcastAppendLocked(forceHeartbeat bool) {
	if n.role != Leader {
		return
	}
	term := n.currentTerm
	// Track heartbeat acknowledgements to renew the leader lease.
	acks := 1
	majority := len(n.peers)/2 + 1
	now := time.Now()
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.sendToPeerLocked(p, term, func() {
			acks++
			if acks >= majority {
				n.leaderLeaseUntil = now.Add(n.electMin)
			}
		})
	}
	if majority == 1 {
		n.leaderLeaseUntil = now.Add(n.electMin)
		n.advanceCommitLocked()
	}
}

// sendToPeerLocked decides between AppendEntries and InstallSnapshot based on
// whether the peer needs entries that have been compacted, then dispatches the
// RPC on its own goroutine. onAck runs under the lock when a heartbeat is
// acknowledged at the current term.
func (n *Node) sendToPeerLocked(p int, term uint64, onAck func()) {
	next := n.nextIndex[p]
	if next <= n.lastSnapIndex {
		n.sendSnapshotLocked(p, term)
		return
	}
	prevIndex := next - 1
	prevTerm, ok := n.termAtLocked(prevIndex)
	if !ok {
		n.sendSnapshotLocked(p, term)
		return
	}
	last := n.lastLogIndexLocked()
	var entries []transport.LogEntry
	if next <= last {
		es, err := n.storage.Entries(next, last+1)
		if err == nil {
			entries = es
		}
	}
	args := &transport.AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	peer := p
	go func() {
		reply, err := n.trans.SendAppendEntries(peer, args)
		if err != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.role != Leader || n.currentTerm != term {
			return
		}
		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			return
		}
		if reply.Success {
			newMatch := args.PrevLogIndex + uint64(len(args.Entries))
			if newMatch > n.matchIndex[peer] {
				n.matchIndex[peer] = newMatch
			}
			n.nextIndex[peer] = n.matchIndex[peer] + 1
			n.advanceCommitLocked()
			if onAck != nil {
				onAck()
			}
			return
		}
		// Fast backtrack using the conflict hint.
		n.nextIndex[peer] = n.backtrackLocked(peer, reply)
	}()
}

func (n *Node) backtrackLocked(peer int, reply *transport.AppendEntriesReply) uint64 {
	if reply.ConflictTerm == 0 {
		if reply.ConflictIndex == 0 {
			return 1
		}
		return reply.ConflictIndex
	}
	// Find the last index in our log with ConflictTerm; if present, resume
	// just after it, otherwise back up to the follower's conflict index.
	last := n.lastLogIndexLocked()
	for i := last; i > n.lastSnapIndex; i-- {
		t, ok := n.termAtLocked(i)
		if !ok {
			break
		}
		if t == reply.ConflictTerm {
			return i + 1
		}
		if t < reply.ConflictTerm {
			break
		}
	}
	if reply.ConflictIndex == 0 {
		return 1
	}
	return reply.ConflictIndex
}

func (n *Node) sendSnapshotLocked(p int, term uint64) {
	args := &transport.InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: n.lastSnapIndex,
		LastIncludedTerm:  n.lastSnapTerm,
		Data:              n.snapData,
	}
	peer := p
	go func() {
		reply, err := n.trans.SendInstallSnapshot(peer, args)
		if err != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.role != Leader || n.currentTerm != term {
			return
		}
		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			return
		}
		if args.LastIncludedIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = args.LastIncludedIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
	}()
}

// advanceCommitLocked moves commitIndex forward to the highest index replicated
// on a majority, but only for entries from the current term, as required by the
// Raft commitment rule.
func (n *Node) advanceCommitLocked() {
	last := n.lastLogIndexLocked()
	for idx := last; idx > n.commitIndex && idx > n.lastSnapIndex; idx-- {
		t, ok := n.termAtLocked(idx)
		if !ok || t != n.currentTerm {
			continue
		}
		count := 0
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= len(n.peers)/2+1 {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			break
		}
	}
}

// HandleRequestVote implements the responder side of the RequestVote RPC,
// including the pre-vote variant.
func (n *Node) HandleRequestVote(args *transport.RequestVoteArgs) *transport.RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &transport.RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	upToDate := n.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm)

	if args.PreVote {
		// Pre-vote: grant only if the candidate's log is at least as up to
		// date as ours and we are not currently a leader. This gates
		// disruptive elections from a partitioned node without mutating our
		// persistent term or vote.
		reply.Term = max64(n.currentTerm, args.Term)
		reply.VoteGranted = upToDate && n.role != Leader
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		reply.Term = n.currentTerm
	}
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persistStateLocked()
		n.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

func (n *Node) candidateUpToDateLocked(lastIdx, lastTerm uint64) bool {
	myTerm := n.lastLogTermLocked()
	myIdx := n.lastLogIndexLocked()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= myIdx
}

// HandleAppendEntries implements the responder side of the AppendEntries RPC.
func (n *Node) HandleAppendEntries(args *transport.AppendEntriesArgs) *transport.AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &transport.AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// Consistency check against the snapshot-aware log.
	if args.PrevLogIndex < n.lastSnapIndex {
		// Trim entries that fall within the snapshot prefix.
		skip := n.lastSnapIndex - args.PrevLogIndex
		if skip >= uint64(len(args.Entries)) {
			reply.Success = true
			n.maybeAdvanceFollowerCommitLocked(args.LeaderCommit)
			return reply
		}
		args.PrevLogIndex = n.lastSnapIndex
		args.PrevLogTerm = n.lastSnapTerm
		args.Entries = args.Entries[skip:]
	}

	prevTerm, ok := n.termAtLocked(args.PrevLogIndex)
	if !ok || prevTerm != args.PrevLogTerm {
		// Provide a conflict hint for fast backtracking.
		last := n.lastLogIndexLocked()
		if args.PrevLogIndex > last {
			reply.ConflictIndex = last + 1
			reply.ConflictTerm = 0
		} else if ok {
			reply.ConflictTerm = prevTerm
			reply.ConflictIndex = n.firstIndexOfTermLocked(prevTerm, args.PrevLogIndex)
		} else {
			reply.ConflictIndex = n.lastSnapIndex + 1
		}
		return reply
	}

	// Append any new entries, truncating the first conflicting suffix.
	n.appendFollowerEntriesLocked(args.PrevLogIndex, args.Entries)
	reply.Success = true
	n.maybeAdvanceFollowerCommitLocked(args.LeaderCommit)
	return reply
}

func (n *Node) appendFollowerEntriesLocked(prevIndex uint64, entries []transport.LogEntry) {
	last := n.lastLogIndexLocked()
	for i, e := range entries {
		idx := prevIndex + uint64(i) + 1
		if idx <= last {
			existing, ok := n.termAtLocked(idx)
			if ok && existing == e.Term {
				continue // already have this entry
			}
			// Conflict: truncate everything from idx onward and append the rest.
			_ = n.storage.TruncateSuffix(idx)
		}
		_ = n.storage.AppendLog([]transport.LogEntry{e})
		last = idx
	}
}

func (n *Node) maybeAdvanceFollowerCommitLocked(leaderCommit uint64) {
	if leaderCommit > n.commitIndex {
		last := n.lastLogIndexLocked()
		if leaderCommit < last {
			n.commitIndex = leaderCommit
		} else {
			n.commitIndex = last
		}
		n.applyCond.Broadcast()
	}
}

func (n *Node) firstIndexOfTermLocked(term, hint uint64) uint64 {
	idx := hint
	for idx > n.lastSnapIndex+1 {
		t, ok := n.termAtLocked(idx - 1)
		if !ok || t != term {
			break
		}
		idx--
	}
	return idx
}

// HandleInstallSnapshot implements the responder side of the InstallSnapshot
// RPC. A follower that receives a snapshot newer than its log adopts it
// wholesale and discards the now-redundant prefix.
func (n *Node) HandleInstallSnapshot(args *transport.InstallSnapshotArgs) *transport.InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &transport.InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	if args.LastIncludedIndex <= n.lastSnapIndex {
		return reply // stale snapshot
	}

	// Persist the snapshot, compacting any log it covers.
	snap := Snapshot{LastIncludedIndex: args.LastIncludedIndex, LastIncludedTerm: args.LastIncludedTerm, Data: args.Data}
	_ = n.storage.SaveSnapshot(snap)
	n.lastSnapIndex = args.LastIncludedIndex
	n.lastSnapTerm = args.LastIncludedTerm
	n.snapData = args.Data
	if args.LastIncludedIndex > n.commitIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > n.lastApplied {
		n.lastApplied = args.LastIncludedIndex
	}
	// Hand the snapshot to the application.
	n.deliverSnapshotLocked(args)
	return reply
}

func (n *Node) deliverSnapshotLocked(args *transport.InstallSnapshotArgs) {
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	go func() {
		select {
		case n.applyCh <- msg:
		case <-n.stopCh:
		}
	}()
}

// applyLoop delivers committed entries to the application in order. It runs in
// its own goroutine and is woken by applyCond whenever commitIndex advances.
func (n *Node) applyLoop() {
	for {
		n.mu.Lock()
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		var msgs []ApplyMsg
		for n.lastApplied < n.commitIndex {
			next := n.lastApplied + 1
			if next <= n.lastSnapIndex {
				n.lastApplied = n.lastSnapIndex
				continue
			}
			es, err := n.storage.Entries(next, next+1)
			if err != nil || len(es) == 0 {
				break
			}
			e := es[0]
			n.lastApplied = next
			// Emit every committed entry, including no-op marker entries
			// (nil command). The application skips applying a nil command but
			// still advances its applied-index watermark, which keeps
			// read-index reads from blocking on a no-op forever.
			msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Command, Index: e.Index, Term: e.Term})
		}
		applied := n.lastApplied
		n.maybeSnapshotLocked(applied)
		n.mu.Unlock()
		for _, m := range msgs {
			select {
			case n.applyCh <- m:
			case <-n.stopCh:
				return
			}
		}
	}
}

// maybeSnapshotLocked asks the application for a snapshot once enough entries
// have accumulated past the last snapshot, then compacts the log.
func (n *Node) maybeSnapshotLocked(applied uint64) {
	if n.snapThreshold == 0 || n.snapProvider == nil {
		return
	}
	if applied-n.lastSnapIndex < n.snapThreshold {
		return
	}
	term, ok := n.termAtLocked(applied)
	if !ok {
		return
	}
	data := n.snapProvider()
	snap := Snapshot{LastIncludedIndex: applied, LastIncludedTerm: term, Data: data}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		return
	}
	n.lastSnapIndex = applied
	n.lastSnapTerm = term
	n.snapData = data
}

// SetSnapshotter registers a callback the node invokes to capture application
// state for compaction. It must be called before Start if snapshotting is
// enabled.
func (n *Node) SetSnapshotter(fn func() []byte) {
	n.mu.Lock()
	n.snapProvider = fn
	n.mu.Unlock()
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

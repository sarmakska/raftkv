package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/sarmakska/raftkv/transport"
)

// Storage is the durability contract the Raft core relies on. The core calls
// SaveState before responding to any RPC that mutates term or vote, and
// AppendLog before acknowledging replicated entries, which is what makes
// committed data survive a crash.
type Storage interface {
	// SaveState persists currentTerm and votedFor.
	SaveState(term uint64, votedFor int) error
	// LoadState returns the persisted term and vote, or zero values on a
	// fresh store.
	LoadState() (term uint64, votedFor int, err error)

	// AppendLog durably appends entries to the log.
	AppendLog(entries []transport.LogEntry) error
	// TruncateSuffix removes every entry with index >= from. It is used when
	// a follower has to overwrite a conflicting tail.
	TruncateSuffix(from uint64) error
	// Entries returns log entries in [lo, hi).
	Entries(lo, hi uint64) ([]transport.LogEntry, error)
	// FirstIndex and LastIndex bound the live log. After a snapshot the first
	// live index advances past the compacted prefix.
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)

	// SaveSnapshot persists a snapshot and compacts the log prefix it covers.
	SaveSnapshot(snap Snapshot) error
	// LoadSnapshot returns the most recent snapshot, if any.
	LoadSnapshot() (Snapshot, bool, error)

	Close() error
}

// Snapshot captures the state machine at a point in the log so the prefix can
// be discarded.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// FileStorage is a crash-safe on-disk Storage. The log is an append-only file
// of length-prefixed, CRC-checked records; state and snapshot are small JSON
// files written atomically via write-to-temp-then-rename. On open the log is
// replayed and any torn trailing record (a half-written entry from a crash
// mid-append) is discarded, which is the property the crash-recovery test
// exercises.
type FileStorage struct {
	dir      string
	logPath  string
	logFile  *os.File
	entries  []transport.LogEntry
	snapshot Snapshot
	hasSnap  bool
}

const stateFile = "state.json"
const snapFile = "snapshot.json"
const logFileName = "log.bin"

type persistentState struct {
	Term     uint64 `json:"term"`
	VotedFor int    `json:"voted_for"`
}

// NewFileStorage opens or creates a FileStorage rooted at dir, replaying any
// existing log and snapshot.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{dir: dir, logPath: filepath.Join(dir, logFileName)}
	if err := fs.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := fs.replayLog(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(fs.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fs.logFile = f
	return fs, nil
}

func (fs *FileStorage) loadSnapshot() error {
	snap, ok, err := fs.LoadSnapshot()
	if err != nil {
		return err
	}
	if ok {
		fs.snapshot = snap
		fs.hasSnap = true
	}
	return nil
}

// replayLog reads the append-only log, validating each record's CRC and
// length. A trailing record that is short or fails its checksum is treated as
// a torn write from a crash and is truncated away.
func (fs *FileStorage) replayLog() error {
	f, err := os.Open(fs.logPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var good int64
	for {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean end or torn header: stop here
			}
			return err
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		want := binary.BigEndian.Uint32(hdr[4:8])
		// hdr[8:12] reserved for alignment/version; currently zero.
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			break // torn body
		}
		if crc32.ChecksumIEEE(buf) != want {
			break // corrupted trailing record
		}
		var e transport.LogEntry
		if err := json.Unmarshal(buf, &e); err != nil {
			break
		}
		fs.entries = append(fs.entries, e)
		good += int64(12 + length)
	}
	// Truncate any torn bytes after the last good record so future appends
	// start from a clean offset.
	if err := os.Truncate(fs.logPath, good); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// SaveState writes term and vote atomically.
func (fs *FileStorage) SaveState(term uint64, votedFor int) error {
	return writeJSONAtomic(filepath.Join(fs.dir, stateFile), persistentState{Term: term, VotedFor: votedFor})
}

// LoadState reads the persisted term and vote.
func (fs *FileStorage) LoadState() (uint64, int, error) {
	var ps persistentState
	data, err := os.ReadFile(filepath.Join(fs.dir, stateFile))
	if os.IsNotExist(err) {
		return 0, -1, nil
	}
	if err != nil {
		return 0, -1, err
	}
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, -1, err
	}
	return ps.Term, ps.VotedFor, nil
}

// AppendLog appends entries to the append-only file and the in-memory index.
func (fs *FileStorage) AppendLog(entries []transport.LogEntry) error {
	for _, e := range entries {
		buf, err := json.Marshal(e)
		if err != nil {
			return err
		}
		var hdr [12]byte
		binary.BigEndian.PutUint32(hdr[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(buf))
		if _, err := fs.logFile.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := fs.logFile.Write(buf); err != nil {
			return err
		}
		fs.entries = append(fs.entries, e)
	}
	return fs.logFile.Sync()
}

// TruncateSuffix drops every in-memory entry at or after from and rewrites the
// log file so the on-disk state matches. Rewriting is acceptable because
// conflicting suffixes are rare and short in a healthy cluster.
func (fs *FileStorage) TruncateSuffix(from uint64) error {
	keep := fs.entries[:0:0]
	for _, e := range fs.entries {
		if e.Index < from {
			keep = append(keep, e)
		}
	}
	fs.entries = keep
	return fs.rewriteLog()
}

func (fs *FileStorage) rewriteLog() error {
	if fs.logFile != nil {
		fs.logFile.Close()
	}
	tmp := fs.logPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, e := range fs.entries {
		buf, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		var hdr [12]byte
		binary.BigEndian.PutUint32(hdr[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(buf))
		if _, err := f.Write(hdr[:]); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, fs.logPath); err != nil {
		return err
	}
	fs.logFile, err = os.OpenFile(fs.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	return err
}

// Entries returns the entries with index in [lo, hi). It returns an error if
// the requested range has been compacted into the snapshot.
func (fs *FileStorage) Entries(lo, hi uint64) ([]transport.LogEntry, error) {
	out := make([]transport.LogEntry, 0, hi-lo)
	for _, e := range fs.entries {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e)
		}
	}
	if uint64(len(out)) != hi-lo {
		return nil, fmt.Errorf("entries [%d,%d) not fully available", lo, hi)
	}
	return out, nil
}

// FirstIndex is one past the snapshot's last included index, or 1 on a fresh
// store.
func (fs *FileStorage) FirstIndex() (uint64, error) {
	if fs.hasSnap {
		return fs.snapshot.LastIncludedIndex + 1, nil
	}
	return 1, nil
}

// LastIndex is the index of the final live entry, or the snapshot index if the
// live log is empty.
func (fs *FileStorage) LastIndex() (uint64, error) {
	if len(fs.entries) > 0 {
		return fs.entries[len(fs.entries)-1].Index, nil
	}
	if fs.hasSnap {
		return fs.snapshot.LastIncludedIndex, nil
	}
	return 0, nil
}

// SaveSnapshot persists the snapshot and discards every log entry it covers.
func (fs *FileStorage) SaveSnapshot(snap Snapshot) error {
	if err := writeJSONAtomic(filepath.Join(fs.dir, snapFile), snap); err != nil {
		return err
	}
	fs.snapshot = snap
	fs.hasSnap = true
	keep := fs.entries[:0:0]
	for _, e := range fs.entries {
		if e.Index > snap.LastIncludedIndex {
			keep = append(keep, e)
		}
	}
	fs.entries = keep
	return fs.rewriteLog()
}

// LoadSnapshot reads the most recent persisted snapshot.
func (fs *FileStorage) LoadSnapshot() (Snapshot, bool, error) {
	data, err := os.ReadFile(filepath.Join(fs.dir, snapFile))
	if os.IsNotExist(err) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

// Close flushes and closes the log file.
func (fs *FileStorage) Close() error {
	if fs.logFile != nil {
		return fs.logFile.Close()
	}
	return nil
}

func writeJSONAtomic(path string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sarmakska/raftkv/transport"
)

func TestStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(7, 3); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	term, vote, err := s2.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if term != 7 || vote != 3 {
		t.Fatalf("got term=%d vote=%d, want 7,3", term, vote)
	}
}

func TestLogAppendAndReload(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStorage(dir)
	for i := uint64(1); i <= 5; i++ {
		if err := s.AppendLog([]transport.LogEntry{{Term: 1, Index: i, Command: []byte("c")}}); err != nil {
			t.Fatal(err)
		}
	}
	if li, _ := s.LastIndex(); li != 5 {
		t.Fatalf("LastIndex=%d want 5", li)
	}
	s.Close()

	// Reload and confirm the entries survived.
	s2, _ := NewFileStorage(dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 5 {
		t.Fatalf("after reload LastIndex=%d want 5", li)
	}
	es, err := s2.Entries(1, 6)
	if err != nil || len(es) != 5 {
		t.Fatalf("Entries returned %d entries, err=%v", len(es), err)
	}
}

func TestTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStorage(dir)
	for i := uint64(1); i <= 5; i++ {
		s.AppendLog([]transport.LogEntry{{Term: 1, Index: i}})
	}
	if err := s.TruncateSuffix(3); err != nil {
		t.Fatal(err)
	}
	if li, _ := s.LastIndex(); li != 2 {
		t.Fatalf("LastIndex=%d want 2", li)
	}
	s.Close()
	s2, _ := NewFileStorage(dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 2 {
		t.Fatalf("after reload LastIndex=%d want 2", li)
	}
}

func TestTornTrailingRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStorage(dir)
	for i := uint64(1); i <= 3; i++ {
		s.AppendLog([]transport.LogEntry{{Term: 1, Index: i, Command: []byte("ok")}})
	}
	s.Close()

	// Simulate a crash mid-append by appending garbage bytes to the log file.
	f, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 50, 1, 2, 3, 4, 0, 0, 0, 0, 9, 9})
	f.Close()

	// On reopen the torn record must be discarded and the three good entries
	// preserved.
	s2, _ := NewFileStorage(dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 3 {
		t.Fatalf("LastIndex=%d want 3 after discarding torn record", li)
	}
	es, err := s2.Entries(1, 4)
	if err != nil || len(es) != 3 {
		t.Fatalf("expected 3 clean entries, got %d err=%v", len(es), err)
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStorage(dir)
	for i := uint64(1); i <= 10; i++ {
		s.AppendLog([]transport.LogEntry{{Term: 1, Index: i}})
	}
	if err := s.SaveSnapshot(Snapshot{LastIncludedIndex: 6, LastIncludedTerm: 1, Data: []byte("snap")}); err != nil {
		t.Fatal(err)
	}
	if fi, _ := s.FirstIndex(); fi != 7 {
		t.Fatalf("FirstIndex=%d want 7", fi)
	}
	if li, _ := s.LastIndex(); li != 10 {
		t.Fatalf("LastIndex=%d want 10", li)
	}
	// Compacted entries must be unavailable.
	if _, err := s.Entries(1, 2); err == nil {
		t.Fatal("expected error reading compacted entry")
	}
	s.Close()

	s2, _ := NewFileStorage(dir)
	defer s2.Close()
	snap, ok, err := s2.LoadSnapshot()
	if err != nil || !ok || snap.LastIncludedIndex != 6 {
		t.Fatalf("snapshot not reloaded: ok=%v idx=%d err=%v", ok, snap.LastIncludedIndex, err)
	}
	if string(snap.Data) != "snap" {
		t.Fatalf("snapshot data mismatch: %q", snap.Data)
	}
}

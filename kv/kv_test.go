package kv

import "testing"

func TestApplyAndGet(t *testing.T) {
	s := NewStore()
	if v := s.Apply(Command{Kind: OpPut, Key: "a", Value: "1"}); v != "1" {
		t.Fatalf("put returned %q", v)
	}
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("get a = %q ok=%v", v, ok)
	}
	s.Apply(Command{Kind: OpDelete, Key: "a"})
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected a deleted")
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Kind: OpPut, Key: "x", Value: "10"})
	s.Apply(Command{Kind: OpPut, Key: "y", Value: "20"})
	snap := s.Snapshot()

	s2 := NewStore()
	s2.Restore(snap)
	if v, _ := s2.Get("x"); v != "10" {
		t.Fatalf("restored x=%q", v)
	}
	if v, _ := s2.Get("y"); v != "20" {
		t.Fatalf("restored y=%q", v)
	}
	if s2.Len() != 2 {
		t.Fatalf("restored len=%d", s2.Len())
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	c := Command{Kind: OpPut, Key: "k", Value: "v"}
	b, err := EncodeCommand(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Fatalf("round trip got %+v want %+v", got, c)
	}
}

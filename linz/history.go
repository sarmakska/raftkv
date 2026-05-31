// Package linz records concurrent operation histories and checks them for
// linearizability. The checker is the flagship of raftkv: it is the evidence
// that the consensus core actually provides the guarantee it claims. A history
// is a sequence of invoke/return events on a register, and the checker decides
// whether there exists a sequential ordering of the operations, consistent with
// the real-time order of non-overlapping operations, that a correct
// single-copy register would produce.
package linz

import (
	"sync"
	"time"
)

// Nil is the sentinel value an operation records when a key is absent.
const Nil = "\x00nil\x00"

// OpKind is the kind of register operation.
type OpKind int

const (
	OpGet OpKind = iota
	OpPut
	OpDelete
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return "?"
	}
}

// Op is the logical operation a caller intends to perform.
type Op struct {
	Kind  OpKind
	Key   string
	Value string
}

// Event is a recorded invocation or completion. An operation is two events: an
// invoke at the moment the client sent the request and a return at the moment
// it observed the result. The window between them is when the operation could
// have taken effect.
type Event struct {
	ID     int
	Op     Op
	Output string // observed value for a get; echoed value for a put
	OK     bool   // false means the client never observed a result
	Invoke time.Time
	Return time.Time
}

// History is a thread-safe recorder of events. Multiple client goroutines call
// Invoke and Return concurrently; the checker consumes the finished list.
type History struct {
	mu     sync.Mutex
	events map[int]*Event
	order  []int
	nextID int
	clock  func() time.Time
}

// NewHistory returns an empty History using the wall clock.
func NewHistory() *History {
	return &History{events: map[int]*Event{}, clock: time.Now}
}

// Invoke records the start of an operation and returns its id.
func (h *History) Invoke(op Op) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextID
	h.nextID++
	h.events[id] = &Event{ID: id, Op: op, Invoke: h.clock()}
	h.order = append(h.order, id)
	return id
}

// Return records the completion of operation id with the observed output. ok is
// false when the client could not confirm the result, in which case the
// operation is treated as possibly-applied by the checker.
func (h *History) Return(id int, output string, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, exists := h.events[id]; exists {
		e.Output = output
		e.OK = ok
		e.Return = h.clock()
	}
}

// Events returns a snapshot of the recorded events in invocation order.
func (h *History) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Event, 0, len(h.order))
	for _, id := range h.order {
		out = append(out, *h.events[id])
	}
	return out
}

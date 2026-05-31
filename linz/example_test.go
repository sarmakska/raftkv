package linz_test

import (
	"fmt"
	"time"

	"github.com/sarmakska/raftkv/linz"
)

// ExampleCheck_staleRead records a register history by hand, with the operations
// spaced out in real time so none of them overlap, and shows the checker
// rejecting it. The read at the end returns 1 after 2 has already been written
// and acknowledged, which no single-copy register could ever produce. This is
// the kind of violation the chaos suite exists to catch on the real cluster.
func ExampleCheck_staleRead() {
	h := linz.NewHistory()

	id := h.Invoke(linz.Op{Kind: linz.OpPut, Key: "x", Value: "1"})
	time.Sleep(2 * time.Millisecond)
	h.Return(id, "1", true)

	time.Sleep(2 * time.Millisecond)
	id = h.Invoke(linz.Op{Kind: linz.OpPut, Key: "x", Value: "2"})
	time.Sleep(2 * time.Millisecond)
	h.Return(id, "2", true)

	time.Sleep(2 * time.Millisecond)
	id = h.Invoke(linz.Op{Kind: linz.OpGet, Key: "x"})
	time.Sleep(2 * time.Millisecond)
	h.Return(id, "1", true) // stale: the live value is 2

	r := linz.Check(h)
	fmt.Printf("linearizable=%v key=%q\n", r.Linearizable, r.Key)
	fmt.Println(r.Reason)
	// Output:
	// linearizable=false key="x"
	// no sequential ordering consistent with real-time order
}

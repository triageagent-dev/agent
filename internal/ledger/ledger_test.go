package ledger

import (
	"fmt"
	"testing"
)

func TestLedgerCountsAndForgets(t *testing.T) {
	var l Ledger
	l.Add("ebpf", "node-a", Ticks, 1)
	l.Add("ebpf", "node-a", Ticks, 1)
	l.Add("ebpf", "node-a", Points, 40)
	l.Add("ebpf", "node-b", NoSnapshot, 1)
	l.Add("ebpf", All, Sent, 3)
	l.Add("ebpf", "node-a", Filtered, 0) // zero adds nothing
	snap := l.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("entries = %d, want 3: %+v", len(snap), snap)
	}
	if snap[1].Source != "node-a" || snap[1].Counters[Ticks] != 2 || snap[1].Counters[Points] != 40 {
		t.Errorf("node-a = %+v", snap[1])
	}
	if _, ok := snap[1].Counters[Filtered]; ok {
		t.Error("a zero add created a counter")
	}
	l.Forget("ebpf", map[string]bool{"node-a": true}) // a collector never lists All
	if got := l.Snapshot(); len(got) != 2 || got[1].Source == "node-b" || got[0].Source != All {
		t.Errorf("after Forget: %+v", got)
	}
}

func TestDropCountsRepeatsAndKeepsTheNewestFirst(t *testing.T) {
	var l Ledger
	l.Drop("red_summary", "/cart", "no namespace")
	l.Drop("red_summary", "/cart", "no namespace")
	l.Drop("topology_snapshot", "/quote", "no namespace")

	got := l.Drops()
	if len(got) != 2 {
		t.Fatalf("drops = %+v, want 2 records", got)
	}
	for _, d := range got {
		if d.Frame == "red_summary" && d.Count != 2 {
			t.Errorf("repeat count = %d, want 2", d.Count)
		}
	}
	l.Reset()
	if len(l.Drops()) != 0 {
		t.Error("Reset kept the drop records")
	}
}

func TestDropIsBounded(t *testing.T) {
	var l Ledger
	for i := 0; i < maxDrops+20; i++ {
		l.Drop("red_summary", fmt.Sprintf("/svc-%d", i), "no namespace")
	}
	if n := len(l.Drops()); n != maxDrops {
		t.Errorf("kept %d records, want %d", n, maxDrops)
	}
}

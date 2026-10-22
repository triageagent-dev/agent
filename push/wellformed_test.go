package push

import (
	"testing"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/internal/ledger"
)

func TestDropMalformed_KeepsTheRestOfTheFrame(t *testing.T) {
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	p := &Pusher{log: logr.Discard()}

	f := newRedSummary("c", 1, 60, []ServiceRedMetrics{
		{Namespace: "otel-demo", Service: "ad", RequestCount: 5},
		{Namespace: "", Service: "cart", RequestCount: 7},
		{Namespace: "otel-demo", Service: "", RequestCount: 1},
	})
	got := p.dropMalformed(f)
	if got == nil || len(got.RedSummary.Services) != 1 || got.RedSummary.Services[0].Service != "ad" {
		t.Fatalf("kept %+v, want only otel-demo/ad", got)
	}
	if len(f.RedSummary.Services) != 3 {
		t.Error("the collector's own slice was modified")
	}
	if n := p.malformedDropped.Load(); n != 2 {
		t.Errorf("malformedDropped = %d, want 2", n)
	}
	drops := ledger.Default.Drops()
	if len(drops) != 2 {
		t.Fatalf("drop records = %+v, want 2", drops)
	}
	found := false
	for _, d := range drops {
		if d.Frame == KindRedSummary && d.Subject == "/cart" && d.Reason == reasonNoNamespace {
			found = true
		}
	}
	if !found {
		t.Errorf("no record naming /cart in %+v", drops)
	}
}

func TestDropMalformed_FrameOfOnlyBadEntriesIsDropped(t *testing.T) {
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	p := &Pusher{log: logr.Discard()}
	if f := p.dropMalformed(newRedMetrics("c", 1, 60, []ServiceRedMetrics{{Service: "cart"}})); f != nil {
		t.Errorf("frame of only nameless services kept: %+v", f)
	}
}

func TestDropMalformed_WellFormedFrameIsUntouched(t *testing.T) {
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	p := &Pusher{log: logr.Discard()}
	f := newRedSummary("c", 1, 60, []ServiceRedMetrics{{Namespace: "a", Service: "b"}})
	if got := p.dropMalformed(f); got != f {
		t.Error("a well-formed frame was copied or changed")
	}
	if len(ledger.Default.Drops()) != 0 {
		t.Error("a well-formed frame left drop records")
	}
}

func TestDropMalformed_TopologyNodesAndEdges(t *testing.T) {
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	p := &Pusher{log: logr.Discard()}
	f := &Frame{Kind: KindTopologySnapshot, TopologySnapshot: &TopologySnapshotPayload{
		Nodes: []TopologyNode{{Namespace: "otel-demo", Workload: "ad"}, {Workload: "cart"}},
		Edges: []TopologyEdge{
			{SrcNamespace: "otel-demo", SrcWorkload: "ad", DstNamespace: "otel-demo", DstWorkload: "flagd", Calls: 3},
			{SrcNamespace: "otel-demo", SrcWorkload: "ad", External: "db.example:5432", Calls: 1},
			{SrcWorkload: "cart", DstNamespace: "otel-demo", DstWorkload: "valkey", Calls: 2},
			{SrcNamespace: "otel-demo", SrcWorkload: "ad", DstWorkload: "quote", Calls: 2},
		},
	}}
	got := p.dropMalformed(f)
	if got == nil || len(got.TopologySnapshot.Nodes) != 1 || len(got.TopologySnapshot.Edges) != 2 {
		t.Fatalf("kept %+v, want 1 node and the 2 well-formed edges", got)
	}
	if n := p.malformedDropped.Load(); n != 3 {
		t.Errorf("malformedDropped = %d, want 3", n)
	}
}

package redsource

import (
	"testing"

	"github.com/triageagent-dev/agent/metricset"
)

// flowPoint builds one obi.network.flow.bytes series with the OBI profile's
// attribute keys.
func flowPoint(srcNS, srcOwner, srcType, dstNS, dstOwner, dstType string, bytesVal int) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.src.namespace":  srcNS,
			"k8s.src.owner.name": srcOwner,
			"k8s.src.owner.type": srcType,
			"k8s.dst.namespace":  dstNS,
			"k8s.dst.owner.name": dstOwner,
			"k8s.dst.owner.type": dstType,
		},
		Value: float64(bytesVal),
	}
}

// flowPointWithDirection adds the "direction" attribute to flowPoint.
func flowPointWithDirection(srcNS, srcOwner, srcType, dstNS, dstOwner, dstType, direction string, bytesVal int) metricset.Point {
	p := flowPoint(srcNS, srcOwner, srcType, dstNS, dstOwner, dstType, bytesVal)
	p.Attrs["direction"] = direction
	return p
}

func flowMetric(points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: "obi.network.flow.bytes", Kind: metricset.Counter, Points: points}
}

func TestIsWorkloadOwnerType(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"Deployment", true},
		{"StatefulSet", true},
		{"DaemonSet", true},
		{"Service", false},
		{"Node", false},
		{"CronJob", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isWorkloadOwnerType(tt.in); got != tt.want {
			t.Errorf("isWorkloadOwnerType(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestFilterFlowSamples_DropsEmptyOwnerAndNodeType(t *testing.T) {
	samples := []flowSample{
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "", dstOwner: "", dstOwnerType: "", bytes: 100},
		{srcNamespace: "", srcOwner: "node-a", srcOwnerType: "Node", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Deployment", bytes: 200},
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "kube-system", dstOwner: "node-b", dstOwnerType: "Node", bytes: 300},
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Deployment", bytes: 400},
	}
	out := filterFlowSamples(samples, false)
	if len(out) != 1 {
		t.Fatalf("filtered = %+v, want exactly the one fully-attributed, non-Node row", out)
	}
	if out[0].bytes != 400 {
		t.Errorf("bytes = %d, want 400", out[0].bytes)
	}
}

func TestFilterFlowSamples_DirectionFiltersToRequestOnly(t *testing.T) {
	samples := []flowSample{
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Deployment", direction: "request", bytes: 100},
		{srcNamespace: "ns-a", srcOwner: "billing", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "orders", dstOwnerType: "Deployment", direction: "response", bytes: 900},
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "inventory", dstOwnerType: "Deployment", direction: "unknown", bytes: 50},
	}
	out := filterFlowSamples(samples, true)
	if len(out) != 1 {
		t.Fatalf("filtered = %+v, want exactly the one direction=request row", out)
	}
	if out[0].dstOwner != "billing" || out[0].bytes != 100 {
		t.Errorf("row = %+v, want dst=billing bytes=100", out[0])
	}
}

func TestFilterFlowSamples_NoDirectionLabelKeepsEverything(t *testing.T) {
	// hasDirection=false (Profile.FlowDirectionLabel unset) — every row's
	// direction field is "" regardless of what the metric might otherwise
	// carry, and nothing is filtered on it (documented v1 limitation).
	samples := []flowSample{
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Deployment", bytes: 100},
		{srcNamespace: "ns-a", srcOwner: "billing", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "orders", dstOwnerType: "Deployment", bytes: 900},
	}
	out := filterFlowSamples(samples, false)
	if len(out) != 2 {
		t.Fatalf("filtered = %+v, want both rows kept (no direction signal to filter on)", out)
	}
}

// TestFilterFlowSamples_DedupesServiceVsWorkloadTypedRow reproduces a real
// observed capture pattern (see docs/todo/service-graph-from-obi.md): OBI
// emits a Service-typed row alongside a Deployment-typed row for what is the
// same logical (src,dst) name pair — the Service-typed alternate must be
// dropped once a workload-typed row exists for that pair, and their bytes
// must NOT be double-counted or silently discarded.
func TestFilterFlowSamples_DedupesServiceVsWorkloadTypedRow(t *testing.T) {
	samples := []flowSample{
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Service", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Deployment", bytes: 35112},
		{srcNamespace: "ns-a", srcOwner: "orders", srcOwnerType: "Deployment", dstNamespace: "ns-a", dstOwner: "billing", dstOwnerType: "Service", bytes: 43472},
	}
	out := filterFlowSamples(samples, false)
	if len(out) != 1 {
		t.Fatalf("filtered = %+v, want ONE deduped row (both are the same src/dst name pair)", out)
	}
	if out[0].bytes != 35112+43472 {
		t.Errorf("bytes = %d, want %d (both rows summed, neither dropped silently)", out[0].bytes, 35112+43472)
	}
	if out[0].srcOwnerType != "Service" && out[0].dstOwnerType != "Service" {
		t.Errorf("row = %+v, want the FIRST-seen row's shape (implementation detail: whichever order groups iterate)", out[0])
	}
}

// TestFilterFlowSamples_NoWorkloadTypedAlternativeKeepsServiceTypedRow
// covers the other half of the dedupe rule: when NO row in a name-pair
// group is fully workload-typed, nothing is dropped — losing the edge
// entirely would be worse than reporting it under a Service's name.
func TestFilterFlowSamples_NoWorkloadTypedAlternativeKeepsServiceTypedRow(t *testing.T) {
	samples := []flowSample{
		{srcNamespace: "kube-system", srcOwner: "kube-dns", srcOwnerType: "Service", dstNamespace: "ns-a", dstOwner: "orders", dstOwnerType: "Deployment", bytes: 1580},
		{srcNamespace: "default", srcOwner: "kubernetes", srcOwnerType: "Service", dstNamespace: "kube-system", dstOwner: "konnectivity-agent", dstOwnerType: "Deployment", bytes: 2158203},
	}
	out := filterFlowSamples(samples, false)
	if len(out) != 2 {
		t.Fatalf("filtered = %+v, want both rows kept (each pair has no workload-typed alternative)", out)
	}
}

func TestComputeFlowEdgeDeltas_NilFamilyIsNoOp(t *testing.T) {
	opts := Options{Profile: ProfileOBI}
	opts.setDefaults()
	c := &Collector{opts: opts, prevFlow: make(map[string]map[flowSeries]float64)}
	if got := c.computeFlowEdgeDeltas("addr", nil); got != nil {
		t.Errorf("computeFlowEdgeDeltas(nil) = %+v, want nil", got)
	}
}

func TestComputeFlowEdgeDeltas_FirstSeenSuppressesBaseline(t *testing.T) {
	opts := Options{Profile: ProfileOBI}
	opts.setDefaults()
	c := &Collector{opts: opts, prevFlow: make(map[string]map[flowSeries]float64)}

	fam := flowMetric(flowPoint("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", 1000))
	first := c.computeFlowEdgeDeltas("addr", fam)
	if len(first) != 1 || first[0].bytes != 0 {
		t.Fatalf("first read = %+v, want one row with delta 0 (first-seen suppression)", first)
	}

	fam2 := flowMetric(flowPoint("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", 1400))
	second := c.computeFlowEdgeDeltas("addr", fam2)
	if len(second) != 1 || second[0].bytes != 400 {
		t.Fatalf("second read = %+v, want one row with delta 400", second)
	}
}

func TestComputeFlowEdgeDeltas_DirectionLabelReadWhenPresent(t *testing.T) {
	opts := Options{Profile: ProfileOBI}
	opts.setDefaults()
	c := &Collector{opts: opts, prevFlow: make(map[string]map[flowSeries]float64)}

	fam := flowMetric(flowPointWithDirection("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", "request", 10))
	out := c.computeFlowEdgeDeltas("addr", fam)
	if len(out) != 1 || out[0].direction != "request" {
		t.Fatalf("computeFlowEdgeDeltas = %+v, want direction=request", out)
	}
}

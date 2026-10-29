package push

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFrameRoundTrip(t *testing.T) {
	app := &VirtualApp{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ops"},
	}
	app.Status.ComponentsReady = "1/1"
	app.Status.Conditions = []Condition{{Type: "Ready", Status: corev1.ConditionTrue}}

	frames := []*Frame{
		newCertHello("cluster-a", "example-tenant", "1.3.8", []byte("test-cert"), []byte("test-proof"), []string{"ops"}),
		newSnapshot("cluster-a", []*VirtualApp{app}),
		newSnapshotEnd("cluster-a"),
		newAppDelta("cluster-a", OpUpdate, app),
		newK8sEvent("cluster-a", &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "evt", Namespace: "ops"},
			Reason:     "BackOff",
			Type:       corev1.EventTypeWarning,
		}),
		newLogMetrics("cluster-a", 1720000000, 60, []ServiceLogMetrics{
			{Namespace: "ops", Service: "api", ErrorCount: 42, WarnCount: 7, TotalCount: 1000, Sample: "panic: nil pointer"},
		}),
		newRedMetrics("cluster-a", 1720000000, 60, []ServiceRedMetrics{
			{Namespace: "ops", Service: "api", RequestCount: 1000, ErrorCount: 42, Sample: "GET /api/orders -> 502 x41"},
		}),
		newSensorState("cluster-a", 1720000000, []SensorStatus{
			{Node: "node-1", Pod: "sensor-node-1", Connected: true, LastSeen: 1720000000,
				Roles: []SensorRoleStatus{{Role: "ebpf"}}},
		}, nil, nil),
		newHeartbeat("cluster-a"),
	}

	for _, f := range frames {
		data, err := encode(f)
		if err != nil {
			t.Fatalf("encode %s: %v", f.Kind, err)
		}
		got, err := decode(data)
		if err != nil {
			t.Fatalf("decode %s: %v", f.Kind, err)
		}
		if got.V != ProtocolVersion || got.Kind != f.Kind || got.Cluster != "cluster-a" {
			t.Errorf("round-trip mismatch for %s: %+v", f.Kind, got)
		}
	}
}

// TestSnapshotPreservesStatus guards the wire-compat contract: the fields triage
// classifies on (ComponentsReady, Ready condition, component objects) survive
// marshal→unmarshal.
func TestSnapshotPreservesStatus(t *testing.T) {
	app := &VirtualApp{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ops", Labels: map[string]string{"app.kubernetes.io/name": "api"}},
	}
	app.Status.ComponentsReady = "0/2"
	app.Status.Conditions = []Condition{{Type: "Ready", Status: corev1.ConditionFalse, Message: "crash loop"}}
	app.Status.Objects = []ObjectStatus{
		{Kind: "Deployment", Name: "api", Status: "InProgress"},
	}

	data, err := encode(newSnapshot("cluster-a", []*VirtualApp{app}))
	if err != nil {
		t.Fatal(err)
	}
	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	ra := got.Snapshot.Apps[0]
	if ra.Status.ComponentsReady != "0/2" {
		t.Errorf("componentsReady = %q", ra.Status.ComponentsReady)
	}
	if len(ra.Status.Conditions) != 1 || ra.Status.Conditions[0].Status != corev1.ConditionFalse {
		t.Errorf("conditions not preserved: %+v", ra.Status.Conditions)
	}
	if len(ra.Status.Objects) != 1 || ra.Status.Objects[0].Kind != "Deployment" {
		t.Errorf("component objects not preserved: %+v", ra.Status.Objects)
	}
}

// TestLogMetricsFrame guards the log_metrics wire contract: field names/values
// (windowStart, windowSec, services[].{namespace,service,errorCount,warnCount,
// totalCount,sample}) must survive marshal→unmarshal exactly, and the JSON keys must
// match the receiver's LogMetricsPayload (internal/remoteagent/protocol.go).
func TestLogMetricsFrame(t *testing.T) {
	f := newLogMetrics("cluster-a", 1720000000, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 42, WarnCount: 7, TotalCount: 1000, Sample: "panic: nil pointer"},
		{Namespace: "ops", Service: "web", ErrorCount: 11},
	})
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}

	// Exact JSON field names, since these are the wire contract with a separate repo.
	raw := string(data)
	for _, want := range []string{
		`"kind":"log_metrics"`,
		`"windowStart":1720000000`,
		`"windowSec":60`,
		`"namespace":"ops"`,
		`"service":"api"`,
		`"errorCount":42`,
		`"warnCount":7`,
		`"totalCount":1000`,
		`"sample":"panic: nil pointer"`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// warnCount/totalCount are omitempty — the second service (zero warn/total) must
	// not carry them, so the receiver's zero-value defaults apply, not an explicit 0
	// that would look identical on decode but wastes wire bytes at scale.
	if strings.Contains(raw, `"service":"web","errorCount":11,"warnCount"`) {
		t.Errorf("omitempty fields present on zero-value service: %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.LogMetrics == nil {
		t.Fatal("LogMetrics payload missing after round-trip")
	}
	if got.LogMetrics.WindowStart != 1720000000 || got.LogMetrics.WindowSec != 60 {
		t.Errorf("window = (%d, %d), want (1720000000, 60)", got.LogMetrics.WindowStart, got.LogMetrics.WindowSec)
	}
	if len(got.LogMetrics.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(got.LogMetrics.Services))
	}
	s0 := got.LogMetrics.Services[0]
	if s0.Namespace != "ops" || s0.Service != "api" || s0.ErrorCount != 42 || s0.WarnCount != 7 || s0.TotalCount != 1000 || s0.Sample != "panic: nil pointer" {
		t.Errorf("service[0] round-trip mismatch: %+v", s0)
	}
}

func TestTruncateSample(t *testing.T) {
	short := "short sample"
	if got := TruncateSample(short); got != short {
		t.Errorf("TruncateSample(%q) = %q, want unchanged", short, got)
	}
	long := strings.Repeat("x", 300)
	got := TruncateSample(long)
	if len(got) != MaxLogMetricsSampleLen {
		t.Errorf("TruncateSample length = %d, want %d", len(got), MaxLogMetricsSampleLen)
	}
}

// TestRedMetricsFrame guards the red_metrics wire contract (docs/adr/0009-red-metrics-
// beyla.md): field names/values must survive marshal→unmarshal exactly, and the JSON
// keys must match the receiver's RedMetricsPayload (internal/remoteagent/protocol.go).
func TestRedMetricsFrame(t *testing.T) {
	f := newRedMetrics("cluster-a", 1720000000, 60, []ServiceRedMetrics{
		{Namespace: "ops", Service: "api", RequestCount: 1000, ErrorCount: 42, Sample: "GET /api/orders -> 502 x41"},
		{Namespace: "ops", Service: "web", RequestCount: 500, ErrorCount: 11},
	})
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	for _, want := range []string{
		`"kind":"red_metrics"`,
		`"windowStart":1720000000`,
		`"windowSec":60`,
		`"namespace":"ops"`,
		`"service":"api"`,
		`"requestCount":1000`,
		`"errorCount":42`,
		// json.Marshal HTML-escapes '>' to > by default.
		`"sample":"GET /api/orders -` + "\\u003e" + ` 502 x41"`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// sample is omitempty — the second service (no Sample) must not carry it.
	if strings.Contains(raw, `"service":"web","requestCount":500,"errorCount":11,"sample"`) {
		t.Errorf("omitempty sample present on zero-value service: %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.RedMetrics == nil {
		t.Fatal("RedMetrics payload missing after round-trip")
	}
	if got.RedMetrics.WindowStart != 1720000000 || got.RedMetrics.WindowSec != 60 {
		t.Errorf("window = (%d, %d), want (1720000000, 60)", got.RedMetrics.WindowStart, got.RedMetrics.WindowSec)
	}
	if len(got.RedMetrics.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(got.RedMetrics.Services))
	}
	s0 := got.RedMetrics.Services[0]
	if s0.Namespace != "ops" || s0.Service != "api" || s0.RequestCount != 1000 || s0.ErrorCount != 42 || s0.Sample != "GET /api/orders -> 502 x41" {
		t.Errorf("service[0] round-trip mismatch: %+v", s0)
	}
}

func TestTruncateRedSample(t *testing.T) {
	short := "GET /api/orders -> 502 x41"
	if got := TruncateRedSample(short); got != short {
		t.Errorf("TruncateRedSample(%q) = %q, want unchanged", short, got)
	}
	long := strings.Repeat("x", 300)
	got := TruncateRedSample(long)
	if len(got) != MaxRedMetricsSampleLen {
		t.Errorf("TruncateRedSample length = %d, want %d", len(got), MaxRedMetricsSampleLen)
	}
}

// TestNewRedMetricsCapsAtMax verifies newRedMetrics defensively re-caps an
// oversized services slice as a last line of defense before the wire — mirrors the
// defensive Sample re-truncation newLogMetrics already does.
// Operation is controlled by the instrumented service (a route or RPC name),
// so it is clipped on a rune boundary before send; StillBreaching is omitted
// from the JSON when false, so an older core sees an unchanged entry.
func TestRedMetricsOperationTruncatedAndStillBreachingOmitEmpty(t *testing.T) {
	long := strings.Repeat("a", MaxOperationLen-1) + "\u00e9tail"
	if got := TruncateOperation(long); len(got) != MaxOperationLen-1 || !utf8.ValidString(got) {
		t.Errorf("TruncateOperation kept %d bytes (valid UTF-8: %v), want %d on a rune boundary", len(got), utf8.ValidString(got), MaxOperationLen-1)
	}
	if got := TruncateOperation("GET /a"); got != "GET /a" {
		t.Errorf("short operation changed: %q", got)
	}
	f := newRedMetrics("c", 1, 60, []ServiceRedMetrics{
		{Namespace: "ns", Service: "a", Breach: BreachOpErrors, Operation: strings.Repeat("x", 4*MaxOperationLen)},
		{Namespace: "ns", Service: "b", Breach: BreachOpLatency, StillBreaching: true},
	})
	if n := len(f.RedMetrics.Services[0].Operation); n != MaxOperationLen {
		t.Errorf("operation on the wire is %d bytes, want %d", n, MaxOperationLen)
	}
	a, _ := json.Marshal(f.RedMetrics.Services[0])
	b, _ := json.Marshal(f.RedMetrics.Services[1])
	if strings.Contains(string(a), "stillBreaching") || !strings.Contains(string(b), `"stillBreaching":true`) {
		t.Errorf("stillBreaching encoding: %s / %s", a, b)
	}
	// stream names a log step's stream and is omitted on any other entry.
	plain, _ := json.Marshal(ServiceLogMetrics{Namespace: "ns", Service: "a", ErrorCount: 12})
	warn, _ := json.Marshal(ServiceLogMetrics{Namespace: "ns", Service: "a", Breach: BreachLogStep, Stream: LogStreamWarn})
	if strings.Contains(string(plain), "stream") || !strings.Contains(string(warn), `"stream":"warn"`) {
		t.Errorf("stream encoding: %s / %s", plain, warn)
	}
}

func TestNewRedMetricsCapsAtMax(t *testing.T) {
	services := make([]ServiceRedMetrics, MaxRedMetricsServices+10)
	for i := range services {
		services[i] = ServiceRedMetrics{Namespace: "ops", Service: fmt.Sprintf("svc-%d", i)}
	}
	f := newRedMetrics("cluster-a", 1720000000, 60, services)
	if len(f.RedMetrics.Services) != MaxRedMetricsServices {
		t.Errorf("services = %d, want %d (defensively capped)", len(f.RedMetrics.Services), MaxRedMetricsServices)
	}
}

// TestRedSummaryFrame guards the red_summary wire contract: field names/values
// must survive marshal→unmarshal exactly, and the JSON keys must match the
// receiver's RedSummaryPayload (internal/remoteagent/protocol.go). Sample is
// deliberately left unset on every service — the contract for this frame is
// that a summary never carries a sample.
func TestRedSummaryFrame(t *testing.T) {
	f := newRedSummary("cluster-a", 1720000000, 60, []ServiceRedMetrics{
		{Namespace: "ops", Service: "api", RequestCount: 1000, ErrorCount: 42, P99Ms: 123.5},
		{Namespace: "ops", Service: "web", RequestCount: 500, ErrorCount: 0},
	})
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	for _, want := range []string{
		`"kind":"red_summary"`,
		`"windowStart":1720000000`,
		`"windowSec":60`,
		`"namespace":"ops"`,
		`"service":"api"`,
		`"requestCount":1000`,
		`"errorCount":42`,
		`"p99Ms":123.5`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// sample is omitempty and always unset for a summary; errorCount is a
	// genuine 0 for "web" and must still be present (not omitempty).
	if strings.Contains(raw, `"sample"`) {
		t.Errorf("encoded frame carries a sample; summaries must never include one: %s", raw)
	}
	if !strings.Contains(raw, `"service":"web","requestCount":500,"errorCount":0`) {
		t.Errorf("encoded frame missing zero-value errorCount for web: %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.RedSummary == nil {
		t.Fatal("RedSummary payload missing after round-trip")
	}
	if got.RedMetrics != nil {
		t.Error("RedMetrics payload set on a red_summary frame, want nil (the two frame kinds are independent)")
	}
	if got.RedSummary.WindowStart != 1720000000 || got.RedSummary.WindowSec != 60 {
		t.Errorf("window = (%d, %d), want (1720000000, 60)", got.RedSummary.WindowStart, got.RedSummary.WindowSec)
	}
	if len(got.RedSummary.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(got.RedSummary.Services))
	}
	s0 := got.RedSummary.Services[0]
	if s0.Namespace != "ops" || s0.Service != "api" || s0.RequestCount != 1000 || s0.ErrorCount != 42 || s0.P99Ms != 123.5 || s0.Sample != "" {
		t.Errorf("service[0] round-trip mismatch: %+v", s0)
	}
}

// TestNewRedSummaryCapsAtMax mirrors TestNewRedMetricsCapsAtMax: newRedSummary
// defensively re-caps an oversized services slice as a last line of defense
// before the wire.
func TestNewRedSummaryCapsAtMax(t *testing.T) {
	services := make([]ServiceRedMetrics, MaxRedMetricsServices+10)
	for i := range services {
		services[i] = ServiceRedMetrics{Namespace: "ops", Service: fmt.Sprintf("svc-%d", i)}
	}
	f := newRedSummary("cluster-a", 1720000000, 60, services)
	if len(f.RedSummary.Services) != MaxRedMetricsServices {
		t.Errorf("services = %d, want %d (defensively capped)", len(f.RedSummary.Services), MaxRedMetricsServices)
	}
}

// TestUseSummaryFrame guards the use_summary wire contract: field names/values
// must survive marshal→unmarshal exactly, and the JSON keys must match the
// receiver's UseSummaryPayload (internal/remoteagent/protocol.go). Unlike
// ServiceUseMetrics, ServiceUseSummary has no Sample field at all — there is
// nothing to assert is absent, only that cpuPsiPct/memPsiPct/ioPsiPct survive
// as genuine (including zero) values.
func TestUseSummaryFrame(t *testing.T) {
	f := newUseSummary("cluster-a", 1720000000, 60, []ServiceUseSummary{
		{Namespace: "ops", Service: "api", CPUPsiPct: 12.5, MemPsiPct: 34, IOPsiPct: 0, OOMKills: 2},
		{Namespace: "ops", Service: "web", CPUPsiPct: 0, MemPsiPct: 0, IOPsiPct: 0},
	})
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	for _, want := range []string{
		`"kind":"use_summary"`,
		`"windowStart":1720000000`,
		`"windowSec":60`,
		`"namespace":"ops"`,
		`"service":"api"`,
		`"cpuPsiPct":12.5`,
		`"memPsiPct":34`,
		`"oomKills":2`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// oomKills is omitempty and genuinely zero for "web"; cpuPsiPct/memPsiPct/
	// ioPsiPct are NOT omitempty and must still be present as zero values.
	if !strings.Contains(raw, `"service":"web","cpuPsiPct":0,"memPsiPct":0,"ioPsiPct":0}`) {
		t.Errorf("encoded frame missing zero-value PSI fields for web: %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.UseSummary == nil {
		t.Fatal("UseSummary payload missing after round-trip")
	}
	if got.UseMetrics != nil {
		t.Error("UseMetrics payload set on a use_summary frame, want nil (the two frame kinds are independent)")
	}
	if got.UseSummary.WindowStart != 1720000000 || got.UseSummary.WindowSec != 60 {
		t.Errorf("window = (%d, %d), want (1720000000, 60)", got.UseSummary.WindowStart, got.UseSummary.WindowSec)
	}
	if len(got.UseSummary.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(got.UseSummary.Services))
	}
	s0 := got.UseSummary.Services[0]
	if s0.Namespace != "ops" || s0.Service != "api" || s0.CPUPsiPct != 12.5 || s0.MemPsiPct != 34 || s0.IOPsiPct != 0 || s0.OOMKills != 2 {
		t.Errorf("service[0] round-trip mismatch: %+v", s0)
	}
}

// TestNewUseSummaryCapsAtMax mirrors TestNewRedSummaryCapsAtMax: newUseSummary
// defensively re-caps an oversized services slice as a last line of defense
// before the wire.
func TestNewUseSummaryCapsAtMax(t *testing.T) {
	services := make([]ServiceUseSummary, MaxUseMetricsServices+10)
	for i := range services {
		services[i] = ServiceUseSummary{Namespace: "ops", Service: fmt.Sprintf("svc-%d", i)}
	}
	f := newUseSummary("cluster-a", 1720000000, 60, services)
	if len(f.UseSummary.Services) != MaxUseMetricsServices {
		t.Errorf("services = %d, want %d (defensively capped)", len(f.UseSummary.Services), MaxUseMetricsServices)
	}
}

func TestChunkServices(t *testing.T) {
	if chunkServices(nil) != nil {
		t.Errorf("chunkServices(nil) should be nil")
	}
	services := make([]ServiceLogMetrics, 250)
	for i := range services {
		services[i] = ServiceLogMetrics{Namespace: "ops", Service: fmt.Sprintf("svc-%d", i)}
	}
	chunks := chunkServices(services)
	if len(chunks) != 3 {
		t.Fatalf("chunks = %d, want 3 (100+100+50)", len(chunks))
	}
	if len(chunks[0]) != MaxLogMetricsServices || len(chunks[1]) != MaxLogMetricsServices || len(chunks[2]) != 50 {
		t.Errorf("chunk sizes = %d, %d, %d, want 100, 100, 50", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}
	// every service must appear exactly once across chunks
	seen := make(map[string]bool)
	for _, c := range chunks {
		for _, s := range c {
			if seen[s.Service] {
				t.Errorf("service %q appears in more than one chunk", s.Service)
			}
			seen[s.Service] = true
		}
	}
	if len(seen) != 250 {
		t.Errorf("total unique services across chunks = %d, want 250", len(seen))
	}
}

func TestTopologySnapshotFrame(t *testing.T) {
	nodes := []TopologyNode{
		{Namespace: "rag", Workload: "app"},
		{Namespace: "rag", Workload: "qdrant"},
	}
	edges := []TopologyEdge{
		{SrcNamespace: "rag", SrcWorkload: "app", DstNamespace: "rag", DstWorkload: "qdrant", Calls: 42, Protocol: "grpc"},
		{SrcNamespace: "rag", SrcWorkload: "app", External: "api.example.com:443", Calls: 5, Protocol: "http"},
	}
	f := newTopologySnapshot("cluster-a", 1720000000, 600, nodes, edges)

	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, want := range []string{
		`"kind":"topology_snapshot"`,
		`"capturedAt":1720000000`,
		`"windowSec":600`,
		`"namespace":"rag"`,
		`"workload":"qdrant"`,
		`"srcWorkload":"app"`,
		`"dstWorkload":"qdrant"`,
		`"external":"api.example.com:443"`,
		`"calls":42`,
		`"protocol":"grpc"`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// Dst fields are omitempty and must not appear on the external edge.
	if strings.Contains(raw, `"external":"api.example.com:443","calls":5,"protocol":"http","dstNamespace"`) {
		t.Errorf("external edge unexpectedly carries a dstNamespace field: %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.TopologySnapshot == nil {
		t.Fatal("TopologySnapshot payload missing after round-trip")
	}
	if got.TopologySnapshot.CapturedAt != 1720000000 || got.TopologySnapshot.WindowSec != 600 {
		t.Errorf("window = (%d, %d), want (1720000000, 600)", got.TopologySnapshot.CapturedAt, got.TopologySnapshot.WindowSec)
	}
	if len(got.TopologySnapshot.Nodes) != 2 || len(got.TopologySnapshot.Edges) != 2 {
		t.Fatalf("nodes/edges = %d/%d, want 2/2", len(got.TopologySnapshot.Nodes), len(got.TopologySnapshot.Edges))
	}
	e0 := got.TopologySnapshot.Edges[0]
	if e0.SrcWorkload != "app" || e0.DstWorkload != "qdrant" || e0.Calls != 42 || e0.Protocol != "grpc" || e0.External != "" {
		t.Errorf("edge[0] round-trip mismatch: %+v", e0)
	}
	e1 := got.TopologySnapshot.Edges[1]
	if e1.External != "api.example.com:443" || e1.DstNamespace != "" || e1.DstWorkload != "" {
		t.Errorf("edge[1] round-trip mismatch: %+v", e1)
	}
}

func TestTruncateTopologyExternal(t *testing.T) {
	short := "qdrant.rag.svc.cluster.local:6333"
	if got := TruncateTopologyExternal(short); got != short {
		t.Errorf("TruncateTopologyExternal(%q) = %q, want unchanged", short, got)
	}
	long := strings.Repeat("x", 300)
	got := TruncateTopologyExternal(long)
	if len(got) != MaxTopologyExternalLen {
		t.Errorf("TruncateTopologyExternal length = %d, want %d", len(got), MaxTopologyExternalLen)
	}
}

// TestNewTopologySnapshotCapsAtMax verifies newTopologySnapshot defensively
// re-caps oversized nodes/edges slices and re-truncates External as a last line
// of defense before the wire — mirrors TestNewRedMetricsCapsAtMax.
func TestNewTopologySnapshotCapsAtMax(t *testing.T) {
	nodes := make([]TopologyNode, MaxTopologyNodes+10)
	for i := range nodes {
		nodes[i] = TopologyNode{Namespace: "ops", Workload: fmt.Sprintf("svc-%d", i)}
	}
	edges := make([]TopologyEdge, MaxTopologyEdges+10)
	for i := range edges {
		edges[i] = TopologyEdge{
			SrcNamespace: "ops", SrcWorkload: "api",
			External: strings.Repeat("y", 300), Calls: int64(i),
		}
	}
	f := newTopologySnapshot("cluster-a", 1720000000, 600, nodes, edges)
	if len(f.TopologySnapshot.Nodes) != MaxTopologyNodes {
		t.Errorf("nodes = %d, want %d (defensively capped)", len(f.TopologySnapshot.Nodes), MaxTopologyNodes)
	}
	if len(f.TopologySnapshot.Edges) != MaxTopologyEdges {
		t.Errorf("edges = %d, want %d (defensively capped)", len(f.TopologySnapshot.Edges), MaxTopologyEdges)
	}
	for _, e := range f.TopologySnapshot.Edges {
		if len(e.External) != MaxTopologyExternalLen {
			t.Errorf("edge External length = %d, want %d (defensively re-truncated)", len(e.External), MaxTopologyExternalLen)
		}
	}
}

// TestAppFramesCarryNoAPIVersion pins the wire-compatibility claim in this
// package's doc comment: VirtualApp carries no TypeMeta at all, so a snapshot
// or app_delta frame never puts an apiVersion on the wire for the receiver to
// disagree with.
func TestAppFramesCarryNoAPIVersion(t *testing.T) {
	app := &VirtualApp{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ops"}}
	app.Status.ComponentsReady = "1/1"

	for _, f := range []*Frame{
		newSnapshot("cluster-a", []*VirtualApp{app}),
		newAppDelta("cluster-a", OpUpdate, app),
	} {
		data, err := encode(f)
		if err != nil {
			t.Fatalf("encode %s: %v", f.Kind, err)
		}
		if strings.Contains(string(data), "apiVersion") {
			t.Errorf("%s frame leaked apiVersion onto the wire: %s", f.Kind, data)
		}
	}
}

// TestSensorStateFrame guards the sensor_state wire contract (ADR-0015 phase
// 1): field names/values must survive marshal→unmarshal exactly, and the
// JSON keys must match the core's mirrored payload. Covers a role with a
// full report and a role named in the hello with no state yet (only Role
// set, everything else omitted).
func TestSensorStateFrame(t *testing.T) {
	f := newSensorState("cluster-a", 1720000000, []SensorStatus{
		{
			Node: "node-1", Pod: "sensor-node-1", Version: "1.4.0", Connected: true, LastSeen: 1720000060,
			Gaps: 2, Goroutines: 40, HeapBytes: 1024, SysBytes: 2048, CPUSeconds: 3.5,
			Reconnects: 1, SendErrors: 0, SendMicros: 900, AckRTTMicros: 1500, TickBytes: 4096, ConnectMicros: 250,
			Roles: []SensorRoleStatus{
				{Role: "ebpf", GatherMicros: 800, Metrics: 12, DataPoints: 120, GatherErrors: 1, LastError: "read: closed"},
				{Role: "nodeexporter"},
			},
		},
	}, nil, nil)
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	for _, want := range []string{
		`"kind":"sensor_state"`,
		`"sensorState":{"at":1720000000,"sensors":[`,
		`"node":"node-1"`,
		`"pod":"sensor-node-1"`,
		`"version":"1.4.0"`,
		`"connected":true`,
		`"lastSeen":1720000060`,
		`"gaps":2`,
		`"goroutines":40`,
		`"heapBytes":1024`,
		`"sysBytes":2048`,
		`"cpuSeconds":3.5`,
		`"reconnects":1`,
		`"sendMicros":900`,
		`"ackRttMicros":1500`,
		`"tickBytes":4096`,
		`"connectMicros":250`,
		`"role":"ebpf"`,
		`"gatherMicros":800`,
		`"metrics":12`,
		`"dataPoints":120`,
		`"gatherErrors":1`,
		`"lastError":"read: closed"`,
		`{"role":"nodeexporter"}`,
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("encoded frame missing %q; got %s", want, raw)
		}
	}
	// sendErrors is omitempty and genuinely zero: must not appear on the wire.
	if strings.Contains(raw, "sendErrors") {
		t.Errorf("sendErrors present despite being zero (omitempty): %s", raw)
	}

	got, err := decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.SensorState == nil || len(got.SensorState.Sensors) != 1 {
		t.Fatalf("decoded sensorState = %+v", got.SensorState)
	}
	s := got.SensorState.Sensors[0]
	if len(s.Roles) != 2 || s.Roles[1] != (SensorRoleStatus{Role: "nodeexporter"}) {
		t.Errorf("role with no state should decode with only Role set: %+v", s.Roles)
	}
}

// TestSensorStateFrame_CarriesIntake pins the intake keys of the sensor_state
// frame, which the core and the UI read by name, and that a frame with no
// intake has no such key.
func TestSensorStateFrame_CarriesIntake(t *testing.T) {
	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	f := newSensorState("cluster-a", 1720000000, nil, nil, []OTLPIntakeStatus{{
		Sender: "obs/collector", Window: 60, Exports: 4, Points: 100, Unread: 80, Refused: 5, RefusedExports: 1,
		Held: 20, Sources: 2, UnreadByName: []NameCount{{Name: "foo.a", Points: 60}},
	}})
	data, err := encode(f)
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		SensorState map[string]any `json:"sensorState"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatal(err)
	}
	list, _ := frame.SensorState["intake"].([]any)
	if len(list) != 1 {
		t.Fatalf("intake = %v, want one entry; frame %s", frame.SensorState["intake"], data)
	}
	entry := list[0].(map[string]any)
	want := []string{"exports", "held", "points", "refused", "refusedExports", "sender", "sources", "unread", "unreadByName", "window"}
	if got := keys(entry); !slices.Equal(got, want) {
		t.Errorf("intake keys = %v, want %v", got, want)
	}
	names := entry["unreadByName"].([]any)
	if got := keys(names[0].(map[string]any)); !slices.Equal(got, []string{"name", "points"}) {
		t.Errorf("unreadByName keys = %v, want name and points", got)
	}

	data, err = encode(newSensorState("cluster-a", 1720000000, []SensorStatus{{Node: "n"}}, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"intake"`) {
		t.Errorf("a frame with no intake carries the key: %s", data)
	}
}

// TestBaselineGateFieldsWire pins the baseline-gate fields on the three gated
// service structs (docs/todo/baseline-gates.md): exact JSON keys, omitted when
// zero, and a lossless round-trip. The core's mirror must carry the same tags.
func TestBaselineGateFieldsWire(t *testing.T) {
	wantKeys := []string{`"breach":"op-errors"`, `"operation":"POST /pay/Charge"`, `"observed":100`, `"baseline":0.5`, `"evidenceSec":300`}
	check := func(name string, f *Frame, plain func(*Frame) bool) {
		t.Helper()
		data, err := encode(f)
		if err != nil {
			t.Fatal(err)
		}
		raw := string(data)
		for _, want := range wantKeys {
			if !strings.Contains(raw, want) {
				t.Errorf("%s: encoded frame missing %q; got %s", name, want, raw)
			}
		}
		got, err := decode(data)
		if err != nil {
			t.Fatal(err)
		}
		if !plain(got) {
			t.Errorf("%s: round-trip lost baseline fields", name)
		}
	}
	check("red", newRedMetrics("c", 1, 60, []ServiceRedMetrics{{
		Namespace: "ops", Service: "pay", RequestCount: 30, ErrorCount: 30,
		Breach: BreachOpErrors, Operation: "POST /pay/Charge", Observed: 100, Baseline: 0.5, EvidenceSec: 300,
	}}), func(g *Frame) bool {
		s := g.RedMetrics.Services[0]
		return s.Breach == BreachOpErrors && s.Operation == "POST /pay/Charge" && s.Observed == 100 && s.Baseline == 0.5 && s.EvidenceSec == 300
	})
	check("use", newUseMetrics("c", 1, 60, []ServiceUseMetrics{{
		Namespace: "ops", Service: "pay",
		Breach: BreachOpErrors, Operation: "POST /pay/Charge", Observed: 100, Baseline: 0.5, EvidenceSec: 300,
	}}), func(g *Frame) bool {
		s := g.UseMetrics.Services[0]
		return s.Breach == BreachOpErrors && s.Operation == "POST /pay/Charge" && s.Observed == 100 && s.Baseline == 0.5 && s.EvidenceSec == 300
	})
	check("log", newLogMetrics("c", 1, 60, []ServiceLogMetrics{{
		Namespace: "ops", Service: "pay", ErrorCount: 60,
		Breach: BreachOpErrors, Operation: "POST /pay/Charge", Observed: 100, Baseline: 0.5, EvidenceSec: 300,
	}}), func(g *Frame) bool {
		s := g.LogMetrics.Services[0]
		return s.Breach == BreachOpErrors && s.Operation == "POST /pay/Charge" && s.Observed == 100 && s.Baseline == 0.5 && s.EvidenceSec == 300
	})

	// All five are omitempty: an entry without a baseline arm carries none.
	data, err := encode(newRedMetrics("c", 1, 60, []ServiceRedMetrics{{Namespace: "ops", Service: "api", RequestCount: 5, ErrorCount: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"breach"`, `"operation"`, `"observed"`, `"baseline"`, `"evidenceSec"`} {
		if strings.Contains(string(data), key) {
			t.Errorf("zero-value entry carries %s: %s", key, data)
		}
	}
	if BreachErrors != "errors" || BreachOpErrors != "op-errors" || BreachLatency != "latency" || BreachOpLatency != "op-latency" ||
		BreachCPU != "cpu" || BreachThrottle != "throttle" || BreachLogStep != "log-step" {
		t.Error("Breach constants changed: they are wire values")
	}
}

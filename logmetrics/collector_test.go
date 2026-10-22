package logmetrics

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/logexporter"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// fakeSender records every SendLogMetrics call for assertions.
type fakeSender struct {
	mu    sync.Mutex
	calls [][]push.ServiceLogMetrics
}

func (f *fakeSender) SendLogMetrics(_ int64, _ int, services []push.ServiceLogMetrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]push.ServiceLogMetrics(nil), services...)
	f.calls = append(f.calls, cp)
}

func (f *fakeSender) allServices() []push.ServiceLogMetrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []push.ServiceLogMetrics
	for _, c := range f.calls {
		out = append(out, c...)
	}
	return out
}

func (f *fakeSender) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeSource is a mutable metricset.Source standing in for the sensor stream
// hub (sensorstream.Hub): target -> metricset.Set, guarded by a mutex so a
// test can drive several runOnce calls with different counter values.
//
// targets and sets are tracked separately so a test can advertise a target
// via Targets() whose Set() call then fails — the real Hub can do this when
// a snapshot goes stale between the two calls (sensorstream/hub.go).
type fakeSource struct {
	mu      sync.Mutex
	targets map[string]bool
	sets    map[string]metricset.Set
}

func newFakeSource() *fakeSource {
	return &fakeSource{targets: make(map[string]bool), sets: make(map[string]metricset.Set)}
}

// set installs (or replaces) target's metric set.
func (f *fakeSource) set(target string, s metricset.Set) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets[target] = true
	f.sets[target] = s
}

// advertiseWithoutSet lists target among Targets() but leaves it with no
// set, so Set() errors for it.
func (f *fakeSource) advertiseWithoutSet(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets[target] = true
	delete(f.sets, target)
}

// remove drops target entirely, as if its sensor stopped reporting.
func (f *fakeSource) remove(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.targets, target)
	delete(f.sets, target)
}

func (f *fakeSource) Targets(_ string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.targets))
	for t := range f.targets {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (f *fakeSource) Set(_, target string) (metricset.Set, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sets[target]
	if !ok {
		return nil, fmt.Errorf("fakeSource: no set for target %q", target)
	}
	return s, nil
}

// newTestCollector builds a Collector via New, reading from src, with a
// *fakeSender the test can inspect. mgr is nil: the collector never uses it.
func newTestCollector(src *fakeSource, opts Options) (*Collector, *fakeSender) {
	sender := &fakeSender{}
	opts.Source = src
	c := New(opts, sender, logr.Discard())
	return c, sender
}

// counterPoint builds one series' point for (namespace, service), optionally
// carrying further attributes (e.g. logexporter.LabelSample) as extra k, v
// pairs.
func counterPoint(namespace, service string, v float64, extra ...string) metricset.Point {
	attrs := map[string]string{
		logexporter.LabelNamespace: namespace,
		logexporter.LabelService:   service,
	}
	for i := 0; i+1 < len(extra); i += 2 {
		attrs[extra[i]] = extra[i+1]
	}
	return metricset.Point{Attrs: attrs, Value: v}
}

func metricOf(name string, points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: name, Kind: metricset.Counter, Points: points}
}

func setOf(metrics ...*metricset.Metric) metricset.Set {
	s := metricset.Set{}
	for _, m := range metrics {
		s[m.Name] = m
	}
	return s
}

// errorSet is a Set carrying a single error-counter point for (namespace, service).
func errorSet(namespace, service string, v float64) metricset.Set {
	return setOf(metricOf(logexporter.MetricLogErrorsTotal, counterPoint(namespace, service, v)))
}

// multiErrorSet is a Set carrying several error-counter points in one metric.
func multiErrorSet(points ...metricset.Point) metricset.Set {
	return setOf(metricOf(logexporter.MetricLogErrorsTotal, points...))
}

// errWarnTotalSet is a Set carrying error, warn and total counter points for
// one (namespace, service) pair.
func errWarnTotalSet(namespace, service string, errs, warns, total float64) metricset.Set {
	return setOf(
		metricOf(logexporter.MetricLogErrorsTotal, counterPoint(namespace, service, errs)),
		metricOf(logexporter.MetricLogWarnsTotal, counterPoint(namespace, service, warns)),
		metricOf(logexporter.MetricLogLinesTotal, counterPoint(namespace, service, total)),
	)
}

// twoNamespaceSet is a Set with one error-counter point in each of two
// namespaces, for the namespace-filter tests.
func twoNamespaceSet(tenantA, ops float64) metricset.Set {
	return multiErrorSet(
		counterPoint("tenant-a", "backend", tenantA),
		counterPoint("ops", "api", ops),
	)
}

// TestCollector_DeltaAcrossTwoReads verifies the second read's delta is
// (cur-prev), not the raw cumulative value, and that it reaches the Sender once it
// crosses the error threshold.
func TestCollector_DeltaAcrossTwoReads(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 5))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // first read: cumulative=5, delta=5 (below threshold) -> no send
	if sender.callCount() != 0 {
		t.Fatalf("after first read (delta=5 < threshold=10): callCount = %d, want 0", sender.callCount())
	}

	src.set("sensor/node-a", errorSet("ops", "api", 20)) // cumulative 5 -> 20, delta = 15
	c.runOnce(ctx)
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services sent = %d, want 1", len(svcs))
	}
	if svcs[0].Namespace != "ops" || svcs[0].Service != "api" || svcs[0].ErrorCount != 15 {
		t.Errorf("service = %+v, want {ops api errorCount=15}", svcs[0])
	}
}

// TestCollector_CounterReset verifies a sensor restart resetting its counter
// (value drops instead of increasing) is treated as "current value is the delta"
// rather than a negative delta or a dropped update.
func TestCollector_CounterReset(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 100))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // cumulative=100, delta=100 -> sent (>= threshold 10)
	if sender.callCount() != 1 {
		t.Fatalf("after first read: callCount = %d, want 1", sender.callCount())
	}

	src.set("sensor/node-a", errorSet("ops", "api", 12)) // sensor restarted: counter reset to 12 (< 100)
	c.runOnce(ctx)
	svcs := sender.allServices()
	last := svcs[len(svcs)-1]
	if last.ErrorCount != 12 {
		t.Errorf("post-reset delta = %d, want 12 (treat current value as the delta, not cur-prev=-88)", last.ErrorCount)
	}
}

// TestCollector_GateBelowThresholdNoSend verifies a service whose error delta never
// crosses the threshold produces no frame at all — not a frame with 0 services, no
// frame.
func TestCollector_GateBelowThresholdNoSend(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 3))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	if sender.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0 (delta=3 < threshold=10)", sender.callCount())
	}
}

// TestCollector_SkipsUnattributedServiceEntries verifies error series from pods
// without a service label (an empty Service attribute) are excluded from the frame
// even when they cross the threshold — the receiver rejects a whole frame over one
// empty-service entry, and an unattributable service could never resolve in the
// hub's graph anyway. Attributed services in the same window must still be sent.
func TestCollector_SkipsUnattributedServiceEntries(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", multiErrorSet(
		counterPoint("ops", "api", 25),
		counterPoint("ops", "", 40),
	))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (unattributed entry must be skipped)", len(svcs))
	}
	if svcs[0].Service != "api" || svcs[0].ErrorCount != 25 {
		t.Errorf("service = %+v, want api/25", svcs[0])
	}
}

// TestCollector_WarnAloneCrossesGate verifies a service with WarnCount >=
// ErrorThreshold qualifies even when ErrorCount is well under it — the gate is an
// OR across both counters, not error-only.
func TestCollector_WarnAloneCrossesGate(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errWarnTotalSet("ops", "api", 2, 15, 500))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (warnCount=15 >= threshold=10 should qualify despite errorCount=2)", len(svcs))
	}
	if svcs[0].ErrorCount != 2 || svcs[0].WarnCount != 15 {
		t.Errorf("service = %+v, want errorCount=2 warnCount=15", svcs[0])
	}
}

// TestCollector_WarnAndTotalOptional verifies warn/total counters are aggregated
// alongside errors when present.
func TestCollector_WarnAndTotalOptional(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errWarnTotalSet("ops", "api", 20, 8, 500))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1", len(svcs))
	}
	if svcs[0].ErrorCount != 20 || svcs[0].WarnCount != 8 || svcs[0].TotalCount != 500 {
		t.Errorf("service = %+v, want errorCount=20 warnCount=8 totalCount=500", svcs[0])
	}
}

// TestCollector_AggregatesAcrossTargets verifies two sensors reporting the same
// (namespace, service) label pair are summed into one entry in the frame, not sent
// as two separate entries.
func TestCollector_AggregatesAcrossTargets(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 6))
	src.set("sensor/node-b", errorSet("ops", "api", 7))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (aggregated across 2 targets)", len(svcs))
	}
	if svcs[0].ErrorCount != 13 { // 6 + 7, both targets' first-read cumulative == delta
		t.Errorf("aggregated errorCount = %d, want 13 (6 from node-a + 7 from node-b)", svcs[0].ErrorCount)
	}
}

// TestCollector_PerTargetBaseline verifies deltas are computed per target, not
// globally: one target's counter climbing does not get diffed against a different
// target's previous value.
func TestCollector_PerTargetBaseline(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 50)) // high baseline
	src.set("sensor/node-b", errorSet("ops", "api", 2))  // low baseline
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background()) // seeds both baselines; first read sends (50+2=52 >= 10)
	sender.mu.Lock()
	sender.calls = nil // reset for the assertion below
	sender.mu.Unlock()

	src.set("sensor/node-a", errorSet("ops", "api", 55)) // +5
	src.set("sensor/node-b", errorSet("ops", "api", 3))  // +1
	c.runOnce(context.Background())

	svcs := sender.allServices()
	if len(svcs) != 0 {
		// combined delta 5+1=6 < threshold 10: correctly gated out. If target
		// baselines were mixed up (e.g. globally keyed), the delta could be
		// miscomputed as negative-then-reset and wrongly appear large.
		t.Fatalf("services sent = %d, want 0 (combined delta 6 < threshold 10); got %+v", len(svcs), svcs)
	}
}

// TestCollector_NoTargetsNoSend verifies an empty target list (no sensor has sent
// log-exporter metrics yet) is handled gracefully — no panic, no send.
func TestCollector_NoTargetsNoSend(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	if sender.callCount() != 0 {
		t.Errorf("callCount = %d, want 0 (no targets)", sender.callCount())
	}
}

// TestCollector_SkipsTargetsWithoutASet verifies a target that Targets() lists but
// whose Set() call then errors (e.g. its snapshot went stale between the two calls)
// is logged and skipped, without blocking the other targets' counters from being
// aggregated and sent.
func TestCollector_SkipsTargetsWithoutASet(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 20))
	src.advertiseWithoutSet("sensor/node-b")
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (node-a's counters must still be sent)", len(svcs))
	}
	if svcs[0].ErrorCount != 20 {
		t.Errorf("ErrorCount = %d, want 20", svcs[0].ErrorCount)
	}
}

// TestNew_DisabledWithoutSource verifies New returns nil (collector disabled) when
// Source is unset, so main.go's unconditional wiring is a no-op by default.
func TestNew_DisabledWithoutSource(t *testing.T) {
	if c := New(Options{}, &fakeSender{}, logr.Discard()); c != nil {
		t.Errorf("New with nil Source = %v, want nil", c)
	}
}

// TestNew_DisabledWithoutSender verifies New returns nil when push mode itself is
// disabled (sender is nil, e.g. because *push.Pusher was nil).
func TestNew_DisabledWithoutSender(t *testing.T) {
	if c := New(Options{Source: newFakeSource()}, nil, logr.Discard()); c != nil {
		t.Errorf("New with nil sender = %v, want nil", c)
	}
}

// TestTypedNilPusherIsNotNilInterface is a regression guard for Go's typed-nil-
// interface trap, which main.go must route around: a nil *push.Pusher (push.New
// returns nil when --triage-endpoint is empty) stored DIRECTLY into a Sender interface
// variable does NOT compare equal to nil, because the interface value still carries
// the concrete type *push.Pusher — only the pointer inside it is nil. If New relied on
// `sender == nil` alone to detect "push mode disabled," a directly-passed nil
// *push.Pusher would slip through and the collector would start reading with
// nowhere to send. main.go avoids this with an explicit
// `var sender logmetrics.Sender; if pusher != nil { sender = pusher }` guard rather
// than passing the pusher variable straight through. This test exists so that guard
// doesn't get "simplified" away by someone who assumes sender == nil is sufficient.
func TestTypedNilPusherIsNotNilInterface(t *testing.T) {
	//nolint:staticcheck // SA4023: never-true is the assertion — see the comment above.
	var nilPusher *push.Pusher // exactly what push.New returns when disabled
	var sender Sender = nilPusher
	//nolint:staticcheck // SA4023: never-true is the assertion — see the comment above.
	if sender == nil {
		t.Fatal("a *push.Pusher stored in a Sender interface compared equal to nil — " +
			"if this now passes, Go's typed-nil-interface behavior changed and main.go's " +
			"explicit-nil guard before calling logmetrics.New may no longer be necessary")
	}
}

// TestCollector_SamplePicksLargestDeltaSeries verifies the Sample forwarded for a
// pair is the LabelSample value of whichever error series moved the most THIS
// window (ADR-0007) — not the series with the highest cumulative value, and not
// simply the first series in family order.
func TestCollector_SamplePicksLargestDeltaSeries(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", multiErrorSet(
		counterPoint("ops", "api", 100, logexporter.LabelSample, "old template: #"),
	))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // seeds baseline: old template delta=100 -> sent, sample="old template: #"

	// old: 100->101 (delta 1); new: 0->50 (delta 50)
	src.set("sensor/node-a", multiErrorSet(
		counterPoint("ops", "api", 101, logexporter.LabelSample, "old template: #"),
		counterPoint("ops", "api", 50, logexporter.LabelSample, "new template: #"),
	))
	c.runOnce(ctx)

	svcs := sender.allServices()
	last := svcs[len(svcs)-1]
	if last.ErrorCount != 51 {
		t.Errorf("ErrorCount = %d, want 51 (1 + 50, summed across both series)", last.ErrorCount)
	}
	if last.Sample != "new template: #" {
		t.Errorf("Sample = %q, want %q (largest delta this window beats higher cumulative value and earlier family position)",
			last.Sample, "new template: #")
	}
}

// logsWithStaleSample is a Set carrying one error series with a fixed sample
// and value, plus a warn counter that varies — for testing the sample fallback
// when the only labeled error series doesn't move.
func logsWithStaleSample(warns float64) metricset.Set {
	return setOf(
		metricOf(logexporter.MetricLogErrorsTotal, counterPoint("ops", "api", 5, logexporter.LabelSample, "stale template: #")),
		metricOf(logexporter.MetricLogWarnsTotal, counterPoint("ops", "api", warns)),
	)
}

// TestCollector_SampleFallsBackWhenNoLabeledSeriesMoves verifies that when a pair
// qualifies for the frame via warns (not errors) and its only labeled error series
// is unchanged, Sample still falls back to that series' value instead of coming
// back empty.
func TestCollector_SampleFallsBackWhenNoLabeledSeriesMoves(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", logsWithStaleSample(0))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx) // first read: error delta=5 (< threshold), warn delta=0 -> no send

	src.set("sensor/node-a", logsWithStaleSample(60)) // warn 0 -> 60 crosses threshold; error series unchanged at 5 (delta 0)
	c.runOnce(ctx)

	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (warn delta 60 >= threshold 10 should qualify)", len(svcs))
	}
	if svcs[0].Sample != "stale template: #" {
		t.Errorf("Sample = %q, want %q (fallback to first non-empty when no labeled series moved)",
			svcs[0].Sample, "stale template: #")
	}
}

// TestCollector_ForgetsGoneTargets verifies runOnce garbage-collects c.prev/
// c.prevSamples baselines for targets that leave the Targets() list (sensor pod
// churn), rather than accreting dead entries forever.
func TestCollector_ForgetsGoneTargets(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", errorSet("ops", "api", 20))
	src.set("sensor/node-b", errorSet("ops", "api", 30))
	c, _ := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx)
	if len(c.prev) != 2 {
		t.Fatalf("len(c.prev) after first runOnce = %d, want 2", len(c.prev))
	}

	src.remove("sensor/node-b") // simulates node-b's sensor going away

	c.runOnce(ctx)
	if len(c.prev) != 1 {
		t.Fatalf("len(c.prev) after second runOnce = %d, want 1 (gone target's baseline must be forgotten)", len(c.prev))
	}
	if _, ok := c.prev["sensor/node-a"]; !ok {
		t.Errorf("c.prev missing surviving target %q; contents: %+v", "sensor/node-a", c.prev)
	}
	if len(c.prevSamples) != 1 {
		t.Fatalf("len(c.prevSamples) after second runOnce = %d, want 1", len(c.prevSamples))
	}
	if _, ok := c.prevSamples["sensor/node-a"]; !ok {
		t.Errorf("c.prevSamples missing surviving target %q", "sensor/node-a")
	}
}

// One sensor DaemonSet can tail several tenants' namespaces, but a push
// connection stamps every frame with its own single handshake-bound tenant. So
// the collector must forward ONLY the namespaces this connection owns; without
// the filter another tenant's log metrics are silently mis-attributed to this one.
func TestCollector_NamespaceFilter_DropsOtherTenants(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", twoNamespaceSet(0, 0))
	c, sender := newTestCollector(src, Options{
		ErrorThreshold: 10,
		Namespaces:     []string{"tenant-a"},
	})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set("sensor/node-a", twoNamespaceSet(50, 90))
	c.runOnce(ctx)

	got := sender.allServices()
	if len(got) != 1 {
		t.Fatalf("services = %d (%+v), want 1 (ops must be filtered out)", len(got), got)
	}
	if got[0].Namespace != "tenant-a" || got[0].Service != "backend" {
		t.Errorf("forwarded %s/%s, want tenant-a/backend", got[0].Namespace, got[0].Service)
	}
}

// Empty Namespaces keeps the pre-filter behaviour: forward everything read.
func TestCollector_NamespaceFilter_EmptyForwardsAll(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", twoNamespaceSet(0, 0))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set("sensor/node-a", twoNamespaceSet(50, 90))
	c.runOnce(ctx)

	if got := sender.allServices(); len(got) != 2 {
		t.Fatalf("services = %d (%+v), want 2 (no filter configured)", len(got), got)
	}
}

// A namespace admitted ONLY by the dynamic NamespaceAllowed hook — e.g. one
// pulled into scope by the triageagent.dev/tenant annotation (push/namespace_scope.go),
// invisible to the static Namespaces list frozen at process start — must enter
// the payload; one the hook rejects must not, even when the static list
// contains it. The hook OVERRIDES the static set entirely.
func TestCollector_NamespaceAllowed_OverridesStaticSet(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", twoNamespaceSet(0, 0))
	c, sender := newTestCollector(src, Options{
		ErrorThreshold:   10,
		Namespaces:       []string{"ops"}, // static set: ops only — no tenant-a
		NamespaceAllowed: func(ns string) bool { return ns == "tenant-a" },
	})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set("sensor/node-a", twoNamespaceSet(50, 90))
	c.runOnce(ctx)

	got := sender.allServices()
	if len(got) != 1 {
		t.Fatalf("services = %d (%+v), want 1 (hook admits tenant-a, rejects statically-listed ops)", len(got), got)
	}
	if got[0].Namespace != "tenant-a" || got[0].Service != "backend" {
		t.Errorf("forwarded %s/%s, want tenant-a/backend", got[0].Namespace, got[0].Service)
	}
}

func TestNamespaceSet_BlankOnlyIsNil(t *testing.T) {
	if got := namespaceSet([]string{"", "  "}); got != nil {
		t.Errorf("namespaceSet(blanks) = %v, want nil (means forward-all)", got)
	}
	if got := namespaceSet([]string{" ops ", "dev"}); !got["ops"] || !got["dev"] || len(got) != 2 {
		t.Errorf("namespaceSet trimmed = %v, want {ops,dev}", got)
	}
}

func TestPickSamples_TopThreeDistinct(t *testing.T) {
	sampled := func(sample string, v float64) metricset.Point {
		return counterPoint("ops", "api", v, logexporter.LabelSample, sample)
	}
	pair := servicePair{Namespace: "ops", Service: "api"}

	t.Run("top three by delta descending", func(t *testing.T) {
		m := metricOf(logexporter.MetricLogErrorsTotal,
			sampled("line five", 5),
			sampled("line three", 3),
			sampled("line nine", 9),
			sampled("line one", 1),
			sampled("line five", 0), // duplicate line: still one entry
		)
		best, top, _ := pickSamples(m, nil)
		want := []string{"line nine", "line five", "line three"}
		if !slices.Equal(top[pair], want) {
			t.Errorf("Samples = %q, want %q", top[pair], want)
		}
		if best[pair] != "line nine" {
			t.Errorf("Sample = %q, want %q", best[pair], "line nine")
		}
	})

	t.Run("ANSI stripped, duplicates merged", func(t *testing.T) {
		m := metricOf(logexporter.MetricLogErrorsTotal,
			sampled("\x1b[31mERR\x1b[0m boom", 4),
			sampled("ERR boom", 2),
			sampled("other", 1),
		)
		best, top, _ := pickSamples(m, nil)
		want := []string{"ERR boom", "other"}
		if !slices.Equal(top[pair], want) {
			t.Errorf("Samples = %q, want %q", top[pair], want)
		}
		if best[pair] != "ERR boom" {
			t.Errorf("Sample = %q, want %q", best[pair], "ERR boom")
		}
	})

	t.Run("no series moved falls back to the first line", func(t *testing.T) {
		m := metricOf(logexporter.MetricLogErrorsTotal, sampled("first", 5), sampled("second", 7))
		_, _, cur := pickSamples(m, nil)
		best, top, _ := pickSamples(m, cur)
		if best[pair] != "first" || !slices.Equal(top[pair], []string{"first"}) {
			t.Errorf("best=%q top=%q, want the fallback %q alone", best[pair], top[pair], "first")
		}
	})
}

func TestStripANSI(t *testing.T) {
	if got := stripANSI("\x1b[1;33mWRN\x1b[0m slow"); got != "WRN slow" {
		t.Errorf("stripANSI = %q, want %q", got, "WRN slow")
	}
}

func TestCollector_SendsTopThreeSamples(t *testing.T) {
	src := newFakeSource()
	src.set("sensor/node-a", logExporterSet("ops", "api", 0, map[string]int64{"a": 0, "b": 0, "c": 0, "d": 0}, 0, 0))
	c, sender := newTestCollector(src, Options{ErrorThreshold: 10})
	c.runOnce(context.Background())
	src.set("sensor/node-a", logExporterSet("ops", "api", 0, map[string]int64{"a": 5, "b": 3, "c": 9, "d": 1}, 0, 100))
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %+v, want 1", svcs)
	}
	if want := []string{"c", "a", "b"}; !slices.Equal(svcs[0].Samples, want) || svcs[0].Sample != "c" {
		t.Errorf("Sample = %q, Samples = %q, want c / %q", svcs[0].Sample, svcs[0].Samples, want)
	}
}

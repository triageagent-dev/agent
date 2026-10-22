package redsource

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// ===========================================================================
// Shared test infrastructure
// ===========================================================================

// fakeSender records every SendRedMetrics/SendRedSummary/SendTopologySnapshot
// call for assertions.
type fakeSender struct {
	mu       sync.Mutex
	red      [][]push.ServiceRedMetrics
	redSum   [][]push.ServiceRedMetrics
	topology []topologyCall
	// topologyFail, when true, makes SendTopologySnapshot report the frame as
	// NOT enqueued — simulating a disconnected push connection or a full send
	// queue (push.Pusher.SendTopologySnapshot's real failure modes) — without
	// recording it in topology. topologyAttempts still counts the call, so a
	// test can assert the collector retried on the next tick.
	topologyFail     bool
	topologyAttempts int
}

type topologyCall struct {
	capturedAt int64
	windowSec  int
	nodes      []push.TopologyNode
	edges      []push.TopologyEdge
}

func (f *fakeSender) SendRedMetrics(_ int64, _ int, services []push.ServiceRedMetrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.red = append(f.red, append([]push.ServiceRedMetrics(nil), services...))
}

func (f *fakeSender) SendRedSummary(_ int64, _ int, services []push.ServiceRedMetrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.redSum = append(f.redSum, append([]push.ServiceRedMetrics(nil), services...))
}

func (f *fakeSender) SendTopologySnapshot(capturedAt int64, windowSec int, nodes []push.TopologyNode, edges []push.TopologyEdge) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topologyAttempts++
	if f.topologyFail {
		return false
	}
	f.topology = append(f.topology, topologyCall{
		capturedAt: capturedAt,
		windowSec:  windowSec,
		nodes:      append([]push.TopologyNode(nil), nodes...),
		edges:      append([]push.TopologyEdge(nil), edges...),
	})
	return true
}

func (f *fakeSender) setTopologyFail(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topologyFail = fail
}

func (f *fakeSender) topologyAttemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topologyAttempts
}

func (f *fakeSender) redServices() []push.ServiceRedMetrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []push.ServiceRedMetrics
	for _, c := range f.red {
		out = append(out, c...)
	}
	return out
}

func (f *fakeSender) redCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.red)
}

func (f *fakeSender) redSummaryCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.redSum)
}

func (f *fakeSender) lastRedSummary() []push.ServiceRedMetrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.redSum[len(f.redSum)-1]
}

func (f *fakeSender) topologyCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.topology)
}

func (f *fakeSender) lastTopology() topologyCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topology[len(f.topology)-1]
}

// fakeSource is a mutable metricset.Source: target -> metricset.Set, guarded
// by a mutex so a test can drive several runOnce calls with different
// counter values, standing in for the sensor stream hub (sensorstream.Hub).
//
// targets and sets are tracked separately so a test can advertise a target
// via Targets() whose Set() call then fails — the real hub can do this when
// a snapshot goes stale between the two calls.
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
// set, so Set() errors for it — simulating a read failure for one
// target while others keep reporting.
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

const (
	testTarget  = "sensor/node-a"
	testTarget2 = "sensor/node-b"
)

// newTestCollector builds a Collector via New, reading from src, with a
// *fakeSender the test can inspect. profile selects the naming table; opts
// carries everything else (ErrorThreshold, MinErrorRatePct, Namespaces,
// EnableRED, EnableTopology, Interval, TopologyInterval, Resolver, Identity).
func newTestCollector(t *testing.T, src *fakeSource, profile Profile, opts Options) (*Collector, *fakeSender) {
	t.Helper()
	sender := &fakeSender{}
	opts.Profile = profile
	opts.Source = src
	c := New(opts, sender, logr.Discard())
	if c == nil {
		t.Fatal("New returned nil; check Options (EnableRED/EnableTopology, Profile, Source, sender)")
	}
	return c, sender
}

func obiRED(t *testing.T, src *fakeSource, errThresh int64, minRate float64, extra Options) (*Collector, *fakeSender) {
	t.Helper()
	extra.EnableRED = true
	extra.ErrorThreshold = errThresh
	extra.MinErrorRatePct = minRate
	return newTestCollector(t, src, ProfileOBI, extra)
}

func obiTopology(t *testing.T, src *fakeSource, extra Options) (*Collector, *fakeSender) {
	t.Helper()
	extra.EnableTopology = true
	extra.Interval = time.Second
	extra.TopologyInterval = time.Second // every runOnce is exactly one flush
	return newTestCollector(t, src, ProfileOBI, extra)
}

// setOf collects metrics into a metricset.Set keyed by name.
func setOf(metrics ...*metricset.Metric) metricset.Set {
	s := metricset.Set{}
	for _, m := range metrics {
		s[m.Name] = m
	}
	return s
}

// ===========================================================================
// ProfileOBI fixture builders — one http.server.request.duration histogram
// carries BOTH the RED request/error counts and the P99 duration buckets
// (RequestsMetric and DurationBucketMetric name the same OTel metric, see
// profiles.go), so a single point can carry a count, a status, and buckets
// together.
// ===========================================================================

// obiPoint builds one http.server.request.duration series with the OBI
// profile's attribute keys. buckets is omitted for a series that carries no
// duration data (e.g. an error status with nothing to interpolate P99 from).
func obiPoint(namespace, service, method, route, status string, count int, buckets ...metricset.Bucket) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.namespace.name":        namespace,
			"service.name":              service,
			"http.request.method":       method,
			"http.route":                route,
			"http.response.status_code": status,
		},
		Value:   float64(count),
		Buckets: buckets,
	}
}

// obiPointWithOwner mirrors obiPoint but carries k8s.owner.name instead of
// method/route/status — used by the identity-resolution topology tests,
// which only need namespace/service/owner.
func obiPointWithOwner(namespace, service, owner string, count int) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.namespace.name": namespace,
			"service.name":       service,
			"k8s.owner.name":     owner,
		},
		Value: float64(count),
	}
}

// obiMetric builds the OBI profile's single http.server.request.duration
// histogram from the given points.
func obiMetric(points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: "http.server.request.duration", Kind: metricset.Histogram, Points: points}
}

// obiBuckets returns the four-bucket ladder (0.1, 0.5, 1, +Inf) the P99
// fixtures below use, with the given cumulative counts.
func obiBuckets(c1, c2, c3, cInf uint64) []metricset.Bucket {
	return []metricset.Bucket{
		{UpperBound: 0.1, CumulativeCount: c1},
		{UpperBound: 0.5, CumulativeCount: c2},
		{UpperBound: 1, CumulativeCount: c3},
		{UpperBound: math.Inf(1), CumulativeCount: cInf},
	}
}

// obiClientPoint builds one client-side series (Profile.ClientMetrics) with
// the OBI profile's caller-identity + server-address attribute keys.
func obiClientPoint(srcNamespace, srcService, serverAddress string, count int) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.namespace.name": srcNamespace,
			"service.name":       srcService,
			"server.address":     serverAddress,
		},
		Value: float64(count),
	}
}

// obiClientPointWithOwner adds k8s.owner.name to obiClientPoint.
func obiClientPointWithOwner(srcNamespace, srcService, owner, serverAddress string, count int) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.namespace.name": srcNamespace,
			"service.name":       srcService,
			"k8s.owner.name":     owner,
			"server.address":     serverAddress,
		},
		Value: float64(count),
	}
}

// obiClientMetric builds one of the OBI profile's ClientMetrics families
// (name is one of http.client.request.duration/rpc.client.call.duration/
// db.client.operation.duration).
func obiClientMetric(name string, points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: name, Kind: metricset.Histogram, Points: points}
}

// fakeIdentityResolver implements IdentityResolver off a plain "ns/owner" ->
// identity map, for tests that don't need the real virtual-app index.
type fakeIdentityResolver map[string]string

func (f fakeIdentityResolver) IdentityFor(namespace, ownerName string) (string, bool) {
	identity, ok := f[namespace+"/"+ownerName]
	return identity, ok
}

// fakeResolverFunc adapts a plain function to Resolver, for a collector-level
// test that only needs to prove Options.Resolver is actually consulted end to
// end — the exhaustive address-form/identity-mapping cases live in
// k8sresolver_test.go against the real k8sResolver + a fake ClusterIndex.
type fakeResolverFunc func(srcNamespace, serverAddress string) (servicePair, string)

func (f fakeResolverFunc) Resolve(srcNamespace, serverAddress string) (servicePair, string) {
	return f(srcNamespace, serverAddress)
}

// ===========================================================================
// ProfileOBI RED fixtures and tests.
// ===========================================================================

func TestOBI_Collector_DeltaAcrossTwoTicks(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "502", 5),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 95),
	)))
	c, sender := obiRED(t, src, 10, 5, Options{})

	ctx := context.Background()
	c.runOnce(ctx)
	if sender.redCallCount() != 0 {
		t.Fatalf("after first read (first-seen suppression): callCount = %d, want 0", sender.redCallCount())
	}

	src.set(testTarget, setOf(obiMetric( // err 5->20 (delta 15), ok 95->180 (delta 85); total delta 100, rate 15%
		obiPoint("ns-a", "api", "GET", "/orders", "502", 20),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 180),
	)))
	c.runOnce(ctx)
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services sent = %d, want 1", len(svcs))
	}
	if svcs[0].Namespace != "ns-a" || svcs[0].Service != "api" || svcs[0].ErrorCount != 15 || svcs[0].RequestCount != 100 {
		t.Errorf("service = %+v, want {ns-a api errorCount=15 requestCount=100}", svcs[0])
	}
}

func TestOBI_Collector_CounterReset(t *testing.T) {
	body := func(errN, okN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "500", errN),
			obiPoint("ns-a", "api", "GET", "/orders", "200", okN),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(100, 0))
	c, sender := obiRED(t, src, 10, 5, Options{})

	ctx := context.Background()
	c.runOnce(ctx)
	if sender.redCallCount() != 0 {
		t.Fatalf("after first read: callCount = %d, want 0 (first-seen suppression)", sender.redCallCount())
	}

	src.set(testTarget, body(150, 0)) // genuine growth against the now-real baseline of 100 -> delta 50
	c.runOnce(ctx)
	if sender.redCallCount() != 1 {
		t.Fatalf("after second read: callCount = %d, want 1", sender.redCallCount())
	}
	if svcs := sender.redServices(); svcs[len(svcs)-1].ErrorCount != 50 {
		t.Fatalf("second-read delta = %d, want 50 (150-100)", svcs[len(svcs)-1].ErrorCount)
	}

	src.set(testTarget, body(12, 0)) // sensor restarted: counter reset to 12 (< 150)
	c.runOnce(ctx)
	svcs := sender.redServices()
	last := svcs[len(svcs)-1]
	if last.ErrorCount != 12 {
		t.Errorf("post-reset delta = %d, want 12 (treat current value as the delta, not cur-prev=-138)", last.ErrorCount)
	}
}

func TestOBI_Collector_FirstTickSuppressesLifetimeCounters(t *testing.T) {
	body := func(errN, okN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "500", errN),
			obiPoint("ns-a", "api", "GET", "/orders", "200", okN),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(4000, 400000))
	c, sender := obiRED(t, src, 10, 5, Options{})

	c.runOnce(context.Background())
	if sender.redCallCount() != 0 {
		t.Fatalf("first read: callCount = %d, want 0 (a service's lifetime counters must never be reported as one window's delta)", sender.redCallCount())
	}

	src.set(testTarget, body(4012, 400100)) // genuine growth: +12 errors, +100 ok
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1", len(svcs))
	}
	if svcs[0].ErrorCount != 12 {
		t.Errorf("ErrorCount = %d, want 12 (the real increment, not the lifetime total)", svcs[0].ErrorCount)
	}
	if svcs[0].RequestCount != 112 {
		t.Errorf("RequestCount = %d, want 112", svcs[0].RequestCount)
	}
}

func TestOBI_Collector_GateRequiresBothThresholdAndRate(t *testing.T) {
	tests := []struct {
		name    string
		errN    int
		okN     int
		wantOne bool
	}{
		{"high volume low rate: 20 errors out of 100000 requests (0.02%)", 20, 99980, false},
		{"high rate low volume: 3 errors out of 10 requests (30%) but below absolute threshold", 3, 7, false},
		{"both thresholds crossed: 15 errors out of 100 requests (15%)", 15, 85, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource()
			src.set(testTarget, setOf(obiMetric(
				obiPoint("ns-a", "api", "GET", "/orders", "503", 0),
				obiPoint("ns-a", "api", "GET", "/orders", "200", 0),
			)))
			c, sender := obiRED(t, src, 10, 5, Options{})
			c.runOnce(context.Background())

			src.set(testTarget, setOf(obiMetric(
				obiPoint("ns-a", "api", "GET", "/orders", "503", tc.errN),
				obiPoint("ns-a", "api", "GET", "/orders", "200", tc.okN),
			)))
			c.runOnce(context.Background())
			got := sender.redCallCount() == 1
			if got != tc.wantOne {
				t.Errorf("callCount = %d (svcs=%+v), want send=%v", sender.redCallCount(), sender.redServices(), tc.wantOne)
			}
		})
	}
}

func TestOBI_Collector_SkipsEmptyServiceEntries(t *testing.T) {
	seed := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "500", 0),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 0),
		obiPoint("ns-a", "", "GET", "/x", "500", 0),
		obiPoint("ns-a", "", "GET", "/x", "200", 0),
	))
	body := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "500", 25),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 75),
		obiPoint("ns-a", "", "GET", "/x", "500", 40),
		obiPoint("ns-a", "", "GET", "/x", "200", 10),
	))
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body)
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (empty-service entry must be skipped)", len(svcs))
	}
	if svcs[0].Service != "api" || svcs[0].ErrorCount != 25 {
		t.Errorf("service = %+v, want api/25", svcs[0])
	}
}

func TestOBI_Collector_AggregatesAcrossTargets(t *testing.T) {
	line := func(errN, okN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "500", errN),
			obiPoint("ns-a", "api", "GET", "/orders", "200", okN),
		))
	}
	src := newFakeSource()
	src.set(testTarget, line(0, 0))
	src.set(testTarget2, line(0, 0))
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, line(6, 4))
	src.set(testTarget2, line(7, 3))
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1 (aggregated across 2 targets)", len(svcs))
	}
	if svcs[0].ErrorCount != 13 {
		t.Errorf("aggregated errorCount = %d, want 13", svcs[0].ErrorCount)
	}
	if svcs[0].RequestCount != 20 {
		t.Errorf("aggregated requestCount = %d, want 20", svcs[0].RequestCount)
	}
}

func TestOBI_Collector_PerTargetBaseline(t *testing.T) {
	line := func(errN int) metricset.Set {
		return setOf(obiMetric(obiPoint("ns-a", "api", "GET", "/orders", "500", errN)))
	}
	src := newFakeSource()
	src.set(testTarget, line(50))
	src.set(testTarget2, line(2))
	// The low-volume rule is off: this route has only errors, and 6 of them
	// at a 100% failure share would fire it. The test is about the deltas.
	c, sender := obiRED(t, src, 10, 5, Options{Baseline: BaselineOptions{ErrSparseMinErrors: -1}})

	c.runOnce(context.Background())
	sender.mu.Lock()
	sender.red = nil
	sender.mu.Unlock()

	src.set(testTarget, line(55)) // +5
	src.set(testTarget2, line(3)) // +1
	c.runOnce(context.Background())

	svcs := sender.redServices()
	if len(svcs) != 0 {
		t.Fatalf("services sent = %d, want 0 (combined delta 5+1=6 < threshold 10); got %+v", len(svcs), svcs)
	}
}

func TestOBI_Collector_SamplePicksLargestDeltaSeries(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "502", 100),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 0),
	)))
	c, sender := obiRED(t, src, 10, 5, Options{})

	ctx := context.Background()
	c.runOnce(ctx)
	if sender.redCallCount() != 0 {
		t.Fatalf("after first read: callCount = %d, want 0 (first-seen suppression)", sender.redCallCount())
	}

	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "502", 101),
		obiPoint("ns-a", "api", "POST", "/checkout", "500", 50),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 200),
	)))
	c.runOnce(ctx)
	// POST /checkout's 500 series is new on a target already read: it started
	// from zero on the sensor, so its 50 errors happened in this window.
	if sender.redCallCount() != 1 {
		t.Fatalf("after second read: callCount = %d, want 1 (a series new on a known target counts from zero)", sender.redCallCount())
	}
	if got := sender.redServices()[0]; got.ErrorCount != 51 || got.Sample != "POST /checkout -> 500 x50" {
		t.Errorf("second read: ErrorCount = %d Sample = %q, want 51 and the new series' 50", got.ErrorCount, got.Sample)
	}

	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "502", 102),
		obiPoint("ns-a", "api", "POST", "/checkout", "500", 70),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 200),
	)))
	c.runOnce(ctx)

	svcs := sender.redServices()
	last := svcs[len(svcs)-1]
	if last.ErrorCount != 21 { // 1 (502: 102-101) + 20 (checkout: 70-50)
		t.Errorf("ErrorCount = %d, want 21", last.ErrorCount)
	}
	want := "POST /checkout -> 500 x20"
	if last.Sample != want {
		t.Errorf("Sample = %q, want %q (largest delta this window)", last.Sample, want)
	}
}

func TestOBI_Collector_SampleFormatAndTruncation(t *testing.T) {
	longRoute := "/" + strings.Repeat("a", 400)
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(obiPoint("ns-a", "api", "GET", longRoute, "500", 0))))
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, setOf(obiMetric(obiPoint("ns-a", "api", "GET", longRoute, "500", 42))))
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1", len(svcs))
	}
	if !strings.HasPrefix(svcs[0].Sample, "GET /"+strings.Repeat("a", 10)) {
		t.Errorf("Sample does not look like a formatted sample: %q", svcs[0].Sample)
	}
	if len(svcs[0].Sample) != push.MaxRedMetricsSampleLen {
		t.Errorf("Sample length = %d, want %d (truncated)", len(svcs[0].Sample), push.MaxRedMetricsSampleLen)
	}
}

func TestOBI_Collector_NonErrorStatusExcludedFromErrorCount(t *testing.T) {
	body := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "200", 50),
		obiPoint("ns-a", "api", "GET", "/orders", "404", 50),
	))
	src := newFakeSource()
	src.set(testTarget, body)
	c, sender := obiRED(t, src, 1, 0.01, Options{})

	c.runOnce(context.Background())
	if sender.redCallCount() != 0 {
		t.Fatalf("callCount = %d, want 0 (no 5xx statuses present)", sender.redCallCount())
	}
}

func TestOBI_Collector_CapsAtMaxServicesKeepingTopByErrorDelta(t *testing.T) {
	const n = 150
	seedPoints := make([]metricset.Point, 0, 2*n)
	points := make([]metricset.Point, 0, 2*n)
	for i := 0; i < n; i++ {
		svc := fmt.Sprintf("svc-%03d", i)
		seedPoints = append(seedPoints, obiPoint("ns-a", svc, "GET", "/x", "500", 0), obiPoint("ns-a", svc, "GET", "/x", "200", 0))
		points = append(points, obiPoint("ns-a", svc, "GET", "/x", "500", 20+i), obiPoint("ns-a", svc, "GET", "/x", "200", 100))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(seedPoints...)))
	c, sender := obiRED(t, src, 10, 0.01, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, setOf(obiMetric(points...)))
	c.runOnce(context.Background())
	if sender.redCallCount() != 1 {
		t.Fatalf("callCount = %d, want 1 (one frame, not chunked)", sender.redCallCount())
	}
	svcs := sender.redServices()
	if len(svcs) != push.MaxRedMetricsServices {
		t.Fatalf("services = %d, want %d (capped)", len(svcs), push.MaxRedMetricsServices)
	}
	minKept := int64(1 << 62)
	for _, s := range svcs {
		if s.ErrorCount < minKept {
			minKept = s.ErrorCount
		}
	}
	if minKept < 70 {
		t.Errorf("lowest kept errorCount = %d, want >= 70 (top 100 of 150 by error delta)", minKept)
	}
}

// ===========================================================================
// red_summary (ungated per-service RED golden signals, every read window) —
// covers redsource.Collector.sendRedSummary, called alongside sendRED whenever
// EnableRED is set.
// ===========================================================================

// TestOBI_Collector_RedSummarySentEveryWindowIncludingWhenGateFiresNothing
// proves the summary is UNGATED: a window whose error delta stays below the
// red_metrics gate (so SendRedMetrics is never called) still produces exactly
// one red_summary frame carrying that service's real window counters.
func TestOBI_Collector_RedSummarySentEveryWindowIncludingWhenGateFiresNothing(t *testing.T) {
	body := func(errN, okN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "502", errN),
			obiPoint("ns-a", "api", "GET", "/orders", "200", okN),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(0, 0))
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(3, 97)) // 3 errors / 100 requests: below errorThreshold=10, gate must not fire
	c.runOnce(context.Background())

	if sender.redCallCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0 (below threshold, gate must not fire)", sender.redCallCount())
	}
	// Two, not one: the summary has no first-seen suppression on ENTRY
	// existence (only on the delta value, via counterDelta's own ok=false ->
	// 0 rule) — the seed tick already saw "api" and reports it with a
	// zero window delta, and the real-delta tick reports it again below.
	if sender.redSummaryCallCount() != 2 {
		t.Fatalf("summary callCount = %d, want 2 (ungated: summary sends every window regardless of the gate, including the seed window)", sender.redSummaryCallCount())
	}
	svcs := sender.lastRedSummary()
	if len(svcs) != 1 || svcs[0].Namespace != "ns-a" || svcs[0].Service != "api" || svcs[0].RequestCount != 100 || svcs[0].ErrorCount != 3 {
		t.Fatalf("summary service = %+v, want one ns-a/api entry requestCount=100 errorCount=3", svcs)
	}
}

// TestOBI_Collector_RedSummaryIncludesGateCrossingServices proves a service
// that DOES cross the red_metrics gate still appears in the red_summary frame
// for the same window — the two frames are independent, not mutually
// exclusive — and that the summary entry never carries the Sample the gated
// frame attaches (WIRE CONTRACT: Sample is always empty on a summary).
func TestOBI_Collector_RedSummaryIncludesGateCrossingServices(t *testing.T) {
	body := func(errN, okN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "502", errN),
			obiPoint("ns-a", "api", "GET", "/orders", "200", okN),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(0, 0))
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(15, 85)) // rate 15%: crosses both errorThreshold=10 and minErrorRatePct=5
	c.runOnce(context.Background())

	if sender.redCallCount() != 1 {
		t.Fatalf("gated callCount = %d, want 1 (must cross the gate)", sender.redCallCount())
	}
	gated := sender.redServices()
	if len(gated) != 1 || gated[0].Service != "api" || gated[0].ErrorCount != 15 || gated[0].RequestCount != 100 {
		t.Fatalf("gated service = %+v, want one api entry errorCount=15 requestCount=100", gated)
	}

	if sender.redSummaryCallCount() != 2 { // seed window (zero delta) + this window
		t.Fatalf("summary callCount = %d, want 2", sender.redSummaryCallCount())
	}
	summary := sender.lastRedSummary()
	if len(summary) != 1 || summary[0].Service != "api" || summary[0].ErrorCount != 15 || summary[0].RequestCount != 100 {
		t.Fatalf("summary service = %+v, want one api entry errorCount=15 requestCount=100 (same window totals as the gated frame)", summary)
	}
	if summary[0].Sample != "" {
		t.Errorf("summary Sample = %q, want empty (summaries never carry a sample, even for a gate-crossing service)", summary[0].Sample)
	}
}

// TestOBI_Collector_RedSummaryCapsAtMaxServices mirrors
// TestOBI_Collector_CapsAtMaxServicesKeepingTopByErrorDelta for the summary
// path: an oversized service set read in one window is capped at push.MaxRedMetricsServices
// (no silent truncation — the collector logs the dropped count), keeping the
// busiest services by request count. ErrorThreshold is set unreachably high so
// the gated red_metrics path never fires, isolating this test to the summary's
// own cap logic.
func TestOBI_Collector_RedSummaryCapsAtMaxServices(t *testing.T) {
	const n = 150
	seedPoints := make([]metricset.Point, 0, n)
	points := make([]metricset.Point, 0, n)
	for i := 0; i < n; i++ {
		svc := fmt.Sprintf("svc-%03d", i)
		seedPoints = append(seedPoints, obiPoint("ns-a", svc, "GET", "/x", "200", 0))
		points = append(points, obiPoint("ns-a", svc, "GET", "/x", "200", 50+i))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(seedPoints...)))
	c, sender := obiRED(t, src, 1_000_000, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, setOf(obiMetric(points...)))
	c.runOnce(context.Background())
	if sender.redCallCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0 (isolating the summary cap)", sender.redCallCount())
	}
	if sender.redSummaryCallCount() != 2 { // seed window (zero delta, also capped) + this window
		t.Fatalf("summary callCount = %d, want 2 (one frame per window, not chunked)", sender.redSummaryCallCount())
	}
	svcs := sender.lastRedSummary()
	if len(svcs) != push.MaxRedMetricsServices {
		t.Fatalf("summary services = %d, want %d (capped)", len(svcs), push.MaxRedMetricsServices)
	}
	minKept := int64(1 << 62)
	for _, s := range svcs {
		if s.RequestCount < minKept {
			minKept = s.RequestCount
		}
	}
	if minKept < 100 {
		t.Errorf("lowest kept requestCount = %d, want >= 100 (top 100 of 150 by request count: 50+50..50+149)", minKept)
	}
}

// TestOBI_Collector_RedSummarySkipsEmptyWindow proves an empty result: every
// sample read that's unattributable to a service (no Service label) — sends no
// red_summary frame at all, mirroring TestOBI_Collector_SkipsEmptyServiceEntries'
// gated-path assertion that an unattributed entry is dropped, not forwarded as a
// bogus zero-value service.
func TestOBI_Collector_RedSummarySkipsEmptyWindow(t *testing.T) {
	seed := setOf(obiMetric(
		obiPoint("ns-a", "", "GET", "/x", "500", 0),
		obiPoint("ns-a", "", "GET", "/x", "200", 0),
	))
	body := setOf(obiMetric(
		obiPoint("ns-a", "", "GET", "/x", "500", 40),
		obiPoint("ns-a", "", "GET", "/x", "200", 10),
	))
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body)
	c.runOnce(context.Background())
	if sender.redSummaryCallCount() != 0 {
		t.Fatalf("summary callCount = %d, want 0 (every entry unattributed; empty frame must not be sent)", sender.redSummaryCallCount())
	}
}

// TestOBI_Collector_RedSummaryAttachesP99WhenGateDoesNotFire proves P99Ms is
// derived for a summary entry independently of the red_metrics gate — mirrors
// TestOBI_Collector_P99Interpolation's fixture but with zero errors, so the
// gate never fires and only the summary frame carries the result.
func TestOBI_Collector_RedSummaryAttachesP99WhenGateDoesNotFire(t *testing.T) {
	body := func(errN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "200", 100, obiBuckets(50, 90, 99, 100)...),
			obiPoint("ns-a", "api", "GET", "/orders", "500", errN),
		))
	}
	seed := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "200", 0, obiBuckets(0, 0, 0, 0)...),
		obiPoint("ns-a", "api", "GET", "/orders", "500", 0),
	))
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(0)) // zero errors: below errorThreshold=10, gate must not fire
	c.runOnce(context.Background())

	if sender.redCallCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0 (below threshold)", sender.redCallCount())
	}
	svcs := sender.lastRedSummary()
	if len(svcs) != 1 {
		t.Fatalf("summary services = %d, want 1", len(svcs))
	}
	if got, want := svcs[0].P99Ms, 1000.0; got != want {
		t.Errorf("summary P99Ms = %v, want %v (derivable even though the gate never fired)", got, want)
	}
}

func TestOBI_Collector_NoTargetsSkipsTick(t *testing.T) {
	src := newFakeSource()
	c, sender := obiRED(t, src, 10, 0, Options{})
	c.runOnce(context.Background())
	if sender.redCallCount() != 0 {
		t.Errorf("callCount = %d, want 0 (no targets)", sender.redCallCount())
	}
}

func TestOBI_Collector_ForgetsStaleTargets(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(obiPoint("ns-a", "api", "GET", "/x", "500", 20))))
	src.set(testTarget2, setOf(obiMetric(obiPoint("ns-a", "api", "GET", "/x", "500", 30))))
	c, sender := obiRED(t, src, 10, 0.01, Options{})

	ctx := context.Background()
	c.runOnce(ctx)
	if len(c.prevReq) != 2 {
		t.Fatalf("len(c.prevReq) after first runOnce = %d, want 2", len(c.prevReq))
	}
	if sender.redCallCount() != 0 {
		t.Fatalf("callCount after first runOnce = %d, want 0 (first-seen suppression)", sender.redCallCount())
	}

	src.remove(testTarget2)
	c.runOnce(ctx)
	if len(c.prevReq) != 1 {
		t.Fatalf("len(c.prevReq) after second runOnce = %d, want 1 (gone target's baseline must be forgotten)", len(c.prevReq))
	}
	if _, ok := c.prevReq[testTarget]; !ok {
		t.Errorf("c.prevReq missing surviving target %q; contents: %+v", testTarget, c.prevReq)
	}
}

func twoNamespaceReqMetric(tenantACount, otherCount int) *metricset.Metric {
	return obiMetric(
		obiPoint("tenant-a", "backend", "GET", "/x", "500", tenantACount),
		obiPoint("ns-b", "api", "GET", "/x", "500", otherCount),
	)
}

func TestOBI_Collector_NamespaceFilter_DropsOtherTenants(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(twoNamespaceReqMetric(0, 0)))
	c, sender := obiRED(t, src, 10, 0.01, Options{Namespaces: []string{"tenant-a"}})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, setOf(twoNamespaceReqMetric(50, 90)))
	c.runOnce(ctx)

	got := sender.redServices()
	if len(got) != 1 {
		t.Fatalf("services = %d (%+v), want 1 (ns-b must be filtered out)", len(got), got)
	}
	if got[0].Namespace != "tenant-a" || got[0].Service != "backend" {
		t.Errorf("forwarded %s/%s, want tenant-a/backend", got[0].Namespace, got[0].Service)
	}
}

func TestOBI_Collector_NamespaceFilter_EmptyForwardsAll(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(twoNamespaceReqMetric(0, 0)))
	c, sender := obiRED(t, src, 10, 0.01, Options{})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, setOf(twoNamespaceReqMetric(50, 90)))
	c.runOnce(ctx)

	if got := sender.redServices(); len(got) != 2 {
		t.Fatalf("services = %d (%+v), want 2 (no filter configured)", len(got), got)
	}
}

// A namespace admitted ONLY by the dynamic NamespaceAllowed hook — e.g. one
// pulled into scope by the triageagent.dev/tenant annotation
// (push/namespace_scope.go), invisible to the static Namespaces list frozen
// at process start — must enter red_metrics payloads; one the hook rejects
// must not, even when the static list contains it. The hook OVERRIDES the
// static set entirely.
func TestOBI_Collector_NamespaceAllowed_OverridesStaticSet(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(twoNamespaceReqMetric(0, 0)))
	c, sender := obiRED(t, src, 10, 0.01, Options{
		Namespaces:       []string{"ns-b"}, // static set: ns-b only — no tenant-a
		NamespaceAllowed: func(ns string) bool { return ns == "tenant-a" },
	})

	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, setOf(twoNamespaceReqMetric(50, 90)))
	c.runOnce(ctx)

	got := sender.redServices()
	if len(got) != 1 {
		t.Fatalf("services = %d (%+v), want 1 (hook admits tenant-a, rejects statically-listed ns-b)", len(got), got)
	}
	if got[0].Namespace != "tenant-a" || got[0].Service != "backend" {
		t.Errorf("forwarded %s/%s, want tenant-a/backend", got[0].Namespace, got[0].Service)
	}
}

func TestOBI_Collector_MinErrorRatePctZeroGatesOnThresholdAlone(t *testing.T) {
	seed := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "500", 0),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 0),
	))
	body := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "500", 20),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 99980),
	))
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := obiRED(t, src, 10, 0, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body)
	c.runOnce(context.Background())
	if sender.redCallCount() != 1 {
		t.Fatalf("callCount = %d, want 1 (rate floor disabled, absolute threshold alone must qualify)", sender.redCallCount())
	}
}

func TestOBI_Collector_P99Interpolation(t *testing.T) {
	body := func(errN int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "200", 100, obiBuckets(50, 90, 99, 100)...),
			obiPoint("ns-a", "api", "GET", "/orders", "500", errN),
		))
	}
	seed := setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "200", 0, obiBuckets(0, 0, 0, 0)...),
		obiPoint("ns-a", "api", "GET", "/orders", "500", 0),
	))
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())
	if sender.redCallCount() != 0 {
		t.Fatalf("after seed read: callCount = %d, want 0 (first-seen suppression)", sender.redCallCount())
	}

	src.set(testTarget, body(20))
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d, want 1", len(svcs))
	}
	if got, want := svcs[0].P99Ms, 1000.0; got != want {
		t.Errorf("P99Ms = %v, want %v (exact bucket-boundary crossing)", got, want)
	}

	src.set(testTarget, body(40))
	c.runOnce(context.Background())
	svcs = sender.redServices()
	last := svcs[len(svcs)-1]
	if last.P99Ms != 0 {
		t.Errorf("P99Ms on a zero-histogram-delta window = %v, want 0 (nothing to derive)", last.P99Ms)
	}
}

func TestOBI_Collector_ComputeBucketDeltasFirstSeenYieldsZeroDeltas(t *testing.T) {
	buckets := func(le1, leInf uint64) []metricset.Bucket {
		return []metricset.Bucket{{UpperBound: 1, CumulativeCount: le1}, {UpperBound: math.Inf(1), CumulativeCount: leInf}}
	}

	opts := Options{Profile: ProfileOBI}
	opts.setDefaults()
	c := &Collector{opts: opts, prevBuckets: make(map[string]map[bucketSeries]map[float64]uint64)}
	pair := servicePair{Namespace: "ns-a", Workload: "api"}

	fam1 := obiMetric(metricset.Point{
		Attrs:   map[string]string{"k8s.namespace.name": "ns-a", "service.name": "api"},
		Value:   1000,
		Buckets: buckets(900, 1000),
	})
	sums := c.computeBucketDeltas("addr1", fam1)
	got := sums[pair]
	if got[1] != 0 || got[math.Inf(1)] != 0 {
		t.Fatalf("first-seen bucket deltas = %+v, want all zero", got)
	}
	if _, ok := p99FromBucketDeltas(got); ok {
		t.Error("p99FromBucketDeltas on first-seen deltas: ok = true, want false")
	}

	fam2 := obiMetric(metricset.Point{
		Attrs:   map[string]string{"k8s.namespace.name": "ns-a", "service.name": "api"},
		Value:   1100,
		Buckets: buckets(950, 1100),
	})
	sums = c.computeBucketDeltas("addr1", fam2)
	got = sums[pair]
	if got[1] != 50 || got[math.Inf(1)] != 100 {
		t.Errorf("second-read bucket deltas = %+v, want {1:50, +Inf:100}", got)
	}
	if _, ok := p99FromBucketDeltas(got); !ok {
		t.Error("p99FromBucketDeltas on real deltas: ok = false, want true")
	}
}

// A series that appears on a target already read is new on the sensor and
// started from zero there, so its bucket counts are this window's. Only the
// first read of a target's histogram yields no delta.
func TestOBI_Collector_ComputeBucketDeltasNewSeriesOnKnownTargetCountsFromZero(t *testing.T) {
	buckets := func(le1, leInf uint64) []metricset.Bucket {
		return []metricset.Bucket{{UpperBound: 1, CumulativeCount: le1}, {UpperBound: math.Inf(1), CumulativeCount: leInf}}
	}
	point := func(route string, le1, leInf uint64) metricset.Point {
		return metricset.Point{
			Attrs:   map[string]string{"k8s.namespace.name": "ns-a", "service.name": "api", "http.route": route},
			Value:   float64(leInf),
			Buckets: buckets(le1, leInf),
		}
	}
	opts := Options{Profile: ProfileOBI}
	opts.setDefaults()
	c := &Collector{opts: opts, prevBuckets: make(map[string]map[bucketSeries]map[float64]uint64)}
	pair := servicePair{Namespace: "ns-a", Workload: "api"}

	c.computeBucketDeltas("addr1", obiMetric(point("/a", 900, 1000)))
	got := c.computeBucketDeltas("addr1", obiMetric(point("/a", 900, 1000), point("/b", 30, 40)))[pair]
	if got[1] != 30 || got[math.Inf(1)] != 40 {
		t.Errorf("bucket deltas = %+v, want the new series' {1:30, +Inf:40}", got)
	}
	// Another target's first read is still baseline only.
	if got := c.computeBucketDeltas("addr2", obiMetric(point("/b", 30, 40)))[pair]; got[1] != 0 || got[math.Inf(1)] != 0 {
		t.Errorf("first read of a second target = %+v, want all zero", got)
	}
}

// One window with 30 errors out of 130 requests on a route that had no 5xx
// series before: the burst is the new series' first value. It used to be
// taken for a first-seen baseline and dropped, so no frame was sent.
func TestOBI_Collector_NewErrorSeriesBurstIsCounted(t *testing.T) {
	src := newFakeSource()
	c, sender := obiRED(t, src, 10, 5, Options{})
	ok, errs := 0, 0
	tick := func() []push.ServiceRedMetrics {
		pts := []metricset.Point{obiPoint("ns-a", "svc", "POST", "/checkout", "200", ok)}
		if errs > 0 {
			pts = append(pts, obiPoint("ns-a", "svc", "POST", "/checkout", "500", errs))
		}
		src.set(testTarget, setOf(obiMetric(pts...)))
		before := len(sender.redServices())
		c.runOnce(context.Background())
		return sender.redServices()[before:]
	}
	tick()
	for i := 0; i < 8; i++ {
		ok += 100
		if got := tick(); len(got) != 0 {
			t.Fatalf("healthy window %d sent %+v", i, got)
		}
	}
	ok += 100
	errs += 30
	got := tick()
	if len(got) != 1 || got[0].ErrorCount != 30 || got[0].RequestCount != 130 {
		t.Fatalf("burst window sent %+v, want one entry with 30 errors of 130 requests", got)
	}
	// The series stays at 30: no further errors, no further frames.
	for i := 0; i < 6; i++ {
		ok += 100
		if got := tick(); len(got) != 0 {
			t.Fatalf("window %d after the burst sent %+v", i, got)
		}
	}
}

// The first read of a target yields no delta for any series, whatever its
// value, and a request metric that is absent on the first read counts from
// zero when it appears later.
func TestOBI_Collector_FirstReadOfTargetStillYieldsNoDelta(t *testing.T) {
	src := newFakeSource()
	c, sender := obiRED(t, src, 10, 5, Options{})
	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/orders", "500", 5000),
		obiPoint("ns-a", "api", "GET", "/orders", "200", 100),
	)))
	c.runOnce(context.Background())
	if n := sender.redCallCount(); n != 0 {
		t.Fatalf("first read sent %d frames, want 0 (lifetime counters)", n)
	}
	// A second target joins later: its first read is baseline only too.
	src.set(testTarget2, setOf(obiMetric(obiPoint("ns-a", "api", "GET", "/orders", "500", 7000))))
	c.runOnce(context.Background())
	if n := sender.redCallCount(); n != 0 {
		t.Fatalf("a new target's first read sent %d frames, want 0", n)
	}
	// gRPC appears on the first target after its HTTP metric was read: new.
	src.set(testTarget, setOf(
		obiMetric(obiPoint("ns-a", "api", "GET", "/orders", "500", 5000), obiPoint("ns-a", "api", "GET", "/orders", "200", 100)),
		grpcMetric(grpcPoint("ns-a", "api", "/a.B/C", "INTERNAL", 40)),
	))
	c.runOnce(context.Background())
	svcs := sender.redServices()
	if len(svcs) != 1 || svcs[0].ErrorCount != 40 {
		t.Fatalf("services = %+v, want the 40 errors of the newly appeared gRPC metric", svcs)
	}
}

// The same raw series under a destination identity that resolves differently
// than on the previous read is the same counter: it must be diffed against its
// previous value, not counted from zero.
func TestOBI_Collector_RegroupedSeriesIsNotNew(t *testing.T) {
	src := newFakeSource()
	ids := fakeIdentityResolver{}
	c, sender := obiRED(t, src, 10, 5, Options{Identity: ids})
	body := func(n int) metricset.Set {
		p := obiPointWithOwner("ns-a", "api-7f9", "api-deploy", n)
		p.Attrs["http.response.status_code"] = "500"
		return setOf(obiMetric(p))
	}
	src.set(testTarget, body(5000))
	c.runOnce(context.Background())
	// The identity index now resolves the owner: the destination changes name.
	ids["ns-a/api-deploy"] = "api"
	src.set(testTarget, body(5003))
	c.runOnce(context.Background())
	if n := sender.redCallCount(); n != 0 {
		t.Fatalf("frames = %d, want 0: 3 new errors, not the series' 5003 lifetime errors (%+v)", n, sender.redServices())
	}
	sum := sender.lastRedSummary()
	if len(sum) != 1 || sum[0].Service != "api" || sum[0].ErrorCount != 3 {
		t.Errorf("summary = %+v, want service api with 3 errors", sum)
	}
}

// The frame cap keeps the entries a latency or baseline arm qualified (their
// ErrorCount may be 0) ahead of error-only ones, and its order never depends
// on map iteration.
func TestSortRedForCap_BreachesFirstAndDeterministic(t *testing.T) {
	build := func() []push.ServiceRedMetrics {
		var q []push.ServiceRedMetrics
		for i := 0; i < 120; i++ {
			q = append(q, push.ServiceRedMetrics{Namespace: "ns", Service: fmt.Sprintf("err-%03d", i), ErrorCount: 50})
		}
		q = append(q,
			push.ServiceRedMetrics{Namespace: "ns", Service: "slow-op", Breach: push.BreachOpLatency},
			push.ServiceRedMetrics{Namespace: "ns", Service: "slow", LatencyBreach: true},
			push.ServiceRedMetrics{Namespace: "ns", Service: "failing-op", Breach: push.BreachOpErrors, ErrorCount: 4},
		)
		return q
	}
	want := build()
	sortRedForCap(want)
	names := func(q []push.ServiceRedMetrics) (out []string) {
		for _, m := range q[:push.MaxRedMetricsServices] {
			out = append(out, m.Service)
		}
		return out
	}
	head := names(want)
	if head[0] != "failing-op" || head[1] != "slow" || head[2] != "slow-op" || head[3] != "err-000" || head[99] != "err-096" {
		t.Errorf("order = %v ..., want the three breaches, then err-000..err-096", head[:5])
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20; i++ {
		q := build()
		rng.Shuffle(len(q), func(a, b int) { q[a], q[b] = q[b], q[a] })
		sortRedForCap(q)
		if got := names(q); !reflect.DeepEqual(got, head) {
			t.Fatalf("shuffle %d: kept set or order differs", i)
		}
	}
}

// obiOwnerBody builds one point carrying k8s.owner.name, a status, a
// request count, and the two-bucket ladder RedSummary_DurationHistogram...
// below interpolates from — Value is the point's own request count, exactly
// as a real OTel http.server.request.duration data point would report both
// together.
func obiOwnerBody(namespace, service, owner string, le1, leInf, reqCount int) *metricset.Metric {
	return obiMetric(obiOwnerPoint(namespace, service, owner, le1, leInf, reqCount, map[string]string{
		"http.request.method": "GET", "http.route": "/",
	}))
}

// obiOwnerPoint is obiOwnerBody's series with the operation labels in op.
func obiOwnerPoint(namespace, service, owner string, le1, leInf, reqCount int, op map[string]string) metricset.Point {
	attrs := map[string]string{
		"k8s.namespace.name":        namespace,
		"service.name":              service,
		"k8s.owner.name":            owner,
		"http.response.status_code": "200",
	}
	for k, v := range op {
		attrs[k] = v
	}
	return metricset.Point{
		Attrs: attrs,
		Value: float64(reqCount),
		Buckets: []metricset.Bucket{
			{UpperBound: 1, CumulativeCount: uint64(le1)},
			{UpperBound: math.Inf(1), CumulativeCount: uint64(leInf)},
		},
	}
}

// An undecoded HTTP/2 stream (no method, no route) counts as a request but
// not as latency: its duration is the stream's lifetime. Only such series
// leave the service with no p99; an identified one beside it still reads.
func TestOBI_Collector_UnidentifiedSeriesLeftOutOfLatency(t *testing.T) {
	src := newFakeSource()
	stream := func(le1, leInf, n int) *metricset.Metric {
		return obiMetric(obiOwnerPoint("ns-a", "flags", "flags", le1, leInf, n, nil))
	}
	src.set(testTarget, setOf(stream(0, 0, 0)))
	c, sender := obiRED(t, src, 10, 0.01, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, setOf(stream(0, 3, 3)))
	c.runOnce(context.Background())
	svcs := sender.lastRedSummary()
	if len(svcs) != 1 || svcs[0].RequestCount != 3 {
		t.Fatalf("red_summary = %+v, want one service with its 3 requests", svcs)
	}
	if svcs[0].P99Ms != 0 {
		t.Errorf("P99Ms = %v, want 0: a stream's lifetime is not request latency", svcs[0].P99Ms)
	}
}

// TestOBI_Collector_RedSummary_DurationHistogramKeyedByResolvedIdentity
// proves computeBucketDeltas keys its destination the same way
// computeRequestDeltas does (destPairFor, shared by both): sendRedSummary
// looks up bucketTotals by the request-side servicePair, so a resolved
// identity must land the duration histogram under that identical key or
// P99Ms never reaches the wire.
func TestOBI_Collector_RedSummary_DurationHistogramKeyedByResolvedIdentity(t *testing.T) {
	cases := []struct {
		name         string
		identity     IdentityResolver
		wantWorkload string
	}{
		{
			name:         "identity_resolver_renames_the_destination_workload",
			identity:     fakeIdentityResolver{"ns-a/owner-a": "svc-identity"},
			wantWorkload: "svc-identity",
		},
		{
			name:         "no_identity_resolver_keeps_the_raw_service_name",
			identity:     nil,
			wantWorkload: "raw-name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource()
			src.set(testTarget, setOf(obiOwnerBody("ns-a", "raw-name", "owner-a", 0, 0, 0)))
			c, sender := obiRED(t, src, 10, 0.01, Options{Identity: tc.identity})
			c.runOnce(context.Background()) // cold-start baseline

			src.set(testTarget, setOf(obiOwnerBody("ns-a", "raw-name", "owner-a", 50, 100, 100)))
			c.runOnce(context.Background())

			svcs := sender.lastRedSummary()
			if len(svcs) != 1 {
				t.Fatalf("red_summary services = %+v, want 1", svcs)
			}
			if svcs[0].Service != tc.wantWorkload {
				t.Fatalf("summary service = %q, want %q", svcs[0].Service, tc.wantWorkload)
			}
			if svcs[0].P99Ms <= 0 {
				t.Errorf("summary P99Ms = %v, want > 0 (duration histogram must be found under the request-side key)", svcs[0].P99Ms)
			}
		})
	}
}

// ===========================================================================
// ProfileOBI TOPOLOGY (client-metric edge derivation) fixtures + tests.
// EnableTopology only, Interval==TopologyInterval so every runOnce is
// exactly one flush.
// ===========================================================================

func TestOBI_Collector_NodesFromServerSeries_PresenceOnly(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "app", "", "", "", 0),
		obiPoint("ns-a", "db", "", "", "", 0),
	)))
	c, sender := obiTopology(t, src, Options{})

	c.runOnce(context.Background())
	if sender.topologyCallCount() != 1 {
		t.Fatalf("callCount = %d, want 1 (nodes are presence-based, not delta-gated)", sender.topologyCallCount())
	}
	nodes := sender.lastTopology().nodes
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v, want 2", nodes)
	}
	want := map[string]bool{"ns-a/app": true, "ns-a/db": true}
	for _, n := range nodes {
		key := n.Namespace + "/" + n.Workload
		if !want[key] {
			t.Errorf("unexpected node %+v", n)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Errorf("missing nodes: %v", want)
	}
}

func TestOBI_Collector_InScopeEdge_ResolvesDstViaInjectedResolver(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", n)),
		)
	}
	resolver := fakeResolverFunc(func(srcNamespace, serverAddress string) (servicePair, string) {
		if srcNamespace == "ns-a" && serverAddress == "db.ns-a" {
			return servicePair{Namespace: "ns-a", Workload: "db"}, ""
		}
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	})
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Resolver: resolver})
	c.runOnce(context.Background())

	src.set(testTarget, body(9))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.SrcNamespace != "ns-a" || e.SrcWorkload != "app" || e.DstNamespace != "ns-a" || e.DstWorkload != "db" || e.External != "" || e.Calls != 9 || e.Protocol != "grpc" {
		t.Errorf("edge = %+v, want in-scope ns-a/app -> ns-a/db, calls=9, protocol=grpc", e)
	}
}

func TestOBI_Collector_NoResolverWired_UnseenDestinationsAreExternal(t *testing.T) {
	// The legacy fallback (docs/todo/service-graph-from-obi.md): with no
	// Options.Resolver set the server_address text is not interpreted, so a
	// destination the collector has not read as a service is External. A
	// destination it has read is named (TestOBI_Collector_NoResolverWired_
	// NamesTheServicesItHasRead).
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "cache.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(4))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.External != "cache.ns-a" || e.DstNamespace != "" || e.DstWorkload != "" || e.Calls != 4 {
		t.Errorf("edge = %+v, want External=cache.ns-a, no Dst, calls=4", e)
	}
}

// An OTLP-only agent has no Service index, so a client span's server.address
// is matched against the services it has read as request destinations: a bare
// name of the caller's namespace and the cluster DNS forms resolve; an IP and
// a name nobody serves stay external.
func TestOBI_Collector_NoResolverWired_NamesTheServicesItHasRead(t *testing.T) {
	for addr, want := range map[string]string{
		"db":                         "ns-a/db",
		"db:5432":                    "ns-a/db",
		"db.ns-a":                    "ns-a/db",
		"db.ns-a.svc":                "ns-a/db",
		"DB.ns-a.svc.cluster.local.": "ns-a/db",
		"db.other-ns.svc":            "", // not a service we have read
		"cache":                      "",
		"10.0.0.7":                   "",
		"db.example.com":             "",
	} {
		t.Run(addr, func(t *testing.T) {
			body := func(n int) metricset.Set {
				return setOf(
					obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
					obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", addr, n)),
				)
			}
			src := newFakeSource()
			src.set(testTarget, body(0))
			c, sender := obiTopology(t, src, Options{})
			c.runOnce(context.Background())
			src.set(testTarget, body(4))
			c.runOnce(context.Background())
			edges := sender.lastTopology().edges
			if len(edges) != 1 {
				t.Fatalf("edges = %+v, want 1", edges)
			}
			got := ""
			if edges[0].External == "" {
				got = edges[0].DstNamespace + "/" + edges[0].DstWorkload
			}
			if got != want {
				t.Errorf("%s resolved to %q (external %q), want %q", addr, got, edges[0].External, want)
			}
		})
	}
}

// A resolver that cannot place an address (the remote resolver of an agent
// with no Kubernetes watch leaves a bare name of an untagged namespace
// external) does not stop the services the collector has read from naming it.
func TestOBI_Collector_ResolverLeavesExternal_ReadServicesStillNameTheCallee(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db", n)),
		)
	}
	external := fakeResolverFunc(func(_, serverAddress string) (servicePair, string) {
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	})
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Resolver: external})
	c.runOnce(context.Background())
	src.set(testTarget, body(4))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 || edges[0].External != "" || edges[0].DstNamespace != "ns-a" || edges[0].DstWorkload != "db" {
		t.Fatalf("edges = %+v, want ns-a/app -> ns-a/db", edges)
	}
}

// TestOBI_Collector_SourceOwnerIdentity_ResolvesViaIdentityResolver proves
// Profile.SourceOwnerLabel + Options.Identity end to end: both the RED/node
// identity (server-side family) and the client-edge source identity
// (client-side family) come from the resolved virtual-app identity, not the
// raw service_name label, when Options.Identity knows the owner.
func TestOBI_Collector_SourceOwnerIdentity_ResolvesViaIdentityResolver(t *testing.T) {
	identity := fakeIdentityResolver{"ns-a/app-deployment": "app"}
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPointWithOwner("ns-a", "app-service-name", "app-deployment", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPointWithOwner("ns-a", "app-service-name", "app-deployment", "db.ns-a", n)),
		)
	}
	resolver := fakeResolverFunc(func(_ string, addr string) (servicePair, string) {
		if addr == "db.ns-a" {
			return servicePair{Namespace: "ns-a", Workload: "db"}, ""
		}
		return servicePair{}, push.TruncateTopologyExternal(addr)
	})
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Identity: identity, Resolver: resolver})
	c.runOnce(context.Background())

	src.set(testTarget, body(7))
	c.runOnce(context.Background())

	nodes := sender.lastTopology().nodes
	wantNode := push.TopologyNode{Namespace: "ns-a", Workload: "app"}
	found := false
	for _, n := range nodes {
		if n == wantNode {
			found = true
		}
		if n.Workload == "app-service-name" {
			t.Errorf("node %+v carries the raw service_name; identity resolution did not apply", n)
		}
	}
	if !found {
		t.Errorf("nodes = %+v, want %+v (resolved identity, not the raw service_name)", nodes, wantNode)
	}

	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	if edges[0].SrcWorkload != "app" {
		t.Errorf("edge src = %q, want resolved identity %q", edges[0].SrcWorkload, "app")
	}
}

// TestOBI_Collector_SourceOwnerIdentity_FallsBackWhenUnknown proves the
// other half: an owner Options.Identity does not recognize falls back to
// the plain service_name value, never an empty/zero identity.
func TestOBI_Collector_SourceOwnerIdentity_FallsBackWhenUnknown(t *testing.T) {
	identity := fakeIdentityResolver{} // knows nothing
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(obiPointWithOwner("ns-a", "app-service-name", "app-deployment", 0))))
	c, sender := obiTopology(t, src, Options{Identity: identity})
	c.runOnce(context.Background())

	nodes := sender.lastTopology().nodes
	if len(nodes) != 1 || nodes[0].Workload != "app-service-name" {
		t.Fatalf("nodes = %+v, want [ns-a/app-service-name] (fallback to service_name)", nodes)
	}
}

// TestOBI_Collector_ClientEdge_UnknownSrcOwnerSkipsRow proves the L7
// client-edge source side of the same rule as the flow path: a caller
// whose owning Deployment isn't tracked (deleted, or a CronJob/Job pod) is
// dropped, never emitted under the raw service_name.
func TestOBI_Collector_ClientEdge_UnknownSrcOwnerSkipsRow(t *testing.T) {
	identity := fakeIdentityResolver{} // knows nothing
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPointWithOwner("ns-a", "app-service-name", "app-deployment", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Identity: identity})
	c.runOnce(context.Background())

	src.set(testTarget, body(9))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 0 {
		t.Errorf("edges = %+v, want 0 (untracked client-edge source skipped entirely)", edges)
	}
	for _, n := range sender.lastTopology().nodes {
		if n.Workload == "app-service-name" {
			t.Errorf("untracked source leaked into nodes: %+v", n)
		}
	}
}

// TestOBI_Collector_FlowEdge_EndToEnd proves the L4 network-flow path
// end to end: a flow-only edge (no L7 client family for this pair) carries
// Bytes, Calls==0, Protocol=="tcp", and its endpoints are resolved through
// the SAME Identity resolver as the L7 path (Profile.Flow{Src,Dst}OwnerLabel).
func TestOBI_Collector_FlowEdge_EndToEnd(t *testing.T) {
	identity := fakeIdentityResolver{
		"ns-a/orders":  "orders",
		"ns-a/billing": "billing",
	}
	// ProfileOBI.FlowDirectionLabel is set ("direction"), so a realistic
	// row must carry it — filterFlowSamples drops a direction-less row once
	// the label is configured (see TestFilterFlowSamples_DirectionFiltersToRequestOnly).
	body := func(bytesVal int) metricset.Set {
		return setOf(flowMetric(flowPointWithDirection("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", "request", bytesVal)))
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Identity: identity})
	c.runOnce(context.Background()) // cold-start baseline

	src.set(testTarget, body(2000))
	c.runOnce(context.Background())

	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.SrcNamespace != "ns-a" || e.SrcWorkload != "orders" || e.DstNamespace != "ns-a" || e.DstWorkload != "billing" ||
		e.Calls != 0 || e.Bytes != 2000 || e.Protocol != "tcp" {
		t.Errorf("edge = %+v, want ns-a/orders -> ns-a/billing, calls=0, bytes=2000, protocol=tcp", e)
	}
}

// TestOBI_Collector_FlowEdge_IdentityNilKeepsRawOwnerNames proves the
// legacy/no-vapps flow path (Options.Identity unset) keeps raw owner names
// as the topology workload names, unchanged.
func TestOBI_Collector_FlowEdge_IdentityNilKeepsRawOwnerNames(t *testing.T) {
	body := func(bytesVal int) metricset.Set {
		return setOf(flowMetric(flowPointWithDirection("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", "request", bytesVal)))
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(500))
	c.runOnce(context.Background())

	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.SrcNamespace != "ns-a" || e.SrcWorkload != "orders" || e.DstNamespace != "ns-a" || e.DstWorkload != "billing" ||
		e.External != "" || e.Bytes != 500 {
		t.Errorf("edge = %+v, want raw owner names ns-a/orders -> ns-a/billing, bytes=500", e)
	}
}

// TestOBI_Collector_FlowEdge_UnknownSrcOwnerSkipsRow reproduces a live
// defect observed in a customer cluster (2026-09-04): a flow row's SOURCE
// owner is no longer (or never was) a tracked workload — deleted between the
// identity recompute and this window's topology flush, or CronJob/Job-owned.
// It must be dropped entirely rather than emitted as an in-scope node under
// its raw owner name.
func TestOBI_Collector_FlowEdge_UnknownSrcOwnerSkipsRow(t *testing.T) {
	identity := fakeIdentityResolver{
		"ns-a/billing": "billing",
	}
	body := func(bytesVal int) metricset.Set {
		return setOf(flowMetric(flowPointWithDirection("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", "request", bytesVal)))
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Identity: identity})
	c.runOnce(context.Background())

	src.set(testTarget, body(2000))
	c.runOnce(context.Background())

	edges := sender.lastTopology().edges
	if len(edges) != 0 {
		t.Errorf("edges = %+v, want 0 (untracked flow source skipped entirely)", edges)
	}
	for _, n := range sender.lastTopology().nodes {
		if n.Workload == "orders" || n.Workload == "billing" {
			t.Errorf("skipped-row node leaked into nodes: %+v", n)
		}
	}
}

// TestOBI_Collector_FlowEdge_UnknownDstOwnerBecomesExternal covers the
// other end of the same defect: the DESTINATION owner is untracked. It must
// resolve as External under its raw name, never a fabricated in-scope node.
func TestOBI_Collector_FlowEdge_UnknownDstOwnerBecomesExternal(t *testing.T) {
	identity := fakeIdentityResolver{
		"ns-a/orders": "orders",
	}
	body := func(bytesVal int) metricset.Set {
		return setOf(flowMetric(flowPointWithDirection("ns-a", "orders", "Deployment", "ns-a", "billing", "Deployment", "request", bytesVal)))
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Identity: identity})
	c.runOnce(context.Background())

	src.set(testTarget, body(2000))
	c.runOnce(context.Background())

	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.SrcNamespace != "ns-a" || e.SrcWorkload != "orders" || e.DstNamespace != "" || e.DstWorkload != "" ||
		e.External != "billing.ns-a" || e.Bytes != 2000 || e.Protocol != "tcp" {
		t.Errorf("edge = %+v, want ns-a/orders -> External billing.ns-a, bytes=2000", e)
	}
	foundSrc := false
	for _, n := range sender.lastTopology().nodes {
		if n.Namespace == "ns-a" && n.Workload == "billing" {
			t.Errorf("untracked destination leaked into nodes under its raw name: %+v", n)
		}
		if n.Namespace == "ns-a" && n.Workload == "orders" {
			foundSrc = true
		}
	}
	if !foundSrc {
		t.Errorf("nodes = %+v, want ns-a/orders present", sender.lastTopology().nodes)
	}
}

func TestOBI_Collector_ExternalEdgeForIP(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0)),
			obiClientMetric("http.client.request.duration", obiClientPoint("ns-a", "app", "10.0.4.12:5432", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(3))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.External != "10.0.4.12:5432" || e.DstNamespace != "" || e.DstWorkload != "" || e.Calls != 3 {
		t.Errorf("edge = %+v, want External=10.0.4.12:5432, no Dst, calls=3", e)
	}
}

func TestOBI_Collector_ExternalEdgeForOutOfScopeNamespace(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0)),
			obiClientMetric("http.client.request.duration", obiClientPoint("ns-a", "app", "billing.ns-b.svc.cluster.local:443", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Namespaces: []string{"ns-a"}})
	c.runOnce(context.Background())

	src.set(testTarget, body(2))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("edges = %+v, want 1", edges)
	}
	e := edges[0]
	if e.External != "billing.ns-b.svc.cluster.local:443" || e.DstNamespace != "" {
		t.Errorf("edge = %+v, want External (out-of-scope namespace), no Dst", e)
	}
}

func TestOBI_Collector_OutOfScopeSrcSkippedEntirely(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-b", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-b", "app", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{Namespaces: []string{"ns-a"}})
	c.runOnce(context.Background())

	src.set(testTarget, body(9))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 0 {
		t.Errorf("edges = %+v, want 0 (out-of-scope src skipped entirely)", edges)
	}
	for _, n := range sender.lastTopology().nodes {
		if n.Namespace == "ns-b" {
			t.Errorf("out-of-scope node leaked into nodes: %+v", n)
		}
	}
}

func TestOBI_Collector_FirstSnapshotIncludesZeroCallsEdges(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
		obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", 0)),
	))
	c, sender := obiTopology(t, src, Options{})

	c.runOnce(context.Background())
	if sender.topologyCallCount() != 1 {
		t.Fatalf("callCount = %d, want 1", sender.topologyCallCount())
	}
	edges := sender.lastTopology().edges
	if len(edges) != 1 {
		t.Fatalf("first-snapshot edges = %+v, want 1 (included despite Calls=0)", edges)
	}
	if edges[0].Calls != 0 {
		t.Errorf("edges[0].Calls = %d, want 0", edges[0].Calls)
	}
}

func TestOBI_Collector_SubsequentSnapshotDropsZeroCallsEdges(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(0))
	c.runOnce(context.Background())
	if sender.topologyCallCount() != 2 {
		t.Fatalf("callCount = %d, want 2", sender.topologyCallCount())
	}
	edges := sender.lastTopology().edges
	if len(edges) != 0 {
		t.Errorf("second-snapshot edges = %+v, want 0 (zero-Calls dropped after the first snapshot)", edges)
	}
}

func TestOBI_Collector_TopologyFirstSeenSuppressionThenRealDelta(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(400000))
	c, sender := obiTopology(t, src, Options{})

	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != 1 || edges[0].Calls != 0 {
		t.Fatalf("first-read edges = %+v, want one edge with Calls=0 (never the lifetime total)", edges)
	}

	src.set(testTarget, body(400012))
	c.runOnce(context.Background())
	edges = sender.lastTopology().edges
	if len(edges) != 1 || edges[0].Calls != 12 {
		t.Fatalf("second-read edges = %+v, want one edge with Calls=12", edges)
	}
}

func TestOBI_Collector_TopologyCounterReset(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(100))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, body(150))
	c.runOnce(context.Background())
	if got := sender.lastTopology().edges[0].Calls; got != 50 {
		t.Fatalf("pre-reset delta = %d, want 50 (150-100)", got)
	}

	src.set(testTarget, body(12))
	c.runOnce(context.Background())
	if got := sender.lastTopology().edges[0].Calls; got != 12 {
		t.Errorf("post-reset delta = %d, want 12 (current value is the delta, not cur-prev=-138)", got)
	}
}

// TestOBI_Collector_TopologyFlushFailureRetainsAccumulators proves the BUG 1
// fix: flushTopology must not clear edgeAccum/nodeAccum/ticksSinceFlush, nor
// latch firstTopologyDone, unless Sender.SendTopologySnapshot reports the
// frame was actually enqueued. Before the fix, a flush attempted while the WS
// push connection was down (or its send queue was full) would still reset
// state — permanently discarding whatever had accumulated, since these
// accumulators (unlike RED/USE, re-derived from cumulative counters every
// window) are the ONLY record of a topology window.
//
// Two consecutive real deltas are accumulated while every delivery attempt
// fails, then delivery succeeds — the final snapshot must carry BOTH deltas
// summed, proving no window was silently dropped in between.
func TestOBI_Collector_TopologyFlushFailureRetainsAccumulators(t *testing.T) {
	body := func(n int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns-a", "db", "", "", "", 0)),
			obiClientMetric("rpc.client.call.duration", obiClientPoint("ns-a", "app", "db.ns-a", n)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	c, sender := obiTopology(t, src, Options{})
	sender.setTopologyFail(true)

	sumEdgeAccum := func() int64 {
		var total int64
		for _, v := range c.edgeAccum {
			total += v.Calls
		}
		return total
	}

	// First-seen baseline read: cold-start flush (includeZero), delivery fails.
	c.runOnce(context.Background())
	if got := sender.topologyCallCount(); got != 0 {
		t.Fatalf("delivered topology frames = %d, want 0 (delivery failed)", got)
	}
	if sender.topologyAttemptCount() != 1 {
		t.Fatalf("delivery attempts = %d, want 1", sender.topologyAttemptCount())
	}
	if c.firstTopologyDone {
		t.Fatal("firstTopologyDone = true after a FAILED flush, want false")
	}
	if len(c.nodeAccum) == 0 {
		t.Fatal("nodeAccum emptied after a FAILED flush, want retained")
	}
	if c.ticksSinceFlush == 0 {
		t.Fatal("ticksSinceFlush reset after a FAILED flush, want retained (so the very next tick retries)")
	}

	// Real delta #1 accumulates on top of the still-retained window; delivery
	// fails again — this delta must survive, not be silently discarded.
	src.set(testTarget, body(9))
	c.runOnce(context.Background())
	if got := sender.topologyCallCount(); got != 0 {
		t.Fatalf("delivered topology frames = %d, want 0 (delivery still failing)", got)
	}
	if sender.topologyAttemptCount() != 2 {
		t.Fatalf("delivery attempts = %d, want 2 (the very next tick retried)", sender.topologyAttemptCount())
	}
	if c.firstTopologyDone {
		t.Fatal("firstTopologyDone = true after a second FAILED flush, want false")
	}
	if got := sumEdgeAccum(); got != 9 {
		t.Fatalf("edgeAccum total after two failed flushes = %d, want 9 (real delta retained across the failed flush)", got)
	}

	// Real delta #2 accumulates; delivery now succeeds.
	src.set(testTarget, body(9+5))
	sender.setTopologyFail(false)
	c.runOnce(context.Background())

	if got := sender.topologyCallCount(); got != 1 {
		t.Fatalf("delivered topology frames = %d, want 1 (the retry finally succeeded)", got)
	}
	if !c.firstTopologyDone {
		t.Error("firstTopologyDone = false after a SUCCESSFUL flush, want true")
	}
	if len(c.edgeAccum) != 0 || len(c.nodeAccum) != 0 || c.ticksSinceFlush != 0 {
		t.Errorf("state not reset after successful flush: edgeAccum=%v nodeAccum=%v ticksSinceFlush=%d",
			c.edgeAccum, c.nodeAccum, c.ticksSinceFlush)
	}
	edges := sender.lastTopology().edges
	if len(edges) != 1 || edges[0].Calls != 14 {
		t.Fatalf("delivered edges = %+v, want one edge with Calls=14 (9+5: both real deltas accumulated across the two failed flushes, delivered together)", edges)
	}
}

func TestOBI_Collector_CapsNodesAlphabeticallyWithLogging(t *testing.T) {
	const n = push.MaxTopologyNodes + 10
	points := make([]metricset.Point, 0, n)
	for i := 0; i < n; i++ {
		points = append(points, obiPoint("ns-a", fmt.Sprintf("svc-%03d", i), "", "", "", 0))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(points...)))
	c, sender := obiTopology(t, src, Options{})

	c.runOnce(context.Background())
	nodes := sender.lastTopology().nodes
	if len(nodes) != push.MaxTopologyNodes {
		t.Fatalf("nodes = %d, want %d (capped)", len(nodes), push.MaxTopologyNodes)
	}
	for _, node := range nodes {
		if node.Workload >= fmt.Sprintf("svc-%03d", push.MaxTopologyNodes) {
			t.Errorf("kept node %+v should have been trimmed (not alphabetically-first)", node)
		}
	}
}

func TestOBI_Collector_CapsEdgesByCallsWithLogging(t *testing.T) {
	const n = push.MaxTopologyEdges + 10
	seedServerPoints := []metricset.Point{obiPoint("ns-a", "app", "", "", "", 0)}
	serverPoints := []metricset.Point{obiPoint("ns-a", "app", "", "", "", 0)}
	var seedClientPoints, clientPoints []metricset.Point
	for i := 0; i < n; i++ {
		dst := fmt.Sprintf("svc-%03d.ns-a", i)
		svc := fmt.Sprintf("svc-%03d", i)
		seedServerPoints = append(seedServerPoints, obiPoint("ns-a", svc, "", "", "", 0))
		serverPoints = append(serverPoints, obiPoint("ns-a", svc, "", "", "", 0))
		seedClientPoints = append(seedClientPoints, obiClientPoint("ns-a", "app", dst, 0))
		clientPoints = append(clientPoints, obiClientPoint("ns-a", "app", dst, 20+i))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(seedServerPoints...), obiClientMetric("rpc.client.call.duration", seedClientPoints...)))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	src.set(testTarget, setOf(obiMetric(serverPoints...), obiClientMetric("rpc.client.call.duration", clientPoints...)))
	c.runOnce(context.Background())
	edges := sender.lastTopology().edges
	if len(edges) != push.MaxTopologyEdges {
		t.Fatalf("edges = %d, want %d (capped)", len(edges), push.MaxTopologyEdges)
	}
	minKept := int64(1 << 62)
	for _, e := range edges {
		if e.Calls < minKept {
			minKept = e.Calls
		}
	}
	if minKept < 30 {
		t.Errorf("lowest kept Calls = %d, want >= 30 (top %d of %d by Calls)", minKept, push.MaxTopologyEdges, n)
	}
}

func TestOBI_Collector_TopologyNoTargetsSkipsTick(t *testing.T) {
	src := newFakeSource()
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())
	if sender.topologyCallCount() != 0 {
		t.Errorf("callCount = %d, want 0 (no targets)", sender.topologyCallCount())
	}
}

func TestOBI_Collector_AllTargetsFailToReadSkipsSend(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(obiMetric(obiPoint("ns-a", "app", "", "", "", 0))))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())
	if sender.topologyCallCount() != 1 {
		t.Fatalf("callCount after first tick = %d, want 1", sender.topologyCallCount())
	}

	src.advertiseWithoutSet(testTarget)
	c.runOnce(context.Background())
	if sender.topologyCallCount() != 1 {
		t.Errorf("callCount after failed tick = %d, want still 1 (no snapshot sent when every target fails to read)", sender.topologyCallCount())
	}
}

func TestTopologyEveryTicks(t *testing.T) {
	cases := []struct {
		interval, topoInterval time.Duration
		want                   int
	}{
		{time.Minute, 10 * time.Minute, 10},
		{time.Minute, 30 * time.Second, 1}, // shorter than Interval clamps to 1
		{time.Minute, time.Minute, 1},
	}
	for _, c := range cases {
		o := Options{Interval: c.interval, TopologyInterval: c.topoInterval}
		if got := o.topologyEveryTicks(); got != c.want {
			t.Errorf("topologyEveryTicks(interval=%v, topoInterval=%v) = %d, want %d", c.interval, c.topoInterval, got, c.want)
		}
	}
}

// ===========================================================================
// Generic engine tests: New/nil-guards, namespaceSet, IsHTTP5xxError,
// FormatSample, sort helpers, p99FromBucketDeltas.
// ===========================================================================

func TestNew_DisabledWithoutSource(t *testing.T) {
	if c := New(Options{Profile: ProfileOBI, EnableRED: true}, &fakeSender{}, logr.Discard()); c != nil {
		t.Errorf("New with nil Source = %v, want nil", c)
	}
}

func TestNew_DisabledWithoutSender(t *testing.T) {
	if c := New(Options{Profile: ProfileOBI, Source: newFakeSource(), EnableRED: true}, nil, logr.Discard()); c != nil {
		t.Errorf("New with nil sender = %v, want nil", c)
	}
}

func TestNew_DisabledWithoutEnableFlag(t *testing.T) {
	if c := New(Options{Profile: ProfileOBI, Source: newFakeSource()}, &fakeSender{}, logr.Discard()); c != nil {
		t.Errorf("New with neither EnableRED nor EnableTopology = %v, want nil", c)
	}
}

func TestNew_DisabledWithoutProfile(t *testing.T) {
	if c := New(Options{Source: newFakeSource(), EnableRED: true}, &fakeSender{}, logr.Discard()); c != nil {
		t.Errorf("New with zero-value Profile = %v, want nil", c)
	}
}

// TestTypedNilPusherIsNotNilInterface is a regression guard for Go's
// typed-nil-interface trap: main.go must route around it (explicit `var
// sender redsource.Sender; if pusher != nil { sender = pusher }` rather than
// passing a possibly-nil *push.Pusher straight through).
func TestTypedNilPusherIsNotNilInterface(t *testing.T) {
	//nolint:staticcheck // SA4023: never-true is the assertion — see the comment above.
	var nilPusher *push.Pusher
	var sender Sender = nilPusher
	//nolint:staticcheck // SA4023: never-true is the assertion — see the comment above.
	if sender == nil {
		t.Fatal("a *push.Pusher stored in a Sender interface compared equal to nil — " +
			"if this now passes, Go's typed-nil-interface behavior changed and main.go's " +
			"explicit-nil guard before calling redsource.New may no longer be necessary")
	}
}

func TestNamespaceSet_BlankOnlyIsNil(t *testing.T) {
	if got := namespaceSet(nil); got != nil {
		t.Errorf("namespaceSet(nil) = %v, want nil", got)
	}
	if got := namespaceSet([]string{"", "  "}); got != nil {
		t.Errorf("namespaceSet(blanks) = %v, want nil (means forward-all)", got)
	}
	if got := namespaceSet([]string{" ns-a ", "ns-b"}); !got["ns-a"] || !got["ns-b"] || len(got) != 2 {
		t.Errorf("namespaceSet trimmed = %v, want {ns-a,ns-b}", got)
	}
}

func TestIsHTTP5xxError(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"500", true}, {"502", true}, {"599", true},
		{"200", false}, {"404", false}, {"499", false},
		{"", false}, {"not-a-number", false},
	}
	for _, tc := range tests {
		if got := IsHTTP5xxError(tc.status); got != tc.want {
			t.Errorf("IsHTTP5xxError(%q) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestFormatSampleOBI(t *testing.T) {
	got := formatSampleOBI(SampleContext{Extra: map[string]string{"http.request.method": "GET", "http.route": "/api/orders"}, Status: "502", Delta: 41})
	if got != "GET /api/orders -> 502 x41" {
		t.Errorf("formatSampleOBI = %q", got)
	}
	got = formatSampleOBI(SampleContext{Extra: map[string]string{}, Status: "500", Delta: 1})
	if got != "? ? -> 500 x1" {
		t.Errorf("formatSampleOBI with missing method/route = %q, want \"? ? -> 500 x1\"", got)
	}
}

func TestHostOnly(t *testing.T) {
	tests := []struct{ in, want string }{
		{"db.ns-a.svc.cluster.local:6333", "db.ns-a.svc.cluster.local"},
		{"db.ns-a", "db.ns-a"},
		{"10.0.4.12:5432", "10.0.4.12"},
		{"10.0.4.12", "10.0.4.12"},
	}
	for _, tt := range tests {
		if got := hostOnly(tt.in); got != tt.want {
			t.Errorf("hostOnly(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSortEdges_OrdersByCallsDescendingThenDeterministicTiebreak(t *testing.T) {
	edges := []push.TopologyEdge{
		{SrcNamespace: "ns-a", SrcWorkload: "b", Calls: 5},
		{SrcNamespace: "ns-a", SrcWorkload: "a", Calls: 10},
		{SrcNamespace: "ns-a", SrcWorkload: "c", Calls: 10},
	}
	sortEdges(edges)
	if edges[0].Calls != 10 || edges[1].Calls != 10 || edges[2].Calls != 5 {
		t.Fatalf("edges not sorted by Calls descending: %+v", edges)
	}
	if edges[0].SrcWorkload != "a" || edges[1].SrcWorkload != "c" {
		t.Errorf("tie-break order = %s, %s, want a, c (deterministic)", edges[0].SrcWorkload, edges[1].SrcWorkload)
	}
}

func TestSortEdges_CallsTieBreaksOnBytes(t *testing.T) {
	edges := []push.TopologyEdge{
		{SrcNamespace: "ns-a", SrcWorkload: "low-bytes", Bytes: 100},
		{SrcNamespace: "ns-a", SrcWorkload: "high-bytes", Bytes: 900},
	}
	sortEdges(edges)
	if edges[0].SrcWorkload != "high-bytes" || edges[1].SrcWorkload != "low-bytes" {
		t.Fatalf("edges not sorted by Bytes descending when Calls tie (both 0): %+v", edges)
	}
}

func TestSortNodes_OrdersByNamespaceThenWorkload(t *testing.T) {
	set := map[servicePair]bool{
		{Namespace: "ns-a", Workload: "z"}: true,
		{Namespace: "ns-b", Workload: "a"}: true,
		{Namespace: "ns-a", Workload: "a"}: true,
	}
	nodes := sortNodes(set)
	if len(nodes) != 3 {
		t.Fatalf("nodes = %d, want 3", len(nodes))
	}
	want := []push.TopologyNode{
		{Namespace: "ns-a", Workload: "a"},
		{Namespace: "ns-a", Workload: "z"},
		{Namespace: "ns-b", Workload: "a"},
	}
	for i, w := range want {
		if nodes[i] != w {
			t.Errorf("nodes[%d] = %+v, want %+v", i, nodes[i], w)
		}
	}
}

func TestP99FromBucketDeltas_InterpolatesWithinBucket(t *testing.T) {
	byLe := map[float64]int64{0.1: 90, 1.0: 100, math.Inf(1): 100}
	got, ok := p99FromBucketDeltas(byLe)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	want := 0.1 + (99.0-90.0)/(100.0-90.0)*(1.0-0.1)
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("p99 = %v, want %v", got, want)
	}
}

func TestP99FromBucketDeltas_InfBucketFallsBackToLargestFinite(t *testing.T) {
	byLe := map[float64]int64{0.1: 1, math.Inf(1): 100}
	got, ok := p99FromBucketDeltas(byLe)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if got != 0.1 {
		t.Errorf("p99 = %v, want 0.1 (largest finite le)", got)
	}
}

func TestP99FromBucketDeltas_NoDataReturnsNotOK(t *testing.T) {
	if _, ok := p99FromBucketDeltas(nil); ok {
		t.Error("nil buckets: ok = true, want false")
	}
	if _, ok := p99FromBucketDeltas(map[float64]int64{math.Inf(1): 0}); ok {
		t.Error("zero total: ok = true, want false")
	}
}

// A service the DaemonSet measured is not counted again from an OTLP
// source; a service only OTLP reports is counted from it.
func TestOBI_Collector_SensorWinsOverOTLPPerService(t *testing.T) {
	src := newFakeSource()
	const otlpTarget = "sensor/otlp/abc"
	tick := func(n int) {
		src.set(testTarget, setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "502", 5*n),
			obiPoint("ns-a", "api", "GET", "/orders", "200", 95*n),
		)))
		src.set(otlpTarget, setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "502", 5*n),
			obiPoint("ns-a", "api", "GET", "/orders", "200", 95*n),
			obiPoint("ns-a", "worker", "GET", "/jobs", "500", 5*n),
			obiPoint("ns-a", "worker", "GET", "/jobs", "200", 95*n),
		)))
	}
	c, sender := obiRED(t, src, 1, 1, Options{})
	ctx := context.Background()
	tick(1)
	c.runOnce(ctx)
	tick(2)
	c.runOnce(ctx)

	got := map[string]int64{}
	for _, s := range sender.redServices() {
		got[s.Service] = s.RequestCount
	}
	if got["api"] != 100 {
		t.Errorf("api requests = %d, want 100 (the sensor's, not doubled)", got["api"])
	}
	if got["worker"] != 100 {
		t.Errorf("worker requests = %d, want 100 from OTLP alone", got["worker"])
	}
}

// What a sender reports as a metric is not counted again from what the OTLP
// receiver derived from the same service's spans; a service only the spans
// cover is counted from them. The derived source is read after the native one
// whatever order the targets come in.
func TestOBI_Collector_NativeOTLPWinsOverDerivedPerService(t *testing.T) {
	derived := func(pts ...metricset.Point) *metricset.Metric {
		for i := range pts {
			pts[i].Attrs[metricset.DerivedAttr] = "pod-a"
		}
		return obiMetric(pts...)
	}
	src := newFakeSource()
	const (
		derivedTarget = "sensor/otlp/aaa" // sorts first on purpose
		nativeTarget  = "sensor/otlp/zzz"
	)
	tick := func(n int) {
		src.set(nativeTarget, setOf(obiMetric(
			obiPoint("ns-a", "api", "GET", "/orders", "502", 5*n),
			obiPoint("ns-a", "api", "GET", "/orders", "200", 95*n),
		)))
		src.set(derivedTarget, setOf(derived(
			obiPoint("ns-a", "api", "GET", "/orders", "502", 5*n),
			obiPoint("ns-a", "api", "GET", "/orders", "200", 95*n),
			obiPoint("ns-a", "worker", "GET", "/jobs", "500", 5*n),
			obiPoint("ns-a", "worker", "GET", "/jobs", "200", 95*n),
		)))
	}
	c, sender := obiRED(t, src, 1, 1, Options{})
	ctx := context.Background()
	tick(1)
	c.runOnce(ctx)
	tick(2)
	c.runOnce(ctx)

	got := map[string]int64{}
	for _, s := range sender.redServices() {
		got[s.Service] = s.RequestCount
	}
	if got["api"] != 100 {
		t.Errorf("api requests = %d, want 100 (the native metric's, not doubled)", got["api"])
	}
	if got["worker"] != 100 {
		t.Errorf("worker requests = %d, want 100 from the derived source alone", got["worker"])
	}
}

func TestTopologyEdge_DstPort(t *testing.T) {
	body := func(n int, extra ...metricset.Point) metricset.Set {
		pts := append([]metricset.Point{obiClientPoint("ns-a", "app", "svc.ns.svc:4317", n)}, extra...)
		return setOf(
			obiMetric(obiPoint("ns-a", "app", "", "", "", 0), obiPoint("ns", "svc", "", "", "", 0)),
			obiClientMetric("http.client.request.duration", pts...),
		)
	}
	resolver := fakeResolverFunc(func(srcNamespace, serverAddress string) (servicePair, string) {
		if hostOnly(serverAddress) == "svc.ns.svc" {
			return servicePair{Namespace: "ns", Workload: "svc"}, ""
		}
		return servicePair{}, push.TruncateTopologyExternal(serverAddress)
	})

	t.Run("port carried and host resolved as before", func(t *testing.T) {
		src := newFakeSource()
		src.set(testTarget, body(0))
		c, sender := obiTopology(t, src, Options{Resolver: resolver})
		c.runOnce(context.Background())

		src.set(testTarget, body(6))
		c.runOnce(context.Background())
		edges := sender.lastTopology().edges
		if len(edges) != 1 {
			t.Fatalf("edges = %+v, want 1", edges)
		}
		e := edges[0]
		if e.DstPort != 4317 || e.DstNamespace != "ns" || e.DstWorkload != "svc" || e.External != "" || e.Calls != 6 {
			t.Errorf("edge = %+v, want ns/svc, DstPort=4317, calls=6", e)
		}
	})

	t.Run("two ports to one destination are two edges", func(t *testing.T) {
		other := func(n int) metricset.Point { return obiClientPoint("ns-a", "app", "svc.ns.svc:8080", n) }
		src := newFakeSource()
		src.set(testTarget, body(0, other(0)))
		c, sender := obiTopology(t, src, Options{Resolver: resolver})
		c.runOnce(context.Background())

		src.set(testTarget, body(6, other(2)))
		c.runOnce(context.Background())
		edges := sender.lastTopology().edges
		if len(edges) != 2 {
			t.Fatalf("edges = %+v, want 2 (one per port)", edges)
		}
		ports := map[int]int64{}
		for _, e := range edges {
			if e.DstNamespace != "ns" || e.DstWorkload != "svc" {
				t.Errorf("edge = %+v, want ns/svc", e)
			}
			ports[e.DstPort] = e.Calls
		}
		if ports[4317] != 6 || ports[8080] != 2 {
			t.Errorf("calls by port = %v, want 4317:6 8080:2", ports)
		}
	})

	t.Run("address without a port leaves DstPort 0", func(t *testing.T) {
		if got := portOf("svc.ns.svc"); got != 0 {
			t.Errorf("portOf(no port) = %d, want 0", got)
		}
		if got := portOf("10.0.0.1:99999"); got != 0 {
			t.Errorf("portOf(out of range) = %d, want 0", got)
		}
	})
}

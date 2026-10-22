package usemetrics

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// fakeSender records every SendUseMetrics/SendUseSummary call for assertions.
type fakeSender struct {
	mu      sync.Mutex
	calls   [][]push.ServiceUseMetrics
	summary [][]push.ServiceUseSummary
}

func (f *fakeSender) SendUseMetrics(_ int64, _ int, services []push.ServiceUseMetrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]push.ServiceUseMetrics(nil), services...)
	f.calls = append(f.calls, cp)
}

func (f *fakeSender) SendUseSummary(_ int64, _ int, services []push.ServiceUseSummary) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]push.ServiceUseSummary(nil), services...)
	f.summary = append(f.summary, cp)
}

func (f *fakeSender) allServices() []push.ServiceUseMetrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []push.ServiceUseMetrics
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

func (f *fakeSender) summaryCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.summary)
}

func (f *fakeSender) lastSummary() []push.ServiceUseSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.summary[len(f.summary)-1]
}

// fakeSource is a mutable metricset.Source: target -> metricset.Set, guarded
// by a mutex so a test can drive several runOnce calls with different
// counter values, standing in for the sensor stream hub (sensorstream.Hub).
// Same shape as redsource/logmetrics' own fakeSource, duplicated per this
// repo's established mirror-not-share convention for these sibling
// collectors' tests.
//
// targets and sets are tracked separately so a test can advertise a target
// via Targets() whose Set() call then fails — the real hub can do this when
// a snapshot goes stale between the two calls.
//
// It is a metricset.TimedSource, like the hub: every set() is received one
// minute (the collector's default interval) after the target's previous one,
// and advance() moves a target's clock for a test that needs a gap.
type fakeSource struct {
	mu      sync.Mutex
	targets map[string]bool
	sets    map[string]metricset.Set
	at      map[string]time.Time
}

func newFakeSource() *fakeSource {
	return &fakeSource{targets: make(map[string]bool), sets: make(map[string]metricset.Set), at: make(map[string]time.Time)}
}

// set installs (or replaces) target's metric set, received a minute after the
// previous one.
func (f *fakeSource) set(target string, s metricset.Set) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets[target] = true
	f.sets[target] = s
	if f.at[target].IsZero() {
		f.at[target] = time.Unix(1_700_000_000, 0)
	}
	f.at[target] = f.at[target].Add(time.Minute)
}

// advance moves target's clock forward: its next set is received d later than
// it otherwise would be.
func (f *fakeSource) advance(target string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at[target] = f.at[target].Add(d)
}

func (f *fakeSource) SetAt(role, target string) (metricset.Set, time.Time, error) {
	s, err := f.Set(role, target)
	f.mu.Lock()
	defer f.mu.Unlock()
	return s, f.at[target], err
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

const testTarget = "sensor/node-a"

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// newTestCollector builds a Collector via New, reading from src, with a
// *fakeSender the test can inspect. extraObjects, when given, seed the fake
// client New wires as the OwnerIdentityResolver client — the Pod/ReplicaSet/Deployment/
// StatefulSet/DaemonSet/Job/CronJob objects a service-identity-resolution
// test needs. Every other call site passes none, so identity resolution
// finds no Pod object and falls back to the reported label.
func newTestCollector(t *testing.T, src *fakeSource, opts Options, extraObjects ...client.Object) (*Collector, *fakeSender) {
	t.Helper()
	return newTestCollectorMode(t, src, opts, nil, extraObjects...)
}

// newTestCollectorMode is newTestCollector with the resolver's identity mode
// hook (push.Pusher.IdentityModeFor in a cluster).
func newTestCollectorMode(t *testing.T, src *fakeSource, opts Options, modeFor func(string) string, extraObjects ...client.Object) (*Collector, *fakeSender) {
	t.Helper()
	scheme := newTestScheme(t)
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(extraObjects...).Build()
	sender := &fakeSender{}
	opts.Source = src
	c := New(opts, &OwnerIdentityResolver{Client: fc, ModeFor: modeFor, Now: opts.now}, sender, logr.Discard())
	if c == nil {
		t.Fatal("New returned nil; check Options (Source, sender)")
	}
	return c, sender
}

// point builds one pod's sample carrying the {k8s.namespace.name,
// service.name, k8s.pod.name} attribute triple every USE-metrics family uses
// (metrics.go).
func point(namespace, service, pod string, v float64) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			LabelNamespace: namespace,
			LabelService:   service,
			LabelPod:       pod,
		},
		Value: v,
	}
}

// gaugeFamily and counterFamily build one named metric from its points —
// computeDeltas reads Points regardless of Kind, but setting it correctly
// keeps a fixture self-documenting against metrics.go's own gauge/counter
// split.
func gaugeFamily(name string, points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: name, Kind: metricset.Gauge, Points: points}
}

func counterFamily(name string, points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: name, Kind: metricset.Counter, Points: points}
}

// setOf collects metrics into a metricset.Set keyed by name.
func setOf(metrics ...*metricset.Metric) metricset.Set {
	s := metricset.Set{}
	for _, m := range metrics {
		s[m.Name] = m
	}
	return s
}

// TestCollector_GateOnMemPsi verifies a service whose max memPsiAvg60 across pods
// crosses MemPsiThreshold qualifies, and one that doesn't, doesn't.
func TestCollector_GateOnMemPsi(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "api-1", 25),
		point("ops", "quiet", "quiet-1", 5),
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %d (%+v), want 1", len(svcs), svcs)
	}
	if svcs[0].Service != "api" || svcs[0].MemPsiAvg60 != 25 {
		t.Errorf("service = %+v, want api/memPsi=25", svcs[0])
	}
}

// TestCollector_GateIsOrAcrossAllFourArms verifies each of the four gate arms
// (mem/cpu/io PSI, OOM kill) independently qualifies a service on its own. Each row
// is read twice: a zero-valued seed (a no-op for the PSI gauges, which are always
// taken at face value; establishes real presence for the oom_kill counter so its
// first-seen delta isn't suppressed) followed by the row's real value.
func TestCollector_GateIsOrAcrossAllFourArms(t *testing.T) {
	tests := []struct {
		name       string
		metric     string
		family     func(string, ...metricset.Point) *metricset.Metric
		seed, body float64
	}{
		{"mem psi alone", MetricMemPsiAvg60, gaugeFamily, 0, 25},
		{"cpu psi alone", MetricCPUPsiAvg60, gaugeFamily, 0, 45},
		{"io psi alone", MetricIOPsiAvg60, gaugeFamily, 0, 45},
		{"oom kill alone", MetricOOMKillTotal, counterFamily, 0, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource()
			src.set(testTarget, setOf(tc.family(tc.metric, point("ops", "svc", "p", tc.seed))))
			c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
			c.runOnce(context.Background()) // seed baseline

			src.set(testTarget, setOf(tc.family(tc.metric, point("ops", "svc", "p", tc.body))))
			c.runOnce(context.Background())
			if sender.callCount() != 1 {
				t.Errorf("callCount = %d, want 1", sender.callCount())
			}
		})
	}
}

// TestCollector_BelowAllThresholdsDoesNotQualify verifies a service under every
// threshold, with no OOM kill, produces no frame.
func TestCollector_BelowAllThresholdsDoesNotQualify(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPsiAvg60, point("ops", "svc", "p", 5)),
		gaugeFamily(MetricCPUPsiAvg60, point("ops", "svc", "p", 5)),
		gaugeFamily(MetricIOPsiAvg60, point("ops", "svc", "p", 5)),
	))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	if sender.callCount() != 0 {
		t.Fatalf("callCount = %d, want 0", sender.callCount())
	}
}

// TestCollector_ZeroThresholdAlwaysQualifies verifies the mirrored rate-floor-zero
// fix: a threshold explicitly set to 0 is NOT re-defaulted, so it always qualifies
// on that dimension (docs/adr/0010-use-metrics-node-exporter.md).
func TestCollector_ZeroThresholdAlwaysQualifies(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricIOPsiAvg60, point("ops", "svc", "p", 0))))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 0})
	if c.opts.IOPsiThreshold != 0 {
		t.Fatalf("IOPsiThreshold after setDefaults = %v, want 0 (must not be re-defaulted)", c.opts.IOPsiThreshold)
	}
	c.runOnce(context.Background())
	if sender.callCount() != 1 {
		t.Fatalf("callCount = %d, want 1 (ioPsi=0 >= threshold=0 must qualify)", sender.callCount())
	}
}

// TestCollector_MaxAcrossPods verifies PSI/mem-pct gauges aggregate as MAX across a
// service's pods, not sum or average.
func TestCollector_MaxAcrossPods(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "api-1", 25),
		point("ops", "api", "api-2", 60),
		point("ops", "api", "api-3", 10),
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].MemPsiAvg60 != 60 {
		t.Fatalf("services = %+v, want one entry with memPsi=60 (max across pods)", svcs)
	}
}

// TestCollector_OOMKillsSumAcrossPodsAndDeltas verifies OOMKills is a SUM across a
// service's pods, and a delta (not a raw cumulative value) across ticks. The first
// tick carries a nonzero baseline (as if these pods had already been running for a
// while) specifically to prove that baseline is NOT reported as this window's delta
// — see TestCollector_FirstTickSuppressesLifetimeOOMKills for the dedicated
// regression test of that behavior in isolation.
func TestCollector_OOMKillsSumAcrossPodsAndDeltas(t *testing.T) {
	body := func(p1, p2 int) metricset.Set {
		return setOf(counterFamily(MetricOOMKillTotal,
			point("ops", "api", "api-1", float64(p1)),
			point("ops", "api", "api-2", float64(p2)),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(1, 0))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background()) // first tick ever: first-seen suppression -> delta 0, no frame
	if sender.callCount() != 0 {
		t.Fatalf("after first tick: callCount = %d, want 0 (first-seen suppression)", sender.callCount())
	}

	src.set(testTarget, body(2, 1)) // api-1 +1, api-2 +1 -> delta sum 2, against the now-real baseline
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].OOMKills != 2 {
		t.Fatalf("after second tick: services = %+v, want OOMKills=2 (sum of per-pod deltas)", svcs)
	}

	src.set(testTarget, body(3, 2)) // api-1 +1, api-2 +1 -> delta sum 2 again
	c.runOnce(context.Background())
	svcs = sender.allServices()
	last := svcs[len(svcs)-1]
	if last.OOMKills != 2 {
		t.Errorf("OOMKills after third tick = %d, want 2 (sum of per-pod deltas)", last.OOMKills)
	}
}

// TestCollector_FirstTickSuppressesLifetimeOOMKills is a direct regression test
// for the observed production bug: a long-lived pod's LIFETIME cumulative oom_kill
// count (accumulated over days, well before this collector process ever read it)
// must never be reported as a single window's delta. Observed live: a 16-day-old pod
// with a lifetime oom_kill count of 15 was reported as "15 OOM kills in 60s window"
// on the collector's first window after deploy.
func TestCollector_FirstTickSuppressesLifetimeOOMKills(t *testing.T) {
	body := func(n float64) metricset.Set {
		return setOf(counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", n)))
	}
	src := newFakeSource()
	src.set(testTarget, body(15)) // the exact lifetime count observed live
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background())
	if sender.callCount() != 0 {
		t.Fatalf("first tick: callCount = %d, want 0 (a pod's lifetime oom_kill count must never be reported as one window's delta)", sender.callCount())
	}

	src.set(testTarget, body(16)) // genuine growth: +1 real OOM kill this window
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].OOMKills != 1 {
		t.Fatalf("after second tick: services = %+v, want exactly one entry with OOMKills=1 (the real increment, not the lifetime total of 16)", svcs)
	}
}

// TestCollector_CounterResetTreatsCurrentValueAsDelta verifies a genuine counter
// reset (the pod cgroup recreated, or the node-exporter pod itself restarted — prev
// present, cur < prev) still reports cur as the delta, unchanged — and that this is
// DISTINCT from first-seen suppression: a real baseline must be established by an
// earlier tick before the reset can be observed at all.
func TestCollector_CounterResetTreatsCurrentValueAsDelta(t *testing.T) {
	body := func(n float64) metricset.Set {
		return setOf(counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", n)))
	}
	src := newFakeSource()
	src.set(testTarget, body(5))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background()) // first tick ever: first-seen suppression, baseline seeded at 5
	if sender.callCount() != 0 {
		t.Fatalf("after first tick: callCount = %d, want 0", sender.callCount())
	}

	src.set(testTarget, body(9)) // genuine growth against the real baseline: +4
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].OOMKills != 4 {
		t.Fatalf("after second tick: services = %+v, want OOMKills=4", svcs)
	}

	src.set(testTarget, body(2)) // cgroup recreated: counter reset to 2 (< 9)
	c.runOnce(context.Background())
	svcs = sender.allServices()
	last := svcs[len(svcs)-1]
	if last.OOMKills != 2 {
		t.Errorf("post-reset OOMKills = %d, want 2 (treat current value as the delta, not cur-prev=-7)", last.OOMKills)
	}
}

// TestCollector_ThrottledPctIsRatioOfSummedDeltas verifies ThrottledPct is computed
// as sum(throttled deltas)/sum(period deltas)*100 across a service's pods.
func TestCollector_ThrottledPctIsRatioOfSummedDeltas(t *testing.T) {
	seed := setOf(
		gaugeFamily(MetricMemPsiAvg60, point("ops", "api", "api-1", 25)), // qualifies the gate throughout, via PSI
		counterFamily(MetricCPUNrPeriodsTotal, point("ops", "api", "api-1", 0), point("ops", "api", "api-2", 0)),
		counterFamily(MetricCPUNrThrottledTotal, point("ops", "api", "api-1", 0), point("ops", "api", "api-2", 0)),
	)
	body := setOf(
		gaugeFamily(MetricMemPsiAvg60, point("ops", "api", "api-1", 25)),
		counterFamily(MetricCPUNrPeriodsTotal, point("ops", "api", "api-1", 100), point("ops", "api", "api-2", 100)),
		counterFamily(MetricCPUNrThrottledTotal, point("ops", "api", "api-1", 10), point("ops", "api", "api-2", 30)),
	)
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background()) // seed: periods/throttled counters established at 0 (PSI alone qualifies this frame too, correctly reporting ThrottledPct=0 — nothing to divide yet)

	src.set(testTarget, body)
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) == 0 {
		t.Fatal("no services sent")
	}
	last := svcs[len(svcs)-1]
	if last.Service != "api" {
		t.Fatalf("service = %+v, want api", last)
	}
	// (10+30)/(100+100)*100 = 20%
	if got, want := last.ThrottledPct, 20.0; got != want {
		t.Errorf("ThrottledPct = %v, want %v", got, want)
	}
}

// TestCollector_SkipsUnattributedPods verifies a pod with an empty service label
// (podKeyOf still returns it — only an empty NAMESPACE is rejected) never crosses
// into a frame, while an attributed service in the same window still sends.
func TestCollector_SkipsUnattributedPods(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "api-1", 25),
		point("ops", "", "orphan-1", 90), // empty service -> unattributed
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "api" {
		t.Fatalf("services = %+v, want exactly one entry for api", svcs)
	}
}

// TestCollector_SampleWorstOffenderPod verifies Sample names the pod with the
// highest badness score (OOM kill dominates a PSI-only reading) and formats it as
// "pod <name>: <parts>".
func TestCollector_SampleWorstOffenderPod(t *testing.T) {
	seed := setOf(
		gaugeFamily(MetricMemPsiAvg60,
			point("ops", "api", "api-quiet", 22), // just barely qualifies
			point("ops", "api", "api-oom", 15),
		),
		counterFamily(MetricOOMKillTotal, point("ops", "api", "api-oom", 0)),
	)
	body := setOf(
		gaugeFamily(MetricMemPsiAvg60,
			point("ops", "api", "api-quiet", 22),
			point("ops", "api", "api-oom", 15),
		),
		counterFamily(MetricOOMKillTotal, point("ops", "api", "api-oom", 1)),
	)
	src := newFakeSource()
	src.set(testTarget, seed)
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background()) // seed: establishes the oom_kill baseline at 0

	src.set(testTarget, body)
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) == 0 {
		t.Fatal("no services sent")
	}
	last := svcs[len(svcs)-1]
	want := "pod api-oom: mem psi 15%, oom_kill +1"
	if last.Sample != want {
		t.Errorf("Sample = %q, want %q (OOM kill must outrank a higher PSI-only pod)", last.Sample, want)
	}
}

// TestCollector_CapsAtMaxServicesKeepingTopByMemPsi verifies a window that qualifies
// more than push.MaxUseMetricsServices keeps only the top services by memPsiAvg60,
// in one frame (no chunking).
func TestCollector_CapsAtMaxServicesKeepingTopByMemPsi(t *testing.T) {
	const n = 150
	points := make([]metricset.Point, 0, n)
	for i := 0; i < n; i++ {
		points = append(points, point("ops", fmt.Sprintf("svc-%03d", i), "p", float64(20+i)))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, points...)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != push.MaxUseMetricsServices {
		t.Fatalf("services sent = %d, want %d", len(svcs), push.MaxUseMetricsServices)
	}
	for _, s := range svcs {
		if s.MemPsiAvg60 < 20+float64(n-push.MaxUseMetricsServices) {
			t.Errorf("service %+v: memPsi below the expected top-%d cutoff", s, push.MaxUseMetricsServices)
		}
	}
}

// ===========================================================================
// use_summary (ungated per-service saturation, the 4th golden signal, every
// window) — covers Collector.sendUseSummary, called every runOnce
// regardless of the gate.
// ===========================================================================

// TestCollector_UseSummarySentEveryWindowIncludingWhenGateDoesNotFire proves
// the summary is UNGATED: a window whose PSI readings stay below every gate
// threshold (so SendUseMetrics is never called) still produces exactly one
// use_summary frame carrying that service's real window values.
func TestCollector_UseSummarySentEveryWindowIncludingWhenGateDoesNotFire(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPsiAvg60, point("ops", "api", "api-1", 5)),
		gaugeFamily(MetricCPUPsiAvg60, point("ops", "api", "api-1", 3)),
		gaugeFamily(MetricIOPsiAvg60, point("ops", "api", "api-1", 1)),
	))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	if sender.callCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0 (below every threshold, gate must not fire)", sender.callCount())
	}
	if sender.summaryCallCount() != 1 {
		t.Fatalf("summary callCount = %d, want 1 (ungated: summary sends every window regardless of the gate)", sender.summaryCallCount())
	}
	svcs := sender.lastSummary()
	if len(svcs) != 1 || svcs[0].Namespace != "ops" || svcs[0].Service != "api" || svcs[0].MemPsiPct != 5 || svcs[0].CPUPsiPct != 3 || svcs[0].IOPsiPct != 1 {
		t.Fatalf("summary service = %+v, want one ops/api entry memPsiPct=5 cpuPsiPct=3 ioPsiPct=1", svcs)
	}
}

// TestCollector_UseSummaryCarriesMemPctWhenEveryPsiDimensionIsZero is the
// observed regression (2026-08-29), reproduced at the numbers observed
// live: a pod at 1976Mi of a 2Gi limit — 96.5% — whose working set is
// resident, so it never stalls in reclaim and every PSI dimension truthfully
// reads 0%, with no OOM kill yet.
//
// Before memPct rode this frame, that pod produced NOTHING: the gate is
// PSI/OOM-only so no use_metrics frame was sent, and the summary carried
// saturation alone, so the one number that showed a service one allocation
// from an OOM kill was computed every 60s and discarded twice over. The
// assertions below are the contract that fixed it — the gate must still stay
// shut (utilization is not pressure, and this test would be worthless if it
// passed by making everything alert), and the summary must still carry 96.5.
func TestCollector_UseSummaryCarriesMemPctWhenEveryPsiDimensionIsZero(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricCPUPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricIOPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricMemPct, point("tenant-a", "neo4j", "neo4j-0", 96.5)),
	))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	if sender.callCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0: utilization alone must not trip the PSI/OOM gate", sender.callCount())
	}
	svcs := sender.lastSummary()
	if len(svcs) != 1 {
		t.Fatalf("summary services = %+v, want exactly one neo4j entry", svcs)
	}
	if svcs[0].MemPct != 96.5 {
		t.Errorf("summary memPct = %v, want 96.5 — the whole point: a pod at 96%% of its limit is visible while every PSI reading is a truthful zero", svcs[0].MemPct)
	}
	if svcs[0].MemPsiPct != 0 || svcs[0].CPUPsiPct != 0 || svcs[0].IOPsiPct != 0 {
		t.Errorf("summary PSI = mem %v cpu %v io %v, want all zero (no reclaim stall — that is the observed state, not missing data)",
			svcs[0].MemPsiPct, svcs[0].CPUPsiPct, svcs[0].IOPsiPct)
	}
}

// TestCollector_UseSummaryPrefersWorkingSetOverMemPct proves the collector
// forwards the OOM-relevant working-set figure when the exporter reports one,
// even though the page-cache-charging use_mem_pct is also present and higher.
//
// The numbers are the same shape as the regression above: memory.current
// pinned near the limit while the working set — what the kernel's OOM
// accounting effectively considers — sits meaningfully below it. Forwarding
// the higher number is what would make a threshold fire forever on any
// cache-heavy service.
func TestCollector_UseSummaryPrefersWorkingSetOverMemPct(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPct, point("tenant-a", "neo4j", "neo4j-0", 99.97)),
		gaugeFamily(MetricMemWorkingSetPct, point("tenant-a", "neo4j", "neo4j-0", 96.5)),
	))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	svcs := sender.lastSummary()
	if len(svcs) != 1 || svcs[0].MemPct != 96.5 {
		t.Fatalf("summary = %+v, want memPct=96.5 (the working set), not the page-cache-inflated 99.97", svcs)
	}
}

// TestCollector_UseSummaryFallsBackToMemPctWithoutWorkingSet covers the
// rolling-upgrade straddle: the node exporter runs on the DaemonSet and this
// collector on the controller Deployment, so the two are independently
// versioned and routinely skew. An exporter predating use_mem_ws_pct must leave
// utilization reported — less precisely — rather than blank for the whole fleet
// until the DaemonSet catches up.
func TestCollector_UseSummaryFallsBackToMemPctWithoutWorkingSet(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPct, point("tenant-a", "neo4j", "neo4j-0", 99.97))))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	svcs := sender.lastSummary()
	if len(svcs) != 1 || svcs[0].MemPct != 99.97 {
		t.Fatalf("summary = %+v, want memPct=99.97 from the older exporter's only reading", svcs)
	}
}

// TestCollector_UseSummaryMemPctMaxAggregatesAcrossPods pins that a service's
// reported utilization is its WORST pod, not an average that would dilute one
// pod at its limit behind healthy siblings — the same max-across-pods rule the
// PSI gauges already use (see the aggregate loop in runOnce).
func TestCollector_UseSummaryMemPctMaxAggregatesAcrossPods(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPct,
		point("ops", "api", "api-1", 12),
		point("ops", "api", "api-2", 91),
		point("ops", "api", "api-3", 8),
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	svcs := sender.lastSummary()
	if len(svcs) != 1 || svcs[0].MemPct != 91 {
		t.Fatalf("summary = %+v, want one ops/api entry memPct=91 (the worst pod, never the mean)", svcs)
	}
}

// TestCollector_UseSummaryOmitsMemPctWhenNoLimitIsSet proves the absent case
// stays absent: nodeexporter.readMemPct omits its series entirely when
// memory.max reads "max", so a service with no memory limit reports no
// utilization rather than a fabricated 0%. This is what makes the receiver's
// MemPct>0 presence test sound — see ServiceUseSummary's doc comment.
func TestCollector_UseSummaryOmitsMemPctWhenNoLimitIsSet(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, point("ops", "unbounded", "unbounded-1", 2))))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	svcs := sender.lastSummary()
	if len(svcs) != 1 || svcs[0].MemPct != 0 {
		t.Fatalf("summary = %+v, want one entry with memPct=0 (no limit → no percentage of it exists)", svcs)
	}
}

// TestCollector_UseSummaryIncludesGateCrossingServices proves a service that
// DOES cross the use_metrics gate still appears in the use_summary frame for
// the same window, alongside a service that never crossed it — the two frames
// are independent, not mutually exclusive.
func TestCollector_UseSummaryIncludesGateCrossingServices(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "api-1", 25),
		point("ops", "quiet", "quiet-1", 5),
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())

	gated := sender.allServices()
	if len(gated) != 1 || gated[0].Service != "api" || gated[0].MemPsiAvg60 != 25 {
		t.Fatalf("gated service = %+v, want one api entry memPsi=25 (only api crosses the gate)", gated)
	}

	if sender.summaryCallCount() != 1 {
		t.Fatalf("summary callCount = %d, want 1", sender.summaryCallCount())
	}
	summary := sender.lastSummary()
	if len(summary) != 2 {
		t.Fatalf("summary services = %+v, want 2 (both api and quiet, ungated)", summary)
	}
	var api, quiet *push.ServiceUseSummary
	for i := range summary {
		switch summary[i].Service {
		case "api":
			api = &summary[i]
		case "quiet":
			quiet = &summary[i]
		}
	}
	if api == nil || api.MemPsiPct != 25 {
		t.Errorf("summary api entry = %+v, want memPsiPct=25 (same window value as the gated frame)", api)
	}
	if quiet == nil || quiet.MemPsiPct != 5 {
		t.Errorf("summary quiet entry = %+v, want memPsiPct=5 (below the gate but still in the summary)", quiet)
	}
}

// TestCollector_UseSummaryCapsAtMaxServices mirrors
// TestCollector_CapsAtMaxServicesKeepingTopByMemPsi for the summary path: an
// oversized read service set is capped at push.MaxUseMetricsServices (no
// silent truncation — the collector logs the dropped count), keeping the
// highest by memPsiPct. Thresholds are set unreachably high so the gated
// use_metrics path never fires, isolating this test to the summary's own cap
// logic.
func TestCollector_UseSummaryCapsAtMaxServices(t *testing.T) {
	const n = 150
	points := make([]metricset.Point, 0, n)
	for i := 0; i < n; i++ {
		points = append(points, point("ops", fmt.Sprintf("svc-%03d", i), "p", float64(i)))
	}
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, points...)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 1_000_000, CPUPsiThreshold: 1_000_000, IOPsiThreshold: 1_000_000})
	c.runOnce(context.Background())

	if sender.callCount() != 0 {
		t.Fatalf("gated callCount = %d, want 0 (isolating the summary cap)", sender.callCount())
	}
	if sender.summaryCallCount() != 1 {
		t.Fatalf("summary callCount = %d, want 1 (one frame per window, not chunked)", sender.summaryCallCount())
	}
	svcs := sender.lastSummary()
	if len(svcs) != push.MaxUseMetricsServices {
		t.Fatalf("summary services = %d, want %d (capped)", len(svcs), push.MaxUseMetricsServices)
	}
	minKept := svcs[0].MemPsiPct
	for _, s := range svcs {
		if s.MemPsiPct < minKept {
			minKept = s.MemPsiPct
		}
	}
	if want := float64(n - push.MaxUseMetricsServices); minKept < want {
		t.Errorf("lowest kept memPsiPct = %v, want >= %v (top %d of %d by memPsiPct)", minKept, want, push.MaxUseMetricsServices, n)
	}
}

// TestCollector_UseSummarySkipsEmptyWindow proves an empty result — every
// read sample unattributable to a service (no service label) — sends no
// use_summary frame at all, mirroring TestCollector_SkipsUnattributedPods'
// gated-path assertion that an unattributed entry is dropped, not forwarded as
// a bogus zero-value service.
func TestCollector_UseSummarySkipsEmptyWindow(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, point("ops", "", "orphan-1", 90)))) // empty service -> unattributed
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	if sender.summaryCallCount() != 0 {
		t.Fatalf("summary callCount = %d, want 0 (every entry unattributed; empty frame must not be sent)", sender.summaryCallCount())
	}
}

// TestCollector_NamespaceFilterDropsOtherTenants verifies Namespaces restricts
// forwarded metrics the same way redsource/logmetrics do.
func TestCollector_NamespaceFilterDropsOtherTenants(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "p", 25),
		point("other-tenant-ns", "backend", "p", 90),
	)))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		Namespaces: []string{"ops"},
	})
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Namespace != "ops" {
		t.Fatalf("services = %+v, want exactly ops/api", svcs)
	}
}

// A namespace admitted ONLY by the dynamic NamespaceAllowed hook — e.g. one
// pulled into scope by the triageagent.dev/tenant annotation (push/namespace_scope.go),
// invisible to the static Namespaces list frozen at process start — must enter
// the payload; one the hook rejects must not, even when the static list
// contains it. The hook OVERRIDES the static set entirely.
func TestCollector_NamespaceAllowed_OverridesStaticSet(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "api", "p", 25),
		point("dyn-ns", "backend", "p", 90),
	)))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		Namespaces:       []string{"ops"}, // static set: ops only — no dyn-ns
		NamespaceAllowed: func(ns string) bool { return ns == "dyn-ns" },
	})
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Namespace != "dyn-ns" || svcs[0].Service != "backend" {
		t.Fatalf("services = %+v, want exactly dyn-ns/backend (hook admits dyn-ns, rejects statically-listed ops)", svcs)
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

// TestNew_DisabledWithoutSource verifies New returns nil (collector disabled)
// when Source is unset, so main.go's unconditional wiring is a no-op by
// default.
func TestNew_DisabledWithoutSource(t *testing.T) {
	if c := New(Options{}, nil, &fakeSender{}, logr.Discard()); c != nil {
		t.Error("New with nil Source should return nil")
	}
}

// TestNew_DisabledWithoutSender verifies New returns nil when push mode
// itself is disabled (sender is nil, e.g. because *push.Pusher was nil).
func TestNew_DisabledWithoutSender(t *testing.T) {
	if c := New(Options{Source: newFakeSource()}, nil, nil, logr.Discard()); c != nil {
		t.Error("New with nil Sender should return nil")
	}
}

func TestFormatUseSample(t *testing.T) {
	if got := formatUseSample("p1", podDelta{memPsi: 34, oomKillDelta: 1}); got != "pod p1: mem psi 34%, oom_kill +1" {
		t.Errorf("formatUseSample = %q", got)
	}
	if got := formatUseSample("p2", podDelta{}); got != "pod p2" {
		t.Errorf("formatUseSample with nothing notable = %q, want \"pod p2\"", got)
	}
}

func TestWorstPodScore(t *testing.T) {
	quiet := worstPodScore(podDelta{cpuPsi: 10, memPsi: 90, ioPsi: 5})
	oom := worstPodScore(podDelta{cpuPsi: 10, memPsi: 5, ioPsi: 5, oomKillDelta: 1})
	if oom <= quiet {
		t.Errorf("worstPodScore(oom)=%v must outrank worstPodScore(high-PSI-only)=%v", oom, quiet)
	}
}

// TestCollector_UtilizationAloneCrossesTheGate is the keystone regression
// (2026-08-29). Before this arm existed, a pod holding a resident working set
// at its memory limit emitted NOTHING: PSI reads a truthful 0% because it never
// enters reclaim, and OOMKills is 0 until the kill itself, so both surviving
// gate arms fire at or after the moment the service starts failing.
//
// The consequence was not merely a missing display value. With no event there
// was nothing to absorb into the incident that eventually opened, nothing for
// precursor backfill, nothing for pattern mining to learn from, and nothing for
// reflection to reason about — which is why an incident on this service
// could say only "app degraded, StatefulSet not ready" and nothing about the
// memory that caused it.
func TestCollector_UtilizationAloneCrossesTheGate(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricCPUPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricIOPsiAvg60, point("tenant-a", "neo4j", "neo4j-0", 0)),
		gaugeFamily(MetricMemWorkingSetPct, point("tenant-a", "neo4j", "neo4j-0", 96.5)),
	))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40, MemPctThreshold: 85,
	})
	c.runOnce(context.Background())

	gated := sender.allServices()
	if len(gated) != 1 {
		t.Fatalf("gated services = %+v, want one: utilization alone must qualify a service", gated)
	}
	if gated[0].Service != "neo4j" || gated[0].MemPct != 96.5 {
		t.Errorf("gated service = %+v, want neo4j at memPct 96.5", gated[0])
	}
	if gated[0].MemPsiAvg60 != 0 || gated[0].OOMKills != 0 {
		t.Errorf("gated service = %+v, want PSI 0 and no OOM kills — that combination is the whole point", gated[0])
	}
}

// TestCollector_UtilizationBelowThresholdDoesNotCrossTheGate keeps the arm from
// becoming a firehose: the measured distribution has one service near
// its limit and every other under 25%, so a correctly-set threshold must let
// exactly the outlier through.
func TestCollector_UtilizationBelowThresholdDoesNotCrossTheGate(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemWorkingSetPct,
		point("tenant-a", "postgres", "postgres-0", 24.1),
		point("tenant-a", "ui", "ui-0", 10.2),
	)))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40, MemPctThreshold: 85,
	})
	c.runOnce(context.Background())

	if n := sender.callCount(); n != 0 {
		t.Errorf("gated callCount = %d, want 0: nothing is near its limit", n)
	}
	if len(sender.lastSummary()) != 2 {
		t.Error("both services must still appear in the ungated summary")
	}
}

// TestCollector_ZeroMemPctThresholdDisablesTheArm pins the inverted default:
// unlike the PSI thresholds where 0 means "always report this dimension", a 0
// utilization threshold DISABLES the arm. 0% utilization is what a pod with no
// memory limit reports, so treating 0 as always-report would qualify every
// unbounded service in the cluster every single window.
func TestCollector_ZeroMemPctThresholdDisablesTheArm(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemWorkingSetPct, point("tenant-a", "neo4j", "neo4j-0", 96.5))))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40, MemPctThreshold: 0,
	})
	c.runOnce(context.Background())

	if n := sender.callCount(); n != 0 {
		t.Errorf("gated callCount = %d, want 0 with the arm disabled", n)
	}
}

// TestCollector_MemNodePctAloneCrossesTheGate is the node-relative sibling of
// TestCollector_UtilizationAloneCrossesTheGate (2026-08-29): a pod with no
// memory LIMIT reports no MemPct at all (see nodeexporter.memLimitBytes), so
// the limit-relative arm has nothing to fire on — but the node's total memory
// always exists, and this arm is what still catches a single pod eating the
// box in that case.
func TestCollector_MemNodePctAloneCrossesTheGate(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemBytes, point("tenant-a", "neo4j", "neo4j-0", 15_000_000_000)),
		gaugeFamily(MetricMemNodeTotalBytes, point("tenant-a", "neo4j", "neo4j-0", 16_000_000_000)),
		// Deliberately no MetricMemPct/MetricMemWorkingSetPct sample — this pod
		// carries no memory limit at all.
	))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		MemPctThreshold: 85, MemNodePctThreshold: 90,
	})
	c.runOnce(context.Background())

	// The GATE crossing is on push.ServiceUseMetrics, which carries no
	// MemNodePct field at all (that derived percentage lives only on the
	// ungated push.ServiceUseSummary, checked below) — this only proves the
	// service qualified, and did so with no limit-relative utilization to
	// have crossed on its own.
	gated := sender.allServices()
	if len(gated) != 1 {
		t.Fatalf("gated services = %+v, want one: node-relative utilization alone must qualify a service", gated)
	}
	if gated[0].Service != "neo4j" {
		t.Errorf("gated service = %+v, want neo4j", gated[0])
	}
	if gated[0].MemPct != 0 {
		t.Errorf("gated service = %+v, want MemPct 0 — this pod has no limit to measure it against", gated[0])
	}

	summary := sender.lastSummary()
	if len(summary) != 1 || summary[0].MemNodePct != 93.75 {
		t.Errorf("summary = %+v, want one service at memNodePct 93.75 (15/16 GB)", summary)
	}
}

// TestCollector_MemNodePctBelowThresholdDoesNotCrossTheGate keeps the arm from
// firing on ordinary usage.
func TestCollector_MemNodePctBelowThresholdDoesNotCrossTheGate(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemBytes, point("tenant-a", "postgres", "postgres-0", 1_000_000_000)),
		gaugeFamily(MetricMemNodeTotalBytes, point("tenant-a", "postgres", "postgres-0", 16_000_000_000)),
	))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		MemPctThreshold: 85, MemNodePctThreshold: 90,
	})
	c.runOnce(context.Background())

	if n := sender.callCount(); n != 0 {
		t.Errorf("gated callCount = %d, want 0: 1GB of 16GB is nowhere near the node", n)
	}
}

// TestCollector_ZeroMemNodePctThresholdDisablesTheArm pins the same inverted-
// default convention as MemPctThreshold: 0 disables rather than opens the arm.
func TestCollector_ZeroMemNodePctThresholdDisablesTheArm(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemBytes, point("tenant-a", "neo4j", "neo4j-0", 15_000_000_000)),
		gaugeFamily(MetricMemNodeTotalBytes, point("tenant-a", "neo4j", "neo4j-0", 16_000_000_000)),
	))
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		MemPctThreshold: 85, MemNodePctThreshold: 0,
	})
	c.runOnce(context.Background())

	if n := sender.callCount(); n != 0 {
		t.Errorf("gated callCount = %d, want 0 with the arm disabled", n)
	}
}

// TestCollector_UseSummaryCarriesNodeAndRequestFields proves the six new
// fields (2026-08-29) reach the ungated use_summary frame.
func TestCollector_UseSummaryCarriesNodeAndRequestFields(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 500)),
		gaugeFamily(MetricMemNodeTotalBytes, point("ops", "api", "api-1", 1000)),
		gaugeFamily(MetricMemRequestBytes, point("ops", "api", "api-1", 250)),
		gaugeFamily(MetricCPURequestCores, point("ops", "api", "api-1", 2)),
	))
	c, sender := newTestCollector(t, src, Options{})
	c.runOnce(context.Background())

	summary := sender.lastSummary()
	if len(summary) != 1 {
		t.Fatalf("summary = %+v, want one service", summary)
	}
	got := summary[0]
	if got.MemNodePct != 50 {
		t.Errorf("MemNodePct = %v, want 50", got.MemNodePct)
	}
	if got.MemReqPct != 200 {
		t.Errorf("MemReqPct = %v, want 200 (not clamped)", got.MemReqPct)
	}
	if got.MemRequestBytes != 250 {
		t.Errorf("MemRequestBytes = %v, want 250", got.MemRequestBytes)
	}
	if got.CPURequestCores != 2 {
		t.Errorf("CPURequestCores = %v, want 2", got.CPURequestCores)
	}
}

// TestCollector_NodeAndRequestFieldsFollowSameChosenPodAsMemPct is the
// "do not let different fields come from different pods" regression: two pods
// of the same service disagree on which is worse by memPct vs by raw node
// total, and the summary's node-relative figures must describe the pod that
// won memPct — never a mix of one pod's bytes against another pod's node
// total.
func TestCollector_NodeAndRequestFieldsFollowSameChosenPodAsMemPct(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		gaugeFamily(MetricMemPct,
			point("ops", "api", "pod-a", 90), // wins on memPct
			point("ops", "api", "pod-b", 10), // loses on memPct
		),
		gaugeFamily(MetricMemBytes,
			point("ops", "api", "pod-a", 900),
			point("ops", "api", "pod-b", 100),
		),
		gaugeFamily(MetricMemNodeTotalBytes,
			point("ops", "api", "pod-a", 1000),
			// pod-b's node total is MUCH larger — if the aggregate ever mixed
			// pod-a's bytes with pod-b's node total, the result would be a
			// wildly different (and wrong) percentage.
			point("ops", "api", "pod-b", 1_000_000),
		),
	))
	c, sender := newTestCollector(t, src, Options{})
	c.runOnce(context.Background())

	summary := sender.lastSummary()
	if len(summary) != 1 {
		t.Fatalf("summary = %+v, want one service", summary)
	}
	got := summary[0]
	if got.MemNodePct != 90 { // pod-a's own ratio: 900/1000*100
		t.Errorf("MemNodePct = %v, want 90 (pod-a's own bytes over pod-a's own node total)", got.MemNodePct)
	}
}

// TestCollector_NodeAndRequestFieldsPopulateWhenEveryPodIsUnlimited is the
// motivating case for the memBytes tiebreak in the chosen-pod selection: when
// every pod of a service has memPct pinned at 0 (no memory limit anywhere),
// the node-relative percentage — the entire point of this feature — must
// still populate rather than silently staying absent because no pod ever
// satisfies a strict memPct > 0 comparison.
func TestCollector_NodeAndRequestFieldsPopulateWhenEveryPodIsUnlimited(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(
		// Neither pod reports MetricMemPct/MetricMemWorkingSetPct at all —
		// both are unbounded.
		gaugeFamily(MetricMemBytes,
			point("ops", "api", "pod-a", 200),
			point("ops", "api", "pod-b", 900), // the bigger consumer
		),
		gaugeFamily(MetricMemNodeTotalBytes,
			point("ops", "api", "pod-a", 1000),
			point("ops", "api", "pod-b", 1000),
		),
	))
	c, sender := newTestCollector(t, src, Options{})
	c.runOnce(context.Background())

	summary := sender.lastSummary()
	if len(summary) != 1 {
		t.Fatalf("summary = %+v, want one service", summary)
	}
	got := summary[0]
	if got.MemNodePct != 90 { // pod-b, the bigger consumer, wins the memBytes tiebreak
		t.Errorf("MemNodePct = %v, want 90 (pod-b via the memBytes tiebreak, not left absent)", got.MemNodePct)
	}
}

// TestCollector_ResolvesServiceIdentityFromOwningWorkload is the live shape
// (docs/todo/virtual-apps.md): the exporter's reported service= label
// ("qdrant", from app.kubernetes.io/name) disagrees with the identity
// virtual-app synthesis gives the same StatefulSet ("tenant-a-qdrant", from
// app.kubernetes.io/instance). The aggregated frame must use the RESOLVED
// identity so the series binds to the service graph node the core actually
// has.
func TestCollector_ResolvesServiceIdentityFromOwningWorkload(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, point("tenant-a", "qdrant", "tenant-a-qdrant-0", 25))))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant-a-qdrant-0", Namespace: "tenant-a",
			OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "tenant-a-qdrant")},
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant-a-qdrant", Namespace: "tenant-a",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "tenant-a-qdrant",
				"app.kubernetes.io/name":     "qdrant",
				"app":                        "qdrant",
			},
		},
	}
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40}, pod, sts)

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "tenant-a-qdrant" {
		t.Fatalf("services = %+v, want one entry attributed to tenant-a-qdrant (the resolved identity), not the reported qdrant label", svcs)
	}
}

// TestCollector_IdentityModeWorkload_ResolvesToTheWorkloadsOwnName proves
// OwnerIdentityResolver.ModeFor threads all the way through resolveOwnerIdentity
// into push.GroupNameFor: a Deployment whose instance label disagrees with
// its own name resolves to the label in the default (labels) mode but to
// its own name once the namespace is declared in workload mode.
func TestCollector_IdentityModeWorkload_ResolvesToTheWorkloadsOwnName(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, point("ops", "checkout", "checkout-api-0", 25))))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "checkout-api-0", Namespace: "ops",
			OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "checkout-api")},
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "checkout-api", Namespace: "ops",
			Labels: map[string]string{"app.kubernetes.io/instance": "checkout-prod"},
		},
	}
	c, sender := newTestCollectorMode(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
	}, func(string) string { return push.IdentityModeWorkload }, pod, sts)

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "checkout-api" {
		t.Fatalf("services = %+v, want one entry attributed to the workload's own name checkout-api, ignoring its instance label checkout-prod", svcs)
	}
}

// TestCollector_UnresolvableIdentityFallsBackToReportedLabel verifies a
// resolution failure (here: no Pod object exists to resolve from — the same
// symptom as a pod deleted between the read and the resolve) never drops the
// sample: the aggregated frame still uses the exporter's own reported
// service= label.
func TestCollector_UnresolvableIdentityFallsBackToReportedLabel(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, point("ops", "legacy-api", "legacy-api-1", 25))))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "legacy-api" {
		t.Fatalf("services = %+v, want one entry attributed to the reported label legacy-api (resolution has no Pod object to find)", svcs)
	}
}

// TestCollector_PodWithNoServiceIsDropped: a pod with no reported service
// label that resolution cannot name either (a kubelet cAdvisor series from an
// OTLP sender) is not sent under an empty service; the named one still is.
func TestCollector_PodWithNoServiceIsDropped(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60,
		point("ops", "", "unnamed-1", 25),
		point("ops", "legacy-api", "legacy-api-1", 25),
	)))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})

	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "legacy-api" {
		t.Fatalf("services = %+v, want only legacy-api; the pod with no service must be dropped", svcs)
	}
	for _, s := range sender.lastSummary() {
		if s.Service == "" {
			t.Fatalf("summary carries a service with no name: %+v", s)
		}
	}
}

// TestCollector_StatusOOMSourceCarriesKillWhenCgroupFlat: the cgroup counter
// never moves, the pod-status counter does. The frame carries the kill, and
// when both move the larger delta wins (max, not sum: both see the same kill).
func TestCollector_StatusOOMSourceCarriesKillWhenCgroupFlat(t *testing.T) {
	body := func(cgroup, status float64) metricset.Set {
		return setOf(
			counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", cgroup)),
			counterFamily(MetricOOMKillStatusTotal, point("ops", "api", "api-1", status)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0, 0))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background()) // baseline

	src.set(testTarget, body(0, 1)) // cgroup flat, status +1
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].OOMKills != 1 {
		t.Fatalf("services = %+v, want one entry with OOMKills=1 from the status source", svcs)
	}
	if got := sender.lastSummary(); len(got) != 1 || got[0].OOMKills != 1 {
		t.Fatalf("summary = %+v, want OOMKills=1", got)
	}

	src.set(testTarget, body(2, 2)) // both +... cgroup +2, status +1 -> max 2
	c.runOnce(context.Background())
	if got := sender.lastSummary(); got[0].OOMKills != 2 {
		t.Fatalf("both sources moved: OOMKills = %d, want max(2,1)=2", got[0].OOMKills)
	}
}

// TestCollector_BaselineSurvivesPodAbsentFromOneRead: a pod missing from a
// single read (not Running for a tick, cgroup dir not found) must keep its
// baseline, so a kill that landed in the gap is reported when it returns.
func TestCollector_BaselineSurvivesPodAbsentFromOneRead(t *testing.T) {
	with := func(n float64) metricset.Set {
		return setOf(counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", n), point("ops", "other", "o-1", 0)))
	}
	without := setOf(counterFamily(MetricOOMKillTotal, point("ops", "other", "o-1", 0)))
	src := newFakeSource()
	src.set(testTarget, with(0))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	src.set(testTarget, without)
	c.runOnce(context.Background())
	src.set(testTarget, with(2))
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "api" || svcs[0].OOMKills != 2 {
		t.Fatalf("services = %+v, want api with OOMKills=2 (the kill during the gap)", svcs)
	}
}

// TestCollector_BaselineSurvivesTargetAbsentThenExpires: a target missing from
// the live list for a few reads keeps its baselines; past baselineGraceReads
// they are dropped (no unbounded accretion) and the pod is first-seen again.
func TestCollector_BaselineSurvivesTargetAbsentThenExpires(t *testing.T) {
	body := func(n float64) metricset.Set {
		return setOf(counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", n)))
	}
	const other = "sensor/node-b"
	run := func(absentReads int) int64 {
		src := newFakeSource()
		src.set(testTarget, body(0))
		src.set(other, setOf(counterFamily(MetricOOMKillTotal, point("ops", "x", "x-1", 0))))
		c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
		c.runOnce(context.Background())
		src.mu.Lock()
		delete(src.targets, testTarget)
		src.mu.Unlock()
		for i := 0; i < absentReads; i++ {
			c.runOnce(context.Background())
		}
		src.set(testTarget, body(2))
		c.runOnce(context.Background())
		for _, s := range sender.allServices() {
			if s.Service == "api" {
				return s.OOMKills
			}
		}
		return 0
	}
	if got := run(1); got != 2 {
		t.Errorf("absent 1 read: OOMKills = %d, want 2", got)
	}
	if got := run(baselineGraceReads + 1); got != 0 {
		t.Errorf("absent past the grace: OOMKills = %d, want 0 (baseline expired, pod first-seen again)", got)
	}
}

// TestCollector_BaselineFollowsServiceLabelChange: the exporter's service
// label for a pod changes between reads; the new podKey must inherit the old
// baseline instead of being "first seen" and reporting 0.
func TestCollector_BaselineFollowsServiceLabelChange(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(counterFamily(MetricOOMKillTotal, point("ops", "api", "api-1", 0))))
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	c.runOnce(context.Background())
	src.set(testTarget, setOf(counterFamily(MetricOOMKillTotal, point("ops", "api-renamed", "api-1", 2))))
	c.runOnce(context.Background())
	svcs := sender.allServices()
	if len(svcs) != 1 || svcs[0].Service != "api-renamed" || svcs[0].OOMKills != 2 {
		t.Fatalf("services = %+v, want api-renamed with OOMKills=2", svcs)
	}
}

// TestCollector_PodExcludedDropsSamples: a pod Options.PodExcluded reports
// out of triage (triageagent.dev/triage: "false") is never aggregated, so
// its memory pressure and OOM kills reach neither use_metrics nor
// use_summary, while a sibling service still does.
func TestCollector_PodExcludedDropsSamples(t *testing.T) {
	body := func(oom float64) metricset.Set {
		return setOf(
			gaugeFamily(MetricMemPsiAvg60,
				point("ops", "load-generator", "load-generator-1", 90),
				point("ops", "api", "api-1", 90),
			),
			counterFamily(MetricOOMKillTotal,
				point("ops", "load-generator", "load-generator-1", oom),
			),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0))
	var asked []string
	c, sender := newTestCollector(t, src, Options{
		MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40,
		PodExcluded: func(_ context.Context, namespace, pod string) bool {
			asked = append(asked, namespace+"/"+pod)
			return pod == "load-generator-1"
		},
	})
	c.runOnce(context.Background())
	src.set(testTarget, body(3))
	c.runOnce(context.Background())

	for _, s := range sender.allServices() {
		if s.Service != "api" {
			t.Errorf("use_metrics carries excluded %+v", s)
		}
	}
	if len(sender.allServices()) == 0 {
		t.Fatal("the included service must still be sent")
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	for _, call := range sender.summary {
		for _, s := range call {
			if s.Service != "api" {
				t.Errorf("use_summary carries excluded %+v", s)
			}
		}
	}
	if len(asked) == 0 {
		t.Error("PodExcluded was never consulted")
	}
}

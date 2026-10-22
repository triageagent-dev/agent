package redsource

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/triageagent-dev/agent/metricset"
)

// ===========================================================================
// Latency gate arm (Options.LatencyMultiplier).
// ===========================================================================

// latBounds is the bucket ladder (seconds) the latency tests drive. With all of
// a window's requests in bucket i, the interpolated p99 is
// lo + 0.99*(hi-lo) of that bucket:
//
//	0: ~5 ms   1: ~99 ms   2: ~397 ms   3: ~796 ms   4: ~998 ms   5: ~4.96 s   6: ~9.95 s
var latBounds = []float64{0.005, 0.1, 0.4, 0.8, 1, 5, 10}

// latHarness drives one service ("ns-a/api") through windows of a cumulative
// histogram, the way grpc_test.go drives its windows through runOnce.
type latHarness struct {
	t      *testing.T
	src    *fakeSource
	c      *Collector
	sender *fakeSender
	pair   servicePair
	cum    []uint64 // cumulative per-bucket counts; last entry is +Inf
	total  int      // cumulative 200 count
	errs   int      // cumulative 500 count
}

func newLatHarness(t *testing.T, extra Options) *latHarness {
	t.Helper()
	if extra.LatencyMultiplier == 0 {
		extra.LatencyMultiplier = 5
	}
	if extra.Baseline.Enabled == nil {
		// These tests pin the ORIGINAL latency arm; the baseline arms (see
		// baseline_arms_test.go) would also fire on the same fixtures.
		off := false
		extra.Baseline.Enabled = &off
	}
	src := newFakeSource()
	c, sender := obiRED(t, src, 10, 5, extra)
	h := &latHarness{t: t, src: src, c: c, sender: sender,
		pair: servicePair{Namespace: "ns-a", Workload: "api"}, cum: make([]uint64, len(latBounds)+1)}
	h.publish() // first-seen baseline: zero deltas
	c.runOnce(context.Background())
	return h
}

func (h *latHarness) publish() {
	bs := make([]metricset.Bucket, 0, len(h.cum))
	for i, le := range latBounds {
		bs = append(bs, metricset.Bucket{UpperBound: le, CumulativeCount: h.cum[i]})
	}
	bs = append(bs, metricset.Bucket{UpperBound: math.Inf(1), CumulativeCount: h.cum[len(latBounds)]})
	h.src.set(testTarget, setOf(obiMetric(
		obiPoint("ns-a", "api", "GET", "/x", "200", h.total, bs...),
		obiPoint("ns-a", "api", "GET", "/x", "500", h.errs),
	)))
}

// window delivers n successful requests all landing in bucket idx, plus errs
// 500s (which carry no buckets), then runs one tick. It returns the number of
// red_metrics frames sent so far.
func (h *latHarness) window(idx, n, errs int) int {
	for i := idx; i < len(h.cum); i++ {
		h.cum[i] += uint64(n)
	}
	h.total += n
	h.errs += errs
	h.publish()
	h.c.runOnce(context.Background())
	return h.sender.redCallCount()
}

func (h *latHarness) warm(idx, n, windows int) {
	for i := 0; i < windows; i++ {
		if got := h.window(idx, n, 0); got != 0 {
			h.t.Fatalf("baseline window %d sent a red_metrics frame", i)
		}
	}
}

// 1: 12 fast windows, then a 10 s window with zero errors.
func TestLatencyArm_FiresOnSlowWindow(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 12)
	if got := h.window(6, 100, 0); got != 1 {
		t.Fatalf("red_metrics frames = %d, want 1", got)
	}
	svcs := h.sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("services = %+v, want 1", svcs)
	}
	s := svcs[0]
	if !s.LatencyBreach || s.ErrorCount != 0 || s.RequestCount != 100 {
		t.Errorf("service = %+v, want LatencyBreach, errors=0, requests=100", s)
	}
	if math.Abs(s.BaselineP99Ms-4.95) > 0.01 {
		t.Errorf("BaselineP99Ms = %v, want ~4.95", s.BaselineP99Ms)
	}
	if !strings.Contains(s.Sample, "p99 9950ms vs baseline 5ms") {
		t.Errorf("sample = %q, want it to describe p99 and baseline", s.Sample)
	}
}

// 2: not enough baseline windows. Under the cold floor (5 s) the arm stays
// silent; see TestLatencyArm_ColdFloorReportsAndDoesNotLearn for above it.
func TestLatencyArm_NeedsBaselineWindows(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 5)
	if got := h.window(5, 100, 0); got != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (only 5 baseline windows, p99 4.96 s)", got)
	}
}

// The baseline is in memory, so after a restart every service is cold and the
// arm learned whatever it read: a restart during a 10 s slowdown made 10 s the
// service's normal. A cold p99 at or above the cold floor is reported and not
// learned, as in the baseline arms.
func TestLatencyArm_ColdFloorReportsAndDoesNotLearn(t *testing.T) {
	h := newLatHarness(t, Options{})
	for i := 1; i <= 15; i++ {
		if got := h.window(6, 100, 0); got != i {
			t.Fatalf("slow window %d: frames = %d, want %d (cold floor)", i, got, i)
		}
	}
	svcs := h.sender.redServices()
	if s := svcs[len(svcs)-1]; !s.LatencyBreach || s.BaselineP99Ms != 0 || !strings.Contains(s.Sample, "p99 9950ms vs baseline 0ms") {
		t.Errorf("service = %+v, want a latency breach with no baseline", s)
	}
	if n := len(h.c.p99Hist[h.pair]); n != 0 {
		t.Fatalf("baseline holds %d slow readings, want 0", n)
	}
	for i := 0; i < 12; i++ {
		if got := h.window(0, 100, 0); got != 15 {
			t.Fatalf("fast window %d sent a frame", i)
		}
	}
	if got := h.window(6, 100, 0); got != 16 {
		t.Fatalf("slow window after recovery: frames = %d, want 16", got)
	}
	svcs = h.sender.redServices()
	if s := svcs[len(svcs)-1]; math.Abs(s.BaselineP99Ms-4.95) > 0.01 {
		t.Errorf("BaselineP99Ms = %v, want the healthy ~4.95", s.BaselineP99Ms)
	}

	// The cold floor off: silent while cold, and learned (the old behaviour).
	off := false
	h = newLatHarness(t, Options{Baseline: BaselineOptions{Enabled: &off, LatColdFloorMs: -1}})
	for i := 0; i < 5; i++ {
		if got := h.window(6, 100, 0); got != 0 {
			t.Fatalf("cold floor disabled: slow window %d sent a frame", i)
		}
	}
}

// 3: under the multiplier, and under the absolute floor.
func TestLatencyArm_NeedsMultiplierAndFloor(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(2, 100, 12)                        // baseline ~397 ms
	if got := h.window(4, 100, 0); got != 0 { // ~998 ms: x2.5
		t.Errorf("x2.5 window: frames = %d, want 0 (under multiplier)", got)
	}
	h = newLatHarness(t, Options{})
	h.warm(4, 100, 12)                        // baseline ~998 ms
	if got := h.window(5, 100, 0); got != 0 { // ~4.96 s: x4.97, over the floor
		t.Errorf("x4.97 window: frames = %d, want 0 (under multiplier, over floor)", got)
	}
	h = newLatHarness(t, Options{})
	h.warm(1, 100, 12)                        // baseline ~99 ms
	if got := h.window(3, 100, 0); got != 0 { // ~796 ms: x8 but under 1000 ms
		t.Errorf("x8 window under floor: frames = %d, want 0", got)
	}
}

// 4: too few requests for the p99 to mean anything.
func TestLatencyArm_NeedsMinRequests(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 12)
	if got := h.window(6, 4, 0); got != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (4 requests < 10)", got)
	}
	if n := len(h.c.p99Hist[h.pair]); n != 12 {
		t.Errorf("baseline len = %d, want 12 (a low-traffic window is not recorded)", n)
	}
}

// 5: a long slowdown keeps firing and is never absorbed into the baseline.
func TestLatencyArm_SlowWindowsAreNotAbsorbed(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 12)
	for i := 1; i <= 10; i++ {
		if got := h.window(6, 100, 0); got != i {
			t.Fatalf("slow window %d: frames = %d, want %d", i, got, i)
		}
	}
	if n := len(h.c.p99Hist[h.pair]); n != 12 {
		t.Errorf("baseline len = %d, want 12", n)
	}
	if got := h.sender.redServices()[0].BaselineP99Ms; math.Abs(got-4.95) > 0.01 {
		t.Errorf("BaselineP99Ms after 10 slow windows = %v, want ~4.95", got)
	}
}

// 6: multiplier 0 disables the arm. newLatHarness defaults a zero multiplier,
// so build the collector directly.
func TestLatencyArm_DisabledAtZero(t *testing.T) {
	h := newLatHarness(t, Options{LatencyMultiplier: 5})
	h.c.opts.LatencyMultiplier = 0
	h.warm(0, 100, 12)
	if got := h.window(6, 100, 0); got != 0 {
		t.Errorf("red_metrics frames = %d, want 0 with the arm disabled", got)
	}
}

// 7: both an error arm and the latency arm: the error sample wins, the
// latency fields are still set.
func TestLatencyArm_AlongsideErrorArm(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 12)
	if got := h.window(6, 100, 20); got != 1 {
		t.Fatalf("red_metrics frames = %d, want 1", got)
	}
	s := h.sender.redServices()[0]
	if !s.LatencyBreach || s.ErrorCount != 20 || s.BaselineP99Ms == 0 {
		t.Errorf("service = %+v, want LatencyBreach, errors=20, baseline set", s)
	}
	if want := "GET /x -> 500 x20"; s.Sample != want {
		t.Errorf("sample = %q, want the error sample %q", s.Sample, want)
	}
}

// 8: a service that stops appearing loses its baseline.
func TestLatencyArm_BaselineGarbageCollected(t *testing.T) {
	h := newLatHarness(t, Options{})
	h.warm(0, 100, 12)
	if len(h.c.p99Hist[h.pair]) != 12 {
		t.Fatalf("baseline not built")
	}
	h.src.set(testTarget, setOf(obiMetric(obiPoint("ns-a", "other", "GET", "/x", "200", 0))))
	h.c.runOnce(context.Background())
	if _, ok := h.c.p99Hist[h.pair]; ok {
		t.Errorf("p99Hist still holds the vanished service")
	}
}

func TestMedianOf(t *testing.T) {
	if got := medianOf([]float64{9, 1, 5}); got != 5 {
		t.Errorf("odd median = %v, want 5", got)
	}
	if got := medianOf([]float64{1, 2, 10, 20}); got != 6 {
		t.Errorf("even median = %v, want 6 (mean of the two middle values)", got)
	}
}

// slowestOperation picks the operation with the highest p99, the one that
// reaches the top bucket for a long-lived stream, and names it with its
// request count; no operation with a histogram gives "".
func TestSlowestOperation(t *testing.T) {
	fast := &opTotals{label: "GET /x", requests: 100, buckets: map[float64]int64{0.005: 100, math.Inf(1): 100}}
	stream := &opTotals{label: "GET /v2/agent/ws", requests: 2, buckets: map[float64]int64{0.005: 0, 10: 1, math.Inf(1): 2}}
	got := slowestOperation(map[string]*opTotals{"a": fast, "b": stream})
	if !strings.HasPrefix(got, "GET /v2/agent/ws p99 10000ms over 2 req") {
		t.Errorf("slowest = %q, want the stream route at the top bucket", got)
	}
	if got := slowestOperation(map[string]*opTotals{"a": {label: "GET /y", requests: 5}}); got != "" {
		t.Errorf("no histogram: slowest = %q, want empty", got)
	}
	if got := slowestOperation(nil); got != "" {
		t.Errorf("nil: slowest = %q, want empty", got)
	}
	b := latencyBreach{p99Ms: 10000, baseline: 180, slowest: "GET /v2/agent/ws p99 10000ms over 2 req"}
	if s := b.sample(); !strings.Contains(s, "(x56); slowest: GET /v2/agent/ws") {
		t.Errorf("sample = %q, want the slowest operation appended", s)
	}
}

// A single request in the tail (a stream closing) is not a latency reading;
// two sharing the tail are.
func TestTailRequests(t *testing.T) {
	one := map[float64]int64{0.5: 29, 5: 29, 10: 30, math.Inf(1): 30}
	p, _ := p99FromBucketDeltas(one)
	if got := tailRequests(one, p); got != 1 {
		t.Errorf("one slow request: tail = %d, want 1", got)
	}
	two := map[float64]int64{0.5: 28, 5: 28, 10: 30, math.Inf(1): 30}
	p, _ = p99FromBucketDeltas(two)
	if got := tailRequests(two, p); got < minTailRequests {
		t.Errorf("two slow requests: tail = %d, want >= %d", got, minTailRequests)
	}
}

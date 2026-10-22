package redsource

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// ===========================================================================
// Baseline arms (BaselineOptions): multi-tick scenarios on low-volume
// operations, the faults the absolute gates missed.
// ===========================================================================

// opSim is one operation's cumulative counters. bucket is the index into
// latBounds that all its requests land in.
type opSim struct {
	route    string
	ok, errs int
	cum      []uint64 // cumulative per-bucket counts, last is +Inf
	hidden   bool     // omit the operation's series from the snapshot
}

func newOpSim(route string) *opSim {
	return &opSim{route: route, cum: make([]uint64, len(latBounds)+1)}
}

// add records n successful requests landing in bucket idx and errs 500s.
func (o *opSim) add(idx, n, errs int) {
	for i := idx; i < len(o.cum); i++ {
		o.cum[i] += uint64(n)
	}
	o.ok += n
	o.errs += errs
}

func (o *opSim) points(ns, svc string) []metricset.Point {
	if o.hidden {
		return nil
	}
	bs := make([]metricset.Bucket, 0, len(o.cum))
	for i, le := range latBounds {
		bs = append(bs, metricset.Bucket{UpperBound: le, CumulativeCount: o.cum[i]})
	}
	bs = append(bs, metricset.Bucket{UpperBound: math.Inf(1), CumulativeCount: o.cum[len(latBounds)]})
	pts := []metricset.Point{obiPoint(ns, svc, "POST", o.route, "200", o.ok, bs...)}
	// Like a real exporter, the 500 series does not exist until the first
	// error: emitting it at 0 from the start would hide a collector that
	// drops a new series' first value.
	if o.errs > 0 {
		pts = append(pts, obiPoint(ns, svc, "POST", o.route, "500", o.errs))
	}
	return pts
}

// reset restarts the operation's counters from zero, as the exporter does
// for a series that was idle long enough to be dropped.
func (o *opSim) reset() {
	o.ok, o.errs = 0, 0
	o.cum = make([]uint64, len(o.cum))
}

// baseHarness drives one service through ticks of several operations.
type baseHarness struct {
	t      *testing.T
	src    *fakeSource
	c      *Collector
	sender *fakeSender
	ops    []*opSim
}

func newBaseHarness(t *testing.T, extra Options, ops ...*opSim) *baseHarness {
	t.Helper()
	src := newFakeSource()
	c, sender := obiRED(t, src, 10, 50, extra)
	c.opts.OperationErrorRatePct = 50
	h := &baseHarness{t: t, src: src, c: c, sender: sender, ops: ops}
	h.tick() // first-seen baseline: zero deltas
	return h
}

func (h *baseHarness) tick() int {
	var pts []metricset.Point
	for _, o := range h.ops {
		pts = append(pts, o.points("ns-a", "svc")...)
	}
	h.src.set(testTarget, setOf(obiMetric(pts...)))
	h.c.runOnce(context.Background())
	return h.sender.redCallCount()
}

// lastFrame returns the service of the newest red_metrics frame.
func (h *baseHarness) lastFrame() push.ServiceRedMetrics {
	h.t.Helper()
	svcs := h.sender.redServices()
	if len(svcs) == 0 {
		h.t.Fatal("no red_metrics frame")
	}
	return svcs[len(svcs)-1]
}

// quiet runs n ticks that must send no frame.
func (h *baseHarness) quiet(n int, step func()) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		step()
		if got := h.tick(); got != 0 {
			h.t.Fatalf("quiet tick %d sent a red_metrics frame: %+v", i, h.sender.redServices())
		}
	}
}

// payment-like: Charge runs 4 req/min, healthy for 8 ticks, then fails 100%.
func TestBaselineArms_PaymentLikeOpErrors(t *testing.T) {
	charge := newOpSim("/pay.Charge/Do")
	other := newOpSim("/pay.Health")
	h := newBaseHarness(t, Options{}, charge, other)
	h.quiet(8, func() { charge.add(0, 4, 0); other.add(0, 40, 0) })

	fired := 0
	for i := 1; i <= 5; i++ {
		charge.add(0, 0, 4) // 4 failing requests this minute
		other.add(0, 40, 0)
		if h.tick() > 0 {
			fired = i
			break
		}
	}
	if fired == 0 {
		t.Fatal("no red_metrics frame within 5 fault ticks")
	}
	s := h.lastFrame()
	if s.Breach != push.BreachOpErrors || s.Operation != "POST /pay.Charge/Do" {
		t.Errorf("breach = %q op = %q, want op-errors on POST /pay.Charge/Do (%+v)", s.Breach, s.Operation, s)
	}
	if s.ErrorCount >= 10 || h.c.opts.ErrorThreshold != 10 {
		t.Errorf("errorCount = %d threshold = %d: the absolute gates must not be what fired", s.ErrorCount, h.c.opts.ErrorThreshold)
	}
	if s.Observed < 15 || s.EvidenceSec != 300 || s.Sample == "" {
		t.Errorf("observed = %v evidenceSec = %d sample = %q, want a >=15%% reading over 300s with a sample", s.Observed, s.EvidenceSec, s.Sample)
	}
	// A sustained fault is not absorbed: it keeps firing every tick.
	for i := 0; i < 5; i++ {
		charge.add(0, 0, 4)
		other.add(0, 40, 0)
		before := h.sender.redCallCount()
		if got := h.tick(); got != before+1 {
			t.Fatalf("sustained fault tick %d: frames = %d, want %d", i, got, before+1)
		}
	}
}

// cart-like: one op at 4 req/min with 1 error/min on a 200 req/min service.
func TestBaselineArms_CartLikeOpErrors(t *testing.T) {
	empty := newOpSim("/cart.EmptyCart")
	browse := newOpSim("/cart.Get")
	h := newBaseHarness(t, Options{}, empty, browse)
	h.quiet(12, func() { empty.add(0, 4, 0); browse.add(0, 200, 0) })

	// Faults 1 and 2: under ErrMinErrors (3), whatever the requests.
	h.quiet(2, func() { empty.add(0, 3, 1); browse.add(0, 200, 0) })
	// Fault 3: 3 errors over 20 requests (15%) vs a zero baseline.
	empty.add(0, 3, 1)
	browse.add(0, 200, 0)
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1 once 3 errors over 20 requests accumulated", h.sender.redCallCount())
	}
	s := h.lastFrame()
	if s.Breach != push.BreachOpErrors || s.Operation != "POST /cart.EmptyCart" {
		t.Errorf("service = %+v, want op-errors on EmptyCart", s)
	}
	if s.RequestCount != 204 || s.ErrorCount != 1 {
		t.Errorf("counts = %d/%d, want the service-wide window counts 1/204", s.ErrorCount, s.RequestCount)
	}
}

// shipping-like: ShipOrder p99 goes 5 ms -> 10 s while GetQuote's traffic
// keeps the service-wide p99 flat.
func TestBaselineArms_ShippingLikeOpLatency(t *testing.T) {
	quote := newOpSim("/ship.GetQuote")
	ship := newOpSim("/ship.ShipOrder")
	h := newBaseHarness(t, Options{}, quote, ship)
	h.quiet(12, func() { quote.add(0, 400, 0); ship.add(0, 3, 0) })

	quote.add(0, 400, 0)
	ship.add(6, 3, 0) // 10 s
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1", h.sender.redCallCount())
	}
	s := h.lastFrame()
	if s.Breach != push.BreachOpLatency || s.Operation != "POST /ship.ShipOrder" {
		t.Errorf("service = %+v, want op-latency on ShipOrder", s)
	}
	if s.Observed < 5000 || s.Baseline > 10 || s.LatencyBreach || s.EvidenceSec != 240 {
		t.Errorf("observed = %v baseline = %v latencyBreach = %v evidenceSec = %d, want ~10s vs ~5ms, service latency arm quiet, 4 windows",
			s.Observed, s.Baseline, s.LatencyBreach, s.EvidenceSec)
	}
	// The service-wide latency tracker must not have breached: its p99 is the
	// 400 fast requests'.
	if v := h.c.latSvc.JudgeQuantile("ns-a/svc", h.c.latGate(), 0.99); v.Breach || v.Observed > 10 {
		t.Errorf("service-wide verdict = %+v, want no breach, ~5ms", v)
	}
}

// chronic: a steady 10% error stream, under the cold floor, is learned as the
// op's normal; nothing fires.
func TestBaselineArms_ChronicLowRateStaysSilent(t *testing.T) {
	op := newOpSim("/x.Flaky")
	h := newBaseHarness(t, Options{}, op)
	h.quiet(20, func() { op.add(0, 27, 3) })
	if n := h.c.errOp.Len(); n != 1 {
		t.Errorf("errOp keys = %d, want 1", n)
	}
}

// chronic and high: at or above the cold floor (15%) the arm fires with no
// baseline, and since a breach is never settled the baseline never forms, so
// it keeps firing, like the absolute gates do. Documented behaviour.
func TestBaselineArms_ChronicHighRateFiresColdAndKeepsFiring(t *testing.T) {
	op := newOpSim("/x.Broken")
	h := newBaseHarness(t, Options{}, op)
	for i := 1; i <= 8; i++ {
		op.add(0, 21, 9) // 30%, 9 errors: under the absolute gate's 10
		if got := h.tick(); got != i {
			t.Fatalf("tick %d: frames = %d, want %d (cold breach every tick)", i, got, i)
		}
	}
	if s := h.lastFrame(); s.Breach != push.BreachOpErrors || s.Baseline != 0 {
		t.Errorf("service = %+v, want a cold op-errors breach (baseline 0)", s)
	}
}

// A cold op failing outright fires on its first full evidence window.
func TestBaselineArms_NewOpFailingFiresCold(t *testing.T) {
	op := newOpSim("/x.New")
	h := newBaseHarness(t, Options{}, op)
	op.add(0, 0, 25)
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1 (100%% errors, 25 requests, no baseline)", h.sender.redCallCount())
	}
}

// no-traffic gap: the op vanishes for 3 ticks and keeps its baseline.
func TestBaselineArms_GapKeepsBaseline(t *testing.T) {
	op := newOpSim("/x.Sometimes")
	steady := newOpSim("/x.Steady")
	h := newBaseHarness(t, Options{}, op, steady)
	h.quiet(8, func() { op.add(0, 30, 0); steady.add(0, 30, 0) })
	op.hidden = true
	h.quiet(3, func() { steady.add(0, 30, 0) })
	if h.c.errOp.Len() != 2 {
		t.Fatalf("errOp keys after the gap = %d, want 2 (baseline kept)", h.c.errOp.Len())
	}
	op.hidden = false
	// Its series were dropped while idle, so they come back from zero, and
	// the first window back already counts: it fails outright.
	op.reset()
	op.add(0, 0, 30)
	steady.add(0, 30, 0)
	if h.tick() != 1 {
		t.Fatal("no breach on the op's first failing window after the gap")
	}
	if s := h.lastFrame(); s.Breach != push.BreachOpErrors || s.Baseline != 0 {
		t.Errorf("service = %+v, want op-errors against its kept (zero) baseline", s)
	}
}

// service-wide error arm: errors spread thinly over many operations (2 each,
// under ErrMinErrors) still breach at the service level.
func TestBaselineArms_ServiceErrorsArm(t *testing.T) {
	var ops []*opSim
	for _, r := range []string{"/a", "/b", "/c", "/d", "/e", "/f"} {
		ops = append(ops, newOpSim(r))
	}
	h := newBaseHarness(t, Options{}, ops...)
	h.quiet(8, func() {
		for _, o := range ops {
			o.add(0, 10, 0)
		}
	})
	for _, o := range ops {
		o.add(0, 8, 2) // 12 errors over 60 requests = 20% service-wide
	}
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1", h.sender.redCallCount())
	}
	if s := h.lastFrame(); s.Breach != push.BreachErrors || s.Operation != "" || s.Observed != 20 {
		t.Errorf("service = %+v, want an errors breach at 20%%", s)
	}
}

func TestBaselineArms_Disabled(t *testing.T) {
	off := false
	charge := newOpSim("/pay.Charge/Do")
	h := newBaseHarness(t, Options{Baseline: BaselineOptions{Enabled: &off}}, charge)
	if h.c.errOp != nil || h.c.latSvc != nil {
		t.Fatal("trackers exist with the baseline disabled")
	}
	h.quiet(8, func() { charge.add(0, 4, 0) })
	h.quiet(6, func() { charge.add(0, 0, 4) })
}

func TestBaselineOptions_Defaults(t *testing.T) {
	var o Options
	o.setDefaults()
	want := BaselineOptions{EvidenceWindows: 5, MinBaselineWindows: 5, Multiplier: 3, ErrMinRequests: 20, ErrMinErrors: 3, ErrFloorPct: 5, LatMinRequests: 10, LatFloorMs: 300,
		ErrColdFloorPct: 15, LatColdFloorMs: 5000, ErrSparseMinErrors: 5, ErrSparseRatePct: 50, LatChronicMinRequests: 100, MaxOperations: 100}
	if got := o.Baseline; got.Enabled != nil || got != want {
		t.Errorf("defaults = %+v, want %+v", got, want)
	}
	if !o.Baseline.enabled() {
		t.Error("zero BaselineOptions must be enabled")
	}
}

func TestBaselineHit_PrecedenceAndTies(t *testing.T) {
	op := &baselineHit{arm: push.BreachOpErrors, op: "b", observed: 10}
	svc := &baselineHit{arm: push.BreachErrors, observed: 90}
	lat := &baselineHit{arm: push.BreachOpLatency, observed: 9000, baseline: 5}
	if !op.beats(svc) || svc.beats(op) || !svc.beats(lat) || !lat.beats(&baselineHit{arm: push.BreachLatency, observed: 1e9}) {
		t.Error("precedence must be op-errors > errors > op-latency > latency")
	}
	big := &baselineHit{arm: push.BreachOpErrors, op: "z", observed: 80}
	if !big.beats(op) || op.beats(big) {
		t.Error("larger observed/baseline ratio must win within an arm")
	}
	a := &baselineHit{arm: push.BreachOpErrors, op: "a", observed: 10}
	if !a.beats(op) || op.beats(a) {
		t.Error("equal ratios must break to the lexically smaller operation")
	}
}

func TestBaselineHit_Describe(t *testing.T) {
	h := &baselineHit{arm: push.BreachOpErrors, op: "POST /pay/Charge", observed: 100, evidenceSec: 300}
	if got, want := h.describe(), "POST /pay/Charge 100% errors over 300s vs baseline 0%"; got != want {
		t.Errorf("describe = %q, want %q", got, want)
	}
	l := &baselineHit{arm: push.BreachLatency, observed: 10000, baseline: 30, evidenceSec: 60}
	if got := l.describe(); !strings.Contains(got, "p99 10000ms over 60s vs baseline 30ms") {
		t.Errorf("describe = %q", got)
	}
}

// Bucket series carry the same operation labels as the request counter (OBI
// emits one histogram for both), so per-operation bucket deltas key like
// opTotalsFor does.
func TestComputeBucketDeltasOps_SplitsByOperation(t *testing.T) {
	src := newFakeSource()
	c, _ := obiRED(t, src, 10, 5, Options{})
	rm := ProfileOBI.RequestMetrics[0]
	pts := func(a, b uint64) *metricset.Metric {
		return obiMetric(
			obiPoint("ns-a", "svc", "GET", "/a", "200", 0, obiBuckets(a, a, a, a)...),
			obiPoint("ns-a", "svc", "GET", "/b", "200", 0, obiBuckets(b, b, b, b)...),
			obiPoint("ns-a", "svc", "", "", "200", 0, obiBuckets(1, 1, 1, 1)...), // no operation identity
		)
	}
	c.computeBucketDeltasOps("t", rm, pts(0, 0))
	svc, ops := c.computeBucketDeltasOps("t", rm, pts(5, 7))
	pair := servicePair{Namespace: "ns-a", Workload: "svc"}
	if got := svc[pair][math.Inf(1)]; got != 12 {
		t.Errorf("service +Inf delta = %d, want 12 (operation-less series add 0 here)", got)
	}
	ka, _, _ := operationKey(rm, map[string]string{"http.request.method": "GET", "http.route": "/a"})
	kb, _, _ := operationKey(rm, map[string]string{"http.request.method": "GET", "http.route": "/b"})
	if ops[pair][ka][0.1] != 5 || ops[pair][kb][0.1] != 7 || len(ops[pair]) != 2 {
		t.Errorf("ops = %+v, want /a:5 and /b:7 and nothing for the label-less series", ops[pair])
	}
	if opLabelFromKey(ka) != "GET /a" {
		t.Errorf("label = %q", opLabelFromKey(ka))
	}
}

// An operation at 3 requests a minute never reaches ErrMinRequests (20) within
// the 5-window lookback, so it was never judged: failing outright for half an
// hour next to a busy healthy operation sent nothing. The low-volume rule
// fires on its failure share.
func TestBaselineArms_SparseOpFailingFires(t *testing.T) {
	charge := newOpSim("/pay.Charge/Do")
	other := newOpSim("/pay.Health")
	h := newBaseHarness(t, Options{}, charge, other)
	h.quiet(8, func() { charge.add(0, 3, 0); other.add(0, 200, 0) })

	first := 0
	for i := 1; i <= 30; i++ {
		charge.add(0, 0, 3) // 3 failing requests a minute, all of the operation
		other.add(0, 200, 0)
		before := h.sender.redCallCount()
		if h.tick() > before && first == 0 {
			first = i
		}
	}
	// 3 healthy windows (9 requests) and 3 failing (9 errors): 9 of 18 is 50%.
	if first != 3 {
		t.Fatalf("first frame at fault tick %d, want 3 (5 errors and a 50%% share over the lookback)", first)
	}
	if got := h.sender.redCallCount(); got != 28 {
		t.Errorf("frames over 30 fault ticks = %d, want 28 (every tick from the third)", got)
	}
	s := h.lastFrame()
	if s.Breach != push.BreachOpErrors || s.Operation != "POST /pay.Charge/Do" || s.Observed != 100 || s.EvidenceSec != 300 {
		t.Errorf("service = %+v, want op-errors on Charge at 100%% over 300s", s)
	}
	// A quiet healthy operation with the odd error stays silent: 2 errors in 15.
	flaky := newOpSim("/x.Rare")
	h = newBaseHarness(t, Options{}, flaky)
	h.quiet(4, func() { flaky.add(0, 3, 0) })
	h.quiet(1, func() { flaky.add(0, 1, 2) })
	h.quiet(6, func() { flaky.add(0, 3, 0) })
}

// The low-volume rule can be switched off.
func TestBaselineArms_SparseRuleDisabled(t *testing.T) {
	charge := newOpSim("/pay.Charge/Do")
	h := newBaseHarness(t, Options{Baseline: BaselineOptions{ErrSparseMinErrors: -1}}, charge)
	h.quiet(10, func() { charge.add(0, 0, 3) })
}

// An agent that starts during a 10 s latency outage has no baseline. The
// outage is reported on the cold floor every tick and never becomes the
// baseline: once the service recovers and a healthy baseline forms, the same
// outage breaches against it.
func TestBaselineArms_StartDuringLatencyOutage(t *testing.T) {
	op := newOpSim("/ship.ShipOrder")
	h := newBaseHarness(t, Options{}, op)
	for i := 1; i <= 30; i++ {
		op.add(6, 100, 0) // 10 s
		if got := h.tick(); got != i {
			t.Fatalf("outage tick %d: frames = %d, want %d (cold breach every tick)", i, got, i)
		}
	}
	if s := h.lastFrame(); s.Breach != push.BreachOpLatency || s.Baseline != 0 || s.Observed < 9000 || s.ErrorCount != 0 {
		t.Errorf("service = %+v, want a cold op-latency breach near 10 s", s)
	}
	base := h.sender.redCallCount()
	for i := 0; i < 8; i++ {
		op.add(0, 100, 0)
		if got := h.tick(); got != base {
			t.Fatalf("healthy tick %d sent a frame: %+v", i, h.lastFrame())
		}
	}
	op.add(6, 100, 0)
	if h.tick() != base+1 {
		t.Fatal("the outage was learned as normal: no breach after recovery")
	}
	if s := h.lastFrame(); s.Baseline <= 0 || s.Baseline > 10 {
		t.Errorf("baseline = %vms, want the healthy ~5ms", s.Baseline)
	}
}

// The same for a 15% error outage, which is under the previous 20% cold floor.
func TestBaselineArms_StartDuringPartialErrorOutage(t *testing.T) {
	op := newOpSim("/cart.EmptyCart")
	h := newBaseHarness(t, Options{}, op)
	for i := 1; i <= 30; i++ {
		op.add(0, 85, 15)
		if got := h.tick(); got != i {
			t.Fatalf("outage tick %d: frames = %d, want %d (cold breach every tick)", i, got, i)
		}
	}
	if s := h.lastFrame(); s.Breach != push.BreachOpErrors || s.Baseline != 0 || s.Observed != 15 {
		t.Errorf("service = %+v, want a cold op-errors breach at 15%%", s)
	}
	base := h.sender.redCallCount()
	for i := 0; i < 8; i++ {
		op.add(0, 100, 0)
		h.tick()
	}
	if got := h.sender.redCallCount(); got != base {
		t.Fatalf("healthy ticks sent %d frames", got-base)
	}
	op.add(0, 85, 15)
	if h.tick() != base+1 {
		t.Fatal("the outage was learned as normal: no breach after recovery")
	}
}

// A cold operation that is slow or failing a little, under the cold floors,
// stays silent and is learned: a cold key must not fire on ordinary readings.
func TestBaselineArms_ColdUnderColdFloorsStaysSilent(t *testing.T) {
	slowish := newOpSim("/report.Build") // ~1 s p99, 10% errors
	h := newBaseHarness(t, Options{}, slowish)
	h.quiet(20, func() { slowish.add(4, 90, 10) })
}

// chronicTicks drives one operation whose p99 spikes to 10 s every third tick
// (a tenth of its requests are slow), calling each with whether a frame was sent.
func chronicTicks(h *baseHarness, op *opSim, requests, ticks int, each func(i int, sent bool)) {
	for i := 0; i < ticks; i++ {
		if i%3 == 0 {
			op.add(0, requests-requests/10, 0)
			op.add(6, requests/10, 0)
		} else {
			op.add(0, requests, 0)
		}
		before := h.sender.redCallCount()
		each(i, h.tick() > before)
	}
}

// A chronically spiky operation is muted, but not silently: the mute is
// logged when it starts and ends, counted in the muted-keys metric, and the
// key keeps reporting every StillBreachingEvery windows with StillBreaching
// set on the wire.
func TestBaselineArms_ChronicOpMuteIsLoggedCountedAndBounded(t *testing.T) {
	op := newOpSim("/quote.Get")
	h := newBaseHarness(t, Options{}, op)
	var logs []string
	h.c.log = funcr.New(func(_, args string) { logs = append(logs, args) }, funcr.Options{})
	count := func(substr string) (n int) {
		for _, l := range logs {
			if strings.Contains(l, substr) {
				n++
			}
		}
		return n
	}
	muted := func(arm string) float64 {
		return agentmetrics.MutedKeys(agentmetrics.SensorRED, arm)
	}
	h.quiet(8, func() { op.add(0, 200, 0) })

	var late []push.ServiceRedMetrics
	lastSent := -1
	chronicTicks(h, op, 200, 90, func(i int, sent bool) {
		if !sent {
			return
		}
		if i >= 30 {
			late = append(late, h.lastFrame())
			if gap := i - lastSent; gap < baseline.StillBreachingEvery {
				t.Errorf("tick %d: report %d ticks after the previous one, want at least %d", i, gap, baseline.StillBreachingEvery)
			}
		}
		lastSent = i
	})
	if len(late) < 4 {
		t.Fatalf("late frames = %d, want the muted key to keep reporting (one per %d ticks)", len(late), baseline.StillBreachingEvery)
	}
	for _, s := range late {
		if !s.StillBreaching || s.Breach != push.BreachOpLatency || !strings.Contains(s.Sample, "still breaching") {
			t.Errorf("late frame = %+v, want a still-breaching op-latency report", s)
		}
		// The wire's baseline is the key's median, as documented, not the
		// spike level the chronic key was judged against.
		if s.Baseline <= 0 || s.Baseline > 10 || s.Observed < 9000 {
			t.Errorf("late frame observed = %v baseline = %v, want ~10 s against the ~5 ms median", s.Observed, s.Baseline)
		}
	}
	// The service key and the operation key are both muted: same traffic.
	if n := count("baseline key muted"); n != 2 {
		t.Errorf("mute log lines = %d, want 2 (service and operation): %v", n, logs)
	}
	if muted(push.BreachOpLatency) != 1 || muted(push.BreachLatency) != 1 || muted(push.BreachOpErrors) != 0 {
		t.Errorf("muted keys metric: op-latency %v latency %v op-errors %v, want 1 1 0",
			muted(push.BreachOpLatency), muted(push.BreachLatency), muted(push.BreachOpErrors))
	}
	// A step above the usual spikes is a plain breach.
	op.add(0, 100, 0)
	op.add(6, 100, 0)
	before := h.sender.redCallCount()
	if h.tick() != before+1 || h.lastFrame().StillBreaching {
		t.Errorf("a slowdown of half the requests on a muted key: %+v, want a plain breach", h.sender.redServices())
	}
	// Calm again: the mute ends, and says so.
	for i := 0; i < 20; i++ {
		op.add(0, 200, 0)
		h.tick()
	}
	if n := count("baseline key no longer muted"); n != 2 {
		t.Errorf("unmute log lines = %d, want 2", n)
	}
	if muted(push.BreachOpLatency) != 0 || muted(push.BreachLatency) != 0 {
		t.Errorf("muted keys metric after calm: %v %v, want 0 0", muted(push.BreachOpLatency), muted(push.BreachLatency))
	}
}

// A p99 over a dozen requests is their maximum: such a reading is reported
// but does not feed chronic learning, so the key is not muted.
func TestBaselineArms_LowSampleSpikesAreNotLearnedAsChronic(t *testing.T) {
	op := newOpSim("/quote.Get")
	h := newBaseHarness(t, Options{}, op)
	h.quiet(8, func() { op.add(0, 20, 0) })
	lateSpikes, lateSent := 0, 0
	chronicTicks(h, op, 20, 60, func(i int, sent bool) {
		if i >= 30 && i%3 == 0 {
			lateSpikes++
			if sent && !h.lastFrame().StillBreaching {
				lateSent++
			}
		}
	})
	if lateSent != lateSpikes || h.c.latOp.Muted() != 0 {
		t.Errorf("late spikes reported = %d of %d, muted keys = %d: want every one reported and no mute", lateSent, lateSpikes, h.c.latOp.Muted())
	}
}

// A service with more operations than MaxOperations tracks the first ones one
// by one and judges the rest together, so its routes cannot grow the trackers
// without limit. A slot is freed once its operation has been absent for as
// long as the trackers keep an absent key.
func TestBaselineArms_MaxOperationsFoldsTheRest(t *testing.T) {
	var ops []*opSim
	for i := 0; i < 10; i++ {
		ops = append(ops, newOpSim(fmt.Sprintf("/item/%02d", i)))
	}
	h := newBaseHarness(t, Options{Baseline: BaselineOptions{MaxOperations: 3}}, ops...)
	h.quiet(8, func() {
		for _, o := range ops {
			o.add(0, 30, 0)
		}
	})
	if e, l := h.c.errOp.Len(), h.c.latOp.Len(); e != 4 || l != 4 {
		t.Fatalf("tracked operations: errOp %d latOp %d, want 4 and 4 (3 own keys and the overflow)", e, l)
	}
	if n := len(h.c.opSeen[servicePair{Namespace: "ns-a", Workload: "svc"}]); n != 3 {
		t.Fatalf("operation slots in use = %d, want 3", n)
	}
	// One of the folded operations fails outright: 30 of the overflow's 210.
	for _, o := range ops[:9] {
		o.add(0, 30, 0)
	}
	ops[9].add(0, 0, 30)
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1", h.sender.redCallCount())
	}
	if s := h.lastFrame(); s.Breach != push.BreachOpErrors || s.Operation != overflowOpLabel {
		t.Errorf("service = %+v, want op-errors on %q", s, overflowOpLabel)
	}
	// The three tracked operations go away; after the absence limit their
	// slots are free and three others get their own key.
	for _, o := range ops[:3] {
		o.hidden = true
	}
	for i := 0; i < h.c.errGate().AbsentRounds()+1; i++ {
		for _, o := range ops[3:] {
			o.add(0, 30, 0)
		}
		h.tick()
	}
	seen := h.c.opSeen[servicePair{Namespace: "ns-a", Workload: "svc"}]
	if len(seen) != 3 {
		t.Fatalf("operation slots in use after the turnover = %d, want 3", len(seen))
	}
	for key := range seen {
		if l := opLabelFromKey(key); l == "POST /item/00" || l == "POST /item/01" || l == "POST /item/02" {
			t.Errorf("slot still held by the absent operation %q", l)
		}
	}
}

// Operation is clipped on the wire.
func TestBaselineArms_OperationTruncatedOnTheWire(t *testing.T) {
	op := newOpSim("/" + strings.Repeat("a", 400))
	h := newBaseHarness(t, Options{}, op)
	op.add(0, 0, 25)
	if h.tick() != 1 {
		t.Fatalf("frames = %d, want 1", h.sender.redCallCount())
	}
	if s := h.lastFrame(); len(s.Operation) != push.MaxOperationLen || !strings.HasPrefix(s.Operation, "POST /aaa") {
		t.Errorf("operation is %d bytes (%q...), want %d", len(s.Operation), s.Operation[:12], push.MaxOperationLen)
	}
}

// A plain breach outranks the periodic report of a muted key.
func TestBaselineHit_StillBreachingRanksLast(t *testing.T) {
	still := &baselineHit{arm: push.BreachOpErrors, op: "a", observed: 90, still: true}
	fresh := &baselineHit{arm: push.BreachLatency, observed: 400, baseline: 100}
	if !fresh.beats(still) || still.beats(fresh) {
		t.Error("a new breach must beat a still-breaching report, whatever the arm")
	}
	if got := still.describe(); !strings.Contains(got, "(chronic, still breaching)") {
		t.Errorf("describe = %q", got)
	}
}

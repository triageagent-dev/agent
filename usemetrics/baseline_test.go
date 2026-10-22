package usemetrics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// cpuDriver feeds one service's cumulative counters window by window.
type cpuDriver struct {
	t      *testing.T
	src    *fakeSource
	c      *Collector
	sender *fakeSender
	usec   float64 // cumulative usage_usec
}

func newCPUDriver(t *testing.T) *cpuDriver {
	src := newFakeSource()
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	return &cpuDriver{t: t, src: src, c: c, sender: sender}
}

// window advances cumulative usage by cores*60s and runs one collection.
func (d *cpuDriver) window(cores float64) []push.ServiceUseMetrics {
	d.usec += cores * 60 * 1e6
	d.src.set(testTarget, setOf(counterFamily(MetricCPUUsageUsecTotal, point("ops", "ad", "ad-1", d.usec))))
	return d.run()
}

func (d *cpuDriver) run() []push.ServiceUseMetrics {
	before := len(d.sender.allServices())
	d.c.runOnce(context.Background())
	return d.sender.allServices()[before:]
}

func near(a, b float64) bool { return math.Abs(a-b) < 0.01 }

func TestBaseline_CPUBurnWithNoLimitBreaches(t *testing.T) {
	d := newCPUDriver(t)
	d.window(0.05) // first read: counter baseline only
	for i := 0; i < 8; i++ {
		if got := d.window(0.05); len(got) != 0 {
			t.Fatalf("window %d: unexpected frame %+v", i, got)
		}
	}
	got := d.window(1.5)
	if len(got) != 1 {
		t.Fatalf("frames = %+v, want one breach", got)
	}
	s := got[0]
	if s.Breach != push.BreachCPU || !near(s.Observed, 1.5) || !near(s.Baseline, 0.05) || s.EvidenceSec != 60 {
		t.Errorf("entry = %+v", s)
	}
	if s.Sample == "" {
		t.Error("sample empty")
	}
}

func TestBaseline_SteadyBusyServiceDoesNotBreach(t *testing.T) {
	d := newCPUDriver(t)
	for i := 0; i < 22; i++ {
		if got := d.window(2); len(got) != 0 {
			t.Fatalf("window %d: unexpected frame %+v", i, got)
		}
	}
}

func TestBaseline_SmallChangeUnderFloorDoesNotBreach(t *testing.T) {
	d := newCPUDriver(t)
	for i := 0; i < 9; i++ {
		d.window(0.01)
	}
	if got := d.window(0.1); len(got) != 0 {
		t.Fatalf("0.01 -> 0.1 cores breached: %+v", got)
	}
}

func TestBaseline_ColdServiceDoesNotBreach(t *testing.T) {
	d := newCPUDriver(t)
	d.window(0.05)
	d.window(0.05)
	if got := d.window(2); len(got) != 0 {
		t.Fatalf("cold service breached: %+v", got)
	}
}

func TestBaseline_CPUSumsAcrossPods(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	var usec float64
	run := func(perPod float64) {
		usec += perPod * 60 * 1e6
		src.set(testTarget, setOf(counterFamily(MetricCPUUsageUsecTotal,
			point("ops", "ad", "ad-1", usec), point("ops", "ad", "ad-2", usec), point("ops", "ad", "ad-3", usec))))
		c.runOnce(context.Background())
	}
	for i := 0; i < 8; i++ {
		run(0.05)
	}
	run(0.5) // 3 pods x 0.5 = 1.5 cores in total
	svcs := sender.allServices()
	if len(svcs) != 1 || !near(svcs[0].Observed, 1.5) {
		t.Fatalf("frames = %+v, want one with 1.5 cores", svcs)
	}
}

func TestBaseline_ThrottlingWithLimitBreaches(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	var periods, throttled float64
	run := func(pct float64) {
		periods += 600
		throttled += 600 * pct / 100
		src.set(testTarget, setOf(
			gaugeFamily(MetricCPULimitCores, point("ops", "api", "api-1", 0.5)),
			counterFamily(MetricCPUNrPeriodsTotal, point("ops", "api", "api-1", periods)),
			counterFamily(MetricCPUNrThrottledTotal, point("ops", "api", "api-1", throttled)),
		))
		c.runOnce(context.Background())
	}
	for i := 0; i < 9; i++ {
		run(1)
	}
	if n := len(sender.allServices()); n != 0 {
		t.Fatalf("frames before the step = %d", n)
	}
	run(40)
	svcs := sender.allServices()
	if len(svcs) != 1 {
		t.Fatalf("frames = %+v", svcs)
	}
	s := svcs[0]
	if s.Breach != push.BreachThrottle || !near(s.Observed, 40) || !near(s.Baseline, 1) {
		t.Errorf("entry = %+v", s)
	}
}

func TestBaseline_CounterResetDoesNotBreach(t *testing.T) {
	d := newCPUDriver(t)
	for i := 0; i < 9; i++ {
		d.window(0.05)
	}
	// Pod restarts: usage drops to a small value. The reset delta is the
	// current value (0.05 cores worth), not a negative or giant number.
	d.usec = 0
	if got := d.window(0.05); len(got) != 0 {
		t.Fatalf("counter reset breached: %+v", got)
	}
	if got := d.window(0.05); len(got) != 0 {
		t.Fatalf("after reset breached: %+v", got)
	}
}

func TestBaseline_DisabledSendsNothing(t *testing.T) {
	d := newCPUDriver(t)
	off := false
	d.c.opts.Baseline.Enabled = &off
	for i := 0; i < 9; i++ {
		d.window(0.05)
	}
	if got := d.window(3); len(got) != 0 {
		t.Fatalf("disabled arm sent %+v", got)
	}
}

func TestBaseline_CPUOnlyServiceSurvivesTopNCap(t *testing.T) {
	const n = 150
	src := newFakeSource()
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	var usec float64
	run := func(cores float64) {
		usec += cores * 60 * 1e6
		var pts, use []metricset.Point
		for i := 0; i < n; i++ {
			pts = append(pts, point("ops", fmt.Sprintf("svc-%03d", i), "p", float64(20+i)))
		}
		use = append(use, point("ops", "hot", "hot-1", usec))
		src.set(testTarget, setOf(gaugeFamily(MetricMemPsiAvg60, pts...), counterFamily(MetricCPUUsageUsecTotal, use...)))
		c.runOnce(context.Background())
	}
	for i := 0; i < 8; i++ {
		run(0.05)
	}
	before := len(sender.allServices())
	run(1.5)
	got := sender.allServices()[before:]
	if len(got) != push.MaxUseMetricsServices {
		t.Fatalf("sent %d, want cap %d", len(got), push.MaxUseMetricsServices)
	}
	found := false
	for _, s := range got {
		if s.Service == "hot" && s.Breach == push.BreachCPU {
			found = true
		}
	}
	if !found {
		t.Fatal("cpu-only breach was cut by the cap")
	}
}

// A sensor that delivers nothing for a few reads and then catches up used to
// inflate the reading: the usage of several intervals was divided by one, so a
// steady 0.20-core service read as 0.80 cores and breached. The delta is
// divided by the real time between the two sets, and a read with no fresh set
// is skipped.
func TestBaseline_SensorGapDoesNotInflateCPU(t *testing.T) {
	for _, gap := range []int{1, 2, 3} {
		d := newCPUDriver(t)
		d.window(0.2)
		for i := 0; i < 10; i++ {
			if got := d.window(0.2); len(got) != 0 {
				t.Fatalf("gap %d: unexpected frame while steady: %+v", gap, got)
			}
		}
		for i := 0; i < gap; i++ {
			if got := d.run(); len(got) != 0 { // the same set read again
				t.Fatalf("gap %d: stale read %d sent %+v", gap, i, got)
			}
		}
		// The usage of the gap arrives with the next set, which is that much later.
		d.usec += 0.2 * 60 * 1e6 * float64(gap)
		d.src.advance(testTarget, time.Duration(gap)*time.Minute)
		if got := d.window(0.2); len(got) != 0 {
			t.Fatalf("gap %d: catch-up read sent %+v, want nothing (true usage is still 0.20 cores)", gap, got)
		}
		// Neither the stale reads (0 cores) nor an inflated catch-up reading
		// entered the baseline: a real 3x step still breaches against 0.20.
		got := d.window(0.7)
		if len(got) != 1 || got[0].Breach != push.BreachCPU || !near(got[0].Observed, 0.7) || !near(got[0].Baseline, 0.2) {
			t.Fatalf("gap %d: step after the gap = %+v, want a cpu breach 0.70 vs 0.20", gap, got)
		}
	}
}

// The same when one pod is missing from its sensor's set for a few reads while
// the sensor keeps delivering: its counters are kept, and its usage on return
// spans the whole absence.
func TestBaseline_PodAbsentFromSetDoesNotInflateCPU(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(t, src, Options{MemPsiThreshold: 20, CPUPsiThreshold: 40, IOPsiThreshold: 40})
	var usec float64
	run := func(present bool) []push.ServiceUseMetrics {
		usec += 0.2 * 60 * 1e6
		pts := []metricset.Point{point("ops", "other", "other-1", usec)}
		if present {
			pts = append(pts, point("ops", "ad", "ad-1", usec))
		}
		src.set(testTarget, setOf(counterFamily(MetricCPUUsageUsecTotal, pts...)))
		before := len(sender.allServices())
		c.runOnce(context.Background())
		return sender.allServices()[before:]
	}
	for i := 0; i < 10; i++ {
		run(true)
	}
	for i := 0; i < 3; i++ {
		run(false)
	}
	if got := run(true); len(got) != 0 {
		t.Fatalf("pod back after 3 reads sent %+v, want nothing (0.20 cores over 4 minutes)", got)
	}
}

// plainSource hides the fake's SetAt: a metricset.Source that is not timed.
type plainSource struct{ f *fakeSource }

func (p plainSource) Targets(role string) []string { return p.f.Targets(role) }
func (p plainSource) Set(role, target string) (metricset.Set, error) {
	return p.f.Set(role, target)
}

// A source that does not say when a set was received keeps the collector's
// interval as the divisor.
func TestBaseline_UntimedSourceUsesTheInterval(t *testing.T) {
	d := newCPUDriver(t)
	d.c.opts.Source = plainSource{d.src}
	d.window(0.05)
	for i := 0; i < 8; i++ {
		d.window(0.05)
	}
	got := d.window(1.5)
	if len(got) != 1 || !near(got[0].Observed, 1.5) {
		t.Fatalf("frames = %+v, want a cpu breach at 1.5 cores", got)
	}
}

// With a cold floor set, an agent that starts while a service burns CPU
// reports it every window and does not learn it; by default (no cold floor) a
// cold service stays silent, as TestBaseline_ColdServiceDoesNotBreach pins.
func TestBaseline_ColdCPUFloorReportsAndDoesNotLearn(t *testing.T) {
	d := newCPUDriver(t)
	d.c.opts.Baseline.CPUColdFloorCores = 1
	d.window(2) // first read: counter baseline only
	for i := 0; i < 10; i++ {
		got := d.window(2)
		if len(got) != 1 || got[0].Breach != push.BreachCPU || !near(got[0].Observed, 2) || got[0].Baseline != 0 {
			t.Fatalf("burn window %d = %+v, want a cold cpu breach at 2 cores", i, got)
		}
	}
	for i := 0; i < 6; i++ {
		if got := d.window(0.05); len(got) != 0 {
			t.Fatalf("calm window %d sent %+v", i, got)
		}
	}
	got := d.window(2)
	if len(got) != 1 || !near(got[0].Baseline, 0.05) {
		t.Fatalf("burn after calm = %+v, want a breach against the calm 0.05 baseline", got)
	}
	// Under the cold floor a cold service is silent and learned.
	d = newCPUDriver(t)
	d.c.opts.Baseline.CPUColdFloorCores = 1
	for i := 0; i < 8; i++ {
		if got := d.window(0.6); len(got) != 0 {
			t.Fatalf("0.6 cores under a 1-core cold floor sent %+v", got)
		}
	}
}

// A service whose CPU spikes every third window is muted, not silently: the
// mute is logged and counted, and it reports again every
// StillBreachingEvery windows with StillBreaching set.
func TestBaseline_ChronicCPUMuteIsLoggedCountedAndBounded(t *testing.T) {
	d := newCPUDriver(t)
	var logs []string
	d.c.log = funcr.New(func(_, args string) { logs = append(logs, args) }, funcr.Options{})
	for i := 0; i < 8; i++ {
		d.window(0.05)
	}
	var late []push.ServiceUseMetrics
	for i := 0; i < 90; i++ {
		cores := 0.05
		if i%3 == 0 {
			cores = 1.5
		}
		if got := d.window(cores); i >= 30 {
			late = append(late, got...)
		}
	}
	if len(late) < 4 || len(late) > 6 {
		t.Fatalf("late frames = %d, want one per %d windows over 60", len(late), baseline.StillBreachingEvery)
	}
	for _, s := range late {
		if !s.StillBreaching || s.Breach != push.BreachCPU || !near(s.Baseline, 0.05) {
			t.Errorf("late frame = %+v, want a still-breaching cpu report against the 0.05 median", s)
		}
	}
	n := 0
	for _, l := range logs {
		if strings.Contains(l, "baseline key muted") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("mute log lines = %d, want 1: %v", n, logs)
	}
	if got := agentmetrics.MutedKeys(agentmetrics.SensorUSE, push.BreachCPU); got != 1 {
		t.Errorf("muted keys metric = %v, want 1", got)
	}
}

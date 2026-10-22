package baseline

import (
	"math"
	"testing"
)

// errGate is the RED error-rate arm: percent, 20 requests, 3 errors, 5% floor.
var errGate = Gate{Windows: 5, WindowSec: 60, MinEvidence: 20, MinNum: 3, Floor: 5, Value: Percent}

// window runs one full collector window for a single scalar key.
func window(t *Tracker, key string, g Gate, num, den float64) Verdict {
	t.Observe(key, num, den)
	v := t.Judge(key, g)
	t.Settle(key, v)
	t.GC(g)
	t.Tick()
	return v
}

func histLen(t *Tracker, key string) int {
	if ks := t.keys[key]; ks != nil {
		return len(ks.hist)
	}
	return 0
}

// warm feeds n healthy busy windows (1 error in 100) so the key has a baseline.
func warm(t *Tracker, key string, g Gate, n int) {
	for i := 0; i < n; i++ {
		window(t, key, g, 1, 100)
	}
}

func TestBusyKeyDecidesOnOneWindow(t *testing.T) {
	tr := New()
	warm(tr, "a", errGate, 6)
	v := window(tr, "a", errGate, 50, 100)
	if !v.Ready || !v.Breach || v.Windows != 1 || v.EvidenceSec != 60 {
		t.Fatalf("verdict = %+v, want ready breach on 1 window / 60s", v)
	}
	if v.Observed != 50 || v.Baseline != 1 {
		t.Errorf("observed/baseline = %v/%v, want 50/1", v.Observed, v.Baseline)
	}
}

func TestQuietKeyAccumulatesAndBreachesAtWindowK(t *testing.T) {
	g := errGate
	g.ColdBreach = true
	tr := New()
	// 1 error per 5 requests: den reaches 20 at window 4, errors 4 >= 3.
	var got []Verdict
	for i := 0; i < 5; i++ {
		got = append(got, window(tr, "op", g, 1, 5))
	}
	for i := 0; i < 3; i++ {
		if got[i].Ready || got[i].Breach {
			t.Errorf("window %d: %+v, want not ready", i+1, got[i])
		}
	}
	if !got[3].Ready || !got[3].Breach || got[3].Windows != 4 || got[3].EvidenceSec != 240 || got[3].Observed != 20 {
		t.Errorf("window 4: %+v, want breach over 4 windows, 20%%", got[3])
	}
	if !got[4].Breach || got[4].Windows != 4 {
		t.Errorf("window 5: %+v, want breach still over 4 windows (stops at MinEvidence)", got[4])
	}
}

func TestQuietKeyWarmBreachesWhenFaultAccumulates(t *testing.T) {
	tr := New()
	for i := 0; i < 8; i++ { // ready from window 4, so 5 readings by window 8
		window(tr, "op", errGate, 0, 5)
	}
	var breachAt int
	for i := 1; i <= 4; i++ {
		if v := window(tr, "op", errGate, 1, 5); v.Breach && breachAt == 0 {
			breachAt = i
		}
	}
	// errors 1,2,3 over the newest 4 windows: MinNum 3 met at the third fault window.
	if breachAt != 3 {
		t.Errorf("breach at fault window %d, want 3", breachAt)
	}
}

func TestColdKey(t *testing.T) {
	for _, cold := range []bool{false, true} {
		g := errGate
		g.ColdBreach = cold
		tr := New()
		v := window(tr, "a", g, 50, 100)
		if !v.Ready || !v.Cold || v.Baseline != 0 || v.Breach != cold {
			t.Errorf("ColdBreach=%v: %+v", cold, v)
		}
		if histLen(tr, "a") != 0 && cold {
			t.Errorf("a cold breach was absorbed")
		}
	}
}

func TestSustainedFaultNeverAbsorbed(t *testing.T) {
	tr := New()
	warm(tr, "a", errGate, 6)
	before := histLen(tr, "a")
	for i := 0; i < 20; i++ {
		v := window(tr, "a", errGate, 50, 100)
		if !v.Breach || v.Baseline != 1 {
			t.Fatalf("fault window %d: %+v, want breach vs baseline 1", i+1, v)
		}
	}
	if histLen(tr, "a") != before {
		t.Errorf("baseline grew from %d to %d during the fault", before, histLen(tr, "a"))
	}
}

func TestRecoveryResumesAbsorbing(t *testing.T) {
	tr := New()
	warm(tr, "a", errGate, 6)
	for i := 0; i < 5; i++ {
		window(tr, "a", errGate, 50, 100)
	}
	before := histLen(tr, "a")
	v := window(tr, "a", errGate, 1, 100)
	if v.Breach {
		t.Fatalf("recovered window breached: %+v", v)
	}
	if histLen(tr, "a") != before+1 {
		t.Errorf("baseline len = %d, want %d", histLen(tr, "a"), before+1)
	}
}

func TestMedianZeroUsesFloor(t *testing.T) {
	tr := New()
	for i := 0; i < 6; i++ {
		window(tr, "a", errGate, 0, 100)
	}
	// 4% is under the floor, 6% (6 errors) is over it; baseline median is 0.
	if v := window(tr, "a", errGate, 4, 100); v.Breach || v.Baseline != 0 || v.Cold {
		t.Errorf("4%%: %+v, want no breach", v)
	}
	if v := window(tr, "a", errGate, 6, 100); !v.Breach || v.Baseline != 0 {
		t.Errorf("6%%: %+v, want breach on the floor", v)
	}
}

func TestMinNum(t *testing.T) {
	tr := New()
	warm(tr, "a", errGate, 6)
	// 2 errors in 20 requests = 10%: over floor and 3x, but under MinNum 3.
	if v := window(tr, "a", errGate, 2, 20); v.Breach || !v.Ready {
		t.Errorf("2 errors: %+v, want ready, no breach", v)
	}
	if v := window(tr, "a", errGate, 3, 20); !v.Breach {
		t.Errorf("3 errors: %+v, want breach", v)
	}
}

func TestMultiplierAndFloor(t *testing.T) {
	tests := []struct {
		name   string
		obs    float64 // errors per 100
		breach bool
	}{
		{"under floor", 4, false},
		{"over floor, under 3x baseline", 10, false}, // baseline 4
		{"over floor and 3x", 12, true},
		{"at 3x exactly", 12.0, true},
	}
	g := errGate
	g.MinNum = 0
	for _, tc := range tests {
		tr := New()
		for i := 0; i < 6; i++ {
			window(tr, "a", g, 4, 100)
		}
		if v := window(tr, "a", g, tc.obs, 100); v.Breach != tc.breach {
			t.Errorf("%s: %+v, want breach=%v", tc.name, v, tc.breach)
		}
	}
}

func TestKeepCapsBaseline(t *testing.T) {
	g := errGate
	g.Keep = 8
	tr := New()
	warm(tr, "a", g, 20)
	if n := histLen(tr, "a"); n != 8 {
		t.Errorf("baseline len = %d, want 8", n)
	}
}

func TestGCKeepsAbsentKeyThenDrops(t *testing.T) {
	g := errGate // Windows 5: dropped after 20 absent rounds
	tr := New()
	warm(tr, "op", g, 6)
	for i := 0; i < 18; i++ {
		tr.GC(g)
		tr.Tick()
	}
	tr.GC(g) // 19th absent round
	if tr.keys["op"] == nil || histLen(tr, "op") != 6 {
		t.Fatalf("key lost after 19 absent rounds")
	}
	// It reappears: history intact, stale windows not counted as evidence.
	tr.Observe("op", 50, 100)
	v := tr.Judge("op", g)
	if !v.Breach || v.Windows != 1 || v.Baseline != 1 {
		t.Errorf("after absence: %+v, want breach vs kept baseline on 1 window", v)
	}
	tr.Tick()
	for i := 0; i < 20; i++ {
		tr.GC(g)
		tr.Tick()
	}
	if tr.Len() != 0 {
		t.Errorf("key still tracked after 20 absent rounds")
	}
}

func TestGCDropsAtExactlyWindowsTimesFour(t *testing.T) {
	g := Gate{Windows: 2, WindowSec: 60, MinEvidence: 1}
	tr := New()
	window(tr, "k", g, 1, 1) // seen in round 0; round is now 1
	for absent := 1; absent <= 8; absent++ {
		tr.GC(g)
		want := absent < 8
		if (tr.Len() == 1) != want {
			t.Fatalf("absent %d rounds: tracked=%v, want %v", absent, tr.Len() == 1, want)
		}
		tr.Tick()
	}
}

func TestDoubleSettleIsNoop(t *testing.T) {
	tr := New()
	g := errGate
	tr.Observe("a", 1, 100)
	v := tr.Judge("a", g)
	tr.Settle("a", v)
	tr.Settle("a", v)
	if n := histLen(tr, "a"); n != 1 {
		t.Fatalf("baseline len = %d after double settle, want 1", n)
	}
	tr.Tick()
	tr.Observe("a", 1, 100)
	tr.Settle("a", tr.Judge("a", g))
	if n := histLen(tr, "a"); n != 2 {
		t.Errorf("baseline len = %d after next window, want 2", n)
	}
	tr.Settle("nope", v) // unknown key: no panic
}

func TestSameWindowObserveMerges(t *testing.T) {
	tr := New()
	tr.Observe("a", 1, 10)
	tr.Observe("a", 2, 15)
	v := tr.Judge("a", Gate{WindowSec: 60, MinEvidence: 20, Value: Ratio})
	if !v.Ready || v.Windows != 1 || v.Observed != 3.0/25 {
		t.Errorf("verdict = %+v, want one merged window 3/25", v)
	}
}

func TestNotReadyAndUnknownKey(t *testing.T) {
	tr := New()
	if v := tr.Judge("x", errGate); v.Ready || v.Breach {
		t.Errorf("unknown key: %+v", v)
	}
	tr.Observe("x", 5, 5)
	if v := tr.Judge("x", errGate); v.Ready || v.Breach || v.Windows != 1 {
		t.Errorf("thin key: %+v", v)
	}
	tr.Settle("x", tr.Judge("x", errGate))
	if histLen(tr, "x") != 0 {
		t.Error("a not-ready reading entered the baseline")
	}
}

func TestValueHelpers(t *testing.T) {
	if Ratio(1, 4) != 0.25 || Percent(1, 4) != 25 || Rate(3, 2) != 1.5 {
		t.Error("helper arithmetic wrong")
	}
	if Ratio(1, 0) != 0 || Percent(1, 0) != 0 || Rate(1, 0) != 0 {
		t.Error("zero denominator must give 0")
	}
}

func TestWithDefaults(t *testing.T) {
	g := Gate{}.withDefaults()
	if g.Windows != 5 || g.Keep != 30 || g.MinBaseline != 5 || g.Multiplier != 3 || g.Value == nil || g.Scale != 1 {
		t.Errorf("defaults = %+v", g)
	}
	if g := (Gate{Windows: 99}).withDefaults(); g.Windows != MaxWindows {
		t.Errorf("Windows not capped: %d", g.Windows)
	}
}

func TestQuantile(t *testing.T) {
	inf := math.Inf(1)
	t.Run("interpolates within bucket", func(t *testing.T) {
		got, ok := Quantile(Buckets{0.1: 90, 1.0: 100, inf: 100}, 0.99)
		want := 0.1 + (99.0-90.0)/(100.0-90.0)*(1.0-0.1)
		if !ok || math.Abs(got-want) > 1e-9 {
			t.Errorf("got %v ok=%v, want %v", got, ok, want)
		}
	})
	t.Run("inf falls back to largest finite", func(t *testing.T) {
		got, ok := Quantile(Buckets{0.1: 1, inf: 100}, 0.99)
		if !ok || got != 0.1 {
			t.Errorf("got %v ok=%v, want 0.1", got, ok)
		}
	})
	t.Run("no data", func(t *testing.T) {
		if _, ok := Quantile(nil, 0.99); ok {
			t.Error("nil: ok")
		}
		if _, ok := Quantile(Buckets{inf: 0}, 0.99); ok {
			t.Error("zero total: ok")
		}
		if _, ok := Quantile(Buckets{inf: 5}, 0.99); ok {
			t.Error("only +Inf: ok")
		}
	})
	t.Run("median", func(t *testing.T) {
		got, _ := Quantile(Buckets{1: 50, 2: 100, inf: 100}, 0.5)
		if got != 1 {
			t.Errorf("got %v, want 1", got)
		}
	})
}

var latGate = Gate{Windows: 5, WindowSec: 60, MinEvidence: 10, Floor: 300, Scale: 1000}

// TestJudgeQuantileLowVolumeOperation is the ShipOrder case: one slow operation
// is a sliver of a busy service. The per-operation key breaches; the
// service-wide key over the same traffic does not.
func TestJudgeQuantileLowVolumeOperation(t *testing.T) {
	inf := math.Inf(1)
	tr := New()
	const svc, op = "shop/shipping", "shop/shipping\x00ShipOrder"
	run := func(slow bool) (sv, ov Verdict) {
		// GetQuote: 100 fast requests. ShipOrder: 4 requests, 1 slow (10s) when slow.
		opB, svcB := Buckets{0.05: 4, 10: 4, inf: 4}, Buckets{0.05: 104, 10: 104, inf: 104}
		if slow {
			opB = Buckets{0.05: 3, 10: 4, inf: 4}
			svcB = Buckets{0.05: 103, 10: 104, inf: 104}
		}
		tr.ObserveBuckets(op, opB, 4)
		tr.ObserveBuckets(svc, svcB, 104)
		sv, ov = tr.JudgeQuantile(svc, latGate, 0.99), tr.JudgeQuantile(op, latGate, 0.99)
		tr.Settle(svc, sv)
		tr.Settle(op, ov)
		tr.GC(latGate)
		tr.Tick()
		return sv, ov
	}
	for i := 0; i < 10; i++ {
		sv, ov := run(false)
		if sv.Breach || ov.Breach {
			t.Fatalf("healthy window %d breached: %+v %+v", i, sv, ov)
		}
	}
	sv, ov := run(true)
	if sv.Breach {
		t.Errorf("service-wide key breached: %+v", sv)
	}
	if !ov.Breach || ov.Observed < 5000 || ov.Baseline <= 0 || ov.Baseline > 60 {
		t.Errorf("op key: %+v, want breach near 9s vs ~50ms baseline", ov)
	}
	if ov.Windows != 3 || ov.EvidenceSec != 180 {
		t.Errorf("op evidence = %d windows / %ds, want 3 / 180 (4 requests per window, need 10)", ov.Windows, ov.EvidenceSec)
	}
}

func TestJudgeQuantileNotReady(t *testing.T) {
	tr := New()
	tr.ObserveBuckets("a", Buckets{0.05: 3, math.Inf(1): 3}, 3)
	if v := tr.JudgeQuantile("a", latGate, 0.99); v.Ready || v.Breach {
		t.Errorf("thin: %+v", v)
	}
	tr.Tick()
	// Enough requests but no bucket data: nothing to compute.
	tr.ObserveBuckets("b", nil, 50)
	if v := tr.JudgeQuantile("b", latGate, 0.99); v.Ready {
		t.Errorf("no buckets: %+v", v)
	}
}

// chronicGate judges a scalar reading (p99 ms) on one window.
var chronicGate = Gate{Windows: 1, WindowSec: 60, MinEvidence: 1, MinBaseline: 5, Multiplier: 3, Floor: 300}

// scalar runs one window of a scalar reading and returns the verdict and the
// mute transition Settle reported.
func scalar(tr *Tracker, key string, g Gate, v float64) (Verdict, Transition) {
	tr.Observe(key, v, 1)
	vd := tr.Judge(key, g)
	x := tr.Settle(key, vd)
	tr.GC(g)
	tr.Tick()
	return vd, x
}

// TestChronicMuteIsExplicitAndBounded: a p99 under 5 ms most windows with a
// 2 s spike every third window is muted once the spikes are recurrent. The
// mute is explicit (one MuteStarted, Tracker.Muted, Verdict.Muted) and bounded:
// the key reports again every StillBreachingEvery windows, marked
// StillBreaching. A step well above the spikes still breaches, and the mute
// ends once the key calms down.
func TestChronicMuteIsExplicitAndBounded(t *testing.T) {
	tr := New()
	const key = "ns/quote"
	for range 6 {
		scalar(tr, key, chronicGate, 5)
	}
	started, plainLate, still := 0, 0, 0
	lastBreach := -1
	for i := range 90 {
		v := 5.0
		if i%3 == 0 {
			v = 2000
		}
		vd, x := scalar(tr, key, chronicGate, v)
		if x == MuteStarted {
			started++
		}
		if x == MuteEnded {
			t.Fatalf("window %d: mute ended while the spikes continue", i)
		}
		if i < 21 || v != 2000 {
			if vd.Breach {
				lastBreach = i
			}
			continue
		}
		// Baseline stays the key's median (it is what the wire calls the
		// baseline); the p90 it was judged against is Reference.
		if vd.Baseline != 5 || vd.Reference != 2000 {
			t.Errorf("window %d: baseline = %v reference = %v, want the median 5 and the spike level 2000", i, vd.Baseline, vd.Reference)
		}
		switch {
		case vd.StillBreaching && vd.Breach && vd.Muted:
			still++
			if gap := i - lastBreach; gap < StillBreachingEvery {
				t.Errorf("window %d: still-breaching report %d windows after the previous one, want at least %d", i, gap, StillBreachingEvery)
			}
		case vd.Breach:
			plainLate++
		case !vd.Muted:
			t.Errorf("window %d: a suppressed spike is not marked Muted: %+v", i, vd)
		}
		if vd.Breach {
			lastBreach = i
		}
	}
	if started != 1 || tr.Muted() != 1 {
		t.Fatalf("MuteStarted transitions = %d, Muted() = %d, want 1 and 1", started, tr.Muted())
	}
	if plainLate != 0 {
		t.Errorf("chronic spikes still breach every time late in the run: %d", plainLate)
	}
	// 69 late windows hold 23 spikes; one report per 12 windows is 5.
	if still < 4 {
		t.Errorf("still-breaching reports = %d, want the muted key to keep reporting every %d windows", still, StillBreachingEvery)
	}
	if vd, _ := scalar(tr, key, chronicGate, 20000); !vd.Breach || vd.StillBreaching || vd.Muted || vd.Baseline != 5 {
		t.Fatalf("step above the chronic spikes: %+v, want a plain breach against the median 5", vd)
	}
	if vd, _ := scalar(New(), "calm", chronicGate, 5); vd.Reference != 0 {
		t.Errorf("a key that is not chronic has reference %v, want 0", vd.Reference)
	}
	ended := 0
	for range chronicWindows {
		if _, x := scalar(tr, key, chronicGate, 5); x == MuteEnded {
			ended++
		}
	}
	if ended != 1 || tr.Muted() != 0 {
		t.Errorf("MuteEnded transitions = %d, Muted() = %d after %d calm windows, want 1 and 0", ended, tr.Muted(), chronicWindows)
	}
}

// A muted key that is garbage-collected no longer counts as muted.
func TestGCDropsMutedCount(t *testing.T) {
	tr := New()
	for range 6 {
		scalar(tr, "k", chronicGate, 5)
	}
	for i := range 30 {
		v := 5.0
		if i%3 == 0 {
			v = 2000
		}
		scalar(tr, "k", chronicGate, v)
	}
	if tr.Muted() != 1 {
		t.Fatalf("Muted() = %d, want 1", tr.Muted())
	}
	for range chronicGate.Windows * absentRoundsFactor {
		tr.GC(chronicGate)
		tr.Tick()
	}
	if tr.Len() != 0 || tr.Muted() != 0 {
		t.Errorf("after GC: Len = %d Muted = %d, want 0 and 0", tr.Len(), tr.Muted())
	}
}

// A healthy key that then fails every third window (a crash loop) used to fire
// six times and go silent for good. It is muted, but keeps reporting.
func TestIntermittentFaultKeepsReporting(t *testing.T) {
	tr := New()
	const key = "ns/svc"
	for range 30 {
		window(tr, key, errGate, 0, 100)
	}
	fired, lastFired, maxGap := 0, 0, 0
	for i := range 120 {
		num := 0.0
		if i%3 == 0 {
			num = 30
		}
		if v := window(tr, key, errGate, num, 100); v.Breach {
			fired++
			if gap := i - lastFired; gap > maxGap {
				maxGap = gap
			}
			lastFired = i
		}
	}
	// A fault window comes every 3rd window, so the first one at or past the
	// interval is at most 2 windows late.
	if limit := StillBreachingEvery + 2; maxGap > limit || 119-lastFired > limit {
		t.Errorf("fired %d times, longest silence %d windows, last report at window %d: want a report at least every %d windows",
			fired, maxGap, lastFired, limit)
	}
	if tr.Muted() != 1 {
		t.Errorf("Muted() = %d, want the key counted as muted", tr.Muted())
	}
}

// A sustained fault at the spike level on an already chronic key was silent for
// its first 15 windows. It is reported within the still-breaching interval.
func TestSustainedFaultOnChronicKeyReportsWithinInterval(t *testing.T) {
	tr := New()
	const key = "ns/quote"
	for range 6 {
		scalar(tr, key, chronicGate, 5)
	}
	for i := range 45 {
		v := 5.0
		if i%3 == 0 {
			v = 2000
		}
		scalar(tr, key, chronicGate, v)
	}
	first := -1
	for i := range 20 {
		if vd, _ := scalar(tr, key, chronicGate, 2000); vd.Breach && first < 0 {
			first = i
		}
	}
	if first < 0 || first >= StillBreachingEvery {
		t.Errorf("first report of the sustained fault at window %d, want within %d windows", first, StillBreachingEvery)
	}
}

// A breaching reading with too few samples (a p99 over ten requests is their
// maximum) is reported but never teaches the tracker that the key is chronic.
func TestChronicMinEvidence(t *testing.T) {
	inf := math.Inf(1)
	run := func(requests int64) (late int, muted int) {
		tr := New()
		g := latGate
		g.Windows = 1
		g.ChronicMinEvidence = 100
		const key = "ns/quote"
		win := func(slow bool) Verdict {
			b := Buckets{0.005: requests, 2.5: requests, inf: requests}
			if slow {
				b = Buckets{0.005: requests - requests/10, 2.5: requests, inf: requests}
			}
			tr.ObserveBuckets(key, b, requests)
			v := tr.JudgeQuantile(key, g, 0.99)
			tr.Settle(key, v)
			tr.GC(g)
			tr.Tick()
			return v
		}
		for range 6 {
			win(false)
		}
		for i := range 30 {
			if v := win(i%3 == 0); v.Breach && !v.StillBreaching && i >= 21 {
				late++
			}
		}
		return late, tr.Muted()
	}
	if late, muted := run(10); late != 3 || muted != 0 {
		t.Errorf("10 requests per window: late breaches = %d muted = %d, want every spike reported (3) and no mute", late, muted)
	}
	if late, muted := run(200); late != 0 || muted != 1 {
		t.Errorf("200 requests per window: late breaches = %d muted = %d, want the chronic key muted", late, muted)
	}
}

// An agent that starts during a latency outage has no baseline: with a cold
// floor the outage is reported every window and never becomes the baseline.
func TestColdLatencyOutageReportedNotLearned(t *testing.T) {
	tr := New()
	g := latGate
	g.ColdFloor = 5000
	const key = "ns/svc"
	win := func(b Buckets) Verdict {
		tr.ObserveBuckets(key, b, 100)
		v := tr.JudgeQuantile(key, g, 0.99)
		tr.Settle(key, v)
		tr.GC(g)
		tr.Tick()
		return v
	}
	slow, fast := Buckets{0.005: 0, 1: 0, 10: 100}, Buckets{0.005: 100, 1: 100, 10: 100}
	for i := range 30 {
		if v := win(slow); !v.Breach || !v.Cold || v.Observed < 9000 {
			t.Fatalf("outage window %d: %+v, want a cold breach near 10 s", i, v)
		}
	}
	if n := histLen(tr, key); n != 0 {
		t.Fatalf("baseline holds %d outage readings, want 0", n)
	}
	// Recovery: healthy readings form the baseline, and the outage level then
	// breaches against it.
	for i := range 6 {
		if v := win(fast); v.Breach {
			t.Fatalf("healthy window %d breached: %+v", i, v)
		}
	}
	if v := win(slow); !v.Breach || v.Cold || v.Baseline > 10 {
		t.Errorf("outage after recovery: %+v, want a warm breach against the healthy baseline", v)
	}
}

// The same for an error outage under the previous 20% cold floor.
func TestColdErrorOutageReportedNotLearned(t *testing.T) {
	tr := New()
	g := errGate
	g.ColdFloor = 15
	const key = "ns/svc"
	for i := range 30 {
		if v := window(tr, key, g, 15, 100); !v.Breach || !v.Cold {
			t.Fatalf("outage window %d: %+v, want a cold breach", i, v)
		}
	}
	if n := histLen(tr, key); n != 0 {
		t.Errorf("baseline holds %d outage readings, want 0", n)
	}
}

// A cold key under its cold floor does not breach, and its readings are
// learned as usual: ordinary healthy readings must not fire.
func TestColdKeyUnderColdFloorIsLearned(t *testing.T) {
	tr := New()
	g := errGate
	g.ColdFloor = 15
	for i := range 6 {
		if v := window(tr, "a", g, 10, 100); v.Breach {
			t.Fatalf("window %d: a 10%% reading breached a 15%% cold floor: %+v", i, v)
		}
	}
	if n := histLen(tr, "a"); n != 6 {
		t.Errorf("baseline len = %d, want 6", n)
	}
	// ColdBreach is ignored once ColdFloor is set.
	g.ColdBreach = true
	if v := window(New(), "b", g, 10, 100); v.Breach {
		t.Errorf("ColdBreach overrode ColdFloor: %+v", v)
	}
}

// The low-volume rule: a key that never reaches MinEvidence still breaches on
// a high failure share with a few errors, and nothing is learned from it.
func TestSparseRule(t *testing.T) {
	g := errGate
	g.SparseMinNum, g.SparseFloor = 5, 50
	tr := New()
	// 3 requests a window, all failing: 15 over the 5-window lookback, never 20.
	if v := window(tr, "op", g, 3, 3); v.Breach || v.Ready {
		t.Errorf("window 1 (3 errors): %+v, want no breach under SparseMinNum", v)
	}
	for i := 2; i <= 10; i++ {
		v := window(tr, "op", g, 3, 3)
		if !v.Breach || !v.Sparse || v.Ready || v.Observed != 100 {
			t.Fatalf("window %d: %+v, want a sparse breach at 100%%", i, v)
		}
	}
	if v := window(tr, "op", g, 3, 3); v.Windows != 5 || v.EvidenceSec != 300 {
		t.Errorf("evidence = %d windows / %ds, want the whole lookback, 5 / 300", v.Windows, v.EvidenceSec)
	}
	if histLen(tr, "op") != 0 {
		t.Error("a sparse reading entered the baseline")
	}
	// Healthy, and a low share with enough errors: no breach.
	ok, low := New(), New()
	for i := range 6 {
		if v := window(ok, "ok", g, 0, 3); v.Breach {
			t.Fatalf("healthy sparse window %d breached: %+v", i, v)
		}
		// 1 error in 3 requests: 5 errors over the lookback, but a 33% share.
		if v := window(low, "low", g, 1, 3); v.Breach {
			t.Fatalf("33%% share window %d breached: %+v", i, v)
		}
	}
	// Off by default.
	tr = New()
	for range 6 {
		if v := window(tr, "op", errGate, 3, 3); v.Breach {
			t.Fatalf("sparse rule fired with SparseMinNum unset: %+v", v)
		}
	}
}

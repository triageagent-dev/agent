package logmetrics

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/push"
)

// feed runs one window per entry of lines (error lines in that window) for
// ops/api and returns, per window, the reported entry or nil.
func feed(src *fakeSource, c *Collector, sender *fakeSender, cum *float64, lines ...float64) []*push.ServiceLogMetrics {
	var out []*push.ServiceLogMetrics
	for _, n := range lines {
		*cum += n
		src.set("sensor/node-a", errorSet("ops", "api", *cum))
		before := len(sender.allServices())
		c.runOnce(context.Background())
		all := sender.allServices()
		if len(all) > before {
			s := all[len(all)-1]
			out = append(out, &s)
		} else {
			out = append(out, nil)
		}
	}
	return out
}

func repeat(n float64, times int) []float64 {
	out := make([]float64, times)
	for i := range out {
		out[i] = n
	}
	return out
}

func TestBaseline_ChronicStreamMutedOnceWarm(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var cum float64
	got := feed(src, c, sender, &cum, repeat(15, 30)...)
	for i := 0; i < 5; i++ {
		if got[i] == nil {
			t.Fatalf("cold window %d not reported (presence gate)", i)
		}
		if got[i].Breach != "" {
			t.Errorf("cold window %d Breach = %q, want empty", i, got[i].Breach)
		}
	}
	for i := 5; i < 30; i++ {
		if got[i] != nil {
			t.Fatalf("window %d reported, want muted once the baseline formed", i)
		}
	}
}

func TestBaseline_StepOnChronicSustainedThenRecovery(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var cum float64
	feed(src, c, sender, &cum, repeat(15, 10)...)
	got := feed(src, c, sender, &cum, repeat(80, 4)...)
	for i, g := range got {
		if g == nil {
			t.Fatalf("step window %d not reported (breach must not be absorbed)", i)
		}
		if g.Breach != push.BreachLogStep || g.Stream != push.LogStreamError || g.Observed != 80 || g.Baseline != 15 || g.EvidenceSec != 60 || g.ErrorCount != 80 {
			t.Errorf("window %d = %+v, want log-step observed 80 baseline 15 evidenceSec 60", i, g)
		}
	}
	for i, g := range feed(src, c, sender, &cum, repeat(15, 3)...) {
		if g != nil {
			t.Errorf("recovery window %d reported, want muted", i)
		}
	}
}

func TestBaseline_QuietServiceBreachesOnFloor(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var cum float64
	feed(src, c, sender, &cum, repeat(0, 6)...)
	got := feed(src, c, sender, &cum, 30)
	if got[0] == nil || got[0].Breach != push.BreachLogStep || got[0].Baseline != 0 || got[0].Observed != 30 {
		t.Fatalf("got %+v, want log-step observed 30 baseline 0", got[0])
	}
}

func TestBaseline_BelowFloorStepNotReported(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var cum float64
	feed(src, c, sender, &cum, repeat(1, 6)...)
	if got := feed(src, c, sender, &cum, 4); got[0] != nil {
		t.Fatalf("1 -> 4 lines reported: %+v", got[0])
	}
}

func TestBaseline_WarnStreamUsesOwnTracker(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var cum float64
	for i := 0; i < 8; i++ {
		cum += 15
		src.set("sensor/node-a", errWarnTotalSet("ops", "api", 0, cum, cum))
		c.runOnce(context.Background())
	}
	n := sender.callCount()
	if n != 5 {
		t.Fatalf("frames = %d, want 5 (cold windows only)", n)
	}
	cum += 80
	src.set("sensor/node-a", errWarnTotalSet("ops", "api", 0, cum, cum))
	c.runOnce(context.Background())
	svcs := sender.allServices()
	last := svcs[len(svcs)-1]
	if last.Breach != push.BreachLogStep || last.Observed != 80 {
		t.Fatalf("warn step = %+v", last)
	}
	// The step's 80 lines are warn lines: the entry says so, or the core
	// renders them as error lines.
	if last.Stream != push.LogStreamWarn || last.ErrorCount != 0 {
		t.Errorf("warn step stream = %q errorCount = %d, want %q and 0", last.Stream, last.ErrorCount, push.LogStreamWarn)
	}
	// A presence-gate entry has no step and names no stream.
	if first := svcs[0]; first.Breach != "" || first.Stream != "" {
		t.Errorf("cold entry = %+v, want no breach and no stream", first)
	}
}

func TestBaseline_DisabledKeepsPresenceGate(t *testing.T) {
	off := false
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{Baseline: BaselineOptions{Enabled: &off}})
	var cum float64
	feed(src, c, sender, &cum, repeat(15, 10)...)
	if n := sender.callCount(); n != 10 {
		t.Fatalf("frames = %d, want 10 (presence every window)", n)
	}
}

func TestBaseline_Defaults(t *testing.T) {
	o := Options{ErrorThreshold: 20}
	o.setDefaults()
	b := o.Baseline
	if !b.enabled() || b.EvidenceWindows != 1 || b.MinBaselineWindows != 5 || b.Multiplier != 3 || b.FloorLines != 20 {
		t.Errorf("defaults = %+v", b)
	}
}

// With a cold floor set, an agent that starts during an error storm reports
// the storm as a step every window and does not learn it, so it is not muted
// as the service's normal. Without one (the default) a cold stream is learned
// after MinBaselineWindows, as TestBaseline_ChronicStreamMutedOnceWarm pins.
func TestBaseline_ColdFloorStormReportedNotLearned(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{Baseline: BaselineOptions{ColdFloorLines: 100}})
	var cum float64
	for i, g := range feed(src, c, sender, &cum, repeat(150, 30)...) {
		if g == nil || g.Breach != push.BreachLogStep || g.Observed != 150 || g.Baseline != 0 {
			t.Fatalf("storm window %d = %+v, want a log-step at 150 lines with no baseline", i, g)
		}
	}
	// The storm ends: the calm level becomes the baseline (5 cold windows on
	// the presence gate, then muted), and the storm level then steps over it.
	got := feed(src, c, sender, &cum, repeat(15, 8)...)
	if got[4] == nil || got[4].Breach != "" || got[5] != nil || got[7] != nil {
		t.Fatalf("calm windows = %+v, want 5 presence reports, then muted", got)
	}
	if g := feed(src, c, sender, &cum, 150)[0]; g == nil || g.Breach != push.BreachLogStep || g.Baseline != 15 {
		t.Fatalf("storm after calm = %+v, want a log-step against the calm baseline 15", g)
	}
}

// A stream whose steps recur is muted, not silently: the mute is logged and
// counted, and the stream reports again every StillBreachingEvery windows
// with StillBreaching set.
func TestBaseline_RecurringStepsMuteIsLoggedCountedAndBounded(t *testing.T) {
	src := newFakeSource()
	c, sender := newTestCollector(src, Options{})
	var logs []string
	c.log = funcr.New(func(_, args string) { logs = append(logs, args) }, funcr.Options{})
	var cum float64
	feed(src, c, sender, &cum, repeat(15, 8)...)
	lines := make([]float64, 90)
	for i := range lines {
		lines[i] = 15
		if i%3 == 0 {
			lines[i] = 80
		}
	}
	late := 0
	for i, g := range feed(src, c, sender, &cum, lines...) {
		if i < 30 || g == nil {
			continue
		}
		late++
		if !g.StillBreaching || g.Breach != push.BreachLogStep || g.Baseline != 15 || g.Stream != push.LogStreamError {
			t.Errorf("late window %d = %+v, want a still-breaching error log-step report against the median 15", i, g)
		}
	}
	if late < 4 || late > 6 {
		t.Errorf("late reports = %d, want one per %d windows over 60", late, baseline.StillBreachingEvery)
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
	if got := agentmetrics.MutedKeys(agentmetrics.SensorLog, push.BreachLogStep); got != 1 {
		t.Errorf("muted keys metric = %v, want 1", got)
	}
}

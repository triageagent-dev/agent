package usemetrics

import (
	"testing"
	"time"

	"github.com/triageagent-dev/agent/metricset"
)

// newDeltaTestCollector builds a bare Collector sufficient for computeDeltas —
// no manager, no client, no sender — just the Interval and prev-baseline map
// computeDeltas actually reads.
func newDeltaTestCollector(interval time.Duration) *Collector {
	return &Collector{
		opts: Options{Interval: interval},
		prev: make(map[string]map[podKey]podCounters),
	}
}

func onePodDelta(t *testing.T, deltas map[podKey]podDelta, order []podKey) podDelta {
	t.Helper()
	if len(order) != 1 {
		t.Fatalf("order = %v, want exactly one pod", order)
	}
	return deltas[order[0]]
}

// TestComputeDeltas_MemNodePct verifies memBytes/memNodeTotalBytes derive the
// node-relative percentage.
func TestComputeDeltas_MemNodePct(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	set := setOf(
		gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 500)),
		gaugeFamily(MetricMemNodeTotalBytes, point("ops", "api", "api-1", 1000)),
	)
	deltas, order := c.computeDeltas("addr", set)
	d := onePodDelta(t, deltas, order)
	if d.memNodePct != 50 {
		t.Errorf("memNodePct = %v, want 50", d.memNodePct)
	}
}

// TestComputeDeltas_MemNodePct_AbsentWhenNodeTotalMissing proves the
// denominator's absence yields 0 (omitted on the wire), not a divide-by-zero
// crash or a fabricated percentage.
func TestComputeDeltas_MemNodePct_AbsentWhenNodeTotalMissing(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	set := setOf(
		gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 500)),
		// No MetricMemNodeTotalBytes point at all.
	)
	deltas, order := c.computeDeltas("addr", set)
	d := onePodDelta(t, deltas, order)
	if d.memNodePct != 0 {
		t.Errorf("memNodePct = %v, want 0 (absent denominator)", d.memNodePct)
	}
}

func TestComputeDeltas_MemReqPct(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	set := setOf(
		gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 500)),
		gaugeFamily(MetricMemRequestBytes, point("ops", "api", "api-1", 1000)),
	)
	deltas, order := c.computeDeltas("addr", set)
	d := onePodDelta(t, deltas, order)
	if d.memReqPct != 50 {
		t.Errorf("memReqPct = %v, want 50", d.memReqPct)
	}
}

func TestComputeDeltas_MemReqPct_AbsentWhenRequestMissing(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	set := setOf(gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 500)))
	deltas, order := c.computeDeltas("addr", set)
	d := onePodDelta(t, deltas, order)
	if d.memReqPct != 0 {
		t.Errorf("memReqPct = %v, want 0 (absent denominator)", d.memReqPct)
	}
}

// TestComputeDeltas_MemReqPct_NotClampedAbove100 pins the deliberate
// non-clamping: exceeding your own request is the entire signal on this axis.
func TestComputeDeltas_MemReqPct_NotClampedAbove100(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	set := setOf(
		gaugeFamily(MetricMemBytes, point("ops", "api", "api-1", 200)),
		gaugeFamily(MetricMemRequestBytes, point("ops", "api", "api-1", 100)),
	)
	deltas, order := c.computeDeltas("addr", set)
	d := onePodDelta(t, deltas, order)
	if d.memReqPct != 200 {
		t.Errorf("memReqPct = %v, want 200 (not clamped to 100)", d.memReqPct)
	}
}

// TestComputeDeltas_CPUNodePctAndCPUReqPct drives two reads so the CPU
// counter has a real delta (the first read's counterDelta first-seen rule
// always reports 0 — see counterDelta's doc comment), and verifies
// cpuNodePct/cpuReqPct are derived from the SAME cpuCoresUsed intermediate
// cpuPct itself uses, per pod.
func TestComputeDeltas_CPUNodePctAndCPUReqPct(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second) // windowUsec = 6e7
	base := func(usageUsec float64) metricset.Set {
		return setOf(
			gaugeFamily(MetricCPUNodeCores, point("ops", "api", "api-1", 8)),
			gaugeFamily(MetricCPURequestCores, point("ops", "api", "api-1", 0.5)),
			counterFamily(MetricCPUUsageUsecTotal, point("ops", "api", "api-1", usageUsec)),
		)
	}

	// First read: establishes the baseline, delta is suppressed to 0.
	deltas, order := c.computeDeltas("addr", base(1000000))
	d := onePodDelta(t, deltas, order)
	if d.cpuNodePct != 0 || d.cpuReqPct != 0 {
		t.Fatalf("first read: cpuNodePct/cpuReqPct = %v/%v, want 0/0 (first-seen suppression)", d.cpuNodePct, d.cpuReqPct)
	}

	// Second read: usage grew by exactly 60,000,000us = 60s = one full window
	// -> cpuCoresUsed = 1.0 core.
	deltas, order = c.computeDeltas("addr", base(1000000+60000000))
	d = onePodDelta(t, deltas, order)
	if d.cpuCoresUsed != 1 {
		t.Fatalf("cpuCoresUsed = %v, want 1", d.cpuCoresUsed)
	}
	if d.cpuNodePct != 12.5 { // 1 / 8 cores * 100
		t.Errorf("cpuNodePct = %v, want 12.5", d.cpuNodePct)
	}
	if d.cpuReqPct != 200 { // 1 / 0.5 cores * 100, deliberately not clamped
		t.Errorf("cpuReqPct = %v, want 200", d.cpuReqPct)
	}
}

// TestComputeDeltas_CPUNodePctAndCPUReqPct_AbsentWhenDenominatorMissing
// verifies both stay 0 when their own denominator gauge wasn't reported, even
// though the pod did use CPU this window.
func TestComputeDeltas_CPUNodePctAndCPUReqPct_AbsentWhenDenominatorMissing(t *testing.T) {
	c := newDeltaTestCollector(60 * time.Second)
	body := func(usageUsec float64) metricset.Set {
		return setOf(counterFamily(MetricCPUUsageUsecTotal, point("ops", "api", "api-1", usageUsec)))
	}
	_, _ = c.computeDeltas("addr", body(1000000))
	deltas, order := c.computeDeltas("addr", body(1000000+60000000))
	d := onePodDelta(t, deltas, order)
	if d.cpuCoresUsed != 1 {
		t.Fatalf("cpuCoresUsed = %v, want 1 (usage was real, only the denominators are missing)", d.cpuCoresUsed)
	}
	if d.cpuNodePct != 0 || d.cpuReqPct != 0 {
		t.Errorf("cpuNodePct/cpuReqPct = %v/%v, want 0/0 (no node/request cores reported)", d.cpuNodePct, d.cpuReqPct)
	}
}

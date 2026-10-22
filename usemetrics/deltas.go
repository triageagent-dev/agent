package usemetrics

import (
	"time"

	"github.com/triageagent-dev/agent/metricset"
)

// servicePair is one (namespace, service) identity — same shape as
// redsource/logmetrics' own servicePair, duplicated rather than shared per this
// repo's established mirror-not-share convention for these sibling collectors.
type servicePair struct {
	Namespace string
	Service   string
}

// podKey identifies one pod's USE series: the (namespace, service) pair it belongs
// to, plus the pod name — needed both for the per-service max/sum aggregation in
// collector.go and for picking a worst-offender pod for the Sample string.
type podKey struct {
	pair servicePair
	pod  string
}

// podCounters is one pod's last-seen cumulative counter values from one target's
// read: the USE-metrics analogue of redsource/deltas.go's per-series baseline.
type podCounters struct {
	// oomKill is the pod cgroup's counter (MetricOOMKillTotal); oomKillStatus is
	// the pod-status-derived one (MetricOOMKillStatusTotal). Two independent
	// sources, diffed separately and merged by max in computeDeltas.
	oomKill, oomKillStatus, periods, throttled float64
	// missed counts the consecutive reads this pod was ABSENT from its target's
	// set while its baseline was carried forward (see baselineGraceReads). 0 for
	// a pod present in the read this entry came from.
	missed int
	// cpuUsageUsec is cumulative CPU time. Diffed like the other counters, then
	// divided by the window and the pod's CPU quota to yield a percentage — the
	// step that makes CPU utilization a derived value rather than a gauge, and
	// the reason v1 skipped it.
	cpuUsageUsec float64
	// at is when the set these counters were read from was received (zero when
	// the source does not say). A usage delta is divided by the time between
	// the two sets it spans, which is this, not the collector's interval: an
	// entry carried forward keeps its own time, so a pod or a sensor that was
	// away for several reads is not reported as burning several times its CPU.
	at time.Time
}

// podDelta is one pod's reading for one window: gauges taken at face value (PSI and
// memory utilization are point-in-time, nothing to diff against a previous read),
// counters diffed against that same target's previous read.
type podDelta struct {
	cpuPsi, memPsi, ioPsi float64 // percent, 0-100; a pod with PSI unavailable simply never sets these (stays 0)
	// memPct is the utilization reading forwarded downstream: the WORKING-SET
	// figure (use_mem_ws_pct) when the exporter reports one, falling back to the
	// page-cache-charging use_mem_pct when it does not. 0 means the pod has no
	// memory limit.
	//
	// The fallback exists for the rolling-upgrade straddle: the node exporter
	// runs on the DaemonSet and this collector on the controller Deployment, so
	// the two are independently versioned and routinely skew by a release. An
	// exporter predating use_mem_ws_pct simply omits it and the older, less
	// precise number is used rather than utilization disappearing for the whole
	// fleet until the DaemonSet catches up.
	memPct float64
	// memBytes/memLimitBytes are the absolute form of the same working-set
	// reading, 0 when the exporter does not report them (older DaemonSet).
	memBytes, memLimitBytes float64
	// cpuPct is CPU utilization as a percentage of the cgroup's own quota,
	// DERIVED here rather than read: (usage_usec delta / window micros) /
	// limit cores * 100. 0 means no CPU limit is set, or the window produced no
	// usable delta (first read of a pod — see counterDelta's first-seen rule,
	// which reports 0 rather than a lifetime total).
	cpuPct        float64
	cpuLimitCores float64
	// oomKillDelta is the pod's OOM kills this window: the larger of the cgroup
	// source's delta (normalised to kills, see nodeexporter.readOOMKill) and the
	// pod-status source's. Max, not sum — both observe the same kills, so adding
	// them would double count; max makes either source sufficient on its own.
	oomKillDelta                           int64
	oomKillCgroupDelta, oomKillStatusDelta int64
	periodsDelta                           int64
	throttledDelta                         int64
	cpuUsageDelta                          float64
	// cpuUsageValid is true when cpuUsageDelta is a real window delta: the pod
	// had a previous read to diff against (counterDelta's ok) AND the sensor
	// delivered a set since then. A first-seen pod, or the same set read again,
	// reports a 0 delta that must not be fed to the cpu baseline as "idle".
	cpuUsageValid bool
	// cpuCoresUsed is CPU cores actually consumed this window — usageDelta /
	// the real time between the two sets diffed (the collector's interval when
	// the source does not say), the numerator every CPU utilization percentage divides,
	// factored out of cpuPct's own derivation so memNodePct/memReqPct's CPU
	// siblings (cpuNodePct/cpuReqPct) don't recompute it against a different
	// limit. 0 on the same first-read/no-window terms as cpuPct.
	cpuCoresUsed float64

	// memNodeTotalBytes/cpuNodeCores are the node's own capacity, read straight
	// off the exporter's use_mem_node_total_bytes/use_cpu_node_cores gauges — the
	// same value repeated for every pod on that node (see
	// nodeexporter.resolveNodeMemTotalBytes/resolveNodeCPUCores). Not carried
	// onto ServiceUseSummary: a service's pods can span several nodes, so
	// there is no single correct "node total" to report at that granularity —
	// only the memNodePct/cpuNodePct percentages they produce below are.
	memNodeTotalBytes, cpuNodeCores float64
	// memRequestBytes/cpuRequestCores are the Kubernetes right-sizing
	// denominator: the sum of the pod spec's regular-container requests (see
	// nodeexporter.regularContainerMemRequests/regularContainerCPURequestsMilli).
	// 0 when the exporter reports none (no regular container declared one).
	memRequestBytes, cpuRequestCores float64

	// memNodePct/cpuNodePct/memReqPct/cpuReqPct are the four derived
	// percentages this pod contributes: usage over node capacity, and usage
	// over the pod's own declared request. Each is 0 (absent, matching the
	// wire's omitempty) when its own denominator is missing or zero — see the
	// per-family comments where they're computed below. Request percentages
	// are NOT clamped to 100: exceeding your own request is the entire signal
	// on that axis, and clamping would erase it. Node percentages cannot
	// exceed 100 in practice but are likewise left unclamped.
	memNodePct, cpuNodePct, memReqPct, cpuReqPct float64
}

// computeDeltas extracts every recognized USE-metrics family (metrics.go) from one
// target's latest read and returns one podDelta per reporting pod, plus the
// order pods were first encountered in (fam.GetMetric()'s own slice order, scanned
// family-by-family in the fixed order below) — used by the caller to break
// worst-offender sample ties deterministically, the same reasoning
// redsource/deltas.go documents for its own "order" slice.
//
// Gauges (PSI, memory utilization) are taken at face value — a service stalling on
// PSI still fires on the very first read, since a gauge has nothing to diff
// against. Counters (oom_kill, cpu.stat periods/throttled) are diffed against that
// same target's previous read, with the same counter-reset handling as
// redsource/deltas.go's counterDelta: a raw negative delta (the node-exporter pod
// itself restarted, or the underlying pod cgroup was recreated) is instead reported
// as "current value is this window's count". A podKey with NO entry at all in the
// previous read's map (the collector's first read of a target, or a pod newly
// discovered mid-run) is a distinct case from a reset and is handled separately: its
// current cumulative value is recorded as the new baseline and this window's delta
// is reported as zero, never as the pod's lifetime counter value — see counterDelta.
func (c *Collector) computeDeltas(addr string, set metricset.Set) (map[podKey]podDelta, []podKey) {
	return c.computeDeltasAt(addr, set, time.Time{})
}

// computeDeltasAt is computeDeltas for a set received at the given time (zero
// when unknown, see podCounters.at).
func (c *Collector) computeDeltasAt(addr string, set metricset.Set, at time.Time) (map[podKey]podDelta, []podKey) {
	out := make(map[podKey]podDelta)
	var order []podKey
	seen := make(map[podKey]bool)

	touch := func(key podKey) {
		if !seen[key] {
			seen[key] = true
			order = append(order, key)
		}
	}

	forEachSample := func(metricName string, fn func(podKey, float64)) {
		m := set.Get(metricName)
		if m == nil {
			return
		}
		for _, p := range m.Points {
			key, ok := podKeyOf(p)
			if !ok {
				continue
			}
			touch(key)
			fn(key, p.Value)
		}
	}

	forEachSample(MetricCPUPsiAvg60, func(k podKey, v float64) { d := out[k]; d.cpuPsi = v; out[k] = d })
	forEachSample(MetricMemPsiAvg60, func(k podKey, v float64) { d := out[k]; d.memPsi = v; out[k] = d })
	forEachSample(MetricIOPsiAvg60, func(k podKey, v float64) { d := out[k]; d.ioPsi = v; out[k] = d })
	// Utilization: take use_mem_pct first, then let use_mem_ws_pct overwrite it
	// where present. Order is the whole mechanism — the working-set reading wins
	// for any exporter new enough to emit it, and the older reading survives only
	// as the straddle fallback. See podDelta.memPct.
	forEachSample(MetricMemPct, func(k podKey, v float64) { d := out[k]; d.memPct = v; out[k] = d })
	forEachSample(MetricMemWorkingSetPct, func(k podKey, v float64) { d := out[k]; d.memPct = v; out[k] = d })
	forEachSample(MetricMemBytes, func(k podKey, v float64) { d := out[k]; d.memBytes = v; out[k] = d })
	forEachSample(MetricMemLimitBytes, func(k podKey, v float64) { d := out[k]; d.memLimitBytes = v; out[k] = d })
	forEachSample(MetricCPULimitCores, func(k podKey, v float64) { d := out[k]; d.cpuLimitCores = v; out[k] = d })
	forEachSample(MetricMemNodeTotalBytes, func(k podKey, v float64) { d := out[k]; d.memNodeTotalBytes = v; out[k] = d })
	forEachSample(MetricCPUNodeCores, func(k podKey, v float64) { d := out[k]; d.cpuNodeCores = v; out[k] = d })
	forEachSample(MetricMemRequestBytes, func(k podKey, v float64) { d := out[k]; d.memRequestBytes = v; out[k] = d })
	forEachSample(MetricCPURequestCores, func(k podKey, v float64) { d := out[k]; d.cpuRequestCores = v; out[k] = d })

	// Memory-side derived percentages need no counter delta — memBytes is
	// already a point-in-time gauge, same as memPct — so they're computed here,
	// right after every gauge family above has landed on out[k], rather than
	// waiting for the counter-diff section below. A missing/zero denominator
	// leaves the field at its zero value, which is exactly the wire's
	// "absent" (omitempty) semantics.
	for _, k := range order {
		d := out[k]
		if d.memNodeTotalBytes > 0 {
			d.memNodePct = d.memBytes / d.memNodeTotalBytes * 100
		}
		if d.memRequestBytes > 0 {
			d.memReqPct = d.memBytes / d.memRequestBytes * 100
		}
		out[k] = d
	}

	prev := c.prev[addr]
	cur := make(map[podKey]podCounters)

	// lookup finds a pod's previous counters. An exact podKey match is the norm;
	// failing that, the same (namespace, pod) under another service label also
	// counts (the exporter's service label can change between reads, e.g. a pod
	// relabelled, and the new podKey would otherwise be "first seen" and report 0
	// for a kill that happened across the change). A pod name is unique within
	// its namespace at any moment, so this cannot cross-match two live pods.
	lookup := func(k podKey) (podCounters, bool) {
		if pc, ok := prev[k]; ok {
			return pc, true
		}
		for pk, pc := range prev {
			if pk.pair.Namespace == k.pair.Namespace && pk.pod == k.pod {
				return pc, true
			}
		}
		return podCounters{}, false
	}

	forEachSample(MetricOOMKillTotal, func(k podKey, v float64) {
		pc := cur[k]
		pc.oomKill = v
		cur[k] = pc
		prevPC, ok := lookup(k)
		d := out[k]
		d.oomKillCgroupDelta = counterDelta(prevPC.oomKill, v, ok)
		out[k] = d
		c.logOOMBaseline(addr, k, "cgroup", prevPC.oomKill, v, ok)
	})
	forEachSample(MetricOOMKillStatusTotal, func(k podKey, v float64) {
		pc := cur[k]
		pc.oomKillStatus = v
		cur[k] = pc
		prevPC, ok := lookup(k)
		d := out[k]
		d.oomKillStatusDelta = counterDelta(prevPC.oomKillStatus, v, ok)
		out[k] = d
		c.logOOMBaseline(addr, k, "status", prevPC.oomKillStatus, v, ok)
	})
	for _, k := range order {
		d := out[k]
		d.oomKillDelta = max(d.oomKillCgroupDelta, d.oomKillStatusDelta)
		out[k] = d
		if d.oomKillDelta > 0 {
			// Both sources' values, so a kill one path saw and the other missed
			// is visible in the log without a debugger.
			pc, prevPC := cur[k], prev[k]
			c.log.V(1).Info("oom kill delta",
				"target", addr, "namespace", k.pair.Namespace, "service", k.pair.Service, "pod", k.pod,
				"delta", d.oomKillDelta,
				"cgroupDelta", d.oomKillCgroupDelta, "cgroupPrev", prevPC.oomKill, "cgroupCur", pc.oomKill,
				"statusDelta", d.oomKillStatusDelta, "statusPrev", prevPC.oomKillStatus, "statusCur", pc.oomKillStatus)
		}
	}
	forEachSample(MetricCPUNrPeriodsTotal, func(k podKey, v float64) {
		pc := cur[k]
		pc.periods = v
		cur[k] = pc
		prevPC, ok := prev[k]
		d := out[k]
		d.periodsDelta = counterDelta(prevPC.periods, v, ok)
		out[k] = d
	})
	forEachSample(MetricCPUNrThrottledTotal, func(k podKey, v float64) {
		pc := cur[k]
		pc.throttled = v
		cur[k] = pc
		prevPC, ok := prev[k]
		d := out[k]
		d.throttledDelta = counterDelta(prevPC.throttled, v, ok)
		out[k] = d
	})

	// CPU utilization, derived last because it needs both a counter delta and a
	// gauge: (usage micros this window / wall micros this window) / quota cores.
	// A pod on its first read has a zero delta by counterDelta's first-seen
	// rule and therefore reports 0% for one window rather than a lifetime
	// average — the same one-window warmup every counter-derived reading here
	// has.
	//
	// cpuCoresUsed (usage delta / window micros) is the shared numerator behind
	// all three CPU percentages below — factored out once rather than
	// recomputed per denominator, so cpuPct/cpuNodePct/cpuReqPct can never
	// disagree about how much CPU this pod actually used this window.
	forEachSample(MetricCPUUsageUsecTotal, func(k podKey, v float64) {
		pc := cur[k]
		pc.cpuUsageUsec = v
		cur[k] = pc
		prevPC, ok := prev[k]
		d := out[k]
		d.cpuUsageDelta = float64(counterDelta(prevPC.cpuUsageUsec, v, ok))
		d.cpuUsageValid = ok
		// The delta spans the time between the two sets, which is the
		// collector's interval only when both the sensor and the pod were
		// there on every read: divide by the real time when it is known.
		windowUsec := c.opts.Interval.Seconds() * 1e6
		if ok && !at.IsZero() && !prevPC.at.IsZero() {
			if elapsed := at.Sub(prevPC.at); elapsed > 0 {
				windowUsec = elapsed.Seconds() * 1e6
			} else {
				// The same set as last read: nothing new was measured.
				d.cpuUsageValid = false
			}
		}
		if windowUsec > 0 {
			d.cpuCoresUsed = d.cpuUsageDelta / windowUsec
		}
		if d.cpuLimitCores > 0 && windowUsec > 0 {
			d.cpuPct = d.cpuCoresUsed / d.cpuLimitCores * 100
		}
		if d.cpuNodeCores > 0 && windowUsec > 0 {
			d.cpuNodePct = d.cpuCoresUsed / d.cpuNodeCores * 100
		}
		if d.cpuRequestCores > 0 && windowUsec > 0 {
			d.cpuReqPct = d.cpuCoresUsed / d.cpuRequestCores * 100
		}
		out[k] = d
	})

	// Carry forward the baseline of every pod that was in the previous read but
	// is absent from this one, for up to baselineGraceReads reads. Replacing prev
	// with only the pods seen now (as this once did) erased a pod's baseline the
	// moment it was missing from ONE read — a pod not Running for a tick, its
	// cgroup dir not found, its series gone from one snapshot — and its next
	// appearance was then "first seen", reporting 0 for any kill in the gap.
	// A pod now present under a different service label is not carried (its
	// baseline moved to the new key via lookup above).
	present := make(map[[2]string]bool, len(cur))
	for k, pc := range cur {
		pc.at = at
		cur[k] = pc
		present[[2]string{k.pair.Namespace, k.pod}] = true
	}
	for k, pc := range prev {
		if _, ok := cur[k]; ok || present[[2]string{k.pair.Namespace, k.pod}] {
			continue
		}
		if pc.missed >= baselineGraceReads {
			c.log.V(1).Info("pod baseline dropped: absent from its target's set too long",
				"target", addr, "namespace", k.pair.Namespace, "service", k.pair.Service, "pod", k.pod,
				"missedReads", pc.missed, "cgroupCur", pc.oomKill, "statusCur", pc.oomKillStatus)
			continue
		}
		pc.missed++
		cur[k] = pc
	}

	c.prev[addr] = cur
	return out, order
}

// baselineGraceReads is how many consecutive reads a pod (or a whole target,
// see runOnce) may be absent before its counter baseline is forgotten. Long
// enough to bridge a stream reconnect or a pod flapping out of Running, short
// enough that churned pods do not accrete in Collector.prev.
const baselineGraceReads = 5

// logOOMBaseline logs the two baseline-related OOM transitions worth a line at
// V(1), for either source: a pod first seen with an already non-zero cumulative
// counter (its delta is suppressed to 0, so a kill that happened right before
// the first read is invisible by design), and a counter that went backwards
// (a reset: the delta becomes the current value).
func (c *Collector) logOOMBaseline(addr string, k podKey, source string, prev, cur float64, hadPrev bool) {
	switch {
	case !hadPrev && cur > 0:
		c.log.V(1).Info("oom counter first seen non-zero; baseline only, delta suppressed",
			"target", addr, "namespace", k.pair.Namespace, "service", k.pair.Service, "pod", k.pod,
			"source", source, "cumulative", cur)
	case hadPrev && cur < prev:
		c.log.V(1).Info("oom counter went backwards; treating the current value as the delta",
			"target", addr, "namespace", k.pair.Namespace, "service", k.pair.Service, "pod", k.pod,
			"source", source, "prev", prev, "cur", cur)
	}
}

// podKeyOf extracts a pod's (namespace, service, pod) identity from a sample's
// labels. The exporter always stamps namespace/pod programmatically from the pod
// object it read the cgroup for (never from a possibly-absent k8s label), so an
// empty namespace means the sample is unusable — service, by contrast, can
// legitimately be empty (a pod with neither a recognized service label nor a
// resolvable owner; see nodeexporter's serviceIdentity fallback chain), and such
// pods are still returned here — the caller (Collector.runOnce) is what turns an
// empty Service into the "unattributed" skip, mirroring redsource/logmetrics.
func podKeyOf(p metricset.Point) (podKey, bool) {
	ns := p.Attr(LabelNamespace)
	if ns == "" {
		return podKey{}, false
	}
	svc := p.Attr(LabelService)
	pod := p.Attr(LabelPod)
	return podKey{pair: servicePair{Namespace: ns, Service: svc}, pod: pod}, true
}

// counterDelta returns cur-prev for a counter that was present in the previous
// read's map (ok=true), except when that would be negative (a counter reset) in
// which case cur itself is treated as the delta.
//
// ok=false — the podKey had no entry at all in the previous read (this target's
// first read ever, or a pod newly discovered mid-run) — is a DISTINCT case from a
// reset and always returns zero, never cur. Collapsing the two (as an earlier
// version of this function did, by reading the zero value out of a missing map entry
// and treating it identically to a genuine previous value of 0) reported a pod's
// entire LIFETIME cumulative counter as a single window's delta on every cold start:
// observed live, a 16-day-old pod with a lifetime oom_kill count of 15 was reported
// as "15 OOM kills in 60s window" on the collector's first window after deploy. The
// caller is responsible for still recording cur into the new baseline map on an
// ok=false call, so the next read reports a real delta.
//
// Same semantics as redsource/deltas.go's counterDelta, duplicated rather than
// shared.
func counterDelta(prev, cur float64, ok bool) int64 {
	if !ok {
		return 0
	}
	d := cur - prev
	if d < 0 {
		d = cur
	}
	return int64(d)
}

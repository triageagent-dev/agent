// Package usemetrics implements the sending side of the cgroup/PSI-derived
// USE-metrics pipeline (docs/adr/0010-use-metrics-node-exporter.md): a periodic
// loop that reads each sensor's node-exporter metrics from the sensor stream
// (docs/adr/0015-sensor-stream-grpc.md), aggregates per-
// (namespace, service) USE-method counters (utilization — against a container
// limit, the node's own capacity, and the pod's own request; saturation/PSI;
// errors) across the service's pods, and sends a use_metrics frame
// (push/protocol.go) through the existing Pusher for services whose PSI,
// OOM-kill, or utilization signal trips a gate. Off by default; a no-op when
// push mode itself is disabled, since the frame has nowhere to go. An UNGATED
// use_summary frame (sendUseSummary) additionally carries utilization —
// including the node/request percentages — and saturation for every service
// read, every window, regardless of whether anything crossed the gate.
//
// This package mirrors redsource/ and logmetrics/ structurally (Options/Collector
// shape, per-sensor baselines, counter-reset AND first-seen
// handling — see deltas.go's counterDelta) rather than sharing code with them — same
// reasoning as those two: the label set and gate shape differ enough (a per-pod
// {namespace,service,pod} label triple aggregated with max-for-gauges/sum-for-
// counters, an OR-of-six gate, a top-N cap by memPsi) that a shared abstraction
// would cost more than the small amount of duplication.
package usemetrics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/agentmetrics"
	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// Sender is the subset of *push.Pusher the collector needs, plus SendUseSummary —
// the UNGATED per-window saturation summary companion to SendUseMetrics (every
// service read, not just gate-crossers; see sendUseSummary below). A small
// interface (rather than importing *push.Pusher directly) so tests can fake it,
// and so a nil *push.Pusher (push mode disabled) satisfies it via a plain nil
// interface check in New: same idiom as redsource.Sender/logmetrics.Sender.
type Sender interface {
	SendUseMetrics(windowStart int64, windowSec int, services []push.ServiceUseMetrics)
	SendUseSummary(windowStart int64, windowSec int, services []push.ServiceUseSummary)
}

// Options configures the Collector. All fields have defaults applied in New except
// Source, which is required.
type Options struct {
	// Source delivers each sensor's node-exporter metrics
	// (docs/adr/0015-sensor-stream-grpc.md).
	Source metricset.Source
	// Namespaces, when non-empty, restricts forwarded metrics to samples whose
	// namespace label is in this set. Empty forwards every namespace read. Same
	// reasoning as redsource.Options.Namespaces: a push connection carries a
	// single tenant, and one node-exporter DaemonSet can serve several tenants, so
	// this filter is what keeps a shared exporter from mis-attributing another
	// tenant's USE metrics to this connection's tenant. Set it to the same list as
	// --target-namespaces.
	Namespaces []string
	// NamespaceAllowed, when non-nil, OVERRIDES the static Namespaces set
	// entirely: it is consulted per payload build for every sample's
	// namespace, so it hot-follows dynamic scope changes — main.go wires it
	// to push.Pusher.NamespaceInScope, whose scope union includes namespaces
	// pulled in live by the triageagent.dev/tenant Namespace annotation
	// (push/namespace_scope.go), which a static list frozen at process start
	// can never see. Nil keeps the static Namespaces behavior unchanged
	// (empty list = forward every namespace).
	NamespaceAllowed func(string) bool
	// PodExcluded, when non-nil, reports whether a pod is out of triage
	// (triageagent.dev/triage: "false" on its namespace, its pod template, or
	// any owner up its chain); its samples are then never aggregated, so no
	// pressure, OOM-kill or utilization entry is sent for it. main.go wires
	// it to push.Pusher.PodExcluded. Nil excludes nothing.
	PodExcluded func(ctx context.Context, namespace, pod string) bool
	// Interval is how often the collector reads + evaluates the gate. Default 60s.
	Interval time.Duration
	// MemPsiThreshold/CPUPsiThreshold/IOPsiThreshold are the minimum
	// mem/cpu/io PSI "some avg60" percentage (max across a service's pods) for the
	// service to qualify for the frame — any ONE of the three crossing its
	// threshold qualifies (OR), independent of the other two, and independent of
	// OOMKills >= 1 (also an OR arm). Zero is a legitimate value — "always report
	// this dimension" — and is deliberately NOT re-defaulted in setDefaults; see
	// the note there. Chart/flag defaults: 20/40/40.
	MemPsiThreshold float64
	CPUPsiThreshold float64
	IOPsiThreshold  float64
	// MemPctThreshold is the minimum memory UTILIZATION percentage (working set
	// vs limit, max across a service's pods) that qualifies a service for the
	// frame — a fifth OR arm alongside the three PSI dimensions and OOMKills.
	//
	// It exists because saturation and OOM kills are both LAGGING: a pod whose
	// working set is resident at its limit never enters reclaim, so every PSI
	// dimension reads a truthful 0% until the instant the OOM killer fires, at
	// which point the warning has become an outage. A neo4j pod sat at
	// 96.5% of a 2Gi limit for days emitting nothing at all — no event, so
	// nothing to absorb into an incident, nothing to backfill as a precursor,
	// nothing for pattern mining, and nothing for reflection to reason about.
	// The whole downstream chain was starved by the absence of this one arm.
	//
	// Zero means "never gate on utilization" rather than "gate on everything",
	// unlike the PSI thresholds where 0 legitimately means always-report: a
	// utilization of 0% is a pod that has no limit at all (see
	// nodeexporter.memLimitBytes), so a 0 threshold would qualify every
	// unbounded service every window. Chart/flag default: 85.
	MemPctThreshold float64

	// MemNodePctThreshold is the minimum "% of node total memory" (working set
	// vs the NODE's total memory, max across a service's pods, see
	// nodeexporter.resolveNodeMemTotalBytes) that qualifies a service for the
	// frame — a sixth OR arm alongside MemPctThreshold and the rest.
	//
	// This is the portable sibling of MemPctThreshold: that arm needs a limit
	// to divide by, and a pod with no limit set reports no MemPct at all — this
	// arm's denominator, the node's total memory, always exists, so it can
	// catch a single unlimited pod eating the box even when MemPctThreshold has
	// nothing to fire on.
	//
	// Deliberately defaulted to 0, which DISABLES this arm, same inverted
	// convention as MemPctThreshold (0% utilization there means "no limit is
	// set", not "always report"). Unlike MemPctThreshold, though, this default
	// is not a considered choice from a measured distribution — it is off
	// pending observed numbers from a live fleet. Nobody has yet looked at what
	// a normal service's "% of node" distribution looks like across an actual
	// cluster, and shipping a guessed threshold nobody has seen data for is how
	// a gate arm becomes noise instead of signal. The arm exists, ready to be
	// turned on with one flag once those numbers are in — that is the entire
	// point of adding it now rather than waiting for the flag too.
	MemNodePctThreshold float64

	// Baseline configures the two baseline-relative OR arms (cpu cores and cpu
	// throttling), docs/todo/baseline-gates.md.
	Baseline BaselineOptions

	// now is injectable in tests. Defaults to time.Now.
	now func() time.Time
}

// BaselineOptions configures the baseline arms. They exist because most pods
// have no CPU limit: with no quota, throttling is impossible, utilization vs
// limit is 0, and PSI stays low while the node has spare cores, so a service
// burning 30x its normal CPU crossed no other gate. The cpu arm judges cores
// used (summed across the service's pods) against the service's own median.
// Every field below takes its default in setDefaults when zero/nil, so a zero
// BaselineOptions{} means "enabled, with these defaults" (flags are wired in
// cmd/).
type BaselineOptions struct {
	// Enabled switches both arms. A *bool so that nil (unset) can default to
	// true, which a plain bool cannot express; point at false to disable.
	Enabled *bool
	// EvidenceWindows is how many of the newest windows a verdict may sum (5).
	EvidenceWindows int
	// MinBaselineWindows is the baseline readings a service needs before it is
	// judged against its median; a cold service never breaches the cpu arm (5).
	MinBaselineWindows int
	// Multiplier: breach needs observed >= Multiplier x baseline median (3).
	Multiplier float64
	// CPUFloorCores is the absolute minimum cores used for a cpu breach (0.25).
	CPUFloorCores float64
	// CPUColdFloorCores is the cores used (summed across the service's pods) at
	// or above which a service with NO baseline yet breaches and is not
	// learned, so an agent that starts while a service is burning CPU reports
	// it instead of taking it for normal. 0 (default) disables it: a cold
	// service never breaches the cpu arm and everything it reads is learned.
	// No default, because no number of cores is abnormal for every service.
	CPUColdFloorCores float64
	// ThrottleFloorPct is the minimum throttled-period percentage for a
	// throttle breach (10).
	ThrottleFloorPct float64
	// ThrottleMinPeriods is the cfs periods the summed windows must reach
	// before the throttle share is judged (50).
	ThrottleMinPeriods float64
}

// enabled reports whether the baseline arms run (nil means yes).
func (b BaselineOptions) enabled() bool { return b.Enabled == nil || *b.Enabled }

// throttleColdBreachPct is the throttled share at or above which a service with
// no baseline yet still breaches: it is throttled for most periods, which is
// abnormal whatever its history.
const throttleColdBreachPct = 50

func (o *Options) setDefaults() {
	if o.Interval <= 0 {
		o.Interval = 60 * time.Second
	}
	// MemPsiThreshold/CPUPsiThreshold/IOPsiThreshold are deliberately NOT
	// re-defaulted here — 0 is a legitimate "always report this dimension" value.
	// Mirrors the redsource.Options.MinErrorRatePct rate-floor-zero fix
	// (docs/adr/0010-use-metrics-node-exporter.md): the flag/chart defaults
	// (20/40/40) live in main.go/values.yaml, not here. Do NOT add an
	// `if o.XPsiThreshold <= 0` default — that would silently override an
	// operator's explicit opt-in to "report every window for this dimension".
	// Baseline: unlike the PSI thresholds, a zero here is never meaningful
	// (a 0 floor or multiplier would breach on noise), so zero takes the default.
	b := &o.Baseline
	if b.EvidenceWindows <= 0 {
		b.EvidenceWindows = 5
	}
	if b.MinBaselineWindows <= 0 {
		b.MinBaselineWindows = 5
	}
	if b.Multiplier <= 0 {
		b.Multiplier = 3
	}
	if b.CPUFloorCores <= 0 {
		b.CPUFloorCores = 0.25
	}
	if b.ThrottleFloorPct <= 0 {
		b.ThrottleFloorPct = 10
	}
	if b.ThrottleMinPeriods <= 0 {
		b.ThrottleMinPeriods = 50
	}
	if o.now == nil {
		o.now = time.Now
	}
}

// Collector periodically reads the sensors' node-exporter metrics
// and forwards qualifying services to a Sender. Gated behind leader election so only
// one replica reads + sends.
type Collector struct {
	opts Options
	// identity resolves a pod to its service identity (identity.go). In a
	// cluster it is an OwnerIdentityResolver over the manager's CACHED
	// client (mgr.GetClient()), not an API-server reader: the pod Get itself
	// runs per distinct pod read every window, so it must read informers
	// rather than issue a live API-server call per pod per window. The
	// owner-object Get(s) are bounded by its per-owner TTL cache.
	identity IdentityResolver
	sender   Sender
	log      logr.Logger

	// nsAllow is Options.Namespaces as a set; nil means "forward every namespace".
	nsAllow map[string]bool

	// prev holds the last-seen cumulative counters per (target, podKey), so a
	// delta survives individual pod restarts/scaling independently — see
	// computeDeltas in deltas.go.
	prev map[string]map[podKey]podCounters
	// prevAbsent counts, per target in prev, the consecutive reads it was not a
	// live target; see runOnce's baseline GC.
	prevAbsent map[string]int

	// cpuBase/throttleBase hold the per-service baselines of the two baseline
	// arms, keyed "namespace/service". Touched only from runOnce.
	cpuBase, throttleBase *baseline.Tracker
}

// namespaceSet turns Options.Namespaces into a lookup set. Returns nil for an
// empty/blank-only list, which the collector reads as "forward every namespace".
func namespaceSet(list []string) map[string]bool {
	var set map[string]bool
	for _, ns := range list {
		if ns = strings.TrimSpace(ns); ns != "" {
			if set == nil {
				set = make(map[string]bool, len(list))
			}
			set[ns] = true
		}
	}
	return set
}

// New creates a Collector. Returns nil if opts.Source is nil (collector
// disabled) or sender is nil (push mode disabled — nothing to send to).
// identity may be nil: every pod then keeps the exporter's reported service.
func New(opts Options, identity IdentityResolver, sender Sender, log logr.Logger) *Collector {
	if opts.Source == nil || sender == nil {
		return nil
	}
	opts.setDefaults()
	return &Collector{
		opts:         opts,
		nsAllow:      namespaceSet(opts.Namespaces),
		identity:     identity,
		sender:       sender,
		log:          log.WithName("usemetrics"),
		prev:         make(map[string]map[podKey]podCounters),
		prevAbsent:   make(map[string]int),
		cpuBase:      baseline.New(),
		throttleBase: baseline.New(),
	}
}

// NeedLeaderElection ensures only the leader replica reads + sends.
func (c *Collector) NeedLeaderElection() bool { return true }

// namespaceAllowed reports whether ns passes the collector's namespace filter:
// Options.NamespaceAllowed when non-nil (dynamic — hot-follows annotation-driven
// push-scope changes), else the static nsAllow set (nil set = every namespace).
func (c *Collector) namespaceAllowed(ns string) bool {
	if c.opts.NamespaceAllowed != nil {
		return c.opts.NamespaceAllowed(ns)
	}
	return c.nsAllow == nil || c.nsAllow[ns]
}

// Start evaluates on a fixed interval until ctx ends.
func (c *Collector) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.runOnce(ctx)
		}
	}
}

// aggregate accumulates one (namespace, service) pair's USE counters across every
// pod (possibly on several nodes) reporting for it this window.
type aggregate struct {
	cpuPsi, memPsi, ioPsi, memPct float64 // max across the pair's pods
	// cpuPct is max across the pair's pods, like memPct: a service's
	// utilization is its WORST pod, never an average that would hide one pod
	// pinned at its quota behind healthy siblings.
	cpuPct float64
	// memBytes/memLimitBytes/cpuLimitCores come from the worst pod by memPct so
	// the absolute figures describe the same pod the percentage does. Reporting
	// a max-across-pods percentage beside a different pod's byte count would be
	// two readings of two pods presented as one.
	memBytes, memLimitBytes, cpuLimitCores float64
	// memRequestBytes/cpuRequestCores and memNodePct/cpuNodePct/memReqPct/
	// cpuReqPct (2026-08-29) extend the SAME worst-pod-by-memPct rule above:
	// every new absolute and every new percentage is taken from that identical
	// chosen pod, never recombined from whichever pod happened to maximize a
	// different dimension. A memBytes/memNodeTotalBytes pairing from two
	// different pods would make "% of node" describe a pod that isn't the one
	// the byte figures describe.
	memRequestBytes, cpuRequestCores             float64
	memNodePct, cpuNodePct, memReqPct, cpuReqPct float64
	oomKillSum                                   int64 // sum of deltas across the pair's pods
	periodsSum, throttledSum                     int64 // sum of deltas, used for ThrottledPct

	// cpuUsageSum is the SUM of the pods' usage_usec deltas (a service burning
	// CPU on 3 replicas burns 3x), cpuUsageValid is true when at least one pod
	// had a real delta, and hasCPULimit when any pod has a CPU quota.
	// cpuCoresSum is the same as cores: each pod's delta over the real time
	// between the two sets it spans (podDelta.cpuCoresUsed), which is what the
	// cpu baseline arm judges.
	cpuUsageSum   float64
	cpuCoresSum   float64
	cpuUsageValid bool
	hasCPULimit   bool
	// Baseline-arm result for this window; breach is "" (none), push.BreachCPU
	// or push.BreachThrottle, with the verdict's numbers.
	breach                 string
	brObserved, brBaseline float64
	brEvidenceSec          int
	brStill                bool // the periodic report of a muted key

	bestScore  float64 // worst-offender score seen so far, see worstPodScore
	bestSample string
}

// runOnce reads every sensor's latest node-exporter set, aggregates per-service USE
// counters, applies the gate, and sends. A sensor without a set is logged
// and skipped — one dead node-exporter pod must not block the others' counters from
// being reported.
func (c *Collector) runOnce(ctx context.Context) {
	windowStart := c.opts.now()
	targets := c.opts.Source.Targets(metricset.RoleNodeExporter)
	if len(targets) == 0 {
		ledger.Default.Forget(metricset.RoleNodeExporter, nil)
		c.log.V(1).Info("no sensor has sent node-exporter metrics; nothing to evaluate")
		return
	}

	// Garbage-collect baselines for sensors that are gone. Without
	// this, DaemonSet pod churn (rollouts, node drains) accretes dead baselines in
	// c.prev forever. Keyed off targets (not successful reads), so a temporarily
	// failing target keeps its baseline instead of losing it to one bad read.
	live := make(map[string]bool, len(targets))
	for _, addr := range targets {
		live[addr] = true
	}
	// A target absent from the live list keeps its baselines for up to
	// baselineGraceReads reads rather than losing them at once: dropping them on
	// the first absence (a sensor whose snapshot went briefly stale) made every
	// pod on it "first seen" on return, reporting 0 for any kill in the gap.
	if c.prevAbsent == nil {
		c.prevAbsent = make(map[string]int)
	}
	for addr := range c.prev {
		if live[addr] {
			delete(c.prevAbsent, addr)
			continue
		}
		c.prevAbsent[addr]++
		if c.prevAbsent[addr] > baselineGraceReads {
			c.log.V(1).Info("target baselines dropped: not a live target for too many reads",
				"target", addr, "absentReads", c.prevAbsent[addr], "pods", len(c.prev[addr]))
			delete(c.prev, addr)
			delete(c.prevAbsent, addr)
		}
	}
	// Reclaim owner-identity cache entries past their TTL (identity.go) —
	// same per-tick GC posture as the c.prev cleanup above.
	if g, ok := c.identity.(interface{ GC(time.Time) }); ok {
		g.GC(windowStart)
	}

	ledger.Default.Forget(metricset.RoleNodeExporter, live)
	totals := make(map[servicePair]*aggregate)
	// A pod the DaemonSet read this tick is not read again from an OTLP
	// source (kubelet cAdvisor through a Collector): sensors first.
	type podRef struct{ namespace, pod string }
	sensorPods := make(map[podRef]bool)
	for _, addr := range metricset.SensorsFirst(targets) {
		otlp := metricset.IsOTLPTarget(addr)
		set, at, err := c.readSet(addr)
		if err != nil {
			c.log.V(1).Info("no node-exporter metrics from sensor; skipping", "target", addr, "reason", err.Error())
			ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.NoSnapshot, 1)
			continue
		}
		deltas, order := c.computeDeltasAt(addr, set, at)
		ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.Ticks, 1)
		ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.Points, int64(set.PointCount()))
		ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.Services, int64(len(order)))
		for _, key := range order {
			// Drop other tenants' namespaces before they can be aggregated — this
			// connection stamps every frame with its own single handshake-bound
			// tenant, so forwarding them would mis-attribute them.
			if !c.namespaceAllowed(key.pair.Namespace) {
				ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.Filtered, 1)
				continue
			}
			ref := podRef{namespace: key.pair.Namespace, pod: key.pod}
			if otlp && sensorPods[ref] {
				continue
			}
			if !otlp {
				sensorPods[ref] = true
			}
			if c.opts.PodExcluded != nil && c.opts.PodExcluded(ctx, key.pair.Namespace, key.pod) {
				continue
			}
			pair := c.resolvePair(ctx, key)
			// A pod nobody could name a service for (no workload identity
			// and no reported service label, as kubelet cAdvisor series
			// from an OTLP sender are) has nothing to file under: drop it
			// and count it rather than send a service with no name.
			if pair.Service == "" {
				ledger.Default.Add(metricset.RoleNodeExporter, addr, ledger.Unattributed, 1)
				continue
			}
			d := deltas[key]
			agg := totals[pair]
			if agg == nil {
				agg = &aggregate{}
				totals[pair] = agg
			}
			if d.cpuPsi > agg.cpuPsi {
				agg.cpuPsi = d.cpuPsi
			}
			if d.memPsi > agg.memPsi {
				agg.memPsi = d.memPsi
			}
			if d.ioPsi > agg.ioPsi {
				agg.ioPsi = d.ioPsi
			}
			// The chosen pod is the one with the highest memPct, with ties
			// broken by memBytes (2026-08-29). The tiebreak is not cosmetic:
			// memPct is legitimately 0 for any pod with no memory limit set
			// (see nodeexporter.memLimitBytes), so a service where EVERY pod
			// is unlimited has every d.memPct tied at 0 against agg's
			// zero-initialized starting value — without a tiebreak, the
			// strict '>' below would never select ANY pod, and
			// memNodePct/memReqPct (whose entire purpose is covering exactly
			// the no-limit case) would silently stay unpopulated for the one
			// class of service that most needs them. Breaking the tie by
			// actual bytes used still picks the single most relevant pod.
			if d.memPct > agg.memPct || (d.memPct == agg.memPct && d.memBytes > agg.memBytes) {
				agg.memPct = d.memPct
				// Absolutes follow the worst pod by memPct, so the byte
				// figures and the percentage always describe one pod. The
				// node/request percentages and the request absolutes
				// (2026-08-29) ride the same selection for the same reason.
				agg.memBytes = d.memBytes
				agg.memLimitBytes = d.memLimitBytes
				agg.memRequestBytes = d.memRequestBytes
				agg.cpuRequestCores = d.cpuRequestCores
				agg.memNodePct = d.memNodePct
				agg.cpuNodePct = d.cpuNodePct
				agg.memReqPct = d.memReqPct
				agg.cpuReqPct = d.cpuReqPct
			}
			if d.cpuPct > agg.cpuPct {
				agg.cpuPct = d.cpuPct
				agg.cpuLimitCores = d.cpuLimitCores
			}
			if d.cpuUsageValid {
				agg.cpuUsageValid = true
				agg.cpuUsageSum += d.cpuUsageDelta
				agg.cpuCoresSum += d.cpuCoresUsed
			}
			if d.cpuLimitCores > 0 {
				agg.hasCPULimit = true
			}
			if d.oomKillDelta > 0 {
				agg.oomKillSum += d.oomKillDelta
			}
			if d.periodsDelta > 0 {
				agg.periodsSum += d.periodsDelta
			}
			if d.throttledDelta > 0 {
				agg.throttledSum += d.throttledDelta
			}
			// Strict '>' (not '>=') means the FIRST pod to reach a given max score
			// wins ties — order is deterministic (see computeDeltas), so this is
			// deterministic run to run rather than dependent on map iteration.
			if score := worstPodScore(d); score > agg.bestScore {
				agg.bestScore = score
				agg.bestSample = formatUseSample(key.pod, d)
			}
		}
	}

	c.sendUseSummary(windowStart, totals)
	c.judgeBaselines(totals)

	var qualifying []push.ServiceUseMetrics
	unattributed := int64(0)
	for pair, agg := range totals {
		// Gate: any one of the three PSI floors, OR memory utilization at/above
		// its own floor, OR at least one OOM kill this window qualifies a
		// service — see docs/adr/0010-use-metrics-node-exporter.md. Unlike
		// redsource's AND gate, these are independent OR arms: each measures a
		// different resource, and pressure on any one of them is worth
		// surfacing on its own.
		//
		// The utilization arm (2026-08-29) is the LEADING one. The other four
		// all fire at or after the moment a service starts suffering — PSI
		// needs an actual reclaim stall, OOMKills needs a kill that already
		// happened — so a pod holding a resident working set at its limit
		// emitted nothing whatsoever until it died. A zero threshold disables
		// this arm rather than opening it, because 0% utilization means "no
		// limit is set" here; see Options.MemPctThreshold.
		//
		// The node-utilization arm (also 2026-08-29, Options.MemNodePctThreshold)
		// is the portable sibling of the limit-relative one above: it fires off
		// the node's total memory, which always exists, so it can still catch a
		// single pod eating the box even when the pod carries no limit for the
		// arm above to divide by. Defaulted to 0/disabled pending observed
		// fleet numbers — see that field's doc comment.
		qualifies := agg.memPsi >= c.opts.MemPsiThreshold ||
			agg.cpuPsi >= c.opts.CPUPsiThreshold ||
			agg.ioPsi >= c.opts.IOPsiThreshold ||
			(c.opts.MemPctThreshold > 0 && agg.memPct >= c.opts.MemPctThreshold) ||
			(c.opts.MemNodePctThreshold > 0 && agg.memNodePct >= c.opts.MemNodePctThreshold) ||
			agg.oomKillSum >= 1 ||
			agg.breach != ""
		if !qualifies {
			ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Gated, 1)
			continue
		}
		// Pods with no attributable service leave Service empty. Skip them: the
		// receiver rejects the whole frame over one empty entry, and an
		// unattributable service could never resolve in the hub's graph anyway.
		if pair.Service == "" {
			unattributed++
			ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Unattributed, 1)
			continue
		}
		var throttledPct float64
		if agg.periodsSum > 0 {
			throttledPct = float64(agg.throttledSum) * 100 / float64(agg.periodsSum)
		}
		sample := agg.bestSample
		if sample == "" && agg.breach == push.BreachCPU {
			sample = fmt.Sprintf("cpu %.2g cores vs baseline %.2g cores", agg.brObserved, agg.brBaseline)
		} else if sample == "" && agg.breach == push.BreachThrottle {
			sample = fmt.Sprintf("throttled %.0f%% of periods vs baseline %.0f%%", agg.brObserved, agg.brBaseline)
		}
		qualifying = append(qualifying, push.ServiceUseMetrics{
			Namespace:   pair.Namespace,
			Service:     pair.Service,
			CPUPsiAvg60: agg.cpuPsi,
			MemPsiAvg60: agg.memPsi,
			IOPsiAvg60:  agg.ioPsi,
			// CPUPct stays 0 in v1 — see metrics.go's MetricMemPct doc comment for
			// why CPU utilization-vs-limit isn't computed yet.
			MemPct:       agg.memPct,
			OOMKills:     agg.oomKillSum,
			ThrottledPct: throttledPct,
			Sample:       push.TruncateUseSample(sample),
			Breach:       agg.breach,
			Observed:     agg.brObserved,
			Baseline:     agg.brBaseline,
			EvidenceSec:  agg.brEvidenceSec,

			StillBreaching: agg.brStill,
		})
	}
	if unattributed > 0 {
		c.log.Info("skipped unattributable pods (no resolvable service identity)",
			"count", unattributed)
	}
	ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Ticks, 1)
	ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Services, int64(len(totals)))
	if len(qualifying) == 0 {
		c.log.V(1).Info("no services crossed the PSI/OOM gate; no frame sent",
			"memPctThreshold", c.opts.MemPctThreshold, "memNodePctThreshold", c.opts.MemNodePctThreshold,
			"memPsiThreshold", c.opts.MemPsiThreshold, "cpuPsiThreshold", c.opts.CPUPsiThreshold,
			"ioPsiThreshold", c.opts.IOPsiThreshold, "servicesRead", len(totals))
		return
	}

	// Sender-side cap (docs/adr/0010-use-metrics-node-exporter.md): a window that
	// qualifies more than MaxUseMetricsServices keeps only the top services and
	// drops the rest for this window, mirroring redsource's own cap (see that
	// ADR's "Why one frame, capped, instead of chunking" for the reasoning).
	// Baseline-breached services rank first: a cpu-only breach has memPsi 0 and
	// would otherwise always be the first one cut. Then memPsiAvg60 desc, then
	// namespace/service so the cut does not depend on map iteration.
	if len(qualifying) > push.MaxUseMetricsServices {
		sort.Slice(qualifying, func(i, j int) bool {
			a, b := qualifying[i], qualifying[j]
			if (a.Breach != "") != (b.Breach != "") {
				return a.Breach != ""
			}
			if a.MemPsiAvg60 != b.MemPsiAvg60 {
				return a.MemPsiAvg60 > b.MemPsiAvg60
			}
			if a.Namespace != b.Namespace {
				return a.Namespace < b.Namespace
			}
			return a.Service < b.Service
		})
		c.log.Info("more services qualified than fit in one frame; keeping breaches, then the top by memPsiAvg60",
			"qualified", len(qualifying), "kept", push.MaxUseMetricsServices)
		ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Capped, int64(len(qualifying)-push.MaxUseMetricsServices))
		qualifying = qualifying[:push.MaxUseMetricsServices]
	}
	ledger.Default.Add(metricset.RoleNodeExporter, ledger.All, ledger.Sent, int64(len(qualifying)))

	windowSec := int(c.opts.Interval / time.Second)
	c.sender.SendUseMetrics(windowStart.Unix(), windowSec, qualifying)
	c.log.Info("sent use_metrics frame", "services", len(qualifying), "windowSec", windowSec)
}

// judgeBaselines runs the cpu-cores and cpu-throttling baseline arms over this
// window's per-service totals and records the result on each aggregate
// (agg.breach and friends), which the gate then ORs in. Per window it follows
// baseline's contract: Observe, Judge, Settle for every service, then GC and
// Tick once.
//
// cpu: cores = each pod's usage delta over the real time between the two sets
// it spans, summed (sum, not max: usage is a counter and a service burning on
// N replicas burns Nx), observed as cpu-seconds over window-seconds so Ratio
// gives cores. A window with no fresh set is skipped. One window is enough
// evidence; a cold service breaches only at CPUColdFloorCores (off by
// default), so a fresh agent stays quiet until MinBaselineWindows readings
// exist.
//
// throttle: only for services with a CPU quota. Observed as the throttled share
// of cfs periods over up to EvidenceWindows windows until ThrottleMinPeriods
// periods are summed. A cold service breaches only at >= 50% throttled, and
// such a reading is not learned.
//
// If both breach, throttle wins: it is direct saturation evidence, while cpu
// merely says usage rose.
func (c *Collector) judgeBaselines(totals map[servicePair]*aggregate) {
	b := c.opts.Baseline
	if !b.enabled() || c.cpuBase == nil {
		return
	}
	windowSec := c.opts.Interval.Seconds()
	cpuGate := baseline.Gate{
		Windows: b.EvidenceWindows, WindowSec: int(windowSec), MinEvidence: windowSec,
		MinBaseline: b.MinBaselineWindows, Multiplier: b.Multiplier, Floor: b.CPUFloorCores,
		ColdFloor: b.CPUColdFloorCores, Value: baseline.Rate,
	}
	thrGate := baseline.Gate{
		Windows: b.EvidenceWindows, WindowSec: int(windowSec), MinEvidence: b.ThrottleMinPeriods,
		MinBaseline: b.MinBaselineWindows, Multiplier: b.Multiplier, Floor: b.ThrottleFloorPct,
		ColdFloor: throttleColdBreachPct, Value: baseline.Percent,
	}
	for pair, agg := range totals {
		if pair.Service == "" {
			continue
		}
		key := pair.Namespace + "/" + pair.Service
		if agg.cpuUsageValid {
			// cpu-seconds used over a nominal window = cores x window seconds.
			c.cpuBase.Observe(key, agg.cpuCoresSum*windowSec, windowSec)
			v := c.cpuBase.Judge(key, cpuGate)
			if v.Observed >= 0.05 {
				c.log.V(1).Info("cpu reading", "key", key, "cores", v.Observed, "baseline", v.Baseline, "cold", v.Cold)
			}
			c.settle(c.cpuBase, push.BreachCPU, key, v)
			if v.Breach {
				agg.setBreach(push.BreachCPU, v)
			}
		}
		if agg.hasCPULimit {
			c.throttleBase.Observe(key, float64(agg.throttledSum), float64(agg.periodsSum))
			v := c.throttleBase.Judge(key, thrGate)
			c.settle(c.throttleBase, push.BreachThrottle, key, v)
			if v.Breach {
				agg.setBreach(push.BreachThrottle, v)
			}
		}
		if agg.breach != "" {
			c.log.V(1).Info("baseline breach",
				"namespace", pair.Namespace, "service", pair.Service, "arm", agg.breach,
				"observed", agg.brObserved, "baseline", agg.brBaseline,
				"evidenceSec", agg.brEvidenceSec)
		}
	}
	c.cpuBase.GC(cpuGate)
	c.cpuBase.Tick()
	c.throttleBase.GC(thrGate)
	c.throttleBase.Tick()
	agentmetrics.BaselineMutedKeys.WithLabelValues(agentmetrics.SensorUSE, push.BreachCPU).Set(float64(c.cpuBase.Muted()))
	agentmetrics.BaselineMutedKeys.WithLabelValues(agentmetrics.SensorUSE, push.BreachThrottle).Set(float64(c.throttleBase.Muted()))
}

// readSet reads addr's latest node-exporter set and, when the source knows,
// the time it was received (zero otherwise).
func (c *Collector) readSet(addr string) (metricset.Set, time.Time, error) {
	if ts, ok := c.opts.Source.(metricset.TimedSource); ok {
		return ts.SetAt(metricset.RoleNodeExporter, addr)
	}
	set, err := c.opts.Source.Set(metricset.RoleNodeExporter, addr)
	return set, time.Time{}, err
}

// settle settles the verdict and logs a change of the key's muted state, so a
// mute is never silent (see also agentmetrics.BaselineMutedKeys).
func (c *Collector) settle(t *baseline.Tracker, arm, key string, v baseline.Verdict) {
	switch t.Settle(key, v) {
	case baseline.MuteStarted:
		c.log.Info("baseline key muted: its breaches recur, so it now reports every few windows instead of every window",
			"arm", arm, "key", key, "observed", v.Observed, "median", v.Baseline, "reference", v.Reference,
			"reportEveryWindows", baseline.StillBreachingEvery)
	case baseline.MuteEnded:
		c.log.Info("baseline key no longer muted: its breaches stopped recurring", "arm", arm, "key", key)
	}
}

// setBreach records a baseline verdict on the aggregate. Throttle overrides a
// cpu breach already recorded; cpu never overrides throttle.
func (a *aggregate) setBreach(arm string, v baseline.Verdict) {
	if a.breach == push.BreachThrottle && arm == push.BreachCPU {
		return
	}
	a.breach, a.brObserved, a.brBaseline, a.brEvidenceSec = arm, v.Observed, v.Baseline, v.EvidenceSec
	a.brStill = v.StillBreaching
}

// resolvePair resolves the (namespace, service) aggregation key for one
// pod read this window: the controller-resolved workload identity (identity.go) when
// that resolution succeeds, otherwise the exporter's own reported service=
// label unchanged. With neither, the service is empty and the caller drops
// the sample.
func (c *Collector) resolvePair(ctx context.Context, key podKey) servicePair {
	identity, ok := c.resolveServiceIdentity(ctx, key.pair.Namespace, key.pod)
	if !ok {
		c.log.V(1).Info("service identity resolution failed; falling back to the reported service label",
			"namespace", key.pair.Namespace, "pod", key.pod, "reported", key.pair.Service)
		return key.pair
	}
	return servicePair{Namespace: key.pair.Namespace, Service: identity}
}

// sendUseSummary sends an UNGATED use_summary frame covering every service
// read this window — not just the ones that crossed runOnce's PSI/OOM gate —
// so the hub can show utilization and saturation for the whole read service
// set, not only the ones anomalous enough to be gated into a use_metrics frame.
//
// Built from the SAME totals this tick's gate loop consumes, but deliberately
// NOT via push.ServiceUseMetrics: ServiceUseSummary is its own struct, not a
// reuse — ThrottledPct/Sample stay gated-frame-only fields that never appear
// here (see ServiceUseSummary's doc comment). An aggregate with no
// attributable service (the Profile couldn't resolve a workload — the same
// "unattributed" case the gate loop above skips into its own counter) is
// excluded here too: it isn't a service to summarize.
//
// MemPct rides this UNGATED frame deliberately (2026-08-29). agg.memPct has
// been max-aggregated across the service's pods on this exact code path since
// the pipeline shipped, and was used only by the GATED frame above — so a
// service's memory-vs-limit ratio reached the hub only after that service had
// already crossed a PSI or OOM threshold, which is precisely when it has
// stopped being an early warning. Sending it every window is what lets the hub
// see a pod climbing toward its limit while every PSI dimension still reads a
// truthful 0%. CPUPct/MemBytes/MemLimitBytes/CPULimitCores followed the same
// day, and MemNodePct/CPUNodePct/MemReqPct/CPUReqPct/MemRequestBytes/
// CPURequestCores (2026-08-29) extend it further — the node/request
// percentages and the request absolutes, all taken from the same
// worst-pod-by-memPct selection as the existing absolutes (see aggregate's own
// doc comment).
func (c *Collector) sendUseSummary(windowStart time.Time, totals map[servicePair]*aggregate) {
	services := make([]push.ServiceUseSummary, 0, len(totals))
	for pair, agg := range totals {
		if pair.Service == "" {
			continue
		}
		services = append(services, push.ServiceUseSummary{
			Namespace:       pair.Namespace,
			Service:         pair.Service,
			CPUPsiPct:       agg.cpuPsi,
			MemPsiPct:       agg.memPsi,
			IOPsiPct:        agg.ioPsi,
			OOMKills:        agg.oomKillSum,
			MemPct:          agg.memPct,
			CPUPct:          agg.cpuPct,
			MemBytes:        agg.memBytes,
			MemLimitBytes:   agg.memLimitBytes,
			CPULimitCores:   agg.cpuLimitCores,
			MemNodePct:      agg.memNodePct,
			CPUNodePct:      agg.cpuNodePct,
			MemReqPct:       agg.memReqPct,
			CPUReqPct:       agg.cpuReqPct,
			MemRequestBytes: agg.memRequestBytes,
			CPURequestCores: agg.cpuRequestCores,
		})
	}
	if len(services) == 0 {
		// Nothing attributable was read this window — an empty frame is
		// noise, not signal (mirrors redsource.Collector.sendRedSummary).
		c.log.V(1).Info("no services read this window; no use_summary frame sent")
		return
	}
	if len(services) > push.MaxUseMetricsServices {
		dropped := len(services) - push.MaxUseMetricsServices
		// Deterministic order: memPsiPct desc — the same "top by memPsiAvg60"
		// ranking runOnce's own gated-overflow cap uses above — then
		// namespace/service asc as a tie-break so ties don't depend on map
		// iteration.
		sort.Slice(services, func(i, j int) bool {
			if services[i].MemPsiPct != services[j].MemPsiPct {
				return services[i].MemPsiPct > services[j].MemPsiPct
			}
			if services[i].Namespace != services[j].Namespace {
				return services[i].Namespace < services[j].Namespace
			}
			return services[i].Service < services[j].Service
		})
		c.log.V(1).Info("more services read than fit in one use_summary frame; keeping the top by memPsiPct",
			"observed", len(services), "kept", push.MaxUseMetricsServices, "dropped", dropped)
		services = services[:push.MaxUseMetricsServices]
	}
	windowSec := int(c.opts.Interval / time.Second)
	c.sender.SendUseSummary(windowStart.Unix(), windowSec, services)
	c.log.V(1).Info("sent use_summary frame", "services", len(services), "windowSec", windowSec)
}

// worstPodScore scores one pod's badness for the Sample worst-offender pick: an OOM
// kill this window always dominates a PSI-only reading (a killed process matters
// more than elevated pressure that didn't kill anything), otherwise the pod's
// highest single PSI dimension.
func worstPodScore(d podDelta) float64 {
	score := d.cpuPsi
	if d.memPsi > score {
		score = d.memPsi
	}
	if d.ioPsi > score {
		score = d.ioPsi
	}
	if d.oomKillDelta > 0 {
		score += 1000 // always outrank a PSI-only reading
	}
	return score
}

// formatUseSample renders one pod's notable USE signals as
// "pod api-7f9: mem psi 34%, oom_kill +1" — only the dimensions that are actually
// nonzero appear, in mem/cpu/io/oom_kill order.
func formatUseSample(pod string, d podDelta) string {
	var parts []string
	if d.memPsi > 0 {
		parts = append(parts, fmt.Sprintf("mem psi %.0f%%", d.memPsi))
	}
	if d.cpuPsi > 0 {
		parts = append(parts, fmt.Sprintf("cpu psi %.0f%%", d.cpuPsi))
	}
	if d.ioPsi > 0 {
		parts = append(parts, fmt.Sprintf("io psi %.0f%%", d.ioPsi))
	}
	if d.oomKillDelta > 0 {
		parts = append(parts, fmt.Sprintf("oom_kill +%d", d.oomKillDelta))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("pod %s", pod)
	}
	return fmt.Sprintf("pod %s: %s", pod, strings.Join(parts, ", "))
}

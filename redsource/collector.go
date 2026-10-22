// Package redsource implements ONE profile-driven collector for the RED
// (red_metrics) and topology (topology_snapshot) frame families
// (docs/adr/0013-red-source-profiles.md). A Profile (profile.go/profiles.go)
// holds the instrumenter's naming: metric names, attribute keys, an error
// predicate and a sample formatter. The engine (this file and deltas.go)
// reads Profile fields and never branches on a profile's name.
//
// The collector reads each sensor's eBPF metrics from the sensor stream
// (docs/adr/0015-sensor-stream-grpc.md), computes first-seen-baseline +
// counter-reset-safe deltas per sensor, applies an absolute-AND-rate RED
// gate, MaxRedMetricsServices/MaxTopologyNodes/MaxTopologyEdges caps, and
// the "no sensor delivered this tick -> skip the send, a lost frame
// heals on the next tick" rule for topology.
//
// RED and topology share ONE read per Options.Interval tick. RED
// evaluates its gate every Interval tick; topology flushes a full snapshot
// every Options.TopologyInterval, from deltas ACCUMULATED across the
// Interval ticks since the last flush rather than a single read at flush
// time, so a transient failure on one intermediate tick does not blank the
// whole topology window (see flushTopology).
package redsource

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/baseline"
	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// Sender is the subset of *push.Pusher the collector needs: SendRedMetrics
// and SendTopologySnapshot, unified into one interface here because this
// collector calls both from the same tick's read, plus SendRedSummary (the
// UNGATED per-window RED summary companion to SendRedMetrics: every service
// read, not just gate-crossers; see sendRedSummary below). A small
// interface (rather than importing *push.Pusher directly) so tests can fake
// it, and so a nil *push.Pusher
// (push mode disabled) satisfies it via a plain nil interface check in New.
type Sender interface {
	SendRedMetrics(windowStart int64, windowSec int, services []push.ServiceRedMetrics)
	SendRedSummary(windowStart int64, windowSec int, services []push.ServiceRedMetrics)
	// SendTopologySnapshot returns whether the frame was actually enqueued
	// (false when push is disconnected, or when the send queue is full) —
	// flushTopology depends on this to decide whether it may clear its
	// accumulators (see flushTopology's doc comment).
	SendTopologySnapshot(capturedAt int64, windowSec int, nodes []push.TopologyNode, edges []push.TopologyEdge) bool
}

// Options configures the Collector. All fields have defaults applied in New
// except Source, Profile, and at least one of EnableRED/EnableTopology,
// which callers must set explicitly.
type Options struct {
	// Profile is the vendor-specific naming table this engine instance is
	// driven by — see profile.go and the two built-ins in profiles.go.
	Profile Profile

	// Source delivers each sensor's eBPF metrics
	// (docs/adr/0015-sensor-stream-grpc.md).
	Source metricset.Source
	// Namespaces, when non-empty, restricts RED to samples whose destination
	// is in this set, and restricts topology edges/nodes to those whose
	// SOURCE (edges) or destination (nodes, RED) is in this set — a
	// destination outside scope on an edge is reported as External rather
	// than dropped. Empty forwards every namespace read. Set it to the
	// same list as --target-namespaces: one push connection carries a single
	// tenant, and a sensor target shared across tenants (a DaemonSet this
	// chart deploys, or a platform-owned agent it doesn't) observes traffic
	// across tenant boundaries.
	Namespaces []string
	// NamespaceAllowed, when non-nil, OVERRIDES the static Namespaces set
	// entirely: it is consulted per payload build for every sample's
	// namespace (RED destinations, topology sources, and the in-scope vs
	// External destination split alike), so it hot-follows dynamic scope
	// changes — main.go wires it to push.Pusher.NamespaceInScope, whose
	// scope union includes namespaces pulled in live by the triageagent.dev/tenant
	// Namespace annotation (push/namespace_scope.go), which a static list
	// frozen at process start can never see. Nil keeps the static
	// Namespaces behavior unchanged (empty list = forward every namespace).
	NamespaceAllowed func(string) bool
	// Interval is how often the collector reads each sensor's snapshot,
	// evaluates the RED gate, and accumulates topology deltas. Default 60s.
	Interval time.Duration
	// TopologyInterval is how often the ACCUMULATED topology deltas (summed
	// across every Interval tick since the last flush) are sent as one
	// topology_snapshot frame. Default 600s — level state, not a windowed
	// anomaly report, does not need RED's latency. Rounded down to the
	// nearest whole multiple of Interval (minimum one tick).
	TopologyInterval time.Duration

	// ErrorThreshold/MinErrorRatePct are the RED gate: a destination must
	// cross BOTH to qualify for a red_metrics frame. MinErrorRatePct's
	// zero-is-legitimate caveat applies here unchanged: do not re-default it
	// away.
	ErrorThreshold  int64
	MinErrorRatePct float64
	// OperationErrorRatePct is a second, independent way through the gate: a
	// service ALSO qualifies when any single operation of it (an HTTP
	// method+route, a gRPC method — RequestMetric.SampleLabels) has
	// ErrorCount >= ErrorThreshold in the window AND an own error rate of at
	// least this percentage. It exists because a wholly broken route is an
	// incident even when page and asset traffic dilutes the service-wide
	// rate under MinErrorRatePct. 0 disables the arm (the service-wide gate
	// alone applies); as with MinErrorRatePct, do not re-default a zero.
	OperationErrorRatePct float64

	// LatencyMultiplier is a third, independent way through the gate: a
	// service ALSO qualifies when its p99 this window is at least this many
	// times the MEDIAN of its own recent p99 readings (the latency baseline,
	// see latencyBaseline) AND at least LatencyFloorMs AND it served at least
	// LatencyMinRequests requests AND it has at least LatencyMinBaselineWindows
	// baseline readings. It exists because a service whose responses are
	// delayed from milliseconds to seconds, with zero errors, crosses no error
	// gate. 0 disables the arm (so there is deliberately NO default applied
	// for it here: main.go's flag default is 5); as with MinErrorRatePct, do
	// not re-default a zero.
	LatencyMultiplier float64
	// LatencyFloorMs is the absolute p99 (milliseconds) the latency arm needs
	// on top of the multiplier, so 1 ms -> 6 ms never fires. Default 1000.
	LatencyFloorMs float64
	// LatencyMinRequests is the request count a window needs for its p99 to be
	// trusted by the latency arm (and to enter the baseline). Default 10.
	LatencyMinRequests int64
	// LatencyMinBaselineWindows is how many prior p99 readings a service needs
	// before the latency arm may fire for it. Default 10.
	LatencyMinBaselineWindows int

	// Baseline configures the baseline arms (docs/todo/baseline-gates.md):
	// additional OR arms next to the three above that judge the service's and
	// each operation's error rate and p99 against their own recent normal over
	// an evidence window of several ticks. See BaselineOptions.
	Baseline BaselineOptions

	// EnableRED/EnableTopology independently switch each frame family on.
	// Both are driven by one engine instance sharing one read. At least
	// one must be true (see New).
	EnableRED      bool
	EnableTopology bool

	// Resolver resolves a !Profile.HasSourceLabels client span's
	// server_address into a virtual-app identity (docs/todo/service-graph-from-obi.md)
	// — nil (every test that doesn't need address resolution, and any
	// deployment before main.go wires the real one for the "ebpf" profile)
	// falls back to always-External (see resolveClientDst). Unused by a
	// Profile.HasSourceLabels profile, which never has a
	// server-address to resolve at all.
	Resolver Resolver
	// Identity resolves Profile.SourceOwnerLabel's owner-name value into a
	// virtual-app identity (docs/todo/service-graph-from-obi.md) — nil
	// leaves every identity as whatever Profile.DestWorkloadLabel/
	// SourceWorkloadLabel already reported. Unused by a profile that leaves
	// SourceOwnerLabel unset.
	Identity IdentityResolver

	// now is injectable in tests. Defaults to time.Now.
	now func() time.Time
}

func (o *Options) setDefaults() {
	if o.Interval <= 0 {
		o.Interval = 60 * time.Second
	}
	if o.TopologyInterval <= 0 {
		o.TopologyInterval = 600 * time.Second
	}
	if o.LatencyFloorMs <= 0 {
		o.LatencyFloorMs = 1000
	}
	if o.LatencyMinRequests <= 0 {
		o.LatencyMinRequests = 10
	}
	if o.LatencyMinBaselineWindows <= 0 {
		o.LatencyMinBaselineWindows = 10
	}
	o.Baseline.setDefaults()
	if o.now == nil {
		o.now = time.Now
	}
}

// maxLatencyBaseline bounds the per-service ring of past p99 readings.
const maxLatencyBaseline = 30

// topologyEveryTicks returns how many Interval-sized ticks make up
// one TopologyInterval-sized flush window, minimum 1.
func (o Options) topologyEveryTicks() int {
	n := int(o.TopologyInterval / o.Interval)
	if n < 1 {
		n = 1
	}
	return n
}

// Collector is a runnable that periodically reads a profile's
// sensor snapshots and forwards RED + topology data to a Sender. Gated
// behind leader election so only one replica reads + sends.
type Collector struct {
	opts   Options
	sender Sender
	log    logr.Logger

	// nsAllow is Options.Namespaces as a set; nil means "forward every namespace".
	nsAllow map[string]bool

	// prevReq/prevBuckets/prevClient/prevFlow hold the last-seen cumulative
	// counters per (target, series), so a delta survives individual sensor
	// restarts independently — see the compute* functions in deltas.go.
	// prevFlow is used only when the profile sets FlowMetric.
	prevReq     map[string]map[requestSeries]float64
	prevBuckets map[string]map[bucketSeries]map[float64]uint64
	prevClient  map[string]map[clientEdgeSeries]float64
	prevFlow    map[string]map[flowSeries]float64
	// readBefore records, per target, the request and bucket metrics already
	// read once (see firstRead in deltas.go). Dropped with the target.
	readBefore map[string]map[string]bool

	// knownDst is every service read as a request destination so far. With no
	// Resolver (an OTLP-only agent has no Service index), it is what names the
	// in-scope callee of a client span: see knownCallee.
	knownDst map[servicePair]bool
	// opSeen is, per service, the operations currently tracked and the round
	// each was last seen in; opRound counts RED ticks. See trackOp.
	opSeen     map[servicePair]map[string]int
	opOverflow map[servicePair]bool
	opRound    int

	// p99Hist is the latency arm's baseline: per service, the p99 (ms) of up
	// to maxLatencyBaseline recent NORMAL windows, oldest first. Keyed by
	// service (not by sensor target) like redTotals, since the p99 is computed
	// from the sum across sensors. In-memory only: after an agent restart the
	// arm is silent for a service until LatencyMinBaselineWindows windows have
	// accumulated again. Garbage-collected every sendRED tick for services that
	// did not appear, the same posture as the prev* maps above.
	p99Hist map[servicePair][]float64

	// errSvc/errOp/latSvc/latOp are the baseline arms' per-key window rings and
	// baselines (see baseline_arms.go): errors and p99 per service and per
	// (service, operation). Nil when Options.Baseline is disabled.
	errSvc, errOp, latSvc, latOp *baseline.Tracker

	// edgeAccum/nodeAccum accumulate topology deltas ACROSS Interval ticks
	// until the next TopologyInterval flush — Calls/Bytes on the eventually-
	// sent frame is the sum of every tick's delta since the last flush, the
	// correct additive semantics for a windowed count. Reset on every flush.
	edgeAccum map[edgeKey]edgeDelta
	nodeAccum map[servicePair]bool
	// ticksSinceFlush counts successful (readAny==true) ticks since the
	// last topology flush; a tick where every sensor target had no snapshot
	// does not advance it — see runOnce and the package doc comment.
	ticksSinceFlush int
	// firstTopologyDone is false until the first topology flush completes.
	// While false, zero-Calls edges are included in that flush — their
	// existence at all IS the level state on a cold start, since first-seen
	// suppression would otherwise zero out every edge on the very first
	// flush and ship an edge-less snapshot.
	firstTopologyDone bool
}

// New creates a Collector. Returns nil if opts.Source is nil
// (collector disabled), sender is nil (push mode disabled — nothing to send
// to), opts.Profile is not usable (no RequestMetrics configured), or
// neither EnableRED nor EnableTopology is set (nothing to do).
func New(opts Options, sender Sender, log logr.Logger) *Collector {
	if opts.Source == nil || sender == nil || len(opts.Profile.RequestMetrics) == 0 {
		return nil
	}
	if !opts.EnableRED && !opts.EnableTopology {
		return nil
	}
	opts.setDefaults()
	return &Collector{
		opts:        opts,
		nsAllow:     namespaceSet(opts.Namespaces),
		sender:      sender,
		log:         log.WithName("redsource").WithValues("profile", opts.Profile.Name),
		prevReq:     make(map[string]map[requestSeries]float64),
		prevBuckets: make(map[string]map[bucketSeries]map[float64]uint64),
		prevClient:  make(map[string]map[clientEdgeSeries]float64),
		prevFlow:    make(map[string]map[flowSeries]float64),
		readBefore:  make(map[string]map[string]bool),
		knownDst:    make(map[servicePair]bool),
		opSeen:      make(map[servicePair]map[string]int),
		opOverflow:  make(map[servicePair]bool),
		p99Hist:     make(map[servicePair][]float64),
		errSvc:      newTracker(opts.Baseline),
		errOp:       newTracker(opts.Baseline),
		latSvc:      newTracker(opts.Baseline),
		latOp:       newTracker(opts.Baseline),
		edgeAccum:   make(map[edgeKey]edgeDelta),
		nodeAccum:   make(map[servicePair]bool),
	}
}

// NeedLeaderElection ensures only the leader replica reads + sends.
func (c *Collector) NeedLeaderElection() bool { return true }

// namespaceAllowed reports whether ns passes the collector's namespace filter:
// Options.NamespaceAllowed when non-nil (dynamic — hot-follows annotation-driven
// push-scope changes), else the static nsAllow set (nil set = every namespace).
// Also passed as the allow predicate into resolveDstLabeled.
func (c *Collector) namespaceAllowed(ns string) bool {
	if c.opts.NamespaceAllowed != nil {
		return c.opts.NamespaceAllowed(ns)
	}
	return c.nsAllow == nil || c.nsAllow[ns]
}

// resolveClientDst resolves a client span's server.address into a virtual-app identity via Options.Resolver — nil
// (no resolver wired) is the legacy fallback: every such destination
// resolves to External, since without a resolver there is no way to tell a
// k8s-shaped address apart from a genuinely external one.
func (c *Collector) resolveClientDst(srcNamespace, serverAddress string) (dst servicePair, external string) {
	if serverAddress == "" {
		return servicePair{}, ""
	}
	if c.opts.Resolver != nil {
		dst, external = c.opts.Resolver.Resolve(srcNamespace, serverAddress)
		if external == "" {
			return dst, ""
		}
	}
	// The resolver (or none) could not place the address: an agent without a
	// Service index cannot, so the services read as request destinations
	// name it.
	if dst, ok := c.knownCallee(srcNamespace, serverAddress); ok {
		return dst, ""
	}
	return servicePair{}, push.TruncateTopologyExternal(serverAddress)
}

// knownCallee names the callee of a client span from the services the
// collector has read as request destinations, for an address the resolver left
// external (a deployment with no Service index leaves them all): a bare "cart"
// (a Service of the caller's own namespace), "cart.shop", "cart.shop.svc" or
// "cart.shop.svc.<domain>". Only a name that matches a service seen as a
// destination, in a namespace in scope, resolves; an IP, an external host or an
// unseen name stays external.
func (c *Collector) knownCallee(srcNamespace, serverAddress string) (servicePair, bool) {
	host := strings.ToLower(strings.TrimSuffix(hostOnly(serverAddress), "."))
	if host == "" {
		return servicePair{}, false
	}
	parts := strings.Split(host, ".")
	var dst servicePair
	switch {
	case len(parts) == 1:
		dst = servicePair{Namespace: srcNamespace, Workload: parts[0]}
	case len(parts) == 2, len(parts) >= 3 && parts[2] == "svc":
		dst = servicePair{Namespace: parts[1], Workload: parts[0]}
	default:
		return servicePair{}, false
	}
	if !c.knownDst[dst] || !c.namespaceAllowed(dst.Namespace) {
		return servicePair{}, false
	}
	return dst, true
}

// Start runs evaluate on a fixed interval until ctx ends.
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

// runOnce reads each sensor target's snapshot ONCE, derives RED + topology
// deltas from that single read per the configured Profile, evaluates the
// RED gate immediately, and accumulates the topology deltas toward the next
// periodic flush. Errors from individual sensor reads are logged and
// skipped — one target with no snapshot must not block the others' counters
// from being reported.
func (c *Collector) runOnce(ctx context.Context) {
	p := c.opts.Profile
	windowStart := c.opts.now()
	targets := c.opts.Source.Targets(metricset.RoleEBPF)
	if len(targets) == 0 {
		ledger.Default.Forget(metricset.RoleEBPF, nil)
		c.log.V(1).Info("no sensor has sent eBPF metrics; nothing to evaluate")
		return
	}

	// Garbage-collect baselines for targets that left the target list —
	// prevents unbounded growth across sensor pod churn.
	live := make(map[string]bool, len(targets))
	for _, addr := range targets {
		live[addr] = true
	}
	for addr := range c.prevReq {
		if !live[addr] {
			delete(c.prevReq, addr)
		}
	}
	for addr := range c.prevBuckets {
		if !live[addr] {
			delete(c.prevBuckets, addr)
		}
	}
	for addr := range c.prevClient {
		if !live[addr] {
			delete(c.prevClient, addr)
		}
	}
	for addr := range c.prevFlow {
		if !live[addr] {
			delete(c.prevFlow, addr)
		}
	}
	for addr := range c.readBefore {
		if !live[addr] {
			delete(c.readBefore, addr)
		}
	}

	ledger.Default.Forget(metricset.RoleEBPF, live)
	redTotals := make(map[servicePair]*push.ServiceRedMetrics)
	bucketTotals := make(map[servicePair]map[float64]int64)
	redBest := make(map[servicePair]*redBestEntry)
	redOps := make(map[servicePair]map[string]*opTotals)
	// statuses counts each service's requests per protocol and status this
	// window, for the "red statuses" debug line: what the sensor saw, before
	// IsError decides what counts as a failure.
	statuses := make(map[servicePair]map[string]int64)
	edgeTick := make(map[edgeKey]edgeDelta)
	nodeTick := make(map[servicePair]bool)

	// A service the DaemonSet measured this tick is not counted again from
	// an OTLP source: the sensors are read first, and an OTLP source only
	// fills in services (and edge sources) they did not report. Deltas are
	// still computed for every source, so a fallback starts warm.
	sensorDst := make(map[servicePair]bool)
	sensorSrc := make(map[servicePair]bool)
	// Likewise, what a sender reports as a metric is not counted again from
	// what the OTLP receiver derived from that service's spans: the native
	// sources are read first, and a derived source only fills in the
	// (service, metric) pairs they did not report.
	nativeDst := make(map[dstMetric]bool)
	nativeSrc := make(map[srcMetric]bool)

	readAny := false
	ordered, derived := c.nativeFirst(targets)
	for _, addr := range ordered {
		otlp := metricset.IsOTLPTarget(addr)
		set, err := c.opts.Source.Set(metricset.RoleEBPF, addr)
		if err != nil {
			c.log.V(1).Info("no eBPF metrics from sensor; skipping", "target", addr, "reason", err.Error())
			ledger.Default.Add(metricset.RoleEBPF, addr, ledger.NoSnapshot, 1)
			continue
		}
		readAny = true
		ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Ticks, 1)
		ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Points, int64(set.PointCount()))

		for _, rm := range p.RequestMetrics {
			reqFam := set.Get(rm.MetricName)
			for _, rd := range c.computeRequestDeltas(addr, rm, reqFam) {
				grp, d := rd.group, rd.delta
				if otlp && sensorDst[grp.dst] {
					continue
				}
				if otlp && derived[addr] && nativeDst[dstMetric{grp.dst, rm.MetricName}] {
					continue // the sender reports this service's metric itself
				}
				if !otlp {
					sensorDst[grp.dst] = true
				} else if !derived[addr] {
					nativeDst[dstMetric{grp.dst, rm.MetricName}] = true
				}

				// RED: grouped by destination, summed across every source and
				// status (and across request metrics: HTTP and gRPC add up) —
				// source scope is irrelevant here (see requestGroup's doc
				// comment): a service being called from an out-of-tenant
				// namespace is still legitimately this tenant's RED accounting
				// for THAT service.
				if grp.dst.Namespace != "" && grp.dst.Workload != "" {
					c.knownDst[grp.dst] = true
				}
				if c.opts.EnableRED && (grp.dst.Namespace != "" || grp.dst.Workload != "") {
					if c.namespaceAllowed(grp.dst.Namespace) {
						agg := redTotals[grp.dst]
						if agg == nil {
							agg = &push.ServiceRedMetrics{Namespace: grp.dst.Namespace, Service: grp.dst.Workload}
							redTotals[grp.dst] = agg
						}
						agg.RequestCount += d
						if d > 0 {
							st := grp.status
							if st == "" {
								st = "<none>"
							}
							if statuses[grp.dst] == nil {
								statuses[grp.dst] = map[string]int64{}
							}
							statuses[grp.dst][strings.SplitN(rm.MetricName, ".", 2)[0]+" "+st] += d
						}
						// Per-operation totals for the OperationErrorRatePct
						// arm, built from this same delta (the metric family
						// is not read again).
						op := c.opTotalsFor(redOps, grp.dst, rm, rd.extra)
						if op != nil {
							op.requests += d
						}
						if rm.IsError(grp.status) {
							agg.ErrorCount += d
							if op != nil {
								op.errors += d
								if d > op.maxDelta {
									op.maxDelta = d
									op.sample = rm.FormatSample(SampleContext{
										DstNamespace: grp.dst.Namespace, DstWorkload: grp.dst.Workload,
										Status: grp.status, Delta: d, Extra: rd.extra,
									})
								}
							}
							pb := redBest[grp.dst]
							if pb == nil {
								pb = &redBestEntry{}
								redBest[grp.dst] = pb
							}
							// Strict '>' (not '>='): the FIRST series to reach a
							// given max delta wins ties — order is
							// fam.GetMetric()'s own slice order (preserved by
							// computeRequestDeltas), so this is deterministic
							// rather than dependent on map iteration.
							if d > pb.maxDelta {
								pb.maxDelta = d
								pb.sample = rm.FormatSample(SampleContext{
									DstNamespace: grp.dst.Namespace, DstWorkload: grp.dst.Workload,
									Status: grp.status, Delta: d, Extra: rd.extra,
								})
							}
						}
					}
				}

				if c.opts.EnableTopology {
					// NODES: presence in any RequestMetrics family alone — no delta needed, a
					// workload's existence isn't gated on call volume (ADR-0011).
					// Edges come from Profile.ClientMetrics and FlowMetric below.
					if grp.dst.Namespace != "" || grp.dst.Workload != "" {
						if c.namespaceAllowed(grp.dst.Namespace) {
							nodeTick[grp.dst] = true
						}
					}
				}
			}

			if c.opts.EnableRED {
				durFam := set.Get(rm.DurationBucketMetric)
				svcBuckets, opBkts := c.computeBucketDeltasOps(addr, rm, durFam)
				if otlp {
					for dst := range sensorDst {
						delete(svcBuckets, dst)
						delete(opBkts, dst)
					}
				}
				if c.baselineOn() {
					for dst, byOp := range opBkts {
						if !c.namespaceAllowed(dst.Namespace) {
							ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Filtered, 1)
							continue
						}
						for key, byLe := range byOp {
							op := c.opFor(redOps, dst, key, opLabelFromKey(key))
							if op.buckets == nil {
								op.buckets = make(map[float64]int64, len(byLe))
							}
							for le, d := range byLe {
								op.buckets[le] += d
							}
						}
					}
				}
				for dst, byLe := range svcBuckets {
					if !c.namespaceAllowed(dst.Namespace) {
						ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Filtered, 1)
						continue
					}
					sum := bucketTotals[dst]
					if sum == nil {
						sum = make(map[float64]int64, len(byLe))
						bucketTotals[dst] = sum
					}
					for le, d := range byLe {
						sum[le] += d
					}
				}
			}
		}

		if c.opts.EnableTopology {
			for _, cm := range p.ClientMetrics {
				fam := set.Get(cm.MetricName)
				for _, s := range c.computeClientEdgeDeltas(addr, fam, cm.Protocol) {
					if s.src.Namespace == "" && s.src.Workload == "" {
						continue // unattributable — can't name an edge's source
					}
					if s.srcUnknown {
						continue // untracked source: no in-scope node to hang an edge on
					}
					if otlp && sensorSrc[s.src] {
						continue // the sensors already reported this caller's edges
					}
					if otlp && derived[addr] && nativeSrc[srcMetric{s.src, cm.MetricName}] {
						continue // the sender reports this caller's client metric itself
					}
					if !otlp {
						sensorSrc[s.src] = true
					} else if !derived[addr] {
						nativeSrc[srcMetric{s.src, cm.MetricName}] = true
					}
					if !c.namespaceAllowed(s.src.Namespace) {
						ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Filtered, 1)
						continue // src out of scope: skip entirely
					}
					dst, external := c.resolveClientDst(s.src.Namespace, s.serverAddress)
					if dst.Namespace == "" && dst.Workload == "" && external == "" {
						continue // no server-address to resolve — nothing to attribute the edge to
					}
					nodeTick[s.src] = true
					if external == "" {
						nodeTick[dst] = true
					}
					addEdgeDelta(edgeTick, edgeKey{src: s.src, dst: dst, external: external, protocol: s.protocol, dstPort: s.dstPort}, s.calls, 0)
				}
			}
		}

		if c.opts.EnableTopology && p.FlowMetric != "" {
			flowFam := set.Get(p.FlowMetric)
			hasDirection := p.FlowDirectionLabel != ""
			for _, fs := range filterFlowSamples(c.computeFlowEdgeDeltas(addr, flowFam), hasDirection) {
				if !c.namespaceAllowed(fs.srcNamespace) {
					ledger.Default.Add(metricset.RoleEBPF, addr, ledger.Filtered, 1)
					continue // src out of scope: skip entirely, same rule as the L7 client-edge path above
				}

				srcWorkload := fs.srcOwner
				if c.opts.Identity != nil {
					identity, ok := c.resolveIdentity(fs.srcNamespace, fs.srcOwner)
					if !ok {
						// The owner is not (or no longer) a tracked workload —
						// deleted, a CronJob/Job pod, or vapps off for it. It
						// must never go out under its raw owner name, and with
						// no reliable current identity there is no in-scope
						// node to hang an edge on either: skip the row.
						continue
					}
					srcWorkload = identity
				}
				src := servicePair{Namespace: fs.srcNamespace, Workload: srcWorkload}

				var dst servicePair
				var external string
				if c.opts.Identity != nil {
					if identity, ok := c.resolveIdentity(fs.dstNamespace, fs.dstOwner); ok {
						dst, external = resolveDstLabeled(servicePair{Namespace: fs.dstNamespace, Workload: identity}, c.namespaceAllowed)
					} else {
						// Untracked destination owner: External under its raw
						// name rather than a fabricated in-scope node.
						external = push.TruncateTopologyExternal(externalLabel(servicePair{Namespace: fs.dstNamespace, Workload: fs.dstOwner}))
					}
				} else {
					dst, external = resolveDstLabeled(servicePair{Namespace: fs.dstNamespace, Workload: fs.dstOwner}, c.namespaceAllowed)
				}

				nodeTick[src] = true
				if external == "" {
					nodeTick[dst] = true
				}
				addEdgeDelta(edgeTick, edgeKey{src: src, dst: dst, external: external, protocol: "tcp", dstPort: fs.dstPort}, 0, fs.bytes)
			}
		}
	}

	c.logStatuses(statuses)

	if !readAny {
		// Every sensor target had no snapshot this tick — nothing new was
		// learned for EITHER signal family. Skip RED gate evaluation and do
		// not advance ticksSinceFlush (see the ticksSinceFlush doc comment
		// and the package doc comment's resilience note).
		c.log.Info("no sensor target had a snapshot this tick; nothing sent", "targets", len(targets))
		return
	}

	if c.opts.EnableRED {
		c.sendRED(windowStart, redTotals, redBest, redOps, bucketTotals)
		c.sendRedSummary(windowStart, redTotals, bucketTotals)
		c.gcOps()
	}

	if c.opts.EnableTopology {
		for k, d := range edgeTick {
			addEdgeDelta(c.edgeAccum, k, d.Calls, d.Bytes)
		}
		for np := range nodeTick {
			c.nodeAccum[np] = true
		}
		c.ticksSinceFlush++
		if c.ticksSinceFlush >= c.opts.topologyEveryTicks() {
			c.flushTopology(windowStart)
		}
	}
}

// redBestEntry tracks, per destination, the largest-delta error series seen
// this tick — the winner becomes that destination's Sample.
type redBestEntry struct {
	maxDelta int64
	sample   string
}

// opTotals is one operation's window totals within one destination service:
// its request and error deltas, plus the formatted sample of its own
// largest-delta error series (strict '>', first wins, like redBestEntry).
type opTotals struct {
	requests, errors int64
	maxDelta         int64
	sample           string
	// label is the human operation name ("GET /pay", "/pay.Charge/Do") the
	// baseline arms put on the wire; buckets is the operation's duration
	// bucket delta for the window (nil when no histogram carried it).
	label   string
	buckets map[float64]int64
}

// opTotalsFor returns the opTotals bucket for one series' operation within
// dst, creating it on first use. The operation key is the request metric's
// name plus its SampleLabels values (HTTP: method+route; gRPC: rpc.method),
// so an HTTP and a gRPC operation never merge. nil means the series has no
// usable operation identity and stays out of the per-operation arm: either
// the metric names no SampleLabels, or every captured label is empty. An
// all-empty bucket would pool the service's unlabelled traffic (the "?"
// sample), so its rate would simply be the service-wide rate judged against
// a different threshold, letting the arm bypass a MinErrorRatePct set above
// OperationErrorRatePct. A PARTLY empty identity (method but no route) is
// still a real bucket.
func (c *Collector) opTotalsFor(redOps map[servicePair]map[string]*opTotals, dst servicePair, rm RequestMetric, extra map[string]string) *opTotals {
	// The per-operation totals also feed the baseline arms, so they are built
	// when either is on (failingOperation itself stays gated on
	// OperationErrorRatePct).
	if c.opts.OperationErrorRatePct <= 0 && !c.baselineOn() {
		return nil
	}
	key, label, ok := operationKey(rm, extra)
	if !ok {
		return nil
	}
	return c.opFor(redOps, dst, key, label)
}

// operationKey is an operation's identity within one destination: the request
// metric's name plus its SampleLabels values, read from attrs (a series'
// attribute set or the captured Extra map). label joins the non-empty values
// with a space. ok is false when rm names no SampleLabels or every value is
// empty (see opTotalsFor).
func operationKey(rm RequestMetric, attrs map[string]string) (key, label string, ok bool) {
	if len(rm.SampleLabels) == 0 {
		return "", "", false
	}
	key = rm.MetricName
	for _, lbl := range rm.SampleLabels {
		v := attrs[lbl]
		if v != "" {
			ok = true
			if label != "" {
				label += " "
			}
			label += v
		}
		key += "\x00" + v
	}
	if !ok {
		return "", "", false
	}
	return key, label, true
}

// opLabelFromKey inverts operationKey's label for an operationKey: the
// non-empty SampleLabels values after the metric name, space-joined.
func opLabelFromKey(key string) string {
	parts := strings.Split(key, "\x00")[1:]
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, " ")
}

// overflowOpKey and overflowOpLabel name the one operation that stands for
// every operation of a service past BaselineOptions.MaxOperations. The key
// cannot collide with an operationKey, which starts with a metric name.
const (
	overflowOpKey   = "\x00overflow"
	overflowOpLabel = "(other operations)"
)

// trackOp bounds how many operations of one service are tracked: the first
// BaselineOptions.MaxOperations distinct operations keep their own key, and
// any further one is folded into the overflow operation, so a service whose
// routes are unbounded (an ID in the path the route matcher did not collapse)
// cannot grow the per-operation trackers without limit. A tracked operation
// frees its slot once it has been absent as long as the trackers keep an
// absent key (gcOps).
func (c *Collector) trackOp(dst servicePair, key, label string) (string, string) {
	limit := c.opts.Baseline.MaxOperations
	if limit <= 0 {
		return key, label
	}
	seen := c.opSeen[dst]
	if seen == nil {
		seen = make(map[string]int)
		c.opSeen[dst] = seen
	}
	if _, ok := seen[key]; ok || len(seen) < limit {
		seen[key] = c.opRound
		return key, label
	}
	if !c.opOverflow[dst] {
		c.opOverflow[dst] = true
		c.log.Info("service has more operations than are tracked one by one; the rest are judged together",
			"namespace", dst.Namespace, "service", dst.Workload, "maxOperations", limit, "as", overflowOpLabel)
	}
	return overflowOpKey, overflowOpLabel
}

// gcOps advances the operation round and frees the slots of operations not
// seen for as long as the baseline trackers keep an absent key.
func (c *Collector) gcOps() {
	limit := c.errGate().AbsentRounds()
	for dst, seen := range c.opSeen {
		for key, last := range seen {
			if c.opRound-last >= limit {
				delete(seen, key)
			}
		}
		if len(seen) < c.opts.Baseline.MaxOperations {
			delete(c.opOverflow, dst)
		}
		if len(seen) == 0 {
			delete(c.opSeen, dst)
		}
	}
	c.opRound++
}

// opFor returns (creating) dst's opTotals for key, or for the overflow
// operation when dst already tracks MaxOperations others (trackOp).
func (c *Collector) opFor(redOps map[servicePair]map[string]*opTotals, dst servicePair, key, label string) *opTotals {
	key, label = c.trackOp(dst, key, label)
	ops := redOps[dst]
	if ops == nil {
		ops = make(map[string]*opTotals)
		redOps[dst] = ops
	}
	op := ops[key]
	if op == nil {
		op = &opTotals{label: label}
		ops[key] = op
	}
	return op
}

// failingOperation returns the operation of one service that trips the
// OperationErrorRatePct arm: errors >= ErrorThreshold AND errors*100 >=
// OperationErrorRatePct*requests. If several trip, the one with the most
// errors wins (ties: the lexically smaller sample, so the pick never depends
// on map iteration order). ok is false when none trips or the arm is off.
func (c *Collector) failingOperation(ops map[string]*opTotals) (best *opTotals, ok bool) {
	if c.opts.OperationErrorRatePct <= 0 {
		return nil, false
	}
	for _, op := range ops {
		// errors > 0 guards a zero ErrorThreshold: 0 >= 0 must not trip.
		if op.errors == 0 || op.errors < c.opts.ErrorThreshold {
			continue
		}
		if float64(op.errors)*100 < c.opts.OperationErrorRatePct*float64(op.requests) {
			continue
		}
		if best == nil || op.errors > best.errors || (op.errors == best.errors && op.sample < best.sample) {
			best = op
		}
	}
	return best, best != nil
}

// dstMetric and srcMetric are one service's one request or client metric, the
// unit a derived OTLP source defers to a native one on.
type (
	dstMetric struct {
		dst    servicePair
		metric string
	}
	srcMetric struct {
		src    servicePair
		metric string
	}
)

// nativeFirst orders targets for one read: the DaemonSet sensors, then the
// OTLP sources whose senders reported their metrics, then the OTLP sources
// the receiver derived from spans, each group in its original order. derived
// says which of the OTLP targets are the last group.
func (c *Collector) nativeFirst(targets []string) (ordered []string, derived map[string]bool) {
	ordered = metricset.SensorsFirst(targets)
	derived = make(map[string]bool)
	for _, addr := range ordered {
		if !metricset.IsOTLPTarget(addr) {
			continue
		}
		if set, err := c.opts.Source.Set(metricset.RoleEBPF, addr); err == nil && set.Derived() {
			derived[addr] = true
		}
	}
	if len(derived) > 0 {
		sort.SliceStable(ordered, func(i, j int) bool { return !derived[ordered[i]] && derived[ordered[j]] })
	}
	return ordered, derived
}

// sendRED applies the gate/cap/p99-attach/send sequence.
func (c *Collector) sendRED(windowStart time.Time, redTotals map[servicePair]*push.ServiceRedMetrics, redBest map[servicePair]*redBestEntry, redOps map[servicePair]map[string]*opTotals, bucketTotals map[servicePair]map[float64]int64) {
	for pair, pb := range redBest {
		if pb.maxDelta > 0 {
			if agg, ok := redTotals[pair]; ok {
				agg.Sample = push.TruncateRedSample(pb.sample)
			}
		}
	}

	latency := c.evalLatency(redTotals, redOps, bucketTotals)
	hits := c.evalBaseline(redTotals, redOps, bucketTotals)

	var qualifying []push.ServiceRedMetrics
	unattributed := int64(0)
	for pair, m := range redTotals {
		// Three independent ways through the gate (the third, latency, is
		// evaluated above into `latency`). (1) service-wide: error count
		// AND service-wide rate. (2) any single operation with its own error
		// count AND rate (OperationErrorRatePct). Arm 2 lets a wholly broken
		// route through when the service's other traffic dilutes its rate.
		serviceGate := m.ErrorCount >= c.opts.ErrorThreshold &&
			float64(m.ErrorCount)*100 >= c.opts.MinErrorRatePct*float64(m.RequestCount)
		lb, latencyGate := latency[pair]
		hit := hits[pair]
		if !serviceGate {
			if op, ok := c.failingOperation(redOps[pair]); ok {
				// Qualified through the operation arm alone: the sample must
				// name the operation that tripped it, which need not be the
				// service's largest-delta error series set above. The frame's
				// counts stay service-wide (wire protocol unchanged).
				m.Sample = push.TruncateRedSample(op.sample)
			} else if latencyGate {
				// (3) latency arm alone: ErrorCount may be 0, so there is no
				// error sample to keep; describe the latency instead.
				m.Sample = push.TruncateRedSample(lb.sample())
			} else if hit != nil {
				// (4) a baseline arm alone: ErrorCount may be 0 or tiny.
				m.Sample = push.TruncateRedSample(hit.describe())
			} else {
				ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Gated, 1)
				continue
			}
		}
		if hit != nil {
			hit.apply(m)
		}
		if latencyGate {
			// Also set when an error arm qualified the service (the error
			// sample is kept): the core should see both signals.
			m.BaselineP99Ms = lb.baseline
			m.LatencyBreach = true
		}
		if m.Service == "" {
			unattributed += m.ErrorCount
			ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Unattributed, m.ErrorCount)
			continue
		}
		qualifying = append(qualifying, *m)
	}
	for i := range qualifying {
		pair := servicePair{Namespace: qualifying[i].Namespace, Workload: qualifying[i].Service}
		if byLe, ok := bucketTotals[pair]; ok {
			if p99Sec, ok := p99FromBucketDeltas(byLe); ok {
				qualifying[i].P99Ms = p99Sec * 1000
			}
		}
	}
	if unattributed > 0 {
		c.log.Info("skipped unattributable errors (samples without a destination workload label)",
			"count", unattributed)
	}
	ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Ticks, 1)
	ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Services, int64(len(redTotals)))
	if len(qualifying) == 0 {
		c.log.V(1).Info("no services crossed the error threshold/rate gate (service-wide or per-operation), the latency gate or a baseline arm (errors, op-errors, latency, op-latency); no red_metrics frame sent",
			"errorThreshold", c.opts.ErrorThreshold, "minErrorRatePct", c.opts.MinErrorRatePct,
			"operationErrorRatePct", c.opts.OperationErrorRatePct,
			"latencyMultiplier", c.opts.LatencyMultiplier, "latencyFloorMs", c.opts.LatencyFloorMs,
			"latencyMinRequests", c.opts.LatencyMinRequests, "latencyMinBaselineWindows", c.opts.LatencyMinBaselineWindows,
			"baselineEnabled", c.baselineOn(), "baselineMultiplier", c.opts.Baseline.Multiplier,
			"baselineEvidenceWindows", c.opts.Baseline.EvidenceWindows,
			"servicesRead", len(redTotals))
		return
	}
	if len(qualifying) > push.MaxRedMetricsServices {
		sortRedForCap(qualifying)
		c.log.Info("more services qualified than fit in one frame; keeping breaches, then the top by error delta",
			"qualified", len(qualifying), "kept", push.MaxRedMetricsServices)
		ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Capped, int64(len(qualifying)-push.MaxRedMetricsServices))
		qualifying = qualifying[:push.MaxRedMetricsServices]
	}
	ledger.Default.Add(metricset.RoleEBPF, ledger.All, ledger.Sent, int64(len(qualifying)))
	windowSec := int(c.opts.Interval / time.Second)
	c.sender.SendRedMetrics(windowStart.Unix(), windowSec, qualifying)
	c.log.Info("sent red_metrics frame", "services", len(qualifying), "windowSec", windowSec)
}

// sortRedForCap orders qualifying entries for the MaxRedMetricsServices cut,
// like usemetrics' cap. Entries a latency or baseline arm qualified rank
// first: a latency breach has ErrorCount 0 and would otherwise always be the
// first one cut. Then the error delta, then namespace/service, so the cut does
// not depend on map iteration.
func sortRedForCap(q []push.ServiceRedMetrics) {
	breached := func(m push.ServiceRedMetrics) bool { return m.Breach != "" || m.LatencyBreach }
	sort.Slice(q, func(i, j int) bool {
		a, b := q[i], q[j]
		if breached(a) != breached(b) {
			return breached(a)
		}
		if a.ErrorCount != b.ErrorCount {
			return a.ErrorCount > b.ErrorCount
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Service < b.Service
	})
}

// latencyBreach is one service's latency-arm result for a window.
type latencyBreach struct {
	p99Ms, baseline float64
	// slowest names the operation with the highest p99 in the window
	// ("GET /v2/agent/ws p99 10000ms over 3 req"); empty when no operation
	// carried a duration histogram.
	slowest string
}

// sample describes the breach for the frame's Sample field, e.g.
// "p99 10000ms vs baseline 5ms (x2000)".
func (b latencyBreach) sample() string {
	s := fmt.Sprintf("p99 %.0fms vs baseline %.0fms", b.p99Ms, b.baseline)
	if b.baseline > 0 {
		s = fmt.Sprintf("p99 %.0fms vs baseline %.0fms (x%.0f)", b.p99Ms, b.baseline, b.p99Ms/b.baseline)
	}
	if b.slowest != "" {
		s += "; slowest: " + b.slowest
	}
	return s
}

// slowestOperation names the operation of one service with the highest p99
// this window (ties: the one with more requests, then the smaller label, so
// the pick never depends on map iteration order). A p99 at the histogram's top
// bucket means "at least that long", the case a long-lived stream produces.
// "" when no operation carries a duration histogram.
func slowestOperation(ops map[string]*opTotals) string {
	var best *opTotals
	var bestP99 float64
	for _, op := range ops {
		p, ok := p99FromBucketDeltas(op.buckets)
		if !ok || op.label == "" {
			continue
		}
		if best == nil || p > bestP99 || (p == bestP99 && (op.requests > best.requests || (op.requests == best.requests && op.label < best.label))) {
			best, bestP99 = op, p
		}
	}
	if best == nil {
		return ""
	}
	return fmt.Sprintf("%s p99 %.0fms over %d req", best.label, bestP99*1000, best.requests)
}

// minTailRequests is how many requests must sit at or above the p99 bucket
// for the latency arm to trust that p99.
const minTailRequests = 2

// tailRequests counts the requests of one window that landed in the bucket
// holding p99Sec or any slower one (cumulative bucket deltas in byLe).
func tailRequests(byLe map[float64]int64, p99Sec float64) int64 {
	var below float64
	for le := range byLe {
		if !math.IsInf(le, 1) && le < p99Sec && le > below {
			below = le
		}
	}
	total, ok := byLe[math.Inf(1)]
	if !ok {
		for le, n := range byLe {
			if le > below && n > total {
				total = n
			}
		}
	}
	return total - byLe[below]
}

// medianOf returns the median of vals (non-empty). For an even count it is the
// mean of the two middle values. vals is not modified.
func medianOf(vals []float64) float64 {
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

// evalLatency runs the latency arm for every attributable service read this
// window and maintains the p99 baseline (Collector.p99Hist). It returns the
// services whose p99 breached their baseline.
//
// A window enters the baseline only when it served at least LatencyMinRequests
// requests AND did not itself breach: a long slowdown must not become the new
// normal and silence itself. A low-traffic window is neither judged nor
// recorded. Services absent from this window are dropped from p99Hist (the
// per-tick GC posture of the prev* maps). The arm is silent for a service
// until it has LatencyMinBaselineWindows readings.
//
// p99 here is the same number sendRED attaches as P99Ms: interpolated from the
// bucket deltas summed across HTTP and gRPC and across sensors. Consistent
// window to window, so the baseline stays comparable. A response slower than
// the histogram's top finite bucket is reported as that boundary
// (p99FromBucketDeltas), which only under-states it.
func (c *Collector) evalLatency(redTotals map[servicePair]*push.ServiceRedMetrics, redOps map[servicePair]map[string]*opTotals, bucketTotals map[servicePair]map[float64]int64) map[servicePair]latencyBreach {
	for pair := range c.p99Hist {
		if m, ok := redTotals[pair]; !ok || m.Service == "" {
			delete(c.p99Hist, pair)
		}
	}
	var out map[servicePair]latencyBreach
	for pair, m := range redTotals {
		if m.Service == "" || m.RequestCount < c.opts.LatencyMinRequests {
			continue
		}
		byLe, ok := bucketTotals[pair]
		if !ok {
			continue
		}
		p99Sec, ok := p99FromBucketDeltas(byLe)
		if !ok {
			continue
		}
		// A p99 over fewer than 100 requests is the slowest one, so a single
		// request that ran long (a stream or watch closing, a client's long
		// poll) would read as the service's latency. The window is neither
		// judged nor learned unless a second request shares the tail.
		if tail := tailRequests(byLe, p99Sec); tail < minTailRequests {
			c.log.V(1).Info("latency window skipped: one request in the p99 tail",
				"namespace", pair.Namespace, "workload", pair.Workload, "p99Ms", p99Sec*1000, "tail", tail)
			continue
		}
		p99 := p99Sec * 1000
		hist := c.p99Hist[pair]
		// Cold (the baseline is in memory, so every service is after a
		// restart): a p99 at or above the baseline arms' cold floor is
		// reported and not learned, like there, so a restart during a
		// slowdown does not make it this arm's normal either.
		if cold := c.opts.Baseline.LatColdFloorMs; c.opts.LatencyMultiplier > 0 && cold > 0 &&
			len(hist) < c.opts.LatencyMinBaselineWindows && p99 >= cold && p99 >= c.opts.LatencyFloorMs {
			if out == nil {
				out = make(map[servicePair]latencyBreach)
			}
			out[pair] = latencyBreach{p99Ms: p99, slowest: slowestOperation(redOps[pair])}
			continue
		}
		if c.opts.LatencyMultiplier > 0 && len(hist) >= c.opts.LatencyMinBaselineWindows {
			base := medianOf(hist)
			if p99 >= c.opts.LatencyMultiplier*base && p99 >= c.opts.LatencyFloorMs {
				if out == nil {
					out = make(map[servicePair]latencyBreach)
				}
				out[pair] = latencyBreach{p99Ms: p99, baseline: base, slowest: slowestOperation(redOps[pair])}
				continue // a breaching window is never absorbed into the baseline
			}
		}
		hist = append(hist, p99)
		if len(hist) > maxLatencyBaseline {
			hist = hist[len(hist)-maxLatencyBaseline:]
		}
		c.p99Hist[pair] = hist
	}
	return out
}

// sendRedSummary sends an UNGATED red_summary frame covering every service
// read this window — not just the ones that crossed sendRED's error-
// threshold/rate gate — so the hub can show golden-signal (rate/errors/p99)
// data for the whole service set read this window, not only the ones
// anomalous enough to be gated into a red_metrics frame.
//
// Built from the SAME redTotals/bucketTotals this tick's sendRED consumed,
// but deliberately NOT by copying *m directly: sendRED mutates a
// gate-crossing entry's Sample in place (the loop over redBest above), and
// the wire contract for this frame leaves Sample empty for every entry —
// copying the pointer's value verbatim would leak a gate-crossing service's
// sample into the ungated summary. An entry with no Service label (the
// Profile couldn't attribute it to a workload — the same "unattributed" case
// sendRED skips into its unattributed-errors counter) is excluded here too:
// it isn't a service to summarize.
func (c *Collector) sendRedSummary(windowStart time.Time, redTotals map[servicePair]*push.ServiceRedMetrics, bucketTotals map[servicePair]map[float64]int64) {
	services := make([]push.ServiceRedMetrics, 0, len(redTotals))
	for pair, m := range redTotals {
		if m.Service == "" {
			continue
		}
		sm := push.ServiceRedMetrics{
			Namespace:    m.Namespace,
			Service:      m.Service,
			RequestCount: m.RequestCount,
			ErrorCount:   m.ErrorCount,
		}
		if byLe, ok := bucketTotals[pair]; ok {
			if p99Sec, ok := p99FromBucketDeltas(byLe); ok {
				sm.P99Ms = p99Sec * 1000
			}
		}
		services = append(services, sm)
	}
	if len(services) == 0 {
		// Nothing attributable was read this window — an empty frame is
		// noise, not signal (see docs on RedSummaryPayload).
		c.log.V(1).Info("no services read this window; no red_summary frame sent")
		return
	}
	if len(services) > push.MaxRedMetricsServices {
		dropped := len(services) - push.MaxRedMetricsServices
		// Deterministic order: request count desc, then namespace/service asc
		// as a tie-break — unlike sendRED's error-delta sort, there is no
		// "most urgent" ordering for an ungated summary, so busiest-first is
		// the closest analogue and ties must not depend on map iteration.
		sort.Slice(services, func(i, j int) bool {
			if services[i].RequestCount != services[j].RequestCount {
				return services[i].RequestCount > services[j].RequestCount
			}
			if services[i].Namespace != services[j].Namespace {
				return services[i].Namespace < services[j].Namespace
			}
			return services[i].Service < services[j].Service
		})
		c.log.V(1).Info("more services read than fit in one red_summary frame; keeping the top by request count",
			"observed", len(services), "kept", push.MaxRedMetricsServices, "dropped", dropped)
		services = services[:push.MaxRedMetricsServices]
	}
	windowSec := int(c.opts.Interval / time.Second)
	c.sender.SendRedSummary(windowStart.Unix(), windowSec, services)
	c.log.V(1).Info("sent red_summary frame", "services", len(services), "windowSec", windowSec)
}

// flushTopology sends one topology_snapshot frame covering every edge/node
// accumulated since the previous flush, then resets the accumulator —
// mirrors the former redsource.Collector.runOnce's cap/sort/send sequence,
// applied to the accumulated (not single-read) totals.
//
// Delivery is attempted BEFORE the accumulator reset, and the reset (plus
// latching firstTopologyDone) only happens when Sender.SendTopologySnapshot
// reports the frame was actually enqueued. Unlike RED/USE metrics — re-derived
// fresh from cumulative sensor counters every window regardless of whether
// the prior window's frame was delivered — edgeAccum/nodeAccum are the ONLY
// record of
// this window's topology; clearing them before delivery is confirmed would
// permanently discard up to a full TopologyInterval of data on a WS
// disconnect or a full send queue (AUDIT.md finding, BUG 1), and would
// falsely latch firstTopologyDone on a failed first flush, causing every
// zero-Calls edge to be dropped from every subsequent snapshot even though
// none was ever actually shipped.
//
// On failure ticksSinceFlush is deliberately left as-is (not reset), so the
// very next Interval tick's runOnce immediately retries the flush rather than
// waiting a full TopologyInterval — the retained accumulators keep growing in
// the meantime, and the window this snapshot covers correctly extends until a
// flush finally succeeds, since the accumulators still cover the whole
// extended span. No rate-limiting on the retry log line: flushes are normally
// TopologyInterval (10m) apart, so a run of failures is not a hot loop.
func (c *Collector) flushTopology(capturedAt time.Time) {
	includeZero := !c.firstTopologyDone
	edges := make([]push.TopologyEdge, 0, len(c.edgeAccum))
	for k, d := range c.edgeAccum {
		if d.Calls == 0 && d.Bytes == 0 && !includeZero {
			continue
		}
		e := push.TopologyEdge{
			SrcNamespace: k.src.Namespace,
			SrcWorkload:  k.src.Workload,
			Calls:        d.Calls,
			Bytes:        d.Bytes,
			Protocol:     k.protocol,
			DstPort:      k.dstPort,
		}
		if k.external != "" {
			e.External = k.external
		} else {
			e.DstNamespace = k.dst.Namespace
			e.DstWorkload = k.dst.Workload
		}
		edges = append(edges, e)
	}
	sortEdges(edges)
	if len(edges) > push.MaxTopologyEdges {
		dropped := len(edges) - push.MaxTopologyEdges
		c.log.Info("more edges observed than fit in one snapshot; keeping the top by calls",
			"observed", len(edges), "kept", push.MaxTopologyEdges, "dropped", dropped)
		edges = edges[:push.MaxTopologyEdges]
	}

	nodes := sortNodes(c.nodeAccum)
	if len(nodes) > push.MaxTopologyNodes {
		dropped := len(nodes) - push.MaxTopologyNodes
		c.log.Info("more nodes observed than fit in one snapshot; keeping the alphabetically-first",
			"observed", len(nodes), "kept", push.MaxTopologyNodes, "dropped", dropped)
		nodes = nodes[:push.MaxTopologyNodes]
	}

	windowSec := int(c.opts.TopologyInterval / time.Second)
	if !c.sender.SendTopologySnapshot(capturedAt.Unix(), windowSec, nodes, edges) {
		c.log.Info("topology_snapshot not delivered (push disconnected or send queue full); retaining accumulated edges/nodes for the next flush attempt",
			"edges", len(c.edgeAccum), "nodes", len(c.nodeAccum))
		return
	}

	c.firstTopologyDone = true
	c.edgeAccum = make(map[edgeKey]edgeDelta)
	c.nodeAccum = make(map[servicePair]bool)
	c.ticksSinceFlush = 0
	c.log.Info("sent topology_snapshot frame", "nodes", len(nodes), "edges", len(edges), "windowSec", windowSec)
}

// logStatuses writes one "red statuses" debug line per service: its requests
// this window by protocol and status ("http 500=3, rpc UNKNOWN=2"), so a
// fault the error count misses can be traced to what the sensor reported.
func (c *Collector) logStatuses(statuses map[servicePair]map[string]int64) {
	if !c.log.V(1).Enabled() {
		return
	}
	pairs := make([]servicePair, 0, len(statuses))
	for pair := range statuses {
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Namespace != pairs[j].Namespace {
			return pairs[i].Namespace < pairs[j].Namespace
		}
		return pairs[i].Workload < pairs[j].Workload
	})
	for _, pair := range pairs {
		keys := make([]string, 0, len(statuses[pair]))
		for k := range statuses[pair] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%d", k, statuses[pair][k]))
		}
		c.log.V(1).Info("red statuses", "key", pair.Namespace+"/"+pair.Workload, "statuses", strings.Join(parts, ", "))
	}
}

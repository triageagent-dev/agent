package redsource

import (
	"math"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
)

// servicePair is one (namespace, workload) identity — reused for a series'
// source, its destination, and an output TopologyNode.
type servicePair struct {
	Namespace string
	Workload  string
}

// requestGroup is the (destination, status) grouping identity RED
// aggregates a RequestMetric sample by. The metrics are server-side, so
// the destination is the measured service itself.
type requestGroup struct {
	dst    servicePair
	status string
}

// requestSeries is the delta-baseline IDENTITY for one RequestMetric
// sample: the metric's name (so an HTTP series and a gRPC series of the same
// service can never share a baseline entry, even if their label sets were
// identical), its requestGroup PLUS the sample's full raw label set
// (metricset.Point.Key). Baselining on the full label set — deliberately
// FINER than requestGroup — is what keeps a label dimension a profile
// doesn't group by (the OBI profile's http_request_method/http_route:
// SampleLabels, not part of requestGroup) from corrupting the delta: a new
// route appearing mid-run gets its OWN first-seen baseline entry instead of
// being silently summed into an existing group's cumulative value and
// reported as a spike. This generalizes the same per-series baselining
// clientEdgeSeries (below) uses for topology edges to the requests-metric
// side too: reqSeries carries every one of the OBI profile's 5 label
// dimensions (pair+method+route+status) explicitly, so this engine achieves
// the same correctness for ANY profile's extra dimensions without needing to
// know what they are.
type requestSeries struct {
	metric   string
	group    requestGroup
	labelKey string
}

// requestSeriesDelta pairs one series' requestGroup with its window delta
// and any RequestMetric.SampleLabels values captured from that exact series —
// order (fam.Points's own slice order, preserved by
// computeRequestDeltas) is what makes a sample tie-break deterministic run
// to run rather than dependent on Go's random map iteration order.
type requestSeriesDelta struct {
	group requestGroup
	delta int64
	extra map[string]string
}

// resolveIdentity looks up namespace/owner through Options.Identity — see
// Profile.SourceOwnerLabel/FlowSrcOwnerLabel/FlowDstOwnerLabel. ok is true
// only when Identity is wired, owner is non-empty, AND IdentityFor found a
// currently tracked workload for it; every other case (Identity nil, owner
// empty, or the owner unknown to the index) returns ok=false and an empty
// identity. What an unresolved lookup means — fall back to the profile's own
// raw label value, or treat the row as unattributable — is the caller's
// decision, since the RED destination path and the L4/L7 source paths need
// different answers to the same failure (see computeRequestDeltas,
// computeClientEdgeDeltas, and collector.go's L4 flow block).
func (c *Collector) resolveIdentity(namespace, owner string) (identity string, ok bool) {
	if c.opts.Identity == nil || owner == "" {
		return "", false
	}
	return c.opts.Identity.IdentityFor(namespace, owner)
}

// counterDelta returns cur-prev for a series present in the previous
// read's map (ok=true), except when that would be negative (a counter
// reset — the measured process restarted), in which case cur itself is
// treated as the delta. ok=false — the series had no entry at all in the
// previous read — always returns zero, never cur: reporting a service's
// entire LIFETIME cumulative count as a single window's delta on every
// collector restart is exactly the production bug this distinction fixes.
//
// The request and bucket deltas below pass ok=false only on the FIRST read of
// a target's metric. A series that appears on a later read is new on the
// sensor, where it started from zero, so they pass prev=0, ok=true for it
// (see firstRead). The topology deltas keep ok=false for every unseen series.
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

// firstRead reports whether this is the first time the collector reads metric
// (kind "req" or "bkt" plus the metric name) from target addr, and records the
// read. On a first read every series carries its lifetime count, so none
// yields a delta. On a later read a series that was not there before is new
// on the sensor (the exporter drops an idle series and restarts it from zero,
// and a route's first 5xx creates its series), so its whole value is this
// window's delta: without this, a burst on a new series was lost, and so was
// the first window of every fault on one.
func (c *Collector) firstRead(addr, kind, metric string) bool {
	if c.readBefore == nil {
		c.readBefore = make(map[string]map[string]bool)
	}
	seen := c.readBefore[addr]
	if seen == nil {
		seen = make(map[string]bool)
		c.readBefore[addr] = seen
	}
	k := kind + "\x00" + metric
	first := !seen[k]
	seen[k] = true
	return first
}

// destPairFor resolves one series' destination servicePair from
// Profile.DestNamespaceLabel/DestWorkloadLabel, with Workload overridden by
// c.resolveIdentity(namespace, SourceOwnerLabel) when the identity index
// currently tracks that owner — see resolveIdentity's doc comment for what
// "currently tracks" means. Namespace is never overridden. Shared by
// computeRequestDeltas and computeBucketDeltas so RED's destination grouping
// and the duration histogram's destination grouping can never key the same
// destination differently — sendRED/sendRedSummary look up bucketTotals by
// the servicePair computeRequestDeltas produced (see redTotals/bucketTotals
// in collector.go).
func (c *Collector) destPairFor(m metricset.Point) servicePair {
	p := c.opts.Profile
	dstNamespace := m.Attr(p.DestNamespaceLabel)
	dstWorkload := m.Attr(p.DestWorkloadLabel)
	if identity, ok := c.resolveIdentity(dstNamespace, m.Attr(p.SourceOwnerLabel)); ok {
		dstWorkload = identity
	}
	return servicePair{Namespace: dstNamespace, Workload: dstWorkload}
}

// computeRequestDeltas extracts every series of one RequestMetric from
// a single target's parsed snapshot and returns each series' requestGroup +
// delta against that same target's previous snapshot — see requestSeries'
// doc comment for why the baseline is keyed FINER than the returned
// grouping. A series with no destination identity is dropped up front. fam may be nil (the
// metric absent from this snapshot), in which case every previously-tracked
// series' delta is computed against zero.
//
// prevReq[addr] holds the baselines of EVERY request metric for the target,
// so this call replaces only rm's own entries and leaves the other metrics'
// untouched.
func (c *Collector) computeRequestDeltas(addr string, rm RequestMetric, fam *metricset.Metric) []requestSeriesDelta {
	cur := make(map[requestSeries]float64)
	extraFor := make(map[requestSeries]map[string]string)
	var order []requestSeries

	if fam != nil {
		for _, m := range fam.Points {
			grp := requestGroup{
				dst:    c.destPairFor(m),
				status: m.Attr(rm.StatusLabel),
			}
			if grp.dst.Namespace == "" && grp.dst.Workload == "" {
				continue // nothing to attribute this sample to
			}
			s := requestSeries{metric: rm.MetricName, group: grp, labelKey: m.Key()}
			if _, seen := cur[s]; !seen {
				order = append(order, s)
				if len(rm.SampleLabels) > 0 {
					ex := make(map[string]string, len(rm.SampleLabels))
					for _, lbl := range rm.SampleLabels {
						ex[lbl] = m.Attr(lbl)
					}
					extraFor[s] = ex
				}
			}
			cur[s] += m.Value
		}
	}

	prev := c.prevReq[addr]
	first := c.firstRead(addr, "req", rm.MetricName)
	// byLabel finds a series' previous value under another group: the same raw
	// label set whose destination identity resolved differently last read. It
	// is the same counter, so it must not be taken for a new series.
	var byLabel map[string]float64
	out := make([]requestSeriesDelta, 0, len(order))
	for _, s := range order {
		prevVal, ok := prev[s]
		if !ok && !first {
			if byLabel == nil {
				byLabel = make(map[string]float64, len(prev))
				for ps, v := range prev {
					if ps.metric == rm.MetricName {
						byLabel[ps.labelKey] = v
					}
				}
			}
			// Not in the previous read at all: new on the sensor, from zero.
			prevVal, ok = byLabel[s.labelKey], true
		}
		out = append(out, requestSeriesDelta{group: s.group, delta: counterDelta(prevVal, cur[s], ok), extra: extraFor[s]})
	}
	next := make(map[requestSeries]float64, len(cur))
	for s, v := range prev {
		if s.metric != rm.MetricName {
			next[s] = v
		}
	}
	for s, v := range cur {
		next[s] = v
	}
	c.prevReq[addr] = next
	return out
}

// edgeKey is the aggregation identity for one output TopologyEdge: (src,
// dst-or-external, protocol).
type edgeKey struct {
	src      servicePair
	dst      servicePair
	external string
	protocol string
	dstPort  int // 0 when unknown
}

// edgeDelta is one edge's accumulated window contribution — Calls from L7
// client spans (RequestMetrics/ClientMetrics), Bytes from L4 network flows
// (FlowMetric). An edge normally carries exactly one of the two (a
// protocol's family contributes one or the other), but the type allows
// both so a future profile emitting both for the same (src,dst,protocol)
// key is not precluded.
type edgeDelta struct {
	Calls int64
	Bytes int64
}

// addEdgeDelta accumulates calls/bytes onto m[k] — map values can't be
// mutated in place through indexing, so every accumulation site (runOnce's
// per-tick edgeTick, and its fold into Collector.edgeAccum) goes through
// this instead of repeating the read-modify-write.
func addEdgeDelta(m map[edgeKey]edgeDelta, k edgeKey, calls, bytes int64) {
	d := m[k]
	d.Calls += calls
	d.Bytes += bytes
	m[k] = d
}

// externalLabel renders an out-of-scope destination as "<workload>.<namespace>".
// A destination with no namespace falls
// back to the bare workload name.
func externalLabel(dst servicePair) string {
	if dst.Namespace == "" {
		return dst.Workload
	}
	return dst.Workload + "." + dst.Namespace
}

// resolveDstLabeled classifies one network-flow destination as either an
// in-scope k8s workload (dst set, external empty) or an out-of-scope one
// (external set, dst zero-value). A flow row names the destination's own
// (namespace, owner) directly, so the allow predicate is the whole test: no
// server-address text to parse. allow is the collector's namespaceAllowed
// (static set or dynamic Options.NamespaceAllowed); nil means unrestricted.
// A destination with no identity at all resolves to neither — the caller
// treats that as unattributable.
func resolveDstLabeled(dst servicePair, allow func(string) bool) (out servicePair, external string) {
	if dst.Namespace == "" && dst.Workload == "" {
		return servicePair{}, ""
	}
	if allow == nil || allow(dst.Namespace) {
		return dst, ""
	}
	return servicePair{}, push.TruncateTopologyExternal(externalLabel(dst))
}

// hostOnly strips a trailing ":port" from a server_address value, if
// present.
func hostOnly(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// portOf returns the numeric port of a "host:port" server_address value, or
// 0 when addr carries none or it is not a valid port.
func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(p)
	if err != nil || n < 0 || n > 65535 {
		return 0
	}
	return n
}

// flowPort parses a flow row's destination-port label value; 0 when the
// label is unset, empty or not a valid port.
func flowPort(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > 65535 {
		return 0
	}
	return n
}

// clientEdgeSeries identifies one CLIENT-side series' delta baseline — the
// exact raw label set on the sample, plus the protocol its family belongs
// to (so two families can never collide on the same key even if a future
// profile reuses identical label values across them).
type clientEdgeSeries struct {
	protocol string
	labelKey string
}

// clientEdgeSample is one client-side series' derived delta for this
// read: the caller's identity, the raw server-address text it called, and
// the window's call-count delta. Dst/External resolution happens in the
// caller (resolveDstAddress), not here. srcUnknown is true when
// Profile.SourceOwnerLabel named an owner Options.Identity does not
// currently track — the source has no in-scope node to hang an edge on, so
// runOnce drops the row instead of emitting it under the raw service_name.
type clientEdgeSample struct {
	src           servicePair
	srcUnknown    bool
	serverAddress string
	dstPort       int // port parsed from serverAddress before the host is resolved; 0 if none
	protocol      string
	calls         int64
}

// computeClientEdgeDeltas extracts every series of one
// !Profile.HasSourceLabels client-side family from a single target's
// parsed snapshot and returns each series' delta against that same target's
// previous snapshot of the SAME family. fam may be nil (the family absent
// from this snapshot). It reads the caller identity and server-address label
// keys from the Profile rather than hardcoded names, so it works for any
// profile's client-side family shape.
//
// Baselines are maintained for EVERY series regardless of whether its src is
// in-scope (the tenant guard is applied at aggregation in runOnce, not
// here), so a sensor target shared across several tenants keeps correct
// per-series deltas for all of them even though only one tenant's frame
// ends up carrying any given result.
func (c *Collector) computeClientEdgeDeltas(addr string, fam *metricset.Metric, protocol string) []clientEdgeSample {
	if fam == nil {
		return nil
	}
	p := c.opts.Profile
	prev := c.prevClient[addr]
	if prev == nil {
		prev = make(map[clientEdgeSeries]float64)
		c.prevClient[addr] = prev
	}

	type rowMeta struct {
		src           servicePair
		srcUnknown    bool
		serverAddress string
	}
	cur := make(map[clientEdgeSeries]float64)
	meta := make(map[clientEdgeSeries]rowMeta)

	for _, m := range fam.Points {
		key := clientEdgeSeries{protocol: protocol, labelKey: m.Key()}
		cur[key] += m.Value
		srcNamespace := m.Attr(p.SourceNamespaceLabel)
		owner := m.Attr(p.SourceOwnerLabel)
		srcWorkload := m.Attr(p.SourceWorkloadLabel)
		srcUnknown := false
		if identity, ok := c.resolveIdentity(srcNamespace, owner); ok {
			srcWorkload = identity
		} else if c.opts.Identity != nil && owner != "" {
			srcUnknown = true
		}
		meta[key] = rowMeta{
			src:           servicePair{Namespace: srcNamespace, Workload: srcWorkload},
			srcUnknown:    srcUnknown,
			serverAddress: m.Attr(p.ServerAddressLabel),
		}
	}

	out := make([]clientEdgeSample, 0, len(cur))
	for key, curVal := range cur {
		prevVal, ok := prev[key]
		d := counterDelta(prevVal, curVal, ok)
		prev[key] = curVal
		md := meta[key]
		out = append(out, clientEdgeSample{src: md.src, srcUnknown: md.srcUnknown, serverAddress: md.serverAddress, dstPort: portOf(md.serverAddress), protocol: protocol, calls: d})
	}
	return out
}

// bucketSeries identifies one target-local baseline for the duration
// histogram's bucket set: the metric it belongs to, the destination it will be aggregated into, PLUS
// the sample's full raw label set (metricset.Point.Key). As with
// requestSeries, baselining on the full label set — finer than the
// destination-only grouping computeBucketDeltas aggregates into — is what
// keeps a label dimension the grouping ignores (the OBI profile's
// method+route; a profile's caller identity, if any) from corrupting the
// delta when such a sub-series appears or disappears between ticks.
// Display-only: never influences the RED gate.
type bucketSeries struct {
	metric   string
	dst      servicePair
	labelKey string
}

// computeBucketDeltas extracts this window's per-bucket cumulative-count
// DELTA from one target's histogram-typed duration family, summed across
// every series sharing a destination — p99 is a per-service display figure.
// The destination is resolved through destPairFor, the SAME helper
// computeRequestDeltas uses, so a resolved identity's histogram lands under
// the identical servicePair sendRED/sendRedSummary use to look up
// bucketTotals: see destPairFor's doc comment. It has the same
// counter-reset AND first-seen handling as counterDelta, generalized to
// baseline on the full label set rather than a fixed (method,route,status)
// tuple, the same generalization requestSeries applies on the RED side. fam
// must be HISTOGRAM-typed; nil or
// non-histogram returns nil.
func (c *Collector) computeBucketDeltas(addr string, fam *metricset.Metric) map[servicePair]map[float64]int64 {
	sums, _ := c.computeBucketDeltasOps(addr, RequestMetric{}, fam)
	return sums
}

// opBuckets is one destination's per-operation bucket deltas, keyed by
// operationKey.
type opBuckets map[string]map[float64]int64

// computeBucketDeltasOps is computeBucketDeltas plus, when rm names
// SampleLabels, the same deltas split per operation within each destination.
// The duration histogram's series carry the same operation labels as its
// request counter (OBI emits ONE histogram for both), so each bucket series'
// operation is read off its own Attrs with operationKey, the helper
// opTotalsFor keys by, and the two stay aligned. When rm names SampleLabels,
// a series with none of them set is left out of the latency sums entirely
// (its requests and errors still count): OBI emits that only for an HTTP/2
// stream whose headers it could not decode, typically a long-lived stream on
// a connection opened before instrumentation (a gRPC watch or event stream),
// whose duration is the stream's lifetime, not a request's latency. Counted,
// a service whose only traffic is such streams reads p99 at the top bucket.
func (c *Collector) computeBucketDeltasOps(addr string, rm RequestMetric, fam *metricset.Metric) (map[servicePair]map[float64]int64, map[servicePair]opBuckets) {
	name := rm.DurationBucketMetric
	if fam != nil {
		name = fam.Name
	}
	// Recorded even when the family is absent, so that its first appearance
	// on a target already read counts from zero (see firstRead).
	first := name == "" || c.firstRead(addr, "bkt", name)
	if fam == nil || fam.Kind != metricset.Histogram {
		return nil, nil
	}
	var ops map[servicePair]opBuckets
	prevAddr := c.prevBuckets[addr]
	// byLabel: as in computeRequestDeltas, a series whose destination resolved
	// differently last read is the same counter, not a new one.
	var byLabel map[string]map[float64]uint64
	curAddr := make(map[bucketSeries]map[float64]uint64)
	sums := make(map[servicePair]map[float64]int64)

	for _, m := range fam.Points {
		dst := c.destPairFor(m)
		if dst.Namespace == "" && dst.Workload == "" {
			continue
		}
		hs := bucketSeries{metric: fam.Name, dst: dst, labelKey: m.Key()}
		prev, hadPrev := prevAddr[hs]
		if !hadPrev && !first {
			if byLabel == nil {
				byLabel = make(map[string]map[float64]uint64, len(prevAddr))
				for ps, v := range prevAddr {
					if ps.metric == fam.Name {
						byLabel[ps.labelKey] = v
					}
				}
			}
			prev, hadPrev = byLabel[hs.labelKey]
		}
		// A series absent from the previous read of a target already read is
		// new on the sensor: its cumulative counts are this window's.
		newSeries := !hadPrev && !first
		var opByLe map[float64]int64
		key, _, identified := operationKey(rm, m.Attrs)
		if identified {
			if ops == nil {
				ops = make(map[servicePair]opBuckets)
			}
			if ops[dst] == nil {
				ops[dst] = make(opBuckets)
			}
			opByLe = ops[dst][key]
			if opByLe == nil {
				opByLe = make(map[float64]int64)
				ops[dst][key] = opByLe
			}
		}
		cur := make(map[float64]uint64, len(m.Buckets))
		byLe := sums[dst]
		if byLe == nil {
			byLe = make(map[float64]int64)
			sums[dst] = byLe
		}
		for _, b := range m.Buckets {
			le := b.UpperBound
			cc := b.CumulativeCount
			cur[le] = cc
			prevCC, ok := prev[le]
			var d int64
			if ok {
				d = int64(cc) - int64(prevCC)
				if d < 0 {
					d = int64(cc) // counter reset: current cumulative value is this window's count
				}
			} else if newSeries {
				d = int64(cc)
			}
			// Otherwise (the first read of this target's histogram, or a
			// boundary missing from the series' previous read) d stays 0
			// rather than reporting cc, the lifetime cumulative count, as
			// this window's delta.
			if identified || len(rm.SampleLabels) == 0 {
				byLe[le] += d
			}
			if opByLe != nil {
				opByLe[le] += d
			}
		}
		curAddr[hs] = cur
	}
	// prevBuckets[addr] holds every request metric's duration histogram (HTTP
	// and gRPC alike); replace only this family's entries.
	for hs, cur := range prevAddr {
		if hs.metric != fam.Name {
			curAddr[hs] = cur
		}
	}
	c.prevBuckets[addr] = curAddr
	return sums, ops
}

// p99FromBucketDeltas linearly interpolates the 99th-percentile duration
// (same unit as the bucket upper bounds — seconds) from a destination's
// combined bucket-count deltas for one window, including the +Inf
// fallback-to-largest-finite-boundary behavior. ok is false when there is
// nothing to compute from.
func p99FromBucketDeltas(byLe map[float64]int64) (p99 float64, ok bool) {
	var les []float64
	for le := range byLe {
		if !math.IsInf(le, 1) {
			les = append(les, le)
		}
	}
	if len(les) == 0 {
		return 0, false
	}
	sort.Float64s(les)

	total, hasInf := byLe[math.Inf(1)]
	if !hasInf {
		total = byLe[les[len(les)-1]]
	}
	if total <= 0 {
		return 0, false
	}
	target := float64(total) * 0.99

	var prevLe, prevCum float64
	for _, le := range les {
		cum := float64(byLe[le])
		if cum >= target {
			if cum == prevCum {
				return le, true
			}
			return prevLe + (target-prevCum)/(cum-prevCum)*(le-prevLe), true
		}
		prevLe, prevCum = le, cum
	}
	// The crossing was never reached within the finite buckets — it falls in
	// +Inf. Report the largest finite boundary rather than interpolating
	// into infinity.
	return les[len(les)-1], true
}

// sortNodes returns nodeSet's members sorted by (Namespace, Workload) —
// alphabetically stable, the ranking the node cap trims by (nodes have no
// natural magnitude, unlike edges' Calls).
func sortNodes(nodeSet map[servicePair]bool) []push.TopologyNode {
	out := make([]push.TopologyNode, 0, len(nodeSet))
	for p := range nodeSet {
		out = append(out, push.TopologyNode{Namespace: p.Namespace, Workload: p.Workload})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Workload < out[j].Workload
	})
	return out
}

// sortEdges orders edges by Calls descending, then Bytes descending — the
// cap in flushTopology keeps the top by that ordering. Bytes as the
// secondary key (rather than straight to the tie-break string) means a
// flow-only edge (Calls==0 on every such edge) still ranks by traffic
// volume among its peers instead of falling through to alphabetical order.
// A deterministic tie-break string breaks the remaining ties so output
// order, and which edges survive an over-cap trim, does not depend on Go's
// random map iteration order.
func sortEdges(edges []push.TopologyEdge) {
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Calls != edges[j].Calls {
			return edges[i].Calls > edges[j].Calls
		}
		if edges[i].Bytes != edges[j].Bytes {
			return edges[i].Bytes > edges[j].Bytes
		}
		return edgeSortKey(edges[i]) < edgeSortKey(edges[j])
	})
}

func edgeSortKey(e push.TopologyEdge) string {
	return e.SrcNamespace + "/" + e.SrcWorkload + ">" + e.DstNamespace + "/" + e.DstWorkload + e.External + "/" + e.Protocol + ":" + strconv.Itoa(e.DstPort)
}

// namespaceSet turns Options.Namespaces into a lookup set. Returns nil for
// an empty/blank-only list, which the collector reads as "forward every
// namespace".
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

// --- L4 network-flow edges (Profile.FlowMetric, docs/todo/service-graph-from-obi.md) ---

// flowSeries identifies one network-flow series' delta baseline: its full
// raw label set (metricset.Point.Key) — same "baseline finer than
// aggregation" discipline as requestSeries/clientEdgeSeries.
type flowSeries struct {
	labelKey string
}

// flowSample is one network-flow series' derived delta for this read,
// before the drop/dedupe/identity-resolution rules filterFlowSamples and
// its caller in runOnce apply.
type flowSample struct {
	srcNamespace, srcOwner, srcOwnerType string
	dstNamespace, dstOwner, dstOwnerType string
	// direction is "" when Profile.FlowDirectionLabel is unset — see
	// filterFlowSamples' hasDirection parameter, which is what actually
	// distinguishes "no direction signal" from "direction says something
	// other than request".
	direction string
	dstPort   int // 0 when Profile.FlowDstPortLabel is unset or the row has none
	bytes     int64
}

// computeFlowEdgeDeltas extracts every series of Profile.FlowMetric from a
// single target's parsed snapshot and returns each series' delta against
// that same target's previous snapshot of the SAME family — mirrors
// computeClientEdgeDeltas' per-target baseline discipline exactly. fam
// may be nil (FlowMetric absent from this snapshot — the sensor's OBI
// instance doesn't have network flows enabled), in which case this is a
// no-op.
func (c *Collector) computeFlowEdgeDeltas(addr string, fam *metricset.Metric) []flowSample {
	if fam == nil {
		return nil
	}
	p := c.opts.Profile
	prev := c.prevFlow[addr]
	if prev == nil {
		prev = make(map[flowSeries]float64)
		c.prevFlow[addr] = prev
	}

	type rowMeta struct {
		srcNamespace, srcOwner, srcOwnerType string
		dstNamespace, dstOwner, dstOwnerType string
		direction                            string
		dstPort                              int
	}
	cur := make(map[flowSeries]float64)
	meta := make(map[flowSeries]rowMeta)

	for _, m := range fam.Points {
		key := flowSeries{labelKey: m.Key()}
		cur[key] += m.Value
		meta[key] = rowMeta{
			srcNamespace: m.Attr(p.FlowSrcNamespaceLabel),
			srcOwner:     m.Attr(p.FlowSrcOwnerLabel),
			srcOwnerType: m.Attr(p.FlowSrcOwnerTypeLabel),
			dstNamespace: m.Attr(p.FlowDstNamespaceLabel),
			dstOwner:     m.Attr(p.FlowDstOwnerLabel),
			dstOwnerType: m.Attr(p.FlowDstOwnerTypeLabel),
			direction:    m.Attr(p.FlowDirectionLabel),
			dstPort:      flowPort(m.Attr(p.FlowDstPortLabel)),
		}
	}

	out := make([]flowSample, 0, len(cur))
	for key, curVal := range cur {
		prevVal, ok := prev[key]
		d := counterDelta(prevVal, curVal, ok)
		prev[key] = curVal
		md := meta[key]
		out = append(out, flowSample{
			srcNamespace: md.srcNamespace, srcOwner: md.srcOwner, srcOwnerType: md.srcOwnerType,
			dstNamespace: md.dstNamespace, dstOwner: md.dstOwner, dstOwnerType: md.dstOwnerType,
			direction: md.direction, dstPort: md.dstPort, bytes: d,
		})
	}
	return out
}

// isWorkloadOwnerType reports whether t is one of the three kinds
// virtual-app tracking (push/virtualapps.go) actually follows. A network
// flow's owner can also be "Service" (a redirection object, never a
// workload) or "Node" (infrastructure, not a workload) — see
// filterFlowSamples.
func isWorkloadOwnerType(t string) bool {
	switch t {
	case "Deployment", "StatefulSet", "DaemonSet":
		return true
	default:
		return false
	}
}

// filterFlowSamples applies the drop/keep rules a network-flow row needs
// before it can become a topology edge:
//
//  1. Drop a row with an empty owner name on either end, or a Node-typed
//     owner on either end (infrastructure, not a workload).
//  2. When hasDirection (Profile.FlowDirectionLabel is set), keep only rows
//     whose direction is "request" — the initiator's own row (see the
//     vendored OBI's Direction attribute, third_party/obi/pkg/internal/
//     netolly/ebpf/record_getters.go: request/response/unknown). When NOT
//     hasDirection, every surviving row is kept regardless of its direction
//     field (which is "" in that case) — a documented v1 limitation, not a
//     silent one (see Profile.FlowDirectionLabel's doc comment).
//  3. Dedupe by (srcNamespace, srcOwner, dstNamespace, dstOwner) — a NAME-only
//     key, type-blind: OBI has been observed emitting both a Service-typed
//     and a Deployment/StatefulSet/DaemonSet-typed row for what is the same
//     logical pair (e.g. the fixture's orders-app-api<->billing-app-api
//     flow, resolved through the ClusterIP-owning Service on one row and
//     the backing Deployment on another). When at least one row in a name
//     pair's group is fully workload-typed (both ends), every row in that
//     group where either end is NOT workload-typed is dropped and the
//     workload-typed rows' bytes are summed; otherwise every row in the
//     group is kept, summed together.
func filterFlowSamples(samples []flowSample, hasDirection bool) []flowSample {
	kept := make([]flowSample, 0, len(samples))
	for _, s := range samples {
		if s.srcOwner == "" || s.dstOwner == "" || s.srcOwnerType == "Node" || s.dstOwnerType == "Node" {
			continue
		}
		if hasDirection && s.direction != "request" {
			continue
		}
		kept = append(kept, s)
	}

	type pairKey struct{ srcNS, srcOwner, dstNS, dstOwner string }
	type group struct {
		rows []int // index into kept, insertion order
	}
	groups := make(map[pairKey]*group)
	var order []pairKey
	for i, s := range kept {
		k := pairKey{s.srcNamespace, s.srcOwner, s.dstNamespace, s.dstOwner}
		g := groups[k]
		if g == nil {
			g = &group{}
			groups[k] = g
			order = append(order, k)
		}
		g.rows = append(g.rows, i)
	}

	out := make([]flowSample, 0, len(groups))
	for _, k := range order {
		g := groups[k]
		hasWorkload := false
		for _, i := range g.rows {
			if isWorkloadOwnerType(kept[i].srcOwnerType) && isWorkloadOwnerType(kept[i].dstOwnerType) {
				hasWorkload = true
				break
			}
		}
		var sum int64
		var base flowSample
		haveBase := false
		for _, i := range g.rows {
			row := kept[i]
			if hasWorkload && (!isWorkloadOwnerType(row.srcOwnerType) || !isWorkloadOwnerType(row.dstOwnerType)) {
				continue // Service-typed (or otherwise non-workload) alternate for a pair that also has a workload-typed row
			}
			sum += row.bytes
			if !haveBase {
				base, haveBase = row, true
			} else if row.dstPort != base.dstPort {
				base.dstPort = 0 // summed across ports: no single destination port to name
			}
		}
		if !haveBase {
			continue
		}
		base.bytes = sum
		out = append(out, base)
	}
	return out
}

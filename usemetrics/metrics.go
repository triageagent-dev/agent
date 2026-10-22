package usemetrics

// OTLP metric names emitted by the node-exporter role
// (nodeexporter package, `triage-agent --mode=node-exporter`) and read back here.
// Both sides of this contract live in this module (unlike the push/protocol.go wire
// contract, which is duplicated across the triage-agent module boundary), so it is a
// literal Go import rather than a copied pair of constants — see
// docs/adr/0010-use-metrics-node-exporter.md.
//
// All metrics are labeled {namespace, service, pod} — per-pod granularity, not
// pre-aggregated — so that Collector can compute the max-across-pods/sum-of-deltas
// aggregation described in ServiceUseMetrics's field docs (push/protocol.go) and pick
// a worst-offender pod for the Sample string, the same "aggregate on the read side"
// division of labor redsource/logmetrics already use for their own per-series label
// sets.
const (
	// MetricCPUPsiAvg60/MemPsiAvg60/IOPsiAvg60 are gauges: the pod cgroup's
	// cpu.pressure/memory.pressure/io.pressure "some avg60" value, as a percentage
	// (0-100). Omitted entirely for a pod when the corresponding PSI file is absent
	// (CONFIG_PSI compiled out) — graceful degrade, not a zero value, so the
	// collector never mistakes "unavailable" for "no pressure".
	MetricCPUPsiAvg60 = "use.cpu.psi.avg60"
	MetricMemPsiAvg60 = "use.mem.psi.avg60"
	MetricIOPsiAvg60  = "use.io.psi.avg60"

	// MetricMemWorkingSetPct is a gauge: (memory.current - inactive_file)/limit*100
	// — the OOM-relevant utilization figure, and the one `kubectl top` and k9s
	// display. PREFER THIS over MetricMemPct: memory.current charges reclaimable
	// page cache, which the kernel drops under pressure rather than OOM-killing
	// over, so a cache-heavy service pins MetricMemPct near 100% indefinitely
	// while never being at risk (a neo4j pod measured 99.97% there against
	// 96.5% here at the same instant, 2026-08-29). Omitted on the same
	// unbounded-pod / unreadable-file terms as MetricMemPct.
	MetricMemWorkingSetPct = "use.mem.ws.pct"

	// MetricMemPct is a gauge: memory.current/limit*100, omitted when the pod is
	// unbounded — matches ServiceUseMetrics.MemPct's "0 when no limit" wire
	// semantics via plain family-absence. Kept alongside MetricMemWorkingSetPct
	// so the page-cache component stays observable (the difference between the
	// two IS the reclaimable cache), but it is NOT the number to threshold on.
	//
	// The limit is the pod cgroup's memory.max when kubelet set one, and
	// otherwise the sum of the pod spec's regular-container limits — see
	// nodeexporter.memLimitBytes. Without that fallback any pod carrying an
	// unlimited sidecar or init container reports no utilization at all,
	// indistinguishably from a pod nobody bothered to limit (observed on
	// qdrant, whose 40Gi-limited container sits beside a limitless
	// `sysctl-tune` init container).
	//
	// CPU utilization is no longer missing: v1 deliberately shipped without it
	// ("materially more involved than the mem.current/mem.max snapshot ratio",
	// docs/adr/0010-use-metrics-node-exporter.md), which was true and which
	// left USE reporting one of its two utilization dimensions for a year. See
	// MetricCPUUsageUsecTotal / MetricCPULimitCores above.
	MetricMemPct = "use.mem.pct"

	// MetricMemBytes/MetricMemLimitBytes are gauges: the cgroup's working-set
	// bytes and the limit they are measured against. Percentages alone answer
	// "how full" but not "how much", and an operator sizing a limit or reading
	// a alongside `kubectl top` needs the absolute figures — 1976Mi of 2Gi is
	// actionable in a way 96% is not. MetricMemLimitBytes is also the only
	// place the pod-spec fallback's resolved denominator becomes visible, so a
	// surprising percentage can be traced to the limit it was divided by.
	MetricMemBytes      = "use.mem.bytes"
	MetricMemLimitBytes = "use.mem.limit.bytes"

	// MetricCPUUsageUsecTotal is a counter: cpu.stat's cumulative usage_usec.
	// Diffed across reads and divided by the window and MetricCPULimitCores
	// to produce CPU utilization — the U of USE for CPU, which did not exist in
	// either repo until 2026-08-29 despite the value sitting on the first line
	// of a file the exporter already opened for throttling counters.
	MetricCPUUsageUsecTotal = "use.cpu.usage.usec"
	// MetricCPULimitCores is a gauge: the cgroup's CPU quota in cores
	// (cpu.max quota/period, or the pod spec's summed container limits when
	// kubelet left the pod cgroup unbounded). Omitted when genuinely unlimited.
	MetricCPULimitCores = "use.cpu.limit.cores"

	// MetricOOMKillTotal is a counter: memory.events' cumulative oom_kill count for
	// the pod cgroup (since the cgroup was created, i.e. since pod start — cgroups
	// are not reused across pod restarts). Collector diffs this against the
	// previous read the same way redsource diffs request/error counters.
	MetricOOMKillTotal = "use.oom.kill"

	// MetricOOMKillStatusTotal is a counter: the number of container
	// terminations with reason OOMKilled that the kubelet recorded in the pod's
	// STATUS, accumulated by the exporter across observations (pod status only
	// shows each container's LAST termination, see nodeexporter.oomStatusTracker).
	// An independent second source beside MetricOOMKillTotal, which reads the
	// cgroup file: the collector diffs both and takes the larger delta per pod,
	// so a kill the cgroup path misses is still reported. Baselines at 0 when the
	// exporter first sees a pod — a pod's history is never reported as new kills.
	MetricOOMKillStatusTotal = "use.oom.kill.status"

	// MetricCPUNrPeriodsTotal/NrThrottledTotal are counters: cpu.stat's cumulative
	// nr_periods/nr_throttled. Collector diffs both and reports the ratio as
	// ServiceUseMetrics.ThrottledPct.
	MetricCPUNrPeriodsTotal   = "use.cpu.nr.periods"
	MetricCPUNrThrottledTotal = "use.cpu.nr.throttled"

	// MetricMemNodeTotalBytes/MetricCPUNodeCores are gauges: the NODE's total
	// memory and online CPU core count — resolved once at exporter startup
	// from /proc/meminfo and /sys/devices/system/cpu/online (never from the k8s
	// Node object; see nodeexporter.resolveNodeMemTotalBytes), and repeated on
	// every pod's sample set since a node has exactly one of each.
	//
	// These exist to answer "is this one pod eating the box" — the portable
	// denominator every prior utilization figure lacked: MetricMemPct/
	// MetricMemWorkingSetPct/MetricCPULimitCores are all measured against a
	// LIMIT, which only exists when someone set one, so a pod with no limit
	// reports no utilization at all and is invisible on that axis. Node
	// capacity always exists — bare metal, a plain container, anything with
	// cgroups — which is the entire point of reading it from /proc and /sys
	// rather than the Kubernetes API.
	//
	// MetricCPUNodeCores has no "omitted" case: its resolution chain ends in
	// runtime.NumCPU(), which never fails. MetricMemNodeTotalBytes is omitted
	// on the rare host where /proc/meminfo itself couldn't be read, the same
	// graceful-degrade convention every other family here follows.
	MetricMemNodeTotalBytes = "use.mem.node.total.bytes"
	MetricCPUNodeCores      = "use.cpu.node.cores"

	// MetricMemRequestBytes/MetricCPURequestCores are gauges: the sum of the pod
	// spec's REGULAR container memory/CPU requests (init containers excluded,
	// same reasoning as regularContainerMemLimits — they are terminated by the
	// time a pod is Running and would inflate the denominator). Omitted
	// entirely for a pod when no regular container declares the corresponding
	// request — never reported as a meaningless 0.
	//
	// This is the Kubernetes right-sizing axis: usage far above request means
	// the pod is a noisy neighbour and the first candidate for eviction under
	// node pressure; usage far below request means reserved capacity going to
	// waste. Unlike MetricMemLimitBytes/MetricCPULimitCores there is no cgroup
	// file to prefer over the spec value — kubelet's cgroup tree reflects
	// limits, never requests — so the pod spec is the sole source, not a
	// fallback.
	MetricMemRequestBytes = "use.mem.request.bytes"
	MetricCPURequestCores = "use.cpu.request.cores"

	// Attribute keys, OpenTelemetry semantic convention names. Fixed: this
	// is our own exporter's schema.
	LabelNamespace = "k8s.namespace.name"
	LabelService   = "service.name"
	LabelPod       = "k8s.pod.name"
)

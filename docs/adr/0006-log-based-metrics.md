# ADR-0006: Log-Based Metrics (Fluent Bit → agent scrape/gate → log_metrics frame)

- **Status:** Implemented (gate widened — see note below)
- **Date:** 2026-07-14
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (outbound push mode — the transport this rides on); triage-agent
  ADR-029 (remote-agent WS receiver); ADR-0007 (fingerprinted `Sample` population)

> **Note (2026-07-16):** the gate originally described below (error count only) was
> widened to `errorCount >= errorThreshold OR warnCount >= errorThreshold` — either
> counter independently qualifies a service, reusing the same threshold for both. A
> warn-heavy service with zero errors previously never crossed the gate even though
> `WarnCount` was already being scraped and shipped whenever a frame *did* go out; now
> it can trigger one on its own. See `logmetrics/collector.go`'s qualifying loop.

> **Note (2026-08-22):** Fluent Bit is no longer the only source `logmetrics.Collector`
> below can scrape — ADR-0014 adds a native `logExporter` container producing the
> IDENTICAL metric/label contract, and `fluentbit.*` is now the legacy path. The
> scrape/delta/gate/frame pipeline this ADR describes is unchanged either way; only
> what produces the Prometheus counters differs. See docs/adr/0014-log-exporter-in-
> agent-ds.md.

## Context

Push mode (ADR-0005) already streams two signal types to triage: Application status
deltas and Kubernetes Warning events. Both are **structural** signals — they say
something changed about a resource's shape or a scheduler-level condition. Neither
sees inside a container: an application logging `level=error` at rising volume, with
no crash, no restart, no Warning event, is invisible to both.

Fluent Bit already runs (or can run) as a log-collection DaemonSet in most clusters.
Its `log_to_metrics` filter can turn a log line matching a regex into a Prometheus
counter, labeled per source — without shipping the log body anywhere. That gives us
error/warn volume **per service** at near-zero marginal cost: no new log-shipping
pipeline, no log storage, just counters this controller already knows how to scrape
(it already talks to the k8s API) and forward (it already has an authenticated
outbound channel).

## Decision

Add an opt-in **log-based metrics** pipeline, entirely on the sender side (this
repo). Three new pieces, gated independently:

```
Fluent Bit DaemonSet (fluentbit.enabled)
  tail /var/log/containers/*.log
    -> [FILTER] kubernetes            (namespace + pod labels)
    -> [FILTER] log_to_metrics x3     (error counter, warn counter, total counter)
    -> [OUTPUT] prometheus_exporter   (:2021 /metrics)

triage-agent logmetrics.Collector (logMetrics.enabled)
  every intervalSeconds:
    discover triage-fluentbit Service's Endpoints
    scrape each pod's /metrics (Prometheus text format)
    sum counters per (namespace, service), diff against the previous scrape
    gate: only services with errorDelta >= errorThreshold
    -> push.Pusher.SendLogMetrics(...)  (log_metrics frame, chunked at 100 services)
```

The collector does not classify or judge anything — like the ADR-0005 Application/
event streamers, it reports raw counters and lets triage's classifier pipeline decide
what a rising error count means for an incident. Deltas, not cumulative totals, are
sent: triage's classifier pipeline reasons about *rate of new errors this window*, not
lifetime totals since the Fluent Bit pod started.

### Why scrape-and-forward instead of a native Prometheus remote_write

Triage has no metrics backend to remote_write into — it consumes discrete events
(the `log_metrics` frame), not a metrics time series. A full remote_write receiver
would be a bigger receiver-side commitment for a signal that, in practice, only
needs "did errors spike for this service" — a periodic gated summary, not a queryable
time series.

### Why Endpoints (not a Prometheus scrape config) for discovery

The controller already holds a `client.Client` from its manager; reading one
Service's `Endpoints` is a single `Get` with no new dependency. A full Prometheus
service-discovery + scrape library would be disproportionate for "hit N known pod IPs
on a fixed port."

### Wire contract (log_metrics frame, v1)

Added to `push/protocol.go` (mirrors triage's `internal/remoteagent/protocol.go` —
hub-first rollout required, see Consequences):

```go
const KindLogMetrics = "log_metrics"

type LogMetricsPayload struct {
    WindowStart int64               `json:"windowStart"` // unix seconds, window open
    WindowSec   int                 `json:"windowSec"`    // window length in seconds
    Services    []ServiceLogMetrics `json:"services"`
}

type ServiceLogMetrics struct {
    Namespace  string `json:"namespace"`
    Service    string `json:"service"`
    ErrorCount int64  `json:"errorCount"`
    WarnCount  int64  `json:"warnCount,omitempty"`
    TotalCount int64  `json:"totalCount,omitempty"`
    Sample     string `json:"sample,omitempty"` // ≤256 chars, truncated before send
}
```

`Frame.LogMetrics *LogMetricsPayload` (`json:"logMetrics,omitempty"`) on the shared
envelope. Max 100 services per frame — a scrape that qualifies more is split into
multiple frames for the same window (`push.MaxLogMetricsServices`,
`logmetrics.chunkServices`).

### Counter delta semantics

Fluent Bit's `log_to_metrics` counters are cumulative for the life of the pod. The
collector keeps a **per-endpoint** snapshot (keyed by `pod-ip:port`, not globally) and
computes `delta = cur - prev` on the next scrape. Two edge cases:

- **Counter reset** (Fluent Bit pod restarted, counter dropped instead of climbing):
  a negative raw delta is instead reported as `cur` itself — the counter has been
  accumulating since the reset, so its current value *is* the count of unreported
  events since then.
- **Endpoint churn** (pod added/removed): per-endpoint keying means one endpoint's
  scrape never gets diffed against a different endpoint's baseline.

### Gate

Only `errorCount >= errorThreshold` (default 10) qualifies a service for the frame.
No qualifying services in a window → no frame sent at all (not an empty-services
frame). This keeps steady-state noise at zero — the frame only exists when something
worth looking at happened.

### Metric names: Fluent Bit's naming, not chosen freely

`log_to_metrics` prefixes every emitted metric:
`<metric_namespace>_<metric_mode>_<metric_name>`, with `metric_namespace` defaulting
to `log_metric`. So a filter configured with `metric_name: log_errors_total` and the
default counter mode emits `log_metric_counter_log_errors_total` — not
`log_errors_total`. The collector's defaults are the **actual emitted names**
(verified against Fluent Bit's own docs and a rendered `helm template` output), not
the names one might guess from the `metric_name` config key alone. All three
(error/warn/total) and both label keys (`namespace`, `service`) are overridable via
flags/values, in case a fleet's existing Fluent Bit already emits under different
names.

### Label naming: `add_label`, not `label_field` or `kubernetes_mode`

`log_to_metrics`'s `kubernetes_mode` auto-labels with fixed names
(`namespace_name`, `pod_name`, ...) that don't match the collector's `namespace`/
`service` label names. `label_field` uses the record's own field name as the label
name (no rename). Only `add_label NAME KEY` lets a filter name the label whatever we
want while reading it via record-accessor from a nested field
(`$kubernetes['labels']['app.kubernetes.io.name']`) — so the chart's pipeline uses
`add_label` exclusively for `namespace`/`service`/`service_fallback`.

**Slash-to-dot**: Fluent Bit's kubernetes filter replaces `/` with `.` in label keys
before they land in the record, so the pod label `app.kubernetes.io/name` is read
back as `$kubernetes['labels']['app.kubernetes.io.name']` — not the label's literal
name. The service label defaults to that, with a **second** `add_label` reading the
plain `app` label into `service_fallback`, for pods that only set the older
convention. The collector coalesces: `ServiceLabel` if present, else
`ServiceLabelFallback` — Fluent Bit's record accessor has no coalesce/rename-on-
missing operator, so this two-label-then-merge shape is done in Go, not in the
pipeline config.

### Fluent Bit naming (`triage-fluentbit`)

Named to avoid any collision with GKE's own `fluentbit-gke` DaemonSet, which some
clusters already run for cluster logging. The two are independent: this DaemonSet
only feeds the log-metrics collector, nothing else.

## Consequences

**Positive**
- Error-volume visibility for services that emit no other structural signal — no new
  log-shipping/storage infrastructure, no log bodies leave the cluster (only
  aggregate counters + a truncated sample).
- Both `logMetrics.enabled` and `fluentbit.enabled` are independent opt-in toggles; a
  fleet with its own Fluent Bit only turns on the collector half.
- Counters are cumulative on the Fluent Bit side, so a dropped connection (push mode
  down, receiver unavailable) loses nothing long-term — the next successful scrape's
  delta recovers the gap once connectivity returns. `SendLogMetrics` is correctly a
  silent (debug-logged) drop when disconnected, not a queued retry.

**Negative / risks**
- **Hub-first rollout required.** The triage receiver's `Frame.Validate()`
  (`internal/remoteagent/protocol.go`) rejects any `kind` it doesn't recognize. Until
  the receiver ships `KindLogMetrics = "log_metrics"` handling, an agent with
  `logMetrics.enabled=true` will have its `log_metrics` frames rejected/dropped at
  the hub — harmlessly (the rest of the push stream, hello/snapshot/app_delta/
  k8s_event, is unaffected), but the feature does nothing until both sides deploy.
  Sequence: receiver first, then `logMetrics.enabled=true` on agents.
- Additional DaemonSet + scrape loop is new resource cost per node (small: 50m/128Mi
  requested per the chart defaults) and an extra periodic HTTP fan-out from the
  controller to every Fluent Bit pod.
- Label cardinality is bounded by (namespace, service) pairs actually logging errors
  above the gate — not by raw log volume — so it stays low relative to a naive
  per-line metric.
- `Sample` is best-effort and empty by default: Fluent Bit's `log_to_metrics` doesn't
  natively carry a representative log line (it only counts), so populating `Sample`
  requires a custom label a fleet's own pipeline adds and points
  `logMetrics.serviceLabelFallback`-style config at — out of scope for the default
  chart pipeline in this ADR. **Now implemented** by
  [ADR-0007](0007-error-log-sample-fingerprint.md)'s fingerprint label.

**Neutral**
- Push mode, Application reconciliation, and CRDs are unchanged. This is purely
  additive, following the same shape as ADR-0005: reuse the existing outbound channel,
  ship raw(-ish) aggregates, let triage decide what they mean.

## Alternatives considered

1. **Ship raw log lines instead of counters.** Gives triage full text to reason
   about, but reintroduces a log-shipping pipeline (volume, cost, potential PII) that
   this project has deliberately stayed out of. Rejected for v1; `Sample` (truncated,
   optional) is the compromise — enough context to identify *what* is erroring
   without shipping a log stream.
2. **Push mode carries a Prometheus remote_write payload directly.** Rejected: no
   metrics backend exists on the triage side to receive it, and it would commit the
   receiver to a much larger surface (arbitrary metric names/labels) than the fixed,
   validated `log_metrics` shape here.
3. **Have the collector call the Fluent Bit pods' `/metrics` through the Service VIP
   instead of per-pod via Endpoints.** A ClusterIP VIP would load-balance to one
   random pod per scrape, undercounting the other pods' errors. Endpoints-based
   per-pod scraping is required to sum across the whole DaemonSet.

## Implementation

Sender side (this repo): `push/protocol.go` (frame types), `logmetrics/` (collector),
`charts/triage-agent/templates/fluentbit-*.yaml` (DaemonSet/ConfigMap/Service/RBAC),
`main.go` (`--log-metrics-*` flags). Receiver side: triage-agent
`internal/remoteagent/protocol.go` + `docs/todo/remote-agent-ws-source.md` (ADR-029) —
tracked there, not here; see the hub-first rollout note above.

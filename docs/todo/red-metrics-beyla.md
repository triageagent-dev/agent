# TODO: RED metrics via Beyla (eBPF option)

- **Status**: Implemented (sender side, 2026-08-20) — see `docs/adr/0009-red-metrics-beyla.md`
- **ADR**: 0009 (this repo), triage-side ADR-045 + `docs/todo/ebpf-telemetry-source.md` in triageagent-dev/core
- **Related**: ADR-0005 (push mode), ADR-0006 (log-based metrics, the pattern this mirrors), ADR-0007 (error sample)

## Implementation (sender side, this repo)

- `push/protocol.go`: `KindRedMetrics`, `RedMetricsPayload`/`ServiceRedMetrics`,
  `MaxRedMetricsServices`/`MaxRedMetricsSampleLen`, `newRedMetrics`,
  `TruncateRedSample`, `Frame.RedMetrics`.
- `push/pusher.go`: `Options.RedMetrics`, `SendRedMetrics`, `"red_metrics"` hello
  capability. Tests: `push/protocol_test.go`, `push/pusher_test.go`.
- `redmetrics/collector.go` + `redmetrics/deltas.go`: the collector, mirroring
  `logmetrics/` structurally (Options/Collector shape, Endpoints discovery, scrape,
  counter-reset handling) without sharing code — the label set and gate shape differ
  enough that a shared abstraction wasn't worth it. Handles both a plain-named
  `_count` series AND a properly-typed Prometheus histogram (base-name family,
  `Histogram.SampleCount`) via `resolveFamily` — see ADR-0009's "Histogram-family
  resolution" section for why both shapes needed handling. Tests:
  `redmetrics/collector_test.go` (parse, delta/reset, gate incl. the rate floor,
  sample selection/format/truncation, 100-service cap, namespace filter, endpoint
  churn GC).
- `main.go`: `--red-metrics-*` flags mirroring `--log-metrics-*`.
- `charts/triage-agent/templates/beyla-configmap.yaml` +
  `charts/triage-agent/templates/beyla-daemonset.yaml` (ServiceAccount, ClusterRole,
  ClusterRoleBinding, fixed-name Service `triage-beyla`, DaemonSet — application-mode
  Beyla, `hostPID: true` + `privileged: true`), `charts/triage-agent/templates/
  _helpers.tpl` (`beylaSelectorLabels`), `charts/triage-agent/templates/
  deployment.yaml` (render guard + args), `charts/triage-agent/values.yaml`
  (`redMetrics.*`, `beyla.*`, image pinned `grafana/beyla:3.25.0` — same tag as
  triageagent-dev/core's `hack/servicemap-collector`).
- Deviations from this doc's original plan, both explained in ADR-0009: (1) the
  frame is capped at 100 services (keep-top-by-error-delta), never chunked into
  multiple frames like `log_metrics` — a deliberate difference, not an omission;
  (2) gRPC (`rpc_server_duration_seconds_count`) is confirmed still out of scope for
  v1, HTTP only, as originally planned.

## Not yet done

- Receiver side (`KindRedMetrics` handling in triage's
  `internal/remoteagent/protocol.go`, ADR-045) — hub-first rollout still required
  before any cluster turns on `redMetrics.enabled` (see Rollout below).
- gRPC RED metrics (`rpc_server_duration_seconds_count`) — follow-up, not v1.

## Goal

Give triage-agent an eBPF option: the chart deploys a Grafana Beyla
DaemonSet in application mode (per-service HTTP RED metrics with zero
instrumentation), and the controller scrapes it, gates on error deltas, and
forwards anomalous windows as `red_metrics` frames over the existing push
WebSocket. Same architecture as the Fluent Bit log path: the DaemonSet
produces counters, the controller owns the scrape, the delta math, and the
gate, and only windowed aggregates cross the network.

This is v1 of the consolidation plan in triageagent-dev/core
`docs/todo/ebpf-telemetry-source.md`. Embedding OBI into the controller image
(one image, two modes) is a later phase and is invisible to the hub because
the wire frame stays the same.

## Wire contract (must stay byte-identical with triage `internal/remoteagent/protocol.go`)

```go
const KindRedMetrics = "red_metrics"

type RedMetricsPayload struct {
    WindowStart int64               `json:"windowStart"`
    WindowSec   int                 `json:"windowSec"`
    Services    []ServiceRedMetrics `json:"services"`
}

type ServiceRedMetrics struct {
    Namespace    string `json:"namespace"`
    Service      string `json:"service"`
    RequestCount int64  `json:"requestCount"`
    ErrorCount   int64  `json:"errorCount"`
    Sample       string `json:"sample,omitempty"` // "GET /api/orders -> 502 x41", sender truncates to 256
}
```

Validation on both sides: `WindowSec > 0`, 1..100 services, non-empty
namespace and service, counts >= 0. The receiver re-truncates `Sample`
defensively, same as `log_metrics`.

## Plan

- `redmetrics/` package mirroring `logmetrics/`: scrape the Beyla Service's
  Endpoints per pod, parse `http_server_request_duration_seconds_count`
  series, aggregate per (namespace, service): request delta = all statuses,
  error delta = status >= 500. Counter-reset handling mirrors
  `logmetrics/deltas.go`. Gate: error delta >= errorThreshold AND error rate
  >= minErrorRatePct of the request delta (both knobs in values). Sample =
  top failing (method, route, status) by delta. gRPC
  (`rpc_server_duration_seconds_count`) is a follow-up; v1 is HTTP only.
- `push/protocol.go`: the frame types above.
- `beyla.*` chart block mirroring `fluentbit.*`: templated DaemonSet
  (application mode, privileged + hostPID for cross-process probing),
  ConfigMap (kubernetes attribute decoration, `routes: unmatched: wildcard`
  to bound route cardinality, prometheus_export), fixed-name Service
  `triage-beyla`, RBAC for the decoration informers. Off by default,
  independent from `fluentbit.*`; both can run at once (log and wire signals
  are complementary).
- `redMetrics.*` values block mirroring `logMetrics.*`; render fails when
  enabled without `push.enabled`, same guard as logMetrics.
- Beyla label keys for namespace/service identity are configurable
  (`namespaceLabel`/`serviceLabel`) so a Beyla version drift in label naming
  is a values edit, never a code change.

## Rollout

Hub first, as always for a new frame kind: triage must deploy ADR-045 before
any cluster enables `redMetrics`, otherwise the frames are dropped as unknown
kind (logged, harmless). First target after that: the ops cluster.

# ADR-0009: eBPF RED Metrics (application-mode gate → red_metrics frame), first via Beyla, now embedded OBI

The file name keeps "beyla" because other documents and code comments link to it.

- **Status:** Implemented — **package superseded by ADR-0013** (2026-08-22): the
  `redmetrics/` package this ADR describes is deleted; its behavior (metric names,
  label keys, gate, deltas) is now `redsource.ProfileOBI`, run by the shared
  `redsource/` engine ADR-0013 introduces. The decisions and behavior described
  below are historically accurate and remain the operational contract (same flags,
  same defaults, same wire frames), implemented by the `redsource/` package.
  The eBPF engine that produces these metrics is OBI (OpenTelemetry eBPF
  Instrumentation), embedded directly in the agent binary and run with
  `--mode=ebpf`. There is no separate Beyla container.
- **Date:** 2026-08-20
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (outbound push mode — the transport this rides on); ADR-0006
  (log-based metrics — the pattern this mirrors); ADR-0007 (fingerprinted sample
  population, the pattern this ADR's own sample selection borrows); triage-agent
  ADR-045 + `docs/todo/ebpf-telemetry-source.md` (triageagent-dev/core repo — the receiver
  side and the multi-phase consolidation plan this is v1 of)

## Context

ADR-0006 gave triage error/warn volume *from logs*, per (namespace, service). That
tells us a service is logging errors, but not the shape an on-call engineer actually
reasons about first: request volume, error rate, and latency (RED — Rate, Errors,
Duration) at the wire level, without shipping application code changes or a tracing
SDK into every service.

Grafana Beyla is an eBPF agent that instruments HTTP/gRPC servers with zero code
changes, in one of two modes:

- **Network mode** (already used elsewhere in this project's sibling repo,
  `triageagent-dev/core/hack/servicemap-collector`, for a one-shot traffic-topology capture):
  pure socket/packet-level eBPF, no per-process hooking, `hostNetwork: true`.
- **Application mode**: userspace uprobes into each instrumented process's HTTP
  handling, producing per-request RED metrics (count, status code, method, route,
  duration) — the shape this ADR needs, and the reason the DaemonSet here needs
  `hostPID: true` instead of `hostNetwork: true` (see Decision below).

Same architecture question as ADR-0006: does triage-agent scrape-and-forward, or
does triage get a native Prometheus remote_write receiver? Same answer, for the same
reason — triage has no metrics backend and consumes discrete windowed events, not a
time series. The controller already holds a `client.Client` and an authenticated
outbound channel (push mode); Beyla's Prometheus exporter is one more Endpoints-based
scrape, following the exact shape `logmetrics.Collector` already established.

## Decision

Add an opt-in **eBPF RED-metrics pipeline**, entirely on the sender side (this repo),
mirroring `logmetrics/` structurally without sharing code with it — the label set and
gate shape differ enough (a request/error pair instead of error/warn/total, a rate
floor instead of an OR-of-two-absolute-thresholds gate, a top-N cap instead of frame
chunking) that a shared abstraction would cost more than the duplication:

```
Beyla DaemonSet, application mode (beyla.enabled)
  eBPF uprobes into every instrumented process's HTTP handling
    -> discovery.instrument (k8s_namespace selectors, allow/deny like fluentbit's)
    -> attributes.kubernetes.enable: true   (namespace/service decoration)
    -> routes.unmatched: wildcard           (bounds http.route cardinality)
    -> [prometheus_export]                  (:9464 /metrics)

triage-agent redmetrics.Collector (redMetrics.enabled)
  every intervalSeconds:
    discover triage-beyla Service's Endpoints
    scrape each pod's /metrics (Prometheus text format)
    sum http_server_request_duration_seconds_count series per (namespace, service),
      diff against the previous scrape (per endpoint, reset = "cur is the delta")
    gate: errorDelta >= errorThreshold AND errorDelta*100 >= minErrorRatePct*requestDelta
    cap: keep only the top MaxRedMetricsServices (100) by error delta — no chunking
    -> push.Pusher.SendRedMetrics(...)  (red_metrics frame, ONE frame per window)
```

The collector does not classify or judge anything — same posture as ADR-0006: it
reports raw counters and lets triage's classifier pipeline decide what a rising error
rate means for an incident.

### Wire contract (red_metrics frame, v1 — pinned, do not deviate)

Added to `push/protocol.go` (mirrors triage's `internal/remoteagent/protocol.go` —
hub-first rollout required, see Consequences):

```go
const KindRedMetrics = "red_metrics"

type RedMetricsPayload struct {
    WindowStart int64               `json:"windowStart"` // unix seconds, window open
    WindowSec   int                 `json:"windowSec"`   // window length in seconds
    Services    []ServiceRedMetrics `json:"services"`
}

type ServiceRedMetrics struct {
    Namespace    string `json:"namespace"`
    Service      string `json:"service"`
    RequestCount int64  `json:"requestCount"`
    ErrorCount   int64  `json:"errorCount"`
    Sample       string `json:"sample,omitempty"` // "GET /api/orders -> 502 x41", ≤256 chars
}
```

`Frame.RedMetrics *RedMetricsPayload` (`json:"redMetrics,omitempty"`) on the shared
envelope. Validation on both sides: `WindowSec > 0`, 1..100 services, non-empty
namespace and service, counts >= 0. The receiver re-truncates `Sample` defensively,
same as `log_metrics`; the sender (`newRedMetrics`) does the same as a last line of
defense (defensive re-cap at `MaxRedMetricsServices`, defensive re-truncate at
`MaxRedMetricsSampleLen`) even though `redmetrics.Collector` is expected to have
already done both.

### Why one frame, capped, instead of chunking like log_metrics

`logmetrics.Collector` splits an oversized batch into multiple `log_metrics` frames
(`chunkServices`) — nothing is dropped, just spread across more frames. `red_metrics`
does the opposite: `redmetrics.Collector` keeps only the top `MaxRedMetricsServices`
(100) qualifying services **by error delta** and drops the rest for that window
(`push.MaxRedMetricsServices`, `push.newRedMetrics`'s defensive re-cap). Two reasons:

1. A wire-level error storm can qualify far more services in one window than a
   log-line error counter typically does — RED metrics see every request, not just
   the ones an application chose to log at error level — so the failure mode
   (hundreds of qualifying services in one 60s window) is more likely here.
2. The services dropped are, by construction, the ones erroring *least* among the
   qualifying set; the next window gets another chance at them if the condition
   persists. Losing the tail of a storm is an acceptable trade against the added
   complexity of also chunking this frame kind.

### Counter delta semantics

Same per-endpoint keying and counter-reset handling as ADR-0006's `logmetrics/`:
`redmetrics.Collector` keeps a per-endpoint snapshot of every
`(namespace, service, method, route, status)` series (`reqSeries` in
`redmetrics/deltas.go`) and computes `delta = cur - prev` on the next scrape, with a
negative raw delta (Beyla pod restart) instead reported as `cur` itself. Deltas are
computed at full series granularity, not just per (namespace, service), because
`Sample` selection needs to know which exact tuple moved the most this window — same
"pick the fastest-growing series" idiom ADR-0007 established for the log-metrics
sample, ported to the RED-metrics shape.

### Gate: absolute threshold AND rate floor

`errorDelta >= errorThreshold` (default 10) **AND**
`errorDelta*100 >= minErrorRatePct*requestDelta` (default 5%) — both conditions,
unlike ADR-0006's OR-of-two-counters gate. A high-traffic service producing a steady
trickle of errors at a normal background rate (10 errors in 100,000 requests) should
not qualify on absolute volume alone; a low-traffic service with a high error rate but
below the absolute floor (3 errors out of 10 requests) should not page on statistical
noise either. Both floors are independently configurable.

### Sample: top failing tuple by delta, formatted

`ServiceRedMetrics.Sample` carries the (method, route, status) tuple with the largest
delta among this window's status>=500 series, formatted `"GET /api/orders -> 502
x41"` and truncated to 256 chars. Same selection idiom as ADR-0007's log-metrics
sample (pick the series that moved the most, not the first or the highest cumulative
value) — but there is no fingerprinting step here, because Beyla's own `routes`
config already bounds cardinality (`unmatched: wildcard`), so the label space is
naturally small (one series per real or wildcarded route × method × status), unlike
raw log lines which need the Lua fingerprint filter to collapse.

### Beyla label defaults — verified against Beyla's published docs

Beyla's Prometheus exporter renders its OTel semconv attribute names in their
Prometheus-safe (underscored) form. Verified against `grafana.com/docs/beyla`
(metrics reference + service-discovery pages) at time of writing:

| Field | Default label | OTel semconv source |
|---|---|---|
| namespace | `k8s_namespace_name` | `k8s.namespace.name` |
| service | `service_name` | `service.name` (OTel Resource attribute, not the k8s decorator) |
| status | `http_response_status_code` | `http.response.status_code` |
| method | `http_request_method` | `http.request.method` |
| route | `http_route` | `http.route` |

All five are configurable (`redMetrics.namespaceLabel` etc. / `--red-metrics-*-label`
flags) — this is Beyla's naming, not ours, so a version drift here must be a values
edit, never a code change. **Uncertainty flagged per the implementation brief**: the
docs fetch used to derive this table could not confirm whether every Beyla release
keeps this exact underscoring (dot→underscore) convention for every future attribute,
only that it holds for the fields above as currently documented; configurability is
the deliberate escape hatch.

### Histogram-family resolution: two shapes, one collector

`http_server_request_duration_seconds_count` is pinned as the series to read, but a
**properly-typed** Prometheus histogram (`# TYPE http_server_request_duration_seconds
histogram`) is NOT exposed under that suffixed name at all — `expfmt`'s text parser
groups `_bucket`/`_sum`/`_count` into ONE family keyed by the **base** name, with the
count carried in `Histogram.SampleCount` on each `dto.Metric`, not as an independently
named series. `redmetrics.Collector.resolveFamily` handles both shapes: it first tries
the direct lookup (`fams[MetricName]`, for an exporter that emits an untyped/counter
`_count` series under its own name), and falls back to stripping the `_count` suffix
and checking for a `HISTOGRAM`-typed family under the base name. `sampleValue`
(`redmetrics/deltas.go`) already reads `Histogram.SampleCount` when present. This
keeps the collector correct regardless of which way a given Beyla build/config
presents the series, without needing a second configurable metric name.

### Application-mode DaemonSet requirements: `hostPID`, not `hostNetwork`

Unlike `hack/servicemap-collector`'s network-mode DaemonSet (`hostNetwork: true`, no
`hostPID`), this DaemonSet needs the opposite: `hostPID: true`, no `hostNetwork`.
Application mode hooks into each container's userspace HTTP handling via uprobes,
which requires visibility into every *other* container's PIDs on the node — the
shared host PID namespace is what makes that possible. Network mode only needs the
host's network namespace for raw socket/packet capture and has no reason to see other
containers' processes at all. The controller reaches Beyla pods via their own pod IPs
through a headless Service + Endpoints lookup (exactly `logmetrics.Collector`'s
mechanism) — not through the host-IP/API-server-proxy path
`hack/servicemap-collector` uses for its one-shot, no-Service capture.

**Correction (2026-08-21 audit, finding 1).** This ADR originally said `privileged:
true` was "the plain eBPF-load requirement, unchanged from network mode" — read by a
reviewer as load-bearing for every kernel. It never was: `privileged: true` grants
every capability, host device access, and unconfined seccomp/AppArmor, which is far
more than eBPF load needs. The container's default securityContext is now the
unprivileged, scoped-capability set Grafana documents for application mode on
kernels >= 5.11 (`beyla-daemonset.yaml`: `capabilities.drop: [ALL]`, `add: [BPF,
PERFMON, NET_RAW, SYS_PTRACE, DAC_READ_SEARCH, CHECKPOINT_RESTORE]`,
`allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`,
`seccompProfile: RuntimeDefault`). `beyla.privileged: true` remains as an escape
hatch for an exotic/older kernel that cannot load eBPF under that set.
`beyla.extraCapabilities` covers documented per-kernel additions (`SYS_RESOURCE` for
kernels older than 5.11, `SYS_ADMIN` for Go trace-context propagation on some
kernels) without falling all the way back to `privileged: true`. `hostPID` itself is
unaffected by any of this — it is a pod-level namespace share, not a container
capability, and stays required for application-mode discovery regardless of the
securityContext above it.

### Fixed-name Service, not templated — a deliberate v1 choice, with a caveat

`be91af3` (this chart, 2026-08-17) templated the fluentbit Service's name
(`{{fullname}}-fluentbit`) after two releases of *this same chart* in one namespace
collided on its old fixed `"triage-fluentbit"` name. The Beyla Service here is
deliberately **fixed** (`"triage-beyla"`, `beyla-daemonset.yaml`) instead of
repeating that fix, matching the `redMetrics.serviceName` empty-default contract and
the fixed-naming idiom `triageagent-dev/core/hack/servicemap-collector` already uses for its
own Beyla objects (literal `"beyla"`, no per-release templating). The trade-off is the
identical one be91af3 fixed for fluentbit: two releases of this chart both setting
`beyla.enabled: true` **in the same namespace** will collide on the Service name.
Mitigation is operational, not code — run `beyla.enabled` in only one release per
namespace, or point a second release's `redMetrics.serviceName` at the first
release's Service instead of deploying a second Beyla DaemonSet — documented inline
on the Service object itself so it isn't only findable here.

## Consequences

**Positive**
- Wire-level RED visibility for services with no application-level log-error
  instrumentation at all — Beyla needs zero code changes, unlike a tracing SDK.
- `redMetrics.enabled` and `beyla.enabled` are independent opt-in toggles, exactly
  like `logMetrics.enabled`/`fluentbit.enabled` — a fleet with its own Beyla only
  turns on the collector half. All three signal families (`fluentbit`/`logMetrics`,
  `beyla`/`redMetrics`, and each other) can run simultaneously.
- Counters are cumulative on the Beyla side, so a dropped push connection loses
  nothing long-term — `SendRedMetrics` is a silent (debug-logged) drop when
  disconnected, not a queued retry, same posture as `SendLogMetrics`.

**Negative / risks**
- **Hub-first rollout required**, identical to ADR-0006: the triage receiver's
  `Frame.Validate()` rejects any `kind` it doesn't recognize, so an agent with
  `redMetrics.enabled=true` has its `red_metrics` frames dropped harmlessly until the
  receiver ships `KindRedMetrics` handling (triage ADR-045).
- Application-mode Beyla's resource cost is higher than network mode's pure eBPF
  socket counting (uprobe overhead per instrumented process) — chart defaults sit
  above fluentbit's (128Mi/512Mi vs. 64Mi/128Mi) as a starting point, not a
  measurement; tune per-fleet.
- `hostPID: true` may require a PodSecurity Admission exemption on clusters running
  the `restricted`/`baseline` profile — this holds regardless of the container
  securityContext correction above, since PSA's `restricted` profile forbids
  `hostPID` on its own. Network mode carries the same caveat via `hostNetwork`.
- Fixed Beyla Service name reintroduces the exact same-namespace, two-release
  collision be91af3 fixed for fluentbit — see the caveat above. Accepted for v1
  because the mitigation (one release per namespace, or point a second release at
  the first release's Service) is a day-one deployment decision, not a runtime
  correctness issue.
- A red_metrics window that qualifies more than 100 services drops the tail of the
  distribution for that window rather than chunking (see Decision) — a deliberate
  trade, not an oversight.

**Neutral**
- Push mode, Application reconciliation, and CRDs are unchanged. This is purely
  additive, following the exact shape ADR-0006 established: reuse the existing
  outbound channel, ship raw(-ish) windowed aggregates, let triage decide what they
  mean.

## Alternatives considered

1. **Embed OBI (OpenTelemetry eBPF Instrumentation, Beyla's upstream engine) directly
   into the controller binary** — one image, two modes, no separate DaemonSet.
   Deferred to a later phase (triageagent-dev/core `docs/todo/ebpf-telemetry-source.md`
   Phase 2) — this ADR is v1 of that consolidation plan. The wire frame is designed
   to be invisible to that later change: `red_metrics` carries the same shape
   regardless of which process on which host produced it.
2. **Chunk oversized red_metrics batches like log_metrics does.** Rejected for v1 —
   see "Why one frame, capped, instead of chunking" above.
3. **A native Prometheus remote_write receiver on the triage side.** Rejected for the
   same reason ADR-0006 rejected it: no metrics backend exists to receive it, and it
   would commit the receiver to a much larger surface than the fixed, validated
   `red_metrics` shape here.
4. **Scrape via the Beyla Service's ClusterIP VIP instead of per-pod via
   Endpoints.** Rejected for the same reason ADR-0006 rejected it for fluentbit: a
   VIP load-balances to one random pod per scrape, undercounting the rest of the
   DaemonSet.

## Implementation

Sender side (this repo): `push/protocol.go` (frame types, `MaxRedMetricsServices`,
`MaxRedMetricsSampleLen`, `newRedMetrics`, `TruncateRedSample`), `push/pusher.go`
(`Options.RedMetrics`, `SendRedMetrics`, hello capability), `redmetrics/` (collector +
delta/gate/sample logic), `charts/triage-agent/templates/beyla-{configmap,
daemonset}.yaml` (ConfigMap/DaemonSet/Service/RBAC), `charts/triage-agent/templates/
_helpers.tpl` (`beylaSelectorLabels`), `charts/triage-agent/templates/
deployment.yaml` (render guard + `--red-metrics-*` args), `charts/triage-agent/
values.yaml` (`redMetrics.*`, `beyla.*`), `main.go` (`--red-metrics-*` flags). Receiver
side: triage-agent `internal/remoteagent/protocol.go` + ADR-045 — tracked there, not
here; see the hub-first rollout note above.

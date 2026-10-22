# TODO: Log-Based Metrics (application controller / agent side)

**Status:** Implemented end to end. The sender shipped here; the hub-side receiver
landed in triageagent-dev/core (`internal/remoteagent/receiver.go` handles `KindLogMetrics`,
ADR-039), so this is no longer blocked on anything.
**ADR:** [ADR-0006](../adr/0006-log-based-metrics.md)
**Companion (hub side):** triage-agent → `internal/remoteagent/protocol.go` needs
`KindLogMetrics` handling (out of scope for this repo — read-only reference during
implementation, never modified here).

Add an opt-in log-based metrics pipeline: a Fluent Bit DaemonSet counts error/warn
log lines per (namespace, service); `triage-agent` scrapes it, computes deltas, and
forwards qualifying services as `log_metrics` frames over the existing push
WebSocket. Off by default; existing behavior unchanged when
`logMetrics.enabled`/`fluentbit.enabled` are both `false`.

## Design constraints

- **Additive & opt-in**, gated independently from push mode itself
  (`logMetrics.enabled`, `fluentbit.enabled`) but `logMetrics.enabled` requires
  `push.enabled=true` — the chart fails the render otherwise.
- **Raw counters on the wire**, no classification here — triage owns interpreting
  what an error-rate spike means, same posture as ADR-0005's Application/event
  streams.
- **Wire format is a hand-synced mirror** of the triage receiver's
  `LogMetricsPayload`, exactly like `push/protocol.go`'s other frame types.
- **Hub-first rollout**: the receiver rejects unknown frame kinds, so `log_metrics`
  frames are silently dropped until triage ships `KindLogMetrics` support. Deploying
  this repo's `logMetrics.enabled=true` early is safe (no functional harm to the rest
  of the push stream) but inert until then.

## Steps (each a separate commit)

### 1. Wire protocol
- [x] `push/protocol.go`: `KindLogMetrics`, `LogMetricsPayload`, `ServiceLogMetrics`,
      `Frame.LogMetrics`, `newLogMetrics`, `TruncateSample`, `chunkServices`,
      `MaxLogMetricsServices` / `MaxLogMetricsSampleLen` constants.
- [x] `push.Pusher.SendLogMetrics` + `Options.LogMetrics` (hello capability flag).
- [x] Unit tests: frame round-trip, exact JSON field names, truncation, chunking.

### 2. Collector
- [x] New `logmetrics` package: `Collector` (`manager.Runnable` +
      `LeaderElectionRunnable`), `Options`, `Sender` interface.
- [x] Endpoints discovery (`corev1.Endpoints` via the manager's `client.Client`) +
      per-pod Prometheus text-format scrape (`prometheus/common/expfmt`).
- [x] Per-endpoint counter snapshot + delta (counter-reset handling: negative delta
      → treat current value as the delta).
- [x] Gate on `errorThreshold`; label-pair aggregation across endpoints;
      `ServiceLabel`/`ServiceLabelFallback` coalescing.
- [x] Unit tests: delta across two scrapes, counter reset, gate, warn/total optional,
      cross-endpoint aggregation, per-endpoint baseline isolation, no-endpoints,
      service-label fallback, disabled-by-default guards, typed-nil-interface
      regression guard.

### 3. Flags + main.go wiring
- [x] `--log-metrics-enabled`, `--log-metrics-service-namespace`,
      `--log-metrics-service-name`, `--log-metrics-port`,
      `--log-metrics-interval-seconds`, `--log-metrics-error-threshold`,
      `--log-metrics-{error,warn,total}-metric`,
      `--log-metrics-{namespace,service}-label`,
      `--log-metrics-service-label-fallback`.
- [x] `mgr.Add(collector)` when enabled; explicit-nil `Sender` when push mode is off
      (avoids Go's typed-nil-interface trap — see the regression test).

### 4. Chart
- [x] `fluentbit-configmap.yaml`: tail input (`multiline.parser cri`), kubernetes
      filter, three `log_to_metrics` filters (error/warn/total counters,
      `add_label namespace`/`service`/`service_fallback`), `prometheus_exporter`
      output.
- [x] `fluentbit-daemonset.yaml`: ServiceAccount + ClusterRole (get/list/watch
      pods,namespaces) + ClusterRoleBinding + headless Service (`triage-fluentbit`,
      fixed name) + DaemonSet (read-only hostPath mounts, small resource limits).
- [x] `deployment.yaml`: `--log-metrics-*` args gated on `logMetrics.enabled`; fail
      guard when `logMetrics.enabled` without `push.enabled`.
- [x] `values.yaml` + chart README: every knob documented.

### 5. Tests
- [x] `make test` green (unaffected — scope is `api/`+`controllers/`, this feature
      lives in `push/`+`logmetrics/`, covered by their own `go test`).
- [x] `go vet ./...` + `gofmt -l .` clean.
- [x] `helm template`/`helm lint` render clean, including the
      `logMetrics.enabled && !push.enabled` failure case.

## Out of scope (v1)

- Populating `ServiceLogMetrics.Sample` from the default chart pipeline — Fluent
  Bit's `log_to_metrics` doesn't carry a representative log line natively;
  `SampleLabel`/`ServiceLabelFallback`-style config exists in the collector for a
  fleet that adds a custom label, but the shipped pipeline doesn't populate one.
  **Since implemented** via
  [ADR-0007](../adr/0007-error-log-sample-fingerprint.md) /
  [error-log-sample-fingerprint.md](error-log-sample-fingerprint.md).
- Histogram/gauge-mode metrics (e.g. request latency) — counter-mode error/warn/total
  only.
- A generic Prometheus-remote-write receiver on the triage side; this is a fixed,
  validated frame shape, not an arbitrary metrics ingestion path.

## Open questions

- [ ] Should the collector eventually watch `EndpointSlice` instead of the deprecated
      `corev1.Endpoints`? Deferred — `Endpoints` is simpler and sufficient for a
      single-port DaemonSet Service; revisit if/when the cluster's minimum supported
      Kubernetes version requires it.
- [ ] Hub-side timeline for `KindLogMetrics` support — coordinate rollout so agents
      aren't enabled significantly ahead of the receiver (harmless, but inert).

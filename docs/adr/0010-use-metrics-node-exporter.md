# ADR-0010: USE Metrics via cgroup v2 + PSI (node-exporter mode → use_metrics frame)

- **Status:** Implemented
- **Date:** 2026-08-21
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (outbound push mode — the transport); ADR-0009 (RED
  metrics — the sibling pipeline, and the DaemonSet pod this rides on);
  triage-agent ADR-046 + `docs/todo/ebpf-telemetry-source.md` (triageagent-dev/core
  repo — the receiver side and the USE/RED/golden-signals consolidation this
  is the S of)

## Context

ADR-0009 closed R and E at the wire. Saturation — the USE method's core
question, "is this service starved for a resource" — needs no eBPF at all:
cgroup v2 exposes per-pod PSI (`cpu.pressure`/`memory.pressure`/`io.pressure`
"some avg60" stall percentages), OOM-kill counts (`memory.events`), throttling
(`cpu.stat`), and utilization (`memory.current` vs `memory.max`,
`cpu.stat usage_usec` deltas vs limit) as plain file reads. That works even
where eBPF cannot load, and it fires *before* the failure the other sources
report: a pod climbing toward its memory limit stalls in reclaim (mem PSI
rises) minutes before the OOM kill that finally produces a pod-failure event.

Reading those files requires being on every node with the host cgroup
filesystem mounted — a per-node vantage the controller Deployment does not
have, but the Beyla DaemonSet pod (ADR-0009) already does. Same
architecture answer as ADR-0006/0009 otherwise: scrape-and-forward windowed
events, no metrics backend on either side.

## Decision

Add an opt-in **USE-metrics pipeline** split across a new binary mode and a
new collector, both in this repo:

```
Beyla DaemonSet pod, second container (useMetrics.enabled)
  triage-agent --mode=node-exporter          (same image, no manager/reconciler)
    node-scoped pod informer (spec.nodeName=$NODE_NAME field selector)
    per scrape: read each local pod's cgroup v2 files under a read-only
      /sys/fs/cgroup hostPath mount (PSI avg60, memory.events oom_kill,
      cpu.stat throttling + usage, memory.current/max, memory working set)
      plus the pod SPEC's own regular-container requests (memory/CPU) and,
      resolved ONCE at startup from a read-only /proc + /sys mount, the
      NODE's own total memory and online CPU core count
    -> Prometheus text endpoint :9465, per-(namespace,service,pod) series
    no privileges of its own — text file reads only (but see the
    correction below: it rides beyla's pod, so it IS in the host PID namespace)

triage-agent usemetrics.Collector (controller Deployment, useMetrics.enabled)
  every intervalSeconds (60):
    discover triage-node Service's Endpoints (fixed name, headless)
    scrape each pod's :9465/metrics
    aggregate per (namespace, service): MAX for pressure/utilization gauges,
      SUM for delta counters (oom_kill diffs per pod, reset = cur-is-delta);
      derive memNodePct/cpuNodePct/memReqPct/cpuReqPct from the same scrape
    tenant guard: drop namespaces outside Options.Namespaces (= --target-namespaces)
    gate (OR, not AND — unlike redMetrics' rate floor):
      memPsi >= 20 OR cpuPsi >= 40 OR ioPsi >= 40 OR oomKillDelta >= 1
      OR memPct >= 85 OR memNodePct >= <operator-set, default 0/off>
    cap: top 100 services by memPsiAvg60, dropped-count logged
    -> pusher.SendUseMetrics -> use_metrics frame (push/protocol.go)
    -> pusher.SendUseSummary -> use_summary frame, EVERY window, ungated —
      utilization + node/request percentages for every service scraped
    hello capability "use_metrics" declared when wired (armed-vs-off on the hub)
```

This diagram understated the pipeline for a year before this note: `memPct`
(limit-relative utilization), CPU utilization, the absolute byte/core figures
behind every percentage, and the `memPct`/`memNodePct` gate arms were all
added incrementally after the ADR's original 2026-08-21 date without a
matching update here. See "Utilization: limit, node, and request" below for
the current, complete picture — this section is kept close to its original
form for history, not as the authoritative current shape.

The same change adds `ServiceRedMetrics.P99Ms` to ADR-0009's pipeline:
`redmetrics.Collector` now also diffs Beyla's duration-histogram *bucket*
series per window and derives a linearly-interpolated p99 in milliseconds —
attached AFTER the error gate decides which services qualify, display-only,
zero when the resolved family carries no buckets.

### Wire contract

`UseMetricsPayload`/`ServiceUseMetrics` in `push/protocol.go` are pinned
byte-identical to the receiver's `internal/remoteagent/protocol.go`
(separate Go modules, independent definitions — same rule as every other
frame kind). `newUseMetrics` re-truncates samples (≤256) and re-caps the
service list (≤100) as the last line of defense before the wire; the
receiver clamps PSI percentages into [0,100] on its side rather than
rejecting the frame.

### Why a mode, not a separate binary or a privileged sidecar

- **Same image, `--mode=node-exporter`**: one image to build/push/scan, and
  the mode branch exits before any manager/cache/leader-election setup — a
  DaemonSet of full controllers was never on the table.
- **Second container on the Beyla pod, not its own DaemonSet (v1)**: the
  node-exporter needs exactly the per-node placement Beyla already has, and
  a separate DaemonSet doubles the scheduling/tolerations/nodeSelector
  surface for no isolation gain. The chart `fail`s if `useMetrics.enabled`
  without `beyla.enabled`. A standalone DaemonSet for eBPF-less clusters is
  an explicit follow-up (that is also what unlocks USE on gVisor pools).
- **Asks for no privileges of its own, read-only**: unlike Beyla (which needs a
  scoped eBPF capability set — `privileged: true` only as an escape hatch, see
  ADR-0009's correction — for uprobes), this container only reads text files —
  `/sys/fs/cgroup` mounted read-only, `privileged: false`,
  `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`,
  `runAsNonRoot: true`, `capabilities.drop: [ALL]`,
  `seccompProfile: RuntimeDefault` (the last three added by the same 2026-08-21
  audit remediation as the correction below).

  **Correction (2026-08-21 audit, finding 2).** This ADR originally said "no
  hostPID". That is wrong, and the chart carried the same claim. `hostPID` is a
  **pod-level** setting, and this container is a second container in beyla's
  DaemonSet pod — so it shares the host PID namespace and the `-beyla`
  ServiceAccount whenever beyla is enabled. Its own securityContext is genuinely
  minimal, but its blast radius is the pod's, not its own. Only a standalone
  DaemonSet (the follow-up noted above) would actually deliver the isolation the
  original wording implied.

### Thresholds, zero, and the falsy-zero trap

Zero is a legitimate threshold value ("always report this dimension"), so —
same trap as `redMetrics.minErrorRatePct` (fixed in commit 6251069) — the
chart renders the three PSI threshold args with **no `| default`**, and the
Go side applies defaults only in `flag.*Var` declarations, never by
re-defaulting a parsed zero. This change also removes `redmetrics.New`'s
Go-side `MinErrorRatePct == 0 → 5` re-default, the second half of that trap.

### Utilization: limit, node, and request (2026-08-29)

Utilization against a container LIMIT (`memPct`/`cpuPct`, added after this
ADR's original date) only exists when a limit is set — an unlimited pod
reports nothing on that axis and is invisible. Two more denominators close
that gap, both read straight from `/proc` and `/sys` rather than the
Kubernetes Node object — the entire portability point: they work identically
on bare metal, a plain container, or anything else with cgroups, none of
which need to be a Kubernetes Node for the question "is this pod eating the
box" to make sense.

- **% of node** (`memNodePct`/`cpuNodePct`) — usage against the NODE's total
  memory (`MemTotal` in `/proc/meminfo`) and online CPU core count
  (`/sys/devices/system/cpu/online`'s range list, e.g. `0-7` → 8; falling
  back to counting `processor` lines in `/proc/cpuinfo`, falling back to
  `runtime.NumCPU()`). Resolved ONCE at node-exporter startup — a node's
  capacity doesn't change — and logged, so a wrong reading (a restrictive
  `/sys` forcing the `runtime.NumCPU()` fallback) is diagnosable from the
  startup log alone. A sixth gate arm, `--use-metrics-mem-node-pct-threshold`,
  sits beside the limit-relative one; **defaulted to 0/disabled**, same
  inverted convention as `--use-metrics-mem-pct-threshold` — this one is off
  pending observed numbers from a live fleet, not a considered threshold.
  There is no request-relative gate arm: exceeding a request is normal and
  near-universal, a right-sizing fact rather than an alarm.
- **% of request** (`memReqPct`/`cpuReqPct`) — usage against the pod spec's
  own declared regular-container requests (init containers excluded, same
  reasoning as the limit fallback below — they've terminated by the time a
  pod is Running and would inflate the denominator). This is the Kubernetes
  right-sizing axis: usage far above request marks a noisy neighbour, first
  in line for eviction under node pressure; usage far below request marks
  reserved capacity going to waste. **Deliberately not clamped to 100** —
  exceeding your own request is the entire signal here, and clamping would
  erase it. Node percentages are likewise unclamped, though in practice they
  cannot exceed 100.

Both new percentages are 0 (omitted on the wire, `omitempty`) when their own
denominator is missing — an exporter that hasn't resolved a node reading yet,
or a pod whose regular containers declare no request at all. The exporter
emits raw DENOMINATORS (`use_mem_node_total_bytes`, `use_cpu_node_cores`,
`use_mem_request_bytes`, `use_cpu_request_cores`); the collector does every
division, mirroring the limit-relative pair's existing split.

The per-service aggregate's "absolutes follow the worst pod by memPct" rule
(added alongside `memPct` itself) now extends to every new absolute and
percentage — one chosen pod describes the whole row, never a mix of one pod's
bytes against another pod's node total or request. That selection has a
memBytes tiebreak precisely because `memPct` is legitimately 0 for an
unlimited pod: without it, a service where EVERY pod is unlimited would never
select any pod at all, and `memNodePct` — the feature built for exactly that
case — would stay silently absent for the services that most need it.
`use_summary` carries `memNodePct`/`cpuNodePct`/`memReqPct`/`cpuReqPct`/
`memRequestBytes`/`cpuRequestCores`; node TOTALS are deliberately not on the
wire there, since that frame aggregates a service across pods that may sit on
different nodes and there is no single correct "node total" at that
granularity — only the ratio each pod computed against its own node is.

## Consequences

**Positive:** the S in USE/golden signals with zero new transport/auth, an
earlier precursor than logs or wire errors, and no privileged-surface
growth. **Negative:** cgroup v2 only (v1 hierarchies are skipped with a
log); a fixed-name `triage-node` Service shares the two-releases-per-
namespace caveat with `triage-beyla`; the per-pod cgroup path resolution
depends on the kubepods cgroup driver layout (systemd + cgroupfs both
handled, including a systemd kubelet started with a non-default
`--cgroup-root` such as kind's `/kubelet`, discovered once at startup —
exotic layouts still degrade to "pod not found" logs).

## Rollout note

Hub-first (triage ADR-046 must be deployed before enabling `useMetrics` —
an old hub warn-drops unknown frame kinds, so the failure mode is dropped
frames, not a crash). On this repo's side everything is off by default;
enabling is a values change on the release.

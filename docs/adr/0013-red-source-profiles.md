# ADR-0013: Profile-Driven RED + Topology Sources (Beyla and Hubble on one engine)

- **Status:** Implemented. The "ebpf" profile's engine is OBI (OpenTelemetry
  eBPF Instrumentation), embedded directly in the agent binary and run with
  `--mode=ebpf` (`redsource.ProfileOBI`). There is no separate Beyla
  container. The Hubble profile was removed by ADR-0015: the collectors
  read only the sensor stream, and external sources come in through OTel.
  With it went `Profile.HasSourceLabels` and the same-series edge path it
  selected; edges come only from client metrics and network flows.
- **Date:** 2026-08-22
- **Deciders:** Denis (Principal SRE), operator direction 2026-08-21/22
- **Related:** ADR-0009 (RED metrics via Beyla — retired as a standalone package by
  this ADR, its behavior absorbed into the engine), ADR-0011 (topology snapshot —
  same), ADR-0010 (USE metrics node-exporter — the DaemonSet decoupling this ADR
  also makes), `docs/todo/cilium-hubble-red-source.md` (the design doc this
  implements, including its "Beyond Hubble" section), triageagent-dev/core ADR-022 (planned
  OTLP receiver — the door this ADR deliberately leaves closed, see Alternatives)

## Context

The 2026-08-21 security audit's finding 1 asked for an unprivileged alternative to
Beyla on clusters that already run Cilium: Hubble, Cilium's own observability
agent, exposes flow and (where `httpV2` is configured) HTTP metrics from the SAME
privileged eBPF layer Cilium's CNI plugin already runs as the platform's own
component — so a controller that speaks Hubble's Prometheus endpoint instead of
running its own Beyla DaemonSet ships zero elevated privilege on such a cluster: no
hostPID, no capabilities, no second DaemonSet.

The design doc's "Beyond Hubble" section anticipated the next question before this
ADR was even written: Hubble support adds no library dependency (the collector
speaks Prometheus/OpenMetrics text, already parsed everywhere in this repo) and the
vendor-specific part is only a naming table — metric names, label keys, an error
predicate. Istio, Linkerd, Azure Retina (Hubble-metric-compatible), and Pixie all
repeat the same pattern. Two rules followed from that: (1) refactor the scrape-based
RED sources into ONE profile-driven collector where a profile is that naming table,
rather than growing a package per vendor; (2) anything OTel-native belongs behind
the planned OTLP receiver (triageagent-dev/core ADR-022), not a bespoke scrape collector —
Beyla itself is an OpenTelemetry project, so the *default* mode is already on the
standard, and Hubble is the deliberate scrape-based exception, not the start of a
collection of them.

**This ADR does both refactors in one pass, not two.** An earlier iteration of this
work planned to ship Hubble as a new, additional package alongside the existing
`redmetrics`/`topology` packages, migrating Beyla onto the shared engine later once
a concurrent refactor of those two packages (in progress on `main` at the time)
had landed — accepting a future migration commit as a known follow-up. Operator
direction overrode that sequencing: Beyla moves onto the engine now, in this same
branch, and `redmetrics`/`topology` are deleted here. **This is a known,
accepted merge conflict against the concurrent `main` refactor of those two
packages** — the operator chose to take the conflict now rather than carry two
generations of "how RED metrics get scraped" in the tree at once.

## Decision

One package, `redsource/`, replaces `redmetrics/` and `topology/` entirely.

### Profile: the only place vendor knowledge may live

```go
type Profile struct {
    Name                                             string
    RequestsMetric, DurationBucketMetric             string
    DestNamespaceLabel, DestWorkloadLabel            string
    StatusLabel                                       string
    SourceNamespaceLabel, SourceWorkloadLabel        string
    SampleLabels                                      []string
    HasSourceLabels                                   bool
    ClientMetrics                                     []ClientMetric
    ServerAddressLabel                                string
    IsError      func(status string) bool
    FormatSample func(SampleContext) string
}
```

A profile is pure data plus two small, genuinely vendor-specific predicates (an
error classifier and a sample formatter — the second because the two shipped
profiles' representative-failing-sample text differs in a way a data field
can't express: one names the caller, the other names the method+route). Every
other field is a metric name, a label key, or a bool selecting one of two
topology-edge derivation shapes. The engine (`collector.go`, `deltas.go`) reads
these fields and branches on the DATA a field carries — never on a profile's
name. `grep -i hubble redsource/collector.go redsource/deltas.go redsource/
profile.go` returns nothing; the word appears only in `profiles.go`, the test
file, this ADR, and chart values/templates (flag names, values keys) — see
Verification below.

### `HasSourceLabels`: the one real shape difference

Beyla and Hubble differ in exactly one structural way, and `Profile.
HasSourceLabels` is the whole encoding of it:

- **`true`** (the `hubble` profile): `hubble_http_requests_total` labels BOTH
  ends of a call on the same series — `source_namespace`/`source_workload` and
  `destination_namespace`/`destination_workload` together. RED (grouped by
  destination) and topology edges (grouped by source→destination) both derive
  from ONE scraped family, no address parsing needed.
- **`false`** (the `beyla` profile): the server-side RED series
  (`http_server_request_duration_seconds_count`) names only the measured
  service itself — no caller identity at all. Edges instead come from FOUR
  separate client-side families (`http_client_*`, `rpc_client_*`,
  `sql_client_*`, `redis_client_*`), each decorated with the caller's own
  identity (the SAME `k8s_namespace_name`/`service_name` label keys, read off
  a DIFFERENT family) plus `server_address` naming the callee as unstructured
  text requiring a k8s-DNS-shape parse (`resolveDstAddress`/`parseK8sDNS`/
  `hostOnly` — transcribed byte-for-byte from the former `topology/deltas.go`).

Both branches live in the engine, selected by a bool the Profile sets — not by
an `if profile.Name == "beyla"` conditional anywhere.

### Delta-baseline correctness: finer than the aggregation grouping

Both `requestSeries` (RED/same-family edges) and `bucketSeries` (p99) baseline
on a series' FULL raw label set (`canonicalLabelKey`), not just the fields the
engine groups by. This matters concretely for the `beyla` profile: its
requests-metric samples also carry `http_request_method`/`http_route`, which
`requestGroup` deliberately does NOT include (RED groups by destination alone,
matching the former `redmetrics` package's aggregation). If the delta baseline
were keyed by `requestGroup` too, a new route appearing mid-run would silently
sum its whole lifetime cumulative count into an existing group's baseline and
report it as a spike — the exact bug class `counterDelta`'s first-seen
suppression exists to prevent, reintroduced through a side door. Keying the
baseline on the full label set instead — decoupled from the (coarser)
aggregation grouping — closes it for ANY profile's extra dimensions without the
engine needing to know what they are. `Profile.SampleLabels` (Beyla:
`["http_request_method", "http_route"]`) is how those extra dimensions still
reach the Sample field's formatter, via `SampleContext.Extra`, without being
part of the grouping/baseline key story.

### Gates, caps, and wire contract: unchanged

Same absolute-AND-rate RED gate, same `MaxRedMetricsServices`/
`MaxTopologyNodes`/`MaxTopologyEdges` caps, same `red_metrics`/
`topology_snapshot` frame kinds and field shapes (`push/protocol.go`
untouched), same 8MiB `maxScrapeBodyBytes` `io.LimitReader` idiom (AUDIT.md
finding 3), same first-seen-baseline + counter-reset delta discipline, same
first-topology-flush zero-Calls-edge inclusion, same "every endpoint scrape
failed this tick → skip the send entirely" rule. Every one of these is proven
against BOTH profiles by fixture tests ported from the packages this replaces
(see Testing below) — this is what "same frames, same gates, same caps" means
operationally, not just as a design intent.

### One structural change: RED and topology now share a scrape

The former `redmetrics.Collector` and `topology.Collector` were two
independent `manager.Runnable`s on two independent tickers (60s and 600s
respectively), each doing its own Endpoints discovery and its own scrape.
`redsource.Collector` is ONE runnable on ONE ticker (`Options.Interval`,
default 60s): every tick it scrapes once, evaluates the RED gate immediately
(unchanged send timing), and accumulates topology deltas toward the next
`Options.TopologyInterval` flush (default 600s, unchanged send timing and
content — `topologyEveryTicks()` computes how many Interval ticks make up one
flush window). NODES are still presence-only (not delta-gated) — extracted
from the SAME per-series destination identity RED already computed, not a
second scan.

One resilience property changed as a direct consequence, not as a deliberate
feature: a transient failure on ONE intermediate Interval tick no longer blanks
the WHOLE topology flush window the way a total-scrape-failure on topology's
own single 600s scrape used to — that tick's contribution to the accumulator is
just zero, and the other nine (at the default 60s/600s ratio) still count.
`EnableRED`/`EnableTopology` are independent bools on `Options`, mirroring the
former `redMetrics.enabled`/`topology.enabled` toggles exactly, so an operator
who only ever wanted one family unchanged still gets only that one.

### Beyla now runs on the engine — `redmetrics/` and `topology/` are deleted

`ProfileBeyla` transcribes the former `redmetrics.Options`/`topology.Options`
compiled defaults and label keys exactly (metric names, `k8s_namespace_name`/
`service_name`/`http_response_status_code`/`http_request_method`/`http_route`,
the four client-family names and their protocol tags, the >=500 error rule).
`main.go`'s `--red-metrics-*`/`--topology-*` flags are unchanged in name and
default and now feed `redsource.Options` through this profile instead of the
two retired packages' own `Options` structs — an existing values.yaml with
`redMetrics.*`/`topology.*` set needs no edits to keep working. `--red-source`
(chart: `redSource.profile`, default `"beyla"`) is new: it selects which
profile the shared engine uses, and does nothing by itself — `redMetrics.
enabled`/`topology.enabled` are still what turns either frame family on, same
as before this ADR.

**This supersedes the concurrent `redmetrics`/`topology` refactor in progress
on `main`.** That refactor's changes to those two packages cannot land after
this ADR merges — the packages no longer exist. The operator's explicit
direction was to accept this conflict rather than sequence around it; whoever
resolves the merge should treat this ADR's `redsource/` package as the
destination for any behavioral fix the `main` refactor was carrying, not
attempt to resurrect the deleted packages.

### DaemonSet decoupling: USE metrics no longer requires Beyla

Before this ADR, the node-exporter (USE metrics) container rode the Beyla
DaemonSet pod unconditionally — `useMetrics.enabled` required `beyla.enabled=
true` and the chart failed the render otherwise (docs/adr/0010's own text said
so). That coupling had no technical basis: the node-exporter container reads
local cgroup v2/PSI files by pod UID and has never needed anything Beyla's pod
happened to provide except a place to run. `beyla-daemonset.yaml`'s top-level
gate is now `or beyla.enabled useMetrics.enabled` — the DaemonSet, its
ServiceAccount, and its ClusterRole (shared: the node-exporter's own pod
informer needs the same `pods`/`services`/`nodes`/workload-owner read access
Beyla's kubernetes attribute decorator already had) render whenever EITHER is
on; the beyla container, `hostPID: true`, and every beyla-only volume
(`config`, `bpffs`) are separately gated on `beyla.enabled` alone.

**`hostPID: true` is now genuinely absent, not just unused, on a `beyla.
enabled=false` render.** This is the real hardening payoff of the decoupling:
a cluster running `redSource.profile=hubble` (or simply not wanting Beyla) with
`useMetrics.enabled=true` gets a DaemonSet pod with exactly one container, no
host PID namespace share, no eBPF capabilities, no `bpffs` mount — the
node-exporter's own minimal `securityContext` (already unprivileged,
`readOnlyRootFilesystem`, `capabilities: {drop: [ALL]}`) is now the pod's WHOLE
privilege footprint, not half the story the way ADR-0010's original text
(corrected by the beyla-hardening branch) understated it.

### Double-RED-source guard

`beyla.enabled=true` and `redSource.profile=hubble` together would have both
the platform's own Hubble agent AND this chart's Beyla DaemonSet producing
`red_metrics`/`topology_snapshot` frames for overlapping traffic — double-
reporting every signal to the hub. `deployment.yaml` fails the render
whenever `redSource.profile == "hubble"` (unconditional on `redMetrics.
enabled`/`topology.enabled` — `beyla.enabled=true` alone is the tell that an
operator intends Beyla mode, and the guard must catch that before either
frame-family toggle is even flipped on). The mirror case — `redSource.
profile == "beyla"` (the default) with `redMetrics.enabled` or `topology.
enabled` true but `beyla.enabled` false — also fails: there is no DaemonSet to
scrape. A bare `helm install` with nothing enabled hits neither guard (both
gate on at least one frame family actually being requested), so the chart's
zero-value defaults render unchanged.

**Known compatibility edge case, accepted as part of this decision:** the new
`profile=beyla requires beyla.enabled` guard is broader than any guard that
existed before it. Previously `redMetrics.enabled=true` alone (without `topology.
enabled`) had NO fail-guard tying it to `beyla.enabled` — `redMetrics.
serviceName` could legitimately point at a Beyla instance this chart does not
manage (a shared DaemonSet from another release, or an externally operated
one), with `beyla.enabled` left `false`. That configuration now fails the
render. There is no values-only workaround that preserves it: setting `beyla.
enabled=true` to satisfy the guard would deploy a second, redundant DaemonSet.
An operator in this position needs a values-level escape hatch this ADR does
not add (tracked as a follow-up, not implemented here — no live deployment
was found in this repo's own overlays using this pattern).

## Consequences

**Positive**
- One collector, one scrape, two frame families, two vendors proven against
  the identical code path — closes the "per-vendor adapter sprawl" risk the
  design doc's "Beyond Hubble" section named before it could start.
- Zero elevated privilege on a Cilium-CNI cluster choosing the hubble profile:
  no hostPID, no capabilities, no second DaemonSet — the whole audit-finding-1
  ask.
- USE metrics' DaemonSet dependency on Beyla is gone; `useMetrics.enabled=true`
  with `beyla.enabled=false` is now a real, hardened, single-container
  configuration.
- Delta-baseline correctness bug class (route/method churn corrupting a
  coarser-grouped baseline) fixed generically, for any current or future
  profile's extra label dimensions, not patched per-family the way the two
  retired packages each did independently.

**Negative / risks**
- The known compatibility edge case above (an externally-managed Beyla with
  `beyla.enabled=false` in this chart) now fails the render with no
  values-only workaround.
- `redmetrics/`+`topology/` deletion is a deliberate, accepted merge conflict
  against work in progress on `main` — whoever resolves it needs to read this
  ADR first, not just take "theirs" or "ours".
- v1 profile support is `beyla` and `hubble` only; OSS Hubble metric names —
  GKE-managed Dataplane V2 renames `hubble_http_requests_total` and friends
  and is explicitly out of scope (see `docs/todo/cilium-hubble-red-source.md`'s
  "v1 scope decision"; on managed DPv2, Beyla remains the only full RED
  source).
- v1 topology edges are L7 (HTTP) only for the hubble profile, derived from
  `hubble_http_requests_total`. L4 flow-based edges
  (`hubble_flows_processed_total`, which needs no `httpV2`/`labelsContext`
  configuration and therefore works even where L7 visibility is not enabled)
  are a documented follow-up, not implemented here.
- The hubble profile requires an operator-side Cilium configuration change
  (`hubble.metrics` with `httpV2` and a `labelsContext` naming source AND
  destination namespace/workload labels — see values.yaml's `redSource.hubble`
  comment for the exact Helm snippet) that this chart cannot verify or enforce;
  an unconfigured cluster's `hubble_http_requests_total` either does not exist
  or carries no per-workload labels to aggregate on, and the collector simply
  observes nothing (no error surfaces distinctly from "no traffic this
  window").

**Neutral**
- Push mode, Application reconciliation, CRDs, `logmetrics`/`usemetrics` are
  untouched — different signal families, not part of this refactor.
- `--red-metrics-*`/`--topology-*` flag names, defaults, and chart values are
  unchanged for the beyla profile; an existing values.yaml keeps working
  without edits (see "Beyla now runs on the engine" above).

## Alternatives considered

1. **Ship Hubble as a new, additional package, migrate Beyla onto the shared
   engine later.** The initial plan for this work, superseded by explicit
   operator direction (see Context) — accepted the smaller, later merge
   conflict in exchange for two generations of "how RED gets scraped"
   coexisting in the tree in the interim, with `redmetrics`/`topology` staying
   in place unused by any live deployment. Rejected: the operator judged the
   larger, immediate conflict preferable to that interim state.
2. **A values-only capability flag instead of profile selection
   (`redMetrics.source: beyla | hubble`, per the design doc's original
   "Proposed shape" sketch).** Superseded once the engine itself needed to be
   profile-driven for the code-sharing goal — `redSource.profile` IS that
   selector, just resolved to a `Profile` value in Go rather than staying a
   string the collector switches on internally (the switch lives in `main.go`,
   which is explicitly outside the "no vendor conditionals" constraint the
   engine package itself is held to).
3. **Route Hubble metrics through the planned OTLP receiver instead of a
   bespoke scrape collector**, on the reasoning that Beyla is already
   OpenTelemetry-native. Rejected for Hubble specifically: Hubble is not an
   OTel exporter and has no OTLP mode — the design doc's "Beyond Hubble"
   section draws this line deliberately (rule 2: "Everything that is OTel-
   native goes through the planned OTLP receiver instead of a bespoke
   collector... Hubble is the deliberate exception, not the start of a
   collection"). The OTLP receiver (triageagent-dev/core ADR-022) remains the right
   home for a genuinely OTel-native mesh/source later.
4. **A Hubble Relay gRPC client instead of scraping Hubble's Prometheus
   endpoint.** Rejected for v1: every other collector in this repo already
   speaks Prometheus text exposition, so scraping keeps the engine's transport
   assumption (`resolveFamily`, `expfmt.NewTextParser`) uniform across both
   profiles and adds no new client dependency; a Relay-based source, if ever
   needed for data Hubble's metrics endpoint doesn't expose, is a different
   Profile shape, not a rewrite of this one.

## Testing

`redsource/collector_test.go` ports the FULL former `redmetrics/
collector_test.go` and `topology/{collector,deltas,derivation}_test.go`
suites onto `ProfileBeyla` (same fixtures — Beyla-shaped Prometheus text,
same label keys — same assertions, renamed `TestBeyla_*`), proving the engine
reproduces the retired packages' exact behavior, plus a parallel `TestHubble_*`
suite proving the same engine against `ProfileHubble`'s genuinely different
label/family shape. Generic engine tests (`New` nil-guards, `namespaceSet`,
`IsHTTP5xxError`, both `FormatSample` implementations, `parseK8sDNS`/
`hostOnly`/`resolveDstAddress`, `p99FromBucketDeltas`, sort helpers, the
typed-nil-`*push.Pusher`-interface trap) are unified once rather than
duplicated per profile. `go build ./redsource/...` and the package's own tests
never mention "hubble" outside `profiles.go` and the test file — a `grep -i
hubble redsource/collector.go redsource/deltas.go redsource/profile.go`
returning nothing is a standing invariant of this design, not a one-time
check.

## Implementation

`redsource/` (`profile.go` — `Profile`/`ClientMetric`/`SampleContext` types +
`IsHTTP5xxError`; `profiles.go` — `ProfileBeyla`/`ProfileHubble` + their
`FormatSample` implementations; `collector.go` — the engine: `Options`,
`Collector`, `New`, `Start`, `runOnce`, `sendRED`, `flushTopology`,
`discoverEndpoints`, `scrape`; `deltas.go` — `requestGroup`/`requestSeries`/
`computeRequestDeltas`, `bucketSeries`/`computeBucketDeltas`/
`p99FromBucketDeltas`, `resolveDstLabeled` (hubble-shape) and
`clientEdgeSeries`/`computeClientEdgeDeltas`/`parseK8sDNS`/`hostOnly`/
`resolveDstAddress` (beyla-shape), `sortNodes`/`sortEdges`); `main.go`
(`--red-source` flag + the unified wiring block replacing the three former
`redmetrics.New`/`topology.New`/dedicated-hubble blocks);
`charts/triage-agent/values.yaml` (`redSource.{profile,metricsService,
hubble}`, `topology.*` comment updates); `charts/triage-agent/templates/
deployment.yaml` (the two profile-aware fail guards + args rendering);
`charts/triage-agent/templates/beyla-daemonset.yaml` (the `or beyla.enabled
useMetrics.enabled` top-level gate + per-resource `beyla.enabled` gating).
`redmetrics/` and `topology/` packages deleted in full, including their test
files — their coverage lives on in `redsource/collector_test.go`'s
`TestBeyla_*` suite. Receiver side (triageagent-dev/core) is unaffected: the wire
contract (`push/protocol.go`) did not change.

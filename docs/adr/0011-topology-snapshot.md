# ADR-0011: Graph-Drift Topology Snapshot (Beyla client+server series -> topology_snapshot frame)

- **Status:** Implemented — **package superseded by ADR-0013** (2026-08-22): the
  `topology/` package this ADR describes is deleted; its behavior (node presence
  scan, client-family edge derivation, DNS-shape server_address resolution, caps,
  first-snapshot zero-edge inclusion) now lives in the shared `redsource/` engine
  ADR-0013 introduces, driven by `redsource.ProfileOBI`'s client metrics.
  ADR-0015 replaced the scrape with the sensor stream. The decisions and behavior described below are
  historically accurate and remain the operational contract — only the
  implementing package moved, and the send cadence changed from an independent
  600s scrape to an accumulated flush over the shared 60s scrape (see ADR-0013's
  "One structural change" section for exactly what that does and does not alter).
  The eBPF engine behind that scrape is OBI (OpenTelemetry eBPF Instrumentation),
  embedded directly in the agent binary and run with `--mode=ebpf`. There is no
  separate Beyla container.
- **Date:** 2026-08-21
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0009 (RED metrics), ADR-0010 (USE metrics), triageagent-dev/core
  docs/todo/graph-drift-detection.md (design doc, hub-side work tracked there)

## Context

Tenant service graphs (mental_map MemoryCells, ADR-030/031 on the triage side)
are operator-applied and go stale silently: a new service ships, an existing one
is renamed or removed, and nothing tells the operator the graph no longer matches
reality until an incident's blast-radius or correlation is visibly wrong.

Application-mode Beyla (ADR-0009) already emits caller-side series alongside the
server-side series redmetrics/ reads: http_client_*, rpc_client_*, sql_client_*,
decorated with both the calling workload's own identity and the callee's address
(server.address). That is caller-to-callee edges at L7 from metrics the
controller already scrapes; no new collection is needed to derive an observed
topology, only a new derivation over the same scrape.

## Decision

Add an opt-in **topology-snapshot pipeline**, entirely on the sender side (this
repo), mirroring redmetrics/usemetrics structurally (Options/Collector shape,
Endpoints-discovery + per-pod scrape) but with two deliberate departures:

1. **No gate.** redmetrics/usemetrics/logmetrics only send a frame when a
   threshold trips. topology.Collector sends a FULL snapshot every tick,
   unconditionally - it is level state (the current known topology), not a
   windowed anomaly report. A lost frame heals on the next tick.
2. **Much longer cadence.** 10 minutes default vs. the 60s scrape-and-gate
   collectors - a topology map does not need anomaly-detection latency.

```
Beyla DaemonSet, application mode (beyla.enabled) - SAME pods redmetrics scrapes
  server-side: http_server_request_duration_seconds_count (NODES)
  client-side: http_client_*, rpc_client_*, sql_client_*, redis client family
    (EDGES) - each decorated with the caller's own (namespace, service) labels
    AND server_address naming the callee

triage-agent topology.Collector (topology.enabled)
  every intervalSeconds (600s default):
    discover triage-beyla Service's Endpoints (same Service redmetrics reads)
    scrape each pod's /metrics
    NODES: every (namespace, service) pair seen in the server-side family,
      presence-only - no delta, a workload's existence isn't gated on volume
    EDGES: for each client-side family, diff against the previous scrape (same
      per-endpoint keying, counter-reset, and first-seen-suppression semantics
      as redmetrics/deltas.go's counterDelta), resolve server_address to an
      in-scope k8s workload or External, aggregate per (src, dst, protocol)
    cap: top MaxTopologyEdges (500) by Calls, first MaxTopologyNodes (200)
      alphabetically - dropped counts logged, no silent truncation
    -> push.Pusher.SendTopologySnapshot(...) (topology_snapshot frame, ONE
       frame per tick, unconditionally - including an empty result)
```

The collector does not compare the observed topology against any tenant graph -
that comparison is hub-side work (triageagent-dev/core's design doc), deliberately out
of scope here. This repo only observes and reports.

### Wire contract (topology_snapshot frame, v1 - pinned, do not deviate)

Added to push/protocol.go (the triage receiver defines its own byte-identical
mirror - hub-first rollout required, see Consequences):

```go
const KindTopologySnapshot = "topology_snapshot"

const MaxTopologyNodes = 200
const MaxTopologyEdges = 500
const MaxTopologyExternalLen = 128

type TopologySnapshotPayload struct {
    CapturedAt int64          `json:"capturedAt"`
    WindowSec  int            `json:"windowSec"`
    Nodes      []TopologyNode `json:"nodes"`
    Edges      []TopologyEdge `json:"edges"`
}

type TopologyNode struct {
    Namespace string `json:"namespace"`
    Workload  string `json:"workload"`
}

type TopologyEdge struct {
    SrcNamespace string `json:"srcNamespace"`
    SrcWorkload  string `json:"srcWorkload"`
    DstNamespace string `json:"dstNamespace,omitempty"`
    DstWorkload  string `json:"dstWorkload,omitempty"`
    External     string `json:"external,omitempty"`
    Calls        int64  `json:"calls"`
    Protocol     string `json:"protocol,omitempty"`
}
```

Frame.TopologySnapshot carries this payload (json:"topologySnapshot,omitempty")
on the shared envelope. Same defensive-last-line-of-defense idiom as every other
frame kind: newTopologySnapshot re-caps nodes/edges at the Max constants and
re-truncates every edge's External, even though topology.Collector is expected
to have already done both.

### NODES: presence, not volume

A workload's existence as a node does not depend on call volume this window -
the server-side family is scanned for every (namespace, service) pair present
(in Options.Namespaces), with no delta computation at all. A service that
served zero requests this window (but is still running and was scraped) is
still a node; only a service that Beyla never observed serving anything, ever,
is absent.

### EDGES: derivation and aggregation

topology.Collector.computeEdgeDeltas (mirrors redmetrics/deltas.go's
computeDeltas) keys each client-side series by its FULL raw label set (not just
src + server_address) so a route/method dimension Beyla may attach to a client
series - which topology never reads or reports - cannot corrupt the delta
baseline if such a sub-series appears or disappears between scrapes. Baselines
are maintained per endpoint for every series regardless of whether its source
is in-scope (same as redmetrics: the tenant guard applies at aggregation, not
at baseline-tracking, so a Beyla DaemonSet shared by several tenants keeps
correct per-series deltas for all of them).

After delta computation, Calls deltas are summed per (src, dst-or-external,
protocol) - the wire contract's aggregation key. Calls is always >= 0
(counterDelta's reset handling, same semantics ADR-0009 describes, and the
2026-08-21 first-seen-suppression fix this pipeline reuses verbatim).

### server_address resolution: k8s DNS shape vs. everything else

resolveDst (topology/deltas.go) recognizes two in-cluster DNS shapes: the
shorthand "svc.ns" and the fully (or partially) qualified
"svc.ns.svc[.cluster.local...]", with ns a member of Options.Namespaces.
Anything else - an IP literal, a bare external hostname, or a syntactically
valid k8s-shaped address whose namespace is NOT in scope - resolves to
External (the raw host[:port] value, truncated to MaxTopologyExternalLen),
never a guess. This directly implements rail 1 of the design doc ("cross-tenant
edges report as external in v1").

**v1 limitation, deliberately accepted**: DstWorkload for a k8s-DNS-shaped
address is the k8s Service name as it literally appears in the address text -
there is no independent confirmation that this equals the backing workload's
own service_name label (the identity NODES are keyed by, and the identity a
caused_by correlation on the triage side would need to line up against). A
Service fronting a Deployment with a different name produces an edge whose
DstWorkload will not match any node the SAME collector derived from the
server-side family for that workload. Resolving this correctly needs either a
k8s Service-to-Deployment lookup (a new dependency this collector does not
have - it only reads Prometheus text, never calls the k8s API for anything but
its own Endpoints lookup) or hub-side reconciliation against the tenant's
declared graph, which already knows the Service-to-workload mapping. Left as a
v2 follow-up; the design doc's own rail 2 ("absence of traffic is weak
evidence... v2") suggests the same hub-side comparison logic is the natural
place to resolve it.

### First-snapshot zero-Calls inclusion

counterDelta's first-seen-suppression rule (a series with no baseline entry
reports delta 0) means a collector's very first tick would, if zero-Calls edges
were dropped as usual, ship a topology snapshot with NODES but NO EDGES at all
- even though every edge legitimately exists, it just has no measured delta
yet. Collector.firstSnapshotDone tracks this: on the first successful scrape
cycle (includeZero), zero-Calls edges ARE included - their existence at all IS
the level state on a cold start. Every subsequent tick drops zero-Calls edges
normally. This is a one-time, collector-lifetime (not per-tick) exception.

### Caps: nodes alphabetical, edges by Calls

Unlike redmetrics/usemetrics, which cap by a magnitude the gate already ranked
on (error delta, mem PSI), nodes have no natural per-tick magnitude - a node is
either observed or not. MaxTopologyNodes (200) keeps the alphabetically-first
nodes (a stable, deterministic choice); MaxTopologyEdges (500) keeps the top
edges by Calls. Both trims log the observed/kept/dropped counts at Info - no
silent truncation, per design-doc rail 3.

### Empty-result frames are sent; all-scrapes-failed frames are not

Pusher.SendTopologySnapshot deliberately does NOT early-return on an empty
nodes+edges result (every sibling Send* method does skip an empty/zero result)
- an empty topology snapshot is meaningful level state ("nothing observed this
window"), not "nothing to report" the way a gate that never tripped is. The one
case topology.Collector.runOnce itself skips sending is when EVERY endpoint
scrape failed this tick: nothing new was learned, so shipping an empty snapshot
would incorrectly overwrite the hub's last-known topology instead of just
leaving it to age until the next successful tick - the actual meaning of "a
lost frame heals on the next tick".

## Consequences

**Positive**
- Continuous, zero-additional-collection graph-drift signal - no new eBPF
  instrumentation, no new Service, no new DaemonSet; reuses the exact Beyla
  pods and Endpoints lookup redmetrics already established.
- topology.enabled is an independent opt-in toggle, same posture as
  redMetrics.enabled/useMetrics.enabled - a fleet can run any subset of the
  four Beyla-derived pipelines (redMetrics, useMetrics, topology, plus
  logMetrics from Fluent Bit) simultaneously.
- No gate means no tuning burden (no threshold to get wrong) and no risk of a
  drifted graph going unreported because nothing crossed a rate floor.

**Negative / risks**
- **Hub-first rollout required**, identical to every prior frame-kind ADR in
  this repo: the triage receiver's Frame.Validate() rejects any kind it
  doesn't recognize, so a controller with topology.enabled=true has its
  topology_snapshot frames dropped harmlessly until the receiver ships
  KindTopologySnapshot handling.
- The DstWorkload-from-Service-name limitation above means an edge's callee
  may not line up 1:1 with that callee's own node identity in some
  topologies - a known v1 gap, not a silent one.
- A 10-minute cadence with a 500-edge cap means a very high-cardinality
  topology (many distinct caller-to-callee pairs) drops the tail of the
  distribution every tick the same way it did the tick before, if the tail
  doesn't change - accepted for v1 as the same trade-off ADR-0009 already
  accepted for red_metrics.
- No comparison against the tenant's declared graph happens in this repo - a
  topology.enabled=true controller with no hub-side comparer wired up simply
  produces frames nothing consumes yet.

**Neutral**
- Push mode, Application reconciliation, and CRDs are unchanged. Purely
  additive, reusing the exact scrape/Endpoints/HMAC-handshake machinery every
  prior push-mode frame kind already established.

## Alternatives considered

1. **Delta edges (only report edges that changed) instead of a full
   snapshot.** Rejected - the design doc's explicit decision is level state: a
   full snapshot is simpler for the hub-side comparer to reason about (no
   drift-of-drift from an accumulated delta stream) and self-heals a lost
   frame for free.
2. **Derive Dst from a k8s Service-to-Endpoints-to-pod-owner lookup** instead
   of parsing the DNS-shaped server_address text. Rejected for v1 - it would
   turn this collector into a second, redundant service-topology resolver
   duplicating what the hub-side tenant graph already is, and adds a k8s API
   dependency (Service object reads) the collector does not otherwise need.
   Left as the v2 follow-up noted above.
3. **A network-mode Beyla / Hubble Relay source instead of application-mode
   client series.** The design doc names Hubble Relay as a provider-pluggable
   alternative on GKE Dataplane V2 (ebpf-telemetry-source.md's provider
   matrix) producing an identical topology_snapshot frame; this ADR
   implements only the Beyla application-mode path, since that is the
   DaemonSet already deployed for redMetrics/useMetrics.

## Implementation

Sender side (this repo): push/protocol.go (frame types, MaxTopologyNodes,
MaxTopologyEdges, MaxTopologyExternalLen, newTopologySnapshot,
TruncateTopologyExternal), push/pusher.go (Options.Topology,
SendTopologySnapshot, hello capability), topology/ (collector + delta/
resolution logic, mirroring redmetrics/ structurally),
charts/triage-agent/templates/deployment.yaml (render guards +
--topology-* args, and the shared Beyla-scrape-target arg block now rendered
whenever EITHER redMetrics.enabled OR topology.enabled),
charts/triage-agent/values.yaml (topology.*), main.go
(--topology-enabled/--topology-interval-seconds flags, reusing the
--red-metrics-* scrape-target flag values rather than duplicating them).
Receiver side: triage-agent internal/remoteagent/protocol.go plus the
hub-side comparer/surfacing work - tracked in triageagent-dev/core's
docs/todo/graph-drift-detection.md, not here; see the hub-first rollout note
above.

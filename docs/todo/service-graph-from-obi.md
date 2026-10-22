# Service graph built from OBI: virtual-app identity on every edge

Status: IN PROGRESS (2026-09-04) — this repo's `feature/service-graph-from-obi`
(off `main` @ c3e6d6b) is code-complete and tested (Steps A and B below); the
chart side (Step C, `triageagent-dev/charts`' `feature/network-flows`) and live cluster
verification are still open.

## Problem

The embedded OBI records every HTTP hop as client series
(`http_client_request_duration_seconds_count{service_name=<caller>,
k8s_owner_name=<caller Deployment>, k8s_namespace_name=…,
server_address=…}`), with `server_address` in whatever form the calling app
used: a bare Service name (`orders-app-api`), `<svc>.<ns>`
(`inventory-app-api.vappsim`), a full FQDN
(`billing-app-api.vappsim.svc.cluster.local`), or a raw ClusterIP call OBI
could not name at all (`server_address="outgoing"`, `RenameUnresolvedHostsOutgoing`'s
default — see A4).

`redsource.resolveDstAddress`/`parseK8sDNS` kept only the `svc.ns`/FQDN forms
and named the destination by the **Service** name, while the virtual app the
same agent pushes for that workload is named by the label chain
(`app.kubernetes.io/instance` → `inventory`). The core therefore receives
`inventory-app-api` edges and `inventory` apps with no way to join them; bare
names and IPs dropped to External; NATS/TLS/raw-TCP hops produced no L7 series
at all.

Goal: the agent builds the service graph itself from OBI, with every edge end
named by the virtual-app identity, from two sources — L7 client spans
(resolved through the cluster's own objects, no DNS-shape parsing) and L4
network flows (`obi_network_flow_bytes_total`, see B1).

## Correction to the original brief

The task brief that drove this plan cited xray (map `appctl`) results for
`GroupNameForChain`, `push.LabelChain`, `applyServiceNameLabelsChain`,
`workloadInfo.serviceNameLabels`, and a `runEBPF(ctx, serviceNameLabels
push.LabelChain)` signature. **None of these exist in this checkout at
`main` @ c3e6d6b** — verified by `grep -rn` across every `.go` file including
tests. The xray `appctl` map is stale relative to this repo (it reflects
some other/later state). The actual chain is the plain
`GroupNameFor(w WorkloadIdentity) string` in `push/virtualapps.go` (GroupOverride
→ `part-of` → `instance` → `name`, no `LabelChain` parameter), and
`runEBPF(ctx context.Context) error` takes no label-chain argument. Every
step below is written against the real code, read directly, not the map.

## Design

### A. Identity index + cluster-object resolver (this repo, `push/` + `redsource/`)

- `push/virtualapps.go`: `workloadInfo` gains `PodLabels map[string]string`
  (the Deployment/StatefulSet/DaemonSet's `spec.template.metadata.labels`,
  alongside the object's own `Labels` it already keeps).
- `virtualAppsState` gains an identity index (`identityByOwner`,
  `workloadsByNamespace`), rebuilt once per `recompute()` from the same
  workload snapshot grouping already reads — never a second cache list.
  Exposed via unexported `identityFor`/`workloadForSelector`, and via
  exported `*Pusher` methods `IdentityFor(namespace, ownerName string)
  (string, bool)` and `WorkloadForSelector(namespace string, selector
  map[string]string) (string, bool)`. Both report `false` when
  `--virtual-apps=off` (`p.vapps == nil`) — service-graph identity naming is
  therefore only as good as virtual-app tracking; the chart's default is
  `on` fleet-wide, so this is not a live gap.
- `push/serviceindex.go` (new): controller-runtime informers on
  `corev1.Service`/`networkingv1.Ingress`, registered unconditionally in
  `Pusher.Start` (independent of `--virtual-apps` — this is a plain
  cluster-object lookup) on the SAME manager cache the vapp engine's own
  Deployment/StatefulSet/DaemonSet informers use, so it inherits identical
  namespace scoping for free. Exposes `ServiceByName`, `ServiceByClusterIP`,
  `IngressBackend` (ambiguous host → logged once, unresolved) on `*Pusher`.
- `redsource`: `resolveDstAddress`/`parseK8sDNS` deleted (with their tests).
  New `Resolver` interface on `Options` (`Resolve(srcNamespace,
  serverAddress string) (dst servicePair, external string)`); nil (no
  resolver wired — every test that doesn't need address resolution, and the
  Hubble profile, which never calls this path at all) falls back to
  always-External. `k8sResolver` (`redsource/k8sresolver.go`) implements it
  against a small `ClusterIndex` interface (`ServiceByName`,
  `ServiceByClusterIP`, `IngressBackend`, `WorkloadForSelector`,
  `IdentityFor`) that `*push.Pusher` satisfies structurally — `redsource`
  never imports `client-go`/`controller-runtime` types of its own for this.
  Resolution order: strip `:port`; `"outgoing"` (OBI's own
  unresolved-IP placeholder, see A4) is always External; an IP literal goes
  through `ServiceByClusterIP`; otherwise, in order, `<svc>` in
  `srcNamespace`, `<svc>.<ns>`, `<svc>.<ns>.svc[...]` (any suffix after
  `.svc`), then the Ingress host index; a resolved Service's selector goes
  through `WorkloadForSelector` → `IdentityFor`. Any step failing (unknown
  host, ambiguous ingress, namespace out of scope, unknown selector, unknown
  owner) falls through to External.
- `Profile` gains `SourceOwnerLabel` (ProfileBeyla: `"k8s_owner_name"`;
  Hubble: unset, untouched) — the "self" identity every OBI span already
  carries (the emitting pod's own owner), resolved via a new
  `Options.Identity IdentityResolver` (`IdentityFor`) with fallback to the
  existing `service_name`-label value when the owner isn't a tracked
  workload. Applies to both RED/topology-node identity (server-side family)
  and client-edge source identity (client-side families) — same label key,
  same "self" semantics, different family.
- Wired in `main.go`'s `case "ebpf":` block only, when `pusher != nil`
  (`Options.Resolver = redsource.NewK8sResolver(pusher, pusher.NamespaceInScope)`,
  `Options.Identity = pusher`).

### B. L4 network flows as a second edge source (`redsource/`)

- Vendored OBI (`third_party/obi`) findings (see B1 for exact detail):
  - Metric family: **`obi_network_flow_bytes_total`** (`attributes.NetworkFlow`,
    OTEL name `obi.network.flow.bytes`, Prom-derived) — **not**
    `beyla_network_flow_bytes_total` as in captures taken with the
    standalone Beyla image, whose naming predates this vendored OBI.
  - `direction` attribute **is** exposed for network flows (`attr.Direction`,
    default-enabled in `networkAttributes`), values `request`/`response`/
    `unknown` (`pkg/internal/netolly/ebpf/record_getters.go`) — so B2's
    request-only filter is implemented, not degraded to "keep both
    directions."
  - `k8s.src.owner.name`/`.namespace`/`.owner.type` and the `dst` equivalents
    are in `networkKubeAttributes`, default-enabled whenever
    `attributes.kubernetes.enable: true` (already set by
    `obiEssentialsYAML`) — no explicit `attributes.select` override is
    strictly required, but one is added anyway for explicitness against
    future OBI attribute-default drift.
  - Enabling: `network: {enable: true, source: socket_filter}` — the
    *deprecated* toggle, deliberately: `Config.normalize()` OR's
    `export.FeatureNetwork` onto `Metrics.Features` rather than replacing it,
    so the default `FeatureApplicationRED` (existing RED metrics) survives
    untouched. The "modern" `metrics.features` list would have to
    re-enumerate `application` itself to avoid silently dropping RED.
- `redsource.Profile` gains `FlowMetric` + the six `Flow*Label` fields
  (ProfileBeyla only; Hubble leaves `FlowMetric` empty, a no-op).
  `computeFlowEdgeDeltas` (same full-raw-label-set delta baseline as
  `computeClientEdgeDeltas`) → `filterFlowSamples` drops rows with an empty
  owner or a `Node`-typed owner on either end, keeps only `direction=request`
  rows when the label is present, and **dedupes by (src, dst) name pair**:
  the fixture shows OBI emitting both a `Service`-typed and a
  `Deployment`/`StatefulSet`/`DaemonSet`-typed row for what is the same
  logical pair (e.g. `orders-app-api`↔`billing-app-api` in both a Service and
  a Deployment guise) — when a workload-typed row exists for a pair, the
  Service-typed alternate is dropped.
- `push.TopologyEdge` gains `Bytes int64 \`json:"bytes,omitempty"\`` (JSON tag
  exactly `bytes`, matching the core's parallel field). `flushTopology` drops
  an edge only when `Calls == 0 AND Bytes == 0` (after the first flush); the
  cap sorts by Calls then Bytes.

### C. Chart (`triageagent-dev/charts` `charts/triage-agent`, `feature/network-flows`)

- `values.yaml`: `ebpf.networkFlows.enabled` (default `false`) — enabling it
  sets `hostNetwork: true` + `dnsPolicy: ClusterFirstWithHostNet` on the eBPF
  DaemonSet (socket_filter captures from the host network namespace).
- ConfigMap renders the `network:` block + attribute select when enabled;
  RBAC grants `services`/`ingresses` `get/list/watch` unconditionally (A2
  needs it regardless of the flag).
- Chart version bump 0.18.0 → 0.19.0.

## Status

- [x] Step 0: this plan doc.
- [x] Step A1: `workloadInfo.PodLabels` + identity index (`IdentityFor`,
      `WorkloadForSelector`) on `virtualAppsState`/`*Pusher`.
- [x] Step A2: Service/Ingress informers + index (`push/serviceindex.go`).
- [x] Step A3: `redsource.Resolver`/`k8sResolver`, `resolveDstAddress`/
      `parseK8sDNS` deleted, `SourceOwnerLabel` + `Options.Identity` wired.
- [x] Step A4: `name_resolver: {sources: [k8s]}` made explicit in
      `obiEssentialsYAML` (already the vendored default — see B1 finding
      above, same normalize()-driven default pattern).
- [x] Step A5: tests (`redsource/k8sresolver_test.go`,
      `push/virtualapps_test.go`, `push/serviceindex_test.go`, updated
      `redsource/collector_test.go`).
- [x] Step B1/B2/B3: `network:` block gated by `OBI_NETWORK_FLOWS=true`,
      `computeFlowEdgeDeltas`/`filterFlowSamples`, `TopologyEdge.Bytes`,
      tests from the renamed real-capture fixture.
- [ ] Step C: chart changes on `triageagent-dev/charts` `feature/network-flows`
      (tracked in that repo).
- [ ] Live verification on a cluster (acme-gke/vappsim) — not done as part of
      this branch (no deploy).

Live finding (acme-gke, 2026-09-04, agent v2.5.39-2ef6466-obi,
`ebpf.networkFlows.enabled=true`): every long-lived TCP connection that
existed before the eBPF DaemonSet pod started (`orders-app-api`/
`notifier-app-api` -> `nats` in `vappsim`) exports
`obi_network_flow_bytes_total` with `direction=unknown` — OBI never saw the
SYN, so it cannot tell the initiator, and `filterFlowSamples`'s
request-only filter drops those edges before they reach the topology frame.
HTTP hops whose connections opened after the DS started are unaffected.
Fix: `obiEssentialsYAML` now also emits `network.guess_ports: ordinal`
(`third_party/obi/pkg/obi/network_cfg.go`'s `NetworkConfig.GuessPorts`,
`flowdef.PortGuessPolicy`) whenever `networkFlows` is set — the Direction
getter (`third_party/obi/pkg/internal/netolly/ebpf/record_getters.go`) falls
back to the ordinal port guesser when `Metrics.Initiator` is unknown, which
is correct for every server port that matters since Linux ephemeral ports
are >= 32768. Chart side: `charts/triage-agent/templates/ebpf-configmap.yaml`
carries the same key under its `network:` block.

Live defect (acme-gke, 2026-09-04 21:28 UTC, agent v2.5.40-db9b28a-obi): the six
`vappsim` Deployments were deleted at 21:28:19. The virtual-app recompute
dropped them from the identity index within a second, but the topology flush
at 21:28:39 scraped the LAST window's `obi_network_flow_bytes_total` deltas —
bytes sent before the deletion — and `resolveIdentity` fell back to the raw
owner names (`orders-app-api`, `billing-app-api`, ...) once `IdentityFor` no
longer knew them. Those went out as in-scope topology nodes/edges, and the
core fabricated stub services for them. Fixed by making the fallback
direction-specific instead of uniform: on the L4 flow path and the L7
client-edge path, a SOURCE owner Options.Identity cannot resolve drops the
row entirely (no in-scope node to hang an edge on), and a flow DESTINATION
owner it cannot resolve becomes External under its raw name instead of an
in-scope node. The RED/RequestsMetric destination identity (server side)
keeps its existing service_name fallback — that value comes from OBI's own
label chain, not a raw owner name, so it was never the defect. A workload the
identity index doesn't know is either deleted or was never tracked
(CronJob/Job-owned pods, or vapps off for it); it must never be emitted as an
in-scope workload under its raw name.

## Open: L7 loses raw-ClusterIP calls as `outgoing` (2026-09-04)

Isolation test on acme-gke (agent v2.5.41, `networkFlows` off for two windows):
the L7 client families alone yield four of the five HTTP hops. The missing
one is `billing -> inventory`, which the app calls by ClusterIP
(`http://10.0.130.96:8080/reserve`). OBI's default
`attributes.rename_unresolved_hosts_outgoing` replaces a server address it
could not name with the literal `outgoing`, so the agent's ClusterIP index
(`ServiceByClusterIP`) never sees the IP and the edge falls to External.
With `networkFlows` on the L4 path recovers it as a `tcp` edge.

Fix to evaluate: disable the rename (export the raw `ip:port`) so
`k8sResolver` resolves it through the Service index; check what else in
the RED path keys on the `outgoing` value before flipping it.

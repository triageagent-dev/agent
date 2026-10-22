# ADR-0022: OTLP Sensor Role (raw OTLP intake with transformers, streamed to the controller)

- **Status:** Accepted. Phases 1 to 6 implemented 2026-10-20 (the role, the
  stream and the chart; spans; logs; delta; no-auth mode; the role's
  certificate identity). Phase 7 is done in part: an in-process end-to-end test
  (`TestOTLPRole_RawOTLPReachesTheHubAsREDAndLogSignals`) covers raw OTLP to
  the hub, and a stock Collector sending raw traces, logs and metrics to the
  role ran on a test cluster on 2026-10-20 (no rejected points, RED readings
  per service). A cluster run on 2026-10-21 with a fully raw Collector (no
  filter, no connectors) confirmed RED for every service, topology edges and no
  double counting; see "Findings of the cluster run".
- **Date:** 2026-10-20
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0015 (sensor stream), ADR-0017 (OTLP receiver), ADR-0021
  (OTLP-only agent, no Kubernetes watch)

## Context

ADR-0017 put an OTLP metrics receiver on the controller. It accepts only what
the collectors can diff: cumulative sums and explicit histograms, already named
the way the RED, USE and LOG profiles read them. A customer's Collector
therefore carries the work of producing that shape:

- the `spanmetrics` connector, to turn traces into `traces.span.metrics.*`;
- the `count` connector, to turn logs into `log.errors/warns/lines`;
- the `deltatocumulative` processor, because delta points are refused;
- a `filter` processor, to send only the names the agent reads.

That is a long Collector config to get right in every cluster, and a mistake
shows up as a silent hole in a signal. The intake also runs on the controller,
the process that holds leader election, the replica mesh and the push to the
core, so a burst of spans competes with them.

The goal is fewer components and fewer points of failure: stock Collectors send
raw telemetry, and the agent does the transforming, with no extra Collector
tier, no broker and no second image.

## Decision

A new sensor role, `--mode=otlp`, in the same image. It runs as a Deployment
(two replicas by default, scalable on CPU), next to the eBPF DaemonSet and
independent of it.

- **Intake.** Each pod serves OTLP/gRPC for **traces, logs and metrics** in any
  temporality. It reuses the ADR-0017 receiver unchanged for authentication,
  tenant certificates, limits, convention normalization and the cAdvisor and
  span-metrics translations, so a sender that already runs the connectors keeps
  working.
- **Transformers, in Go, in the pod.**
  - *Traces to RED and client histograms.* Server and consumer spans become the
    RED histograms (`http.server.request.duration`, `rpc.server.call.duration`),
    client and producer spans the client histograms topology reads, with the
    same attributes `translateSpanMetrics` produces. Internal spans are
    ignored.
  - *Logs to counts.* `log.errors`, `log.warns` and `log.lines` per
    (namespace, service), from `severity_number`. Counts only; the error
    sample needs the exemplar the log exporter adds (ADR-0017).
  - *Delta to cumulative.* Delta sums and delta histograms are added into the
    resource's cumulative state. Exponential histograms stay refused per point.
  - *Early drop.* A series no profile reads is discarded at intake, before it
    takes a slot against the limits.
- **Stream.** The pod's role source returns the resource state as cumulative
  OTLP over the existing sensor stream (ADR-0015). The receiver's `Sink` hook
  is the seam: in this role it feeds the stream client instead of the
  controller's hub. The controller does no OTLP intake for these senders; it
  reads its hub as it does for the eBPF roles, with replica gossip unchanged.
- **Source identity.** Every pod is its own source, keyed by its pod name, as an
  eBPF node is. A sender's spans spread over several pods therefore build
  separate cumulative series that the collectors sum, and none overwrites
  another. A restarted pod is a new source, which the collectors already treat
  as a counter reset. (To confirm in phase 2: the RED collectors sum a
  service's histograms across sources.)
- **Buffering.** The sensor client's ring buffer and replay after a reconnect
  cover a controller restart. A sender sees `UNAVAILABLE` only when no pod of
  the role is ready, and retries from its own queue.
- **Settings and checks.** The pod takes the joined tenants and the limits as
  flags from the chart. It cannot know the workspace scope the core sets at
  run time, so the controller applies the tenant and scope checks again to
  every source the stream carries (`otlpreceiver.ScopeFilter`, the hub's
  `SourceFilter`). The stream accepts OTLP sources only from the otlp role's
  ServiceAccount, and that account may run no other role.
- **Compatibility.** The controller's own OTLP listener (ADR-0017) stays and
  is the default. `otlp.sensor.enabled` moves the intake to the role. The
  role's pods have their own Service, `<fullname>-otlp`, since a Service has
  one selector and `<fullname>-sensors` keeps the controller's stream. The
  serving certificate is the controller's, so the chart adds the new Service's
  names to it (`--sensor-extra-names`): enabling the role reissues the
  certificate once, and senders re-pin `<fullname>-sensor-ca` and dial the new
  name.

### Authentication of senders

Three modes, strongest first. The chart renders exactly the one selected.

1. **Mutual TLS** (ADR-0017 and its amendments): client certificate from
   `otlp.clientCASecret` or the agent's own CA. Required for senders outside
   the cluster.
2. **ServiceAccount token** (ADR-0017): projected token with audience
   `triage-agent-otlp`, checked by TokenReview.
3. **No authentication, in-cluster only** (new). A Collector inside the cluster
   usually has neither a certificate nor an agent-audience token. `otlp.insecure:
   true` serves plain gRPC with no sender check. Because anyone who can reach
   the port can then publish, the mode is fenced:
   - the chart refuses to render it without `otlp.insecure.allowFrom`, a list of
     namespace selectors, and renders a NetworkPolicy that admits only those
     namespaces to the OTLP port (default deny);
   - the Service is ClusterIP only, and `extraNames`, `ownCA` and tenant
     certificates have no meaning in it, so they are rejected together;
   - every point files under `target.tenant` (required, one tenant), and a point
     that claims a namespace outside `target.namespaces` is refused, as a token
     sender's claims are today;
   - the limits apply per remote address instead of per authenticated sender,
     and the receiver counts refusals so the first flood is visible.

   The mode can be combined with mutual TLS on the same listener only if the
   plain port is separate from the TLS port. The first version offers one or
   the other per release.

### Authentication of sensor to controller

The eBPF roles present a projected ServiceAccount token that the controller
checks with a TokenReview. That needs a cluster-scoped ClusterRole, which
ADR-0021 removed for the OTLP-only install, so the otlp role does not use it.
Its pods already mount the controller's sensor-stream serving certificate and
key, to serve OTLP under the certificate senders pin. They present that same
certificate as their client certificate. The controller asks for a client
certificate on the stream (`sensorstream.ServerTLS`), accepts none or exactly
its own (pinned as the replica mesh pins it, since it has no client-auth
usage), and treats a peer that presents it as the otlp role
(`--sensor-otlp-role`). The role runs alone: its hello may name no other role,
and a token sensor may not name this one. The pods need no ServiceAccount token
(`automountServiceAccountToken: false`) unless token senders are allowed, and
the install renders no ClusterRole at all with certificate or unauthenticated
senders. The trade: a pod that can read the Secret can act as the role, which is
the trust the mesh already extends to every replica.

## Alternatives considered

- **A Triage-owned Collector tier in front of the agent**, built with the
  OpenTelemetry Collector Builder. Works, but adds a second binary and image,
  and its transforms are stateful, so its replicas need per-replica identity
  anyway. Rejected as more components for the same result.
- **NATS JetStream between the intake and the transforms**, with Synadia's
  `natsexporter` and `natsreceiver` (v0.2.0, development stability, not in any
  Collector distribution). A spike showed OTLP to JetStream to a transforming
  worker works, and that JetStream can partition by a hash of the resource
  through an OTTL subject. Rejected:
  - the exporter is built against OTTL v0.133 and does not compile in the same
    binary as `transformprocessor`, `filterprocessor`, the `spanmetrics` or
    `count` connectors (v0.159), so the transforms cannot run before JetStream
    without forking the exporter, and running them after it needs a second
    tier;
  - JetStream cannot accept gRPC; an OTLP front end must publish for it;
  - it adds a broker to run, secure and size, to buy durability that the
    sensor ring buffer and the senders' own retry queues already give for
    signal data, where the next cumulative snapshot replaces a lost one.
- **rotel** (Rust collector). Its Python processors handle traces and logs but
  not metrics, no TLS options are documented for its OTLP receiver, and it has
  no equivalents of the spanmetrics, count or delta-to-cumulative components.
- **Benthos / Redpanda Connect / Wombat.** It reads and writes OTLP and
  JetStream, but its mapping language is per message. Histograms from spans,
  keyed delta sums and per-resource regrouping would be hand-built in YAML,
  and it would be a third engine to run.
- **Transforms in the controller only.** Smallest change, but the controller
  then holds partial cumulative state per replica that the mesh's newest-wins
  rule would overwrite, and span intake shares a process with leader election
  and the core push.

## Consequences

- A cluster's Collector config shrinks to receivers (`otlp`, `k8s_cluster`,
  `prometheus/cadvisor`), `k8s_attributes` and an OTLP exporter. The connectors,
  `filter` and `deltatocumulative` are no longer required.
- `k8s_attributes`, the cAdvisor scrape and `k8s_cluster` stay on the sender:
  they read the kubelets and the API server, which a remote intake cannot.
- Span volume is processed in the role's pods, sized and scaled on their own,
  not on the controller.
- A sensor pod's state is in memory. Losing a pod resets its counters; senders
  reconnect to another pod and the next snapshot restores the series.
- The unauthenticated mode trades sender identity for installability. It is
  explicit, fenced by NetworkPolicy and limited to one tenant, and must not be
  exposed outside the cluster.
- One more workload in the chart, no new image and no new dependency.

## Findings of the cluster run

A Collector with no `filter` and no connectors, sending everything raw, showed
four things the first design did not cover.

- **Early drop is by name, at intake.** An unfiltered Collector sends hundreds of
  metric names and a resource for every pod, which reached the resource and
  series limits within a minute. `otlpreceiver.Options.Wanted`
  (`ReadByCollectors`) discards a metric no collector reads before it takes a
  slot. Roughly 60 to 90 percent of points are dropped this way and are
  reported as `unread`, not as refused.
- **Native before derived.** A service that reports its own request duration
  metric and sends spans was counted twice. The RED collector reads sensors,
  then native OTLP, then OTLP derived from spans, and skips a derived series
  whose (service, metric) a native source already reported. The derived
  resources are marked with `triage.otlp.replica` (`metricset.DerivedAttr`).
- **gRPC by `rpc.system.name`.** Spans of the current RPC conventions carry
  `rpc.system.name`, not `rpc.system`. They were read as HTTP, which doubled
  `product-catalog`. The transformer now treats `rpc.system`,
  `rpc.system.name` or `rpc.grpc.status_code` as gRPC.
- **No namespace, no frame.** The core refuses a whole frame over one entry
  with no namespace or name (`red_summary service[N] missing namespace`), which
  kept the topology from ever being accepted (`topo armed`). Three services
  arrived without a namespace because `k8sattributes` associated by
  `container.id` first and their SDKs report a wrong one. The sender fix is to
  associate by connection IP; the agent now holds back such an entry by name
  (`push/wellformed.go`), lists it in the local TUI and counts it, so the rest
  of the frame is sent.
- **Callees without a Service index.** With `kubernetes.watch=false` the
  resolver cannot map a server address to a service, so every edge was
  external. `redsource.knownCallee` names a callee from the services already
  read (`name`, `name:port`, `name.namespace`, `name.namespace.svc`), also
  when the resolver returned the address as external. Other addresses stay
  external.

## Known limits of phase 1

- The pods mount the own CA's `ca.crt` only. A client certificate issued from
  a CA the controller has since renewed (kept as `ca.prev.crt`) is refused by
  the pods until it is issued again; the controller itself trusts both.
  Publishing the trust bundle in a ConfigMap, as for `<fullname>-sensor-ca`,
  would remove the limit.
- The role's source snapshots are not gossiped from the pod; the controller
  that received them gossips them (`Hub.Ingest`), so every replica sees them.
- Pods and controller share one key (the serving certificate's), so the role's
  identity is only as private as that Secret.

## Phases

1. **Role.** `--mode=otlp`, the Deployment, the Service, the hello response with
   tenants and limits, and the stream wiring. Metrics only, behavior equal to
   ADR-0017.
2. **Traces.** Spans to RED and client histograms; confirm the collectors sum
   across sources.
3. **Logs.** Counts.
4. **Delta.** Delta to cumulative with the existing memory bounds.
5. **No-auth mode.** `otlp.insecure`, the NetworkPolicy, per-address limits.
6. **Sensor client certificates**, so the OTLP-only install needs no ClusterRole.
7. **Acceptance.** A stock Collector with the minimal config above on a test
   cluster; compare RED, USE, LOG and topology with the connector-based config.

## Tests (to write with each phase)

- Spans: a table of span kinds, statuses and attributes against the histograms
  `translateSpanMetrics` produces for the same traffic.
- Logs: counts per severity boundary, per (namespace, service).
- Delta: sums and histograms accumulate; a reset and an out-of-order point do
  not decrease a counter; limits refuse points past the bound.
- Source identity: two pods receiving halves of one service's spans give two
  sources whose sum equals the whole.
- Insecure mode: the chart refuses to render without `allowFrom` or
  `target.tenant`; a point claiming another namespace is refused; per-address
  limits apply.
- Stream: a controller restart replays the ring without a gap or a double count.

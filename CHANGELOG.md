# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Two images: `agent` without OBI and `agent-obi` with it.** The default image, `agent` (`.ko.yaml`, `make ko-push`), is built without `-tags obi`: it links no OBI package, needs no generated eBPF bindings, is 54 MB against 93 MB for linux/amd64, and runs the controller and the OTLP role; `--mode role ebpf` fails fast in it. `agent-obi` (`hack/ko-obi/.ko.yaml`, `make ko-push-obi`, published to `<repo>-obi`) is the build with the instrumenter for the eBPF DaemonSet. Every file that imports OBI (`ebpf_config.go`, `ebpf_attach.go`, `ebpf_host_red.go`, `ebpf_metrics.go` and their tests) now carries the `obi` build tag, and a CI step fails if the plain build imports one. CI builds, signs and attests both images with the same tags. The chart runs the controller and the OTLP role from `image.repository` and the DaemonSet from `image.ebpf.repository` (default `<image.repository>-obi`, tag `image.ebpf.tag`, default `image.tag`).
- **A Collector can send everything it has.** The OTLP receiver drops a metric no collector reads at intake (`Options.Wanted`, `ReadByCollectors`: the RED histograms and their legacy names, the spanmetrics output, `log.*`, the cAdvisor series, `use.*` and the workload counts), before its resource is stored or counted against a quota. A sender that exports the whole k8s_cluster, runtime and Collector self-telemetry therefore no longer fills the per-sender and total quotas ahead of the resources that matter; the dropped points show as `unread` in the receiver's summary and the ledger, not as refused. (`TestWanted_*`, `TestReadByCollectors`)
- **A metric a sender reports itself is not counted again from the same service's spans.** The RED collector reads the OTLP sources the sender reported before the ones the receiver derived from spans, and a derived source only fills in the (service, metric) pairs and edge sources the native ones did not report, as the DaemonSet sensors already take precedence over OTLP. Without it a service that exports its own `http.server.request.duration` and its spans was counted twice. (`TestOBI_Collector_NativeOTLPWinsOverDerivedPerService`)
- **Entries the core would refuse a frame over are held back by name and shown in the local UI.** The core drops a whole frame when one service has no namespace or name, or a topology node or edge lacks an end, so a single OTLP source that its Collector did not enrich with `k8s.namespace.name` silenced the RED summary and the topology snapshot of every other service. The push queue now drops just those entries, counts them (`Link.MalformedDropped`, `triage_agent_frames_malformed_dropped_total`) and records each by frame kind, name and reason in the ledger; Info lists them under "Held back from the core" and the sources view repeats the list. An OTLP role pod also logs, once per source, the attribute keys of a resource that arrived without a namespace. (`TestDropMalformed_*`, `TestStatsListsTheEntriesHeldBackFromAFrame`)
- **The OTLP receiver accepts delta temporality** (ADR-0022 phase 4). A delta sum or histogram is added to the running total held for its series, so the collectors still see only cumulative data and a Collector no longer needs the `deltatocumulative` processor. An interval that ends no later than the last one added is dropped as a retry, so a re-sent batch is not counted twice; a histogram whose bucket bounds change is refused. Exponential histograms are still refused. (`TestDelta_*`)
- **The OTLP receiver turns spans and log records into the RED and log signals itself** (ADR-0022 phases 2 and 3). It serves the OTLP traces and logs services next to the metrics service. Server, consumer, client and producer spans become the `http.*`/`rpc.*` duration histograms, with the series and attributes `translateSpanMetrics` gives the spanmetrics connector's output for the same traffic; log records become `log.errors`, `log.warns` and `log.lines` counters (severity number from ERROR and from WARN, or the severity text when a library sets no number). What the receiver derives is partial per process, so its resource carries `triage.otlp.replica` (the pod name) and stays a source of its own: replicas never overwrite one another's share. A Collector no longer needs the `spanmetrics` or `count` connector. (`TestTraces_*`, `TestLogs_*`)
- **The OTLP receiver as a sensor role** (`--mode=otlp`, chart `otlp.sensor.enabled`, ADR-0022 phase 1). The receiver runs in its own Deployment (`<release>-otlp`, Service `<release>-otlp`, two replicas) instead of the controller. Its pods serve the controller's certificate, authenticate senders as before, and stream every OTLP source to the controller over the sensor stream (`Snapshot.source`); the controller accepts such sources only from a peer that presents its own serving certificate (`--sensor-otlp-role`, the role's pods mount that certificate to serve OTLP; no token, no TokenReview, no ClusterRole for the role) and applies the joined-tenant and workspace-scope checks again (`otlpreceiver.ScopeFilter`). Enabling it reissues the serving certificate with the new Service's names, so senders re-pin once. Off by default; the controller's own listener is unchanged. (`TestOTLPRole_*`, `TestScopeFilter*`, `TestNewOTLPSensorReceiver_Validates`)
- **OTLP without authentication for senders inside the cluster** (`--otlp-insecure`, chart `otlp.insecure`, ADR-0022 phase 5). Collectors in the cluster usually have neither a client certificate nor a token for the agent. The role's pods then serve plain gRPC and take anything sent to them; every sender is an anonymous forwarder named by its address, so the sender quota and the refusal counters apply per address, and a namespace of the `<tenant>~<namespace>` form is still refused. The chart requires `otlp.sensor.enabled`, refuses it together with `ownCA`, `clientCASecret`, `allowedServiceAccounts` or `forwarders`, needs `otlp.insecure.allowFrom` and renders a NetworkPolicy that admits only those namespaces to the OTLP port. (`TestInsecure_*`)
- **Workspace namespace scope from the core.** The `limits` frame gains `namespaces`, a map from tenant ID to the namespaces the agent may send for it; an absent tenant or an empty list takes every namespace, and each frame replaces the whole scope. Before the first frame arrives everything is allowed (a key join seeds it from the bindings of the join result). The OTLP receiver refuses a tenant-certificate resource whose `k8s.namespace.name` is outside the tenant's scope, and a point that names such a namespace, before anything is stored, counted as refused with the reason `namespace outside the workspace scope`. The push path drops any app, event, metric service or topology node and edge outside its tenant's scope, in single-tenant and cluster mode alike, and counts them in `outOfScopeDropped` (status API, TUI link state, `triage_agent_frames_out_of_scope_dropped_total`). `Pusher.WorkspaceNamespaceInScope(tenant, ns)` is the exported check. Kubernetes Warning events from such a namespace are skipped at the informer. An older core sends no namespaces and nothing changes. (`TestWorkspaceScope_*`, `TestTenant_NamespaceOutsideWorkspaceScopeRefused`)
- **Event caps from the core.** The agent reads the core's new `limits` frame (the daily event cap of each tenant it serves, core policy `budgets.agentEventsDaily`) and holds its Kubernetes events to it with one token bucket per tenant: it refills evenly over 24 hours and holds one hour's share, so an event storm is cut within minutes. App deltas and metric batches carry state and are never held back; the core caps the events it derives from them. Drops are counted per tenant and logged on the first and every thousandth. An older core sends no limits and nothing changes. (`TestEventCaps_BucketHoldsOneHourAndRefills`, `TestPusher_K8sEventsHeldAtCapAppDeltasNever`)

### Fixed

- **A token sensor can no longer file OTLP sources.** The hub handed any snapshot that carried a `source` to the OTLP ingest path whatever role the stream had announced, so a stream authenticated with a sensor token could file a source under a name of its choosing and skip the tenant checks that guard the other roles' snapshots. A snapshot that names a source on a stream that is not the otlp role's (the pinned serving certificate) now ends the stream with `PermissionDenied`. (`TestOTLPRole_ATokenSensorCannotForgeASourceOnADataRoleStream`)
- **The OTLP receiver registers the gzip, zstd and snappy gRPC codecs itself.** A Collector's OTLP exporter compresses with gzip by default (zstd and snappy by setting), and a gRPC server refuses a codec nobody registered (`Decompressor is not installed for grpc-encoding`). The codecs used to be registered by packages that only the OBI build linked; the build without OBI would have refused every default export. (`TestReceiverAcceptsTheCodecsACollectorUses`)
- **Go 1.27.3** in the root, `gcpevents`, `promevents` and vendored OBI modules, for GO-2026-6809 (`crypto/x509`), reachable from the join certificate check.

- **An agent with no Service index names the callee of a client span from the services it has read.** With no Kubernetes watch (OTLP-only, ADR-0021) every topology edge ended at an external host, even `frontend -> cart`. A client span's `server.address` of the form `name`, `name:port`, `name.<namespace>`, `name.<namespace>.svc` or `name.<namespace>.svc.<domain>` now resolves to the in-scope workload when that service has been read as a request destination, also when the resolver of an agent without a Service index left it external; an IP, an external host or a name nobody serves stays external. (`TestOBI_Collector_NoResolverWired_*`)
- **gRPC spans of the current semantic conventions were counted as HTTP.** A span names its system `rpc.system.name` (and its status `rpc.response.status_code`) in newer SDKs; the receiver only recognised `rpc.system` and `rpc.grpc.status_code`, so such a server span became an `http.server.request.duration` point beside the service's own `rpc.server.call.duration`. (`TestTraces_GRPCSpansOfTheCurrentConventionsBecomeRPCHistograms`)

### Changed

- **Chart: the RED, USE, LOG and topology collectors are on by default.** They run in the controller and read whatever source the agent has, so each one runs when `core.host` is set and one of its sources is: OTLP (`otlp.enabled`) for all four, or the `ebpf` role (RED, topology), the node-exporter role (USE) or the log-exporter role (LOG). An OTLP-only install needs no collector values, and the chart no longer fails a render for a collector with no source. The node-exporter sensor role is now opt-in like `ebpf.enabled`: `useMetrics.nodeExporter` defaults to `false`. Set a collector's `enabled: false` to turn it off.
- **BREAKING:** the agent flags `--push-heartbeat`, `--push-insecure-skip-verify` and `--push-allow-plaintext` are now `--core-heartbeat`, `--core-insecure-skip-verify` and `--core-allow-plaintext`, matching the chart's `core.*` values. Host installs that set the old names in their env file must rename them.
- **Chart: an OTLP-only agent takes its tenant from `core.tenants`.** With `kubernetes.watch: false`, no `target.tenant` and exactly one entry in `core.tenants`, that entry is the tenant every frame files under, so a key install (core ADR-090, key kind `otlp`) needs no `target.*` value.
- **Chart: `push.*` is now `core.*`.** The block is the agent's connection to the core, not a push mode. `core.host` (or `core.endpoint`) turns it on, so there is no `enabled`; the `push.join.*` values move up one level (`core.caFingerprint`, `core.tenants`, `core.storage`, `core.existingClaim`) and `push.join.enrollKey` is `core.agentKey`. An install that still sets `push.enabled=true` renders as before: the chart reads `push.*` wherever `core.*` is empty.

### Added

- **One-time agent keys (core ADR-090).** A join carries the key in `enroll.key` of the join store as `enrollKey`, and the core issues the certificate at once with no admin step. The agent removes the key once the certificate is issued and keeps it when the core refuses it. Chart `core.agentKey` writes it into the join Secret (needs `core.storage: secret`). `packaging/install.sh`, stamped with the version and published with each release, installs a Linux host from the command the core returns: it checks and installs the deb or rpm, writes the env file and the key, and starts the service. (`TestJoin_EnrollKeyIssuedAtOnceAndRemoved`, `TestJoin_BadEnrollKeyRefusedAndKept`)

- **`--leader-election-id`** (the chart passes the release's full name): the Lease the replicas elect on was fixed to `triage-agent`, so two releases in one namespace shared one leader. A release named `triage-agent` keeps its Lease.
- **An agent that watches no Kubernetes objects** (`--kubernetes=false`, chart `kubernetes.watch: false`, ADR-0021). For an agent fed only by OTLP: no Namespace, Service, Ingress, workload or Event informers, no virtual apps, no owner reads, and no ClusterRole or ClusterRoleBinding, so it installs with namespace-scoped rights. Needs `--target-tenant` (`target.tenant`), since there is no annotation to read a tenant from; an empty `--target-namespaces` then means every namespace the authenticated senders report. Applications come from the `k8s_cluster` workload metrics a Collector sends over OTLP (desired and ready pods per Deployment, StatefulSet and DaemonSet), else from the services the senders report. The TokenReview ClusterRole is now rendered only when a token authenticates something (the eBPF roles or `otlp.allowedServiceAccounts`). (`TestNoKubernetes_*`, `TestNoIdentityResolver_ResolvesNothing`)
- **The agent is its own client CA for remote OTLP senders** (`otlp.ownCA`). The controller creates a CA on first start in the Secret `<release>-otlp-client-ca` and trusts it for mutual TLS. `triage-agent otlp-cert --tenant … --cn …`, run in the controller pod, issues a sender's certificate (tenant URI SANs) and prints a Secret manifest for the sender, with the agent's serving certificate to pin.
- Chart value `otlp.extraNames`: host names or IP addresses, beyond the Service's own, that the serving certificate covers, so a sender outside the cluster can verify the OTLP endpoint by its own address. The serving certificate now accepts IP addresses (IP SANs) and is reissued when the list changes.
- **Tenant client certificates for remote OTLP senders.** A Collector outside the cluster authenticates with a client certificate (`otlp.clientCASecret`) whose URI SANs name its tenants, `spiffe://<domain>/tenant/<name>`, one or more. The receiver places each resource in one of them (the `triage.tenant` resource attribute picks when the certificate names several), refuses a tenant the certificate does not name or the agent did not join the core with (`--join-tenant`, `--target-tenant`), and stores every namespace as `<tenant>~<namespace>` so two tenants with a `default` namespace never merge. The push layer routes that name to the tenant's connection, keeps the connection active while data arrives, queues the tenant's snapshot sequence before its first data, and strips the prefix from every frame and status, so the core gets the tenant stamp and the sender's own namespace. A remote tenant's client spans resolve their destination from the host name alone (a bare name, or `<svc>.<ns>.svc[...]`); any other host is external. A source with no namespace is not forwarded. Additive: a certificate with no tenant URI, and every token sender, behave as before. (`TestTenant_*`, `TestRemoteTenant_*`, `TestK8sResolver_RemoteTenant`)

### Fixed

- **An OTLP-only agent reported every call as external, so the core drew no edges.** With `--kubernetes=false` no destination resolver was wired. Such an agent now names a remote sender's destinations by host name (a bare Service name, or `<svc>.<ns>.svc...`), as a cluster agent already did for remote senders, and only the rest is external.
- **USE: a pod no service could be named for is dropped, not sent under an empty service.** Kubelet cAdvisor series from an OTLP sender carry a namespace and a pod but no service, and an OTLP-only agent has no Kubernetes watch to find the pod's workload. Such samples are now counted as `unattributed` in the source ledger instead of reaching the core as a nameless service.
- **Virtual apps from a remote OTLP sender carried the internal `<tenant>~<namespace>` name to the core.** Metric and topology frames dropped the prefix, but snapshot and app_delta frames did not, so the core and its UI showed the workspace ID as the namespace. App frames now leave the agent with the plain namespace; the agent's own state keeps the qualified one.
- **Log exporter missed coloured levels.** A logger that colours its level (zerolog's console writer prints `ESC[33mWRN ESC[0m`) left no word boundary before the level, so none of the core's own warnings were counted. The classifier now strips ANSI colour codes first.
- **Log signals named the wrong service.** The log exporter named a pod's service by its `app.kubernetes.io/name` or `app` label, so a DaemonSet whose label differs from its name (`ebpf` against `triage-agent-ebpf`) produced log signals for a service the core's graph does not have, and the core dropped them as unknown. It now names the service as the USE and RED signals do: the `triageagent.dev/virtual-app-group` annotation, else the owning workload (a Deployment behind its ReplicaSet), else the labels.
- **Log exporter missed common log formats.** The default regexes matched `ERROR` and `WARN` but not the `ERR`, `FTL`, `PNC` and `WRN` that zerolog's console writer prints, so a service logging that way (the core, for one) never produced a log signal, however many errors it logged. They now also cover zap, zerolog JSON, logrus, slog, go-kit, klog, pino/bunyan, Serilog, the .NET console logger, nginx, envoy, Python and Java levels, upper-case and spaced JSON levels, and the `severity`, `levelname` and `log.level` keys.

### Fixed

- **A noisy tenant no longer empties a quiet tenant's events.** The TUI kept one buffer of the newest 100 events for all tenants, so a tenant raising an event a minute (a latency window per service) pushed every other tenant's out and its signals read zero. The limit now applies per tenant.

### Added

- **Destination port on topology edges.** A topology edge now carries `dstPort`, the port of the client span's server address (and of an L4 flow row that carries a `dst.port` label; 0 and omitted otherwise). The port is parsed before the host is stripped and is part of the edge's identity, so the same destination on two ports is two edges. Additive; older cores ignore it. (`TestTopologyEdge_DstPort`)
- **Up to three log samples per service.** `log_metrics` entries gain `samples`, up to three distinct sample lines for the service in the window, the fastest-growing error series first. Every sample is ANSI-stripped before it is truncated to 256 characters, including the existing `sample`, which is filled exactly as before. Additive; older cores read `sample`. (`TestPickSamples_TopThreeDistinct`)
- **Signal-path counters per sensor.** Every collector (eBPF RED, USE, logs) and the OTLP receiver now counts, per sensor node or OTLP sender, what it did with the data: reads, reads that found no snapshot, data points, services, entries outside the namespace scope and points that named no workload; and per role the services read, held back below the gate, cut by a frame's cap and sent to the core. The numbers are in the sources API (`ledger`, `roleLedger`), in the TUI's sources panel under the selected source, and as `triage_agent_signal_path_total{role,source,outcome}`, so an empty event list can be told from data that was read and skipped.

- **ServiceAccount and RBAC auth for the monitoring API (cluster mode).** `--api-sa-auth` (chart `api.serviceAccountAuth`) accepts ServiceAccount tokens with audience `triage-agent-api`: the agent runs a TokenReview, then a SubjectAccessReview for `get` on `agent-api.triageagent.dev`. Granted cluster-wide (ClusterRoleBinding of the chart's `-api-reader` ClusterRole) the caller sees every tenant; granted in a namespace (RoleBinding) it sees only that namespace's `triageagent.dev/tenant`, and a namespace without the annotation is refused. Outcomes are cached for 5 minutes (30 seconds when refused). The token file stays for callers outside the cluster and for host mode, where `--api-sa-auth` is refused; either source alone is enough.
- **MCP server and OpenAPI spec on the agent API, both off by default.** `--api-mcp` (chart `api.mcp`) and `--api-docs` (chart `api.docs`) switch them on next to the monitoring API. `POST /mcp` speaks the Model Context Protocol (streamable HTTP, JSON-RPC) with seven read-only tools that mirror the JSON API (status, tenants, apps, services, graph, sources, events) under the same bearer token and tenant scope; a tenant-bound token cannot ask for another tenant. `GET /api/v1/openapi.json` serves the OpenAPI 3 description and `GET /api/v1/docs` a Swagger UI page; both are static and need no token, the calls they make do. The discovery document lists them.
- **One long request no longer reads as a latency breach.** A p99 over fewer than 100 requests is the slowest one, so a single stream, watch or long poll that closed in a window set the service's p99 to the top bucket (10 s) and raised a latency signal about every 6 minutes. The latency arm now judges and learns a window only when at least two requests share the p99 bucket or a slower one.
- **Slowest route in the latency sample.** A latency breach now ends with `; slowest: <method route> p99 <ms> over <n> req`, the operation with the highest p99 in the window, so a service-wide p99 at the top histogram bucket (a request that outlived 10 s, such as a long-lived stream) names the route that caused it. Empty when no operation carried a duration histogram.
- **Standalone mode.** With no `--triage-endpoint` and `--ui-listen` or `--api-listen` set, the agent (cluster and host mode) runs without a core: it resolves apps, tenants and events for the local TUI and monitoring API and never dials out. The TUI shows `standalone`. The chart emits the UI and API flags with `push.enabled=false`; host mode defaults the tenant to `local`.
- **Sensor settings in the TUI.** A sensor now sends its flags (secrets left out) in the stream Hello, and the TUI's `?` view shows them next to the controller's, grouped by roles, version and settings. A new `Setting` message and `Hello.settings` field in `sensor.proto`; older sensors send none and are shown as such.

- **Monitoring API (`--api-listen`).** An optional, token-only listener with
  Prometheus `/metrics` of the derived signals (service RED and USE, app
  readiness, edges, sources, link), `/.well-known/triage-agent` and the JSON
  API, for Prometheus and Grafana. Off by default; it refuses to start without
  `--api-tokens-file` (`name tenant token` lines; a tenant-bound token is
  limited to its tenant by the agent). Only the leader exports signals. Chart
  `api.*`, optional ServiceMonitor. See ADR-0020.

- **Local TUI (`--ui-listen`).** The agent serves a read-only TUI on a
  loopback address: panel 1 shows the virtual apps with their traffic and the
  dependency graph, panel 2 the sources (k8s, eBPF, node-exporter,
  log-exporter, OTLP) beside the events they raise. Off by default; reach it
  with `kubectl port-forward`. Chart value `ui.listen`. `--ui-max-events` (default 100) sets how many of the newest events the TUI and the API keep and show. With replica gossip on, every replica serves the leader's view. Every endpoint takes `?tenant=` and `?` shows the flags in effect and the agent's statistics. Login with an admin key (`--ui-admin-key-file`, chart `ui.adminKey.existingSecret`; host mode may use `--ui-admin-key` or `TRIAGE_AGENT_UI_ADMIN_KEY`). See ADR-0019.

### Security

- **Join challenges must be bound; the TLS channel is bound too.** The agent
  refuses a challenge that does not echo its `X-Triage-Agent-Nonce` (needs a
  core that binds, every core from v3.2.367 on). When the core sets
  `channelBound`, the challenge also signs 32 bytes of TLS exporter keying
  material (label `EXPORTER-triage-agent-join`), so an active relay is
  refused; without it, `--push-insecure-skip-verify` is refused.

- **promevents reads its credentials from files only.** `WEBHOOK_TOKEN` and
  `TRIAGE_TOKEN` are no longer read; `WEBHOOK_TOKEN_FILE` (or
  `WEBHOOK_INSECURE=true`) and `TRIAGE_TOKEN_FILE` are the only sources, as
  the shipped manifest already configures them. A literal token in the
  environment is ignored and the bridge refuses to start without the file.

- **gcpevents and promevents dependencies.** Both modules move to Go 1.27.2;
  gcpevents takes `golang.org/x/net` 0.60.0 and `golang.org/x/crypto` 0.57.0
  with the matching `x/sync`, `x/sys` and `x/text`. govulncheck there goes
  from 9 reachable advisories to none.
- **OTLP receiver.** A token or SPIFFE sender can no longer publish under
  another namespace by setting `k8s.namespace.name` on a data point (or twice
  on the resource): such points are dropped and counted as refusals with the
  reason `namespace <x> is not the sender's namespace`. Forwarders are
  unchanged.
- **Vendored OBI.** `third_party/obi/CHECKSUM` records a SHA-256 of the
  tracked tree; `make obi-verify` (run by `make lint` and CI) fails when the
  fork changes without it, and `make obi-checksum` updates it. The bpf2go
  generator image is pinned by digest.
- **gcpevents.** A decode failure logs only the Pub/Sub message id and the
  error, never the payload. The core token is read from Secret Manager
  (`TRIAGE_TOKEN_SECRET`) with the function's own service account, or from a
  mounted file (`TRIAGE_TOKEN_FILE`); the plain `TRIAGE_TOKEN` variable is no
  longer read. The deploy commands use a dedicated service account.

- **BREAKING for in-cluster installs that used the push token.** An agent in
  Kubernetes now authenticates to the core only by its join certificate. The
  `--push-token` flag, the chart values `push.token` and `push.existingSecret`
  and the chart's token Secret are removed; `push.join.caFingerprint` is
  required when `push.enabled`, and the binary exits at startup in a pod
  without `--join-ca-fingerprint`. `--push-token-file` stays for agents
  outside Kubernetes (Linux host mode, or no in-cluster config). To upgrade,
  set `push.join.caFingerprint` from `triagectl agent list` on the core,
  upgrade, then run `triagectl agent approve <cluster> --fingerprint
  <agent fingerprint>` with the fingerprint from the agent log.
- **Dependencies.** Go 1.27.2; `golang.org/x/net` 0.60.0 and
  `golang.org/x/crypto` 0.57.0 (0.57.0 is the floor x/net 0.60.0 requires)
  in the agent and in the vendored OBI module, which also takes
  `otel/sdk/log` and `otlploggrpc` 0.21.0. Closes the open advisories
  against the earlier versions.
- **Release supply chain.** Every third-party action in the CI workflow is
  pinned to a commit SHA. The workflow defaults to read-only permissions and
  each job grants only what it needs. Release artifacts are signed keyless
  with cosign (a `.sigstore.json` bundle per file) and the image is signed by
  digest; both carry build-provenance attestations. A failing nfpm or package
  build now fails the release instead of shipping without packages. See
  "Verifying a release" in the README.
- **Chart RBAC.** Dropped the `endpoints` rule from the ClusterRole; nothing
  reads Endpoints.

### Fixed

- The host unit is sandboxed further (`PrivateTmp`, `ProtectControlGroups`,
  `ProtectKernelModules`, `RestrictAddressFamilies`, `SystemCallFilter`,
  `MemoryMax=1G`), and `--host-log-files` matches are opened with
  `O_NOFOLLOW`: a symlink or other non-regular file is refused and logged
  once per path.

- The OTLP receiver caches TokenReview outcomes by token hash (5 minutes
  for a success, 30 seconds for a refusal, 10000 entries, oldest evicted
  first), so a sender's requests no longer cost one API call each.

- The OTLP listener requires TLS 1.3, and a client certificate becomes
  mandatory when `otlp.clientCASecret` is set without
  `otlp.allowedServiceAccounts`; with both it stays optional.

- The OTLP receiver's memory bounds are smaller and configurable: 2000
  resources, 5000 series per resource, 500 resources per sender and 4 MiB
  per request by default (`--otlp-max-resources`,
  `--otlp-max-points-per-resource`, `--otlp-max-resources-per-sender`,
  `--otlp-max-request-bytes`; chart `otlp.limits`). A sender past its quota
  is refused with `sender resource quota reached`.

- The OTLP receiver now binds the namespace of a point to its sender: a
  token sender (or a client certificate with a SPIFFE URI SAN) may publish
  only for its own namespace, a missing `k8s.namespace.name` is filled in,
  and refusals are counted per sender. `--otlp-forwarders` /
  `otlp.forwarders` name the Collector gateways that may publish for any
  namespace; a client certificate without a SPIFFE namespace is a forwarder.

- `make packages` produced no deb or rpm: nfpm expands `${BINARY}` in a
  content entry only when the entry sets `expand: true`. The CI step was
  best effort, so the release carried the bare binaries but no packages.

### Added

- **Host mode defaults** (ADR-0018). The default unit selection skips
  `Type=oneshot` and `Type=idle` services (both run once and exit; `idle`
  only waits for the rest of boot first, as `dmesg` does), and the default cluster
  name is the short host name, not the fully qualified one a cloud VM
  reports.

- **Packaging for Linux host mode** (ADR-0018 phase 6). A tag build now
  also uploads `triage-agent_linux_amd64` and `triage-agent_linux_arm64`,
  built with `-tags obi`, with their SHA-256 sums, and (best effort) a deb
  and rpm of each. `packaging/systemd/triage-agent.service` runs
  `--mode=host` as root with the DaemonSet's six capabilities,
  `NoNewPrivileges`, `ProtectSystem=strict` and `ProtectHome`, writing only
  the join state directory; flags come from `/etc/triage-agent/env`
  (`packaging/systemd/env.example`). In host mode `--join-state-dir` now
  defaults to `/var/lib/triage-agent/join`. `make host-binaries` and `make
  packages` build the same files locally. `docs/spec.html` and the README
  describe the host install.

- **RED and topology from OBI in host mode** (ADR-0018 phase 5). In a build
  with `-tags obi`, `--mode=host` runs the embedded OBI over the processes of
  the selected units (read from each unit's cgroup, no Kubernetes metadata)
  and restarts it when a unit gets a process it does not know. Each series
  is stamped `k8s.namespace.name=<host-namespace>` and `service.name=<unit>`
  before the RED collector reads it, so `red_metrics`, `red_summary` and
  `topology_snapshot` work as in a cluster; a call to another host stays an
  external destination. `OBI_NETWORK_FLOWS=true` adds the L4 flow metric. A
  build without the tag logs it and sends no RED.

- **LOG from journald in host mode** (ADR-0018 phase 4). The host runtime
  follows the journal of the selected units with `journalctl --follow
  --output=json --since=now` (restarted with backoff, and with a new match
  list when the unit set changes) and counts lines per unit into the same
  `log.errors`/`log.warns`/`log.lines` series the cluster log exporter
  emits. A `PRIORITY` of 0 to 3 is an error and 4 a warning; other entries
  use the existing regex classifier, and redaction and sampling are
  unchanged. `--host-log-files=<unit>=<glob>` tails plain files of a service
  under its unit name. The followed units are the ones systemd discovery
  selects, so LOG, USE and the virtual apps always describe the same set.

- **USE from unit cgroups in host mode** (ADR-0018 phase 3). `--mode=host`
  reads each selected unit's cgroup under
  `<cgroup-root>/system.slice/<unit>.service` and sends the same CPU, memory
  and PSI series the cluster node exporter does, labelled with the host
  namespace and the unit name as both service and pod. Limits come from the
  unit's own `cpu.max` and `memory.max`; an unlimited unit reports usage
  against the node only. The units are the ones systemd discovery selects
  (`--host-units` globs, or every enabled service outside a denylist of
  system services).

- **Units as virtual apps** (ADR-0018 phase 2). In host mode each systemd
  service selected by `--host-units` (default: every enabled service except
  a built-in denylist) is one virtual app in the host namespace, named by
  the unit, ready while `active (running)`, and a restart changes the app.
  Discovery uses systemd D-Bus with a change subscription and a 10 s poll.
  `push.Pusher.SetAppSource` is the new seam a host Pusher takes workloads
  through; the cluster path is unchanged.

- **Linux host mode skeleton** (ADR-0018 phases 0 and 1). `--mode=host`
  runs the agent as one process on a plain Linux host with no Kubernetes
  API, no controller-runtime manager and no sensor stream listener. It joins
  the core as a cluster of one (the host name, or `--cluster-name`) under
  `--target-tenant`, with one synthetic namespace (`--host-namespace`,
  default `host`), and sends `hello` (with the `host` capability),
  heartbeat and `sensor_state`. Units, USE, LOG and RED follow in later
  phases. The collectors now take an identity resolver in place of the
  manager, and `push.NewHost` and `sensorstream.Local` are the host's
  transport and in-process sensor source; the cluster path is unchanged.

- **More than one controller replica with the sensor stream** (ADR-0015
  phase 4). Every replica takes sensor streams and gossips what its own
  sensors send to the others (`--mesh-listen`, `--mesh-service`, headless
  Service `<fullname>-peers`, port 9471), so the leader sees every node
  whichever replica a sensor connected to, and a newly elected leader
  already holds them. Per node, the newest record wins and a sensor
  connected to a replica is never overwritten by a peer. The replicas pin
  the sensor stream's own certificate both ways; there is no new
  credential. The chart drops the one-replica limit; above one replica it
  turns leader election on and adds a PodDisruptionBudget.

- **OTLP metrics receiver** (ADR-0017). The controller accepts OTLP over
  gRPC from OpenTelemetry SDKs and Collectors (`--otlp-listen`, chart
  `otlp.*`, port 4317 on the `<fullname>-sensors` Service) and feeds the
  RED, topology, USE and log collectors by metric name, so a cluster
  without the eBPF DaemonSet still reports. Older semantic conventions are
  converted; delta temporality and exponential histograms are refused.
  Senders authenticate with mutual TLS against `otlp.clientCASecret` or a
  projected ServiceAccount token (audience `triage-agent-otlp`). Every
  replica receives and gossips, so the leader sees every source.

### Security

- **The join challenge is bound to the connection.** The agent sends a fresh
  random nonce in `X-Triage-Agent-Nonce` and requires the core to echo and sign
  it, so a recorded challenge cannot be replayed.

- **The certificate the core issues is validated before it is saved.** The
  agent parses it and requires that it chains to the trusted CA, is currently
  valid, carries the agent's key and names exactly the requested
  `triage:cluster/` and `triage:tenant/` identities. During a CA rotation the
  cross certificate must also be a CA certificate with the certificate-signing
  key usage and a validity window containing now.

- **Secrets and PII are redacted from `log.sample`.** URI credentials, JWTs,
  Bearer and Basic credentials, secret-named keys, AWS access key IDs and
  email addresses become `***` before the exemplar is truncated. Restores
  the ADR-0007 URI scrub.
- **The eBPF DaemonSet's root baseline is an accepted risk** (ADR-0016), with
  rules for when `SYS_ADMIN`, `SYS_RESOURCE` and `ebpf.privileged: true` are
  allowed. The chart README documents both privilege modes.

### Changed

- **Sensors stream OTLP to the controller over gRPC** (ADR-0015 phase 1).
  The DaemonSet's roles (ebpf, node-exporter, log-exporter) no longer serve
  Prometheus endpoints: each sensor pod keeps one gRPC stream to the
  controller (`--sensor-controller`, Service `<fullname>-sensors`, port
  9470) and sends each role's metrics as OTLP every `--sensor-tick`, plus
  its own state. The controller (`--sensor-listen`) manages its own
  self-signed TLS certificate, publishes it for the sensors to pin, and
  accepts only the sensors' projected ServiceAccount tokens through a
  TokenReview. The RED, USE and log collectors read from the stream.
  Metric names and attributes follow OpenTelemetry (`http.server.request.duration`,
  `k8s.namespace.name`, `use.mem.pct`, `log.errors`). The chart refused
  `replicaCount` above 1 while the stream was on, until phase 4 (Added,
  above).

- **One image instead of two.** `.ko.yaml` now builds with `-tags obi`, so
  the controller Deployment and the eBPF DaemonSet run the same image.
  There is no plain image anymore.

### Removed

- **Prometheus scraping.** The sensor ports 9464, 9465 and 9466 and the
  `triage-ebpf`, `triage-node` and `triage-log` Services; the metrics bearer
  token (`metricsauth`, `metricsAuth` values, `<fullname>-metrics-token`);
  the Hubble RED profile (`--red-source`, `--hubble-*`, `redSource`);
  scraping an external Prometheus log source; and the scrape flags and
  values (`--*-metrics-service-namespace`, `--*-metrics-service-name`,
  `--*-metrics-port`, `--red-metrics-metric-name`, the `--*-label` flags,
  `--node-exporter-port`, `--log-exporter-port`).

- **The plain (non-`-tags obi`) image, and everything that built it only for
  the eBPF DaemonSet to run separately.** The Makefile's `ko-image-obi`,
  `ko-push-obi` and `OBI_VER`, and `.ko.obi.yaml`. The chart's
  `ebpf.image.repository`/`ebpf.image.tag`/`ebpf.image.pullPolicy` values
  and the `triage-agent.ebpfImage` helper (the DaemonSet now renders
  `image.repository`/`image.tag` directly, like the controller).

- **HMAC push authentication.** The agent authenticates to the core only with
  its join certificate. `--push-token-file`, the pre-upgrade signature headers
  on the WebSocket upgrade and the token hello are gone, and so is the Linux
  host fallback to an HMAC key. `--join-ca-fingerprint` is now required
  whenever a core endpoint is set (`--triage-endpoint`), host mode included;
  the process exits at startup without it. A host that ran on a token file
  joins with a one-time key (`packaging/install.sh`) or an admin approval.

### Fixed

- **A start or restart during an outage no longer learns the outage as
  normal.** The baseline gates keep their baselines in memory, so every key
  is cold after a restart, and a cold key learned whatever it read: an agent
  started during a 10 s p99 outage, or a 15% error outage, reported nothing
  and took it for the baseline. A cold key is now judged against a cold floor
  (`--red-baseline-err-cold-floor-pct`, default 15;
  `--red-baseline-lat-cold-floor-ms`, default 5000; throttling 50%): a reading
  at or above it is reported and not learned, one under it is learned as
  before. The error cold floor was 20%. An operation at or above a cold floor
  by nature keeps firing until the floor is raised. CPU and logs get the same
  arm, off by default (`--use-baseline-cpu-cold-floor-cores`,
  `--log-baseline-cold-floor-lines`), because no default is safe for every
  service. Chart: `redMetrics.baseline.errColdFloorPct`, `latColdFloorMs`,
  `useMetrics.baseline.cpuColdFloorCores`, `logMetrics.baseline.coldFloorLines`.

- **A chronically breaching key is muted explicitly and keeps reporting.** A
  key whose breaches recur was silenced for good, with nothing to show for
  it: a healthy service that then failed every third interval fired six times
  and went quiet, and a sustained fault on an already chronic key was silent
  for 15 minutes. The collector now logs when a key becomes muted and when the
  mute ends, the gauge `triage_agent_baseline_muted_keys{sensor,arm}` counts
  the muted keys, and a muted key that is still breaching reports every 10
  intervals with the new `stillBreaching` field (additive, `omitempty`; the
  core's mirror type needs it too). A breaching p99 on fewer than
  `--red-baseline-lat-chronic-min-requests` requests (default 100) is still
  reported but no longer counts toward the mute, because a p99 over a handful
  of requests is their maximum: a low-traffic operation with an occasional
  slow request reports each one again.

- **An operation under about 4 requests a minute is judged.** It never
  reached the 20 requests in 5 intervals an error verdict needs, so one at 3
  requests a minute failing every request for half an hour sent nothing. It
  now qualifies with `--red-baseline-err-sparse-min-errors` errors (default 5)
  and a failure share of `--red-baseline-err-sparse-rate-pct` (default 50)
  over those intervals. `--red-baseline-err-min-requests`,
  `--red-baseline-err-min-errors`, `--red-baseline-lat-min-requests` and
  `--red-baseline-min-baseline-windows` are now flags and chart values
  (`redMetrics.baseline.*`).

- **Errors on a newly appearing series are counted.** A series seen for the
  first time always yielded no delta, so a burst of 30 errors on a route with
  no earlier 5xx series sent no frame, and every fault on a new series lost
  its first interval. Only the first read of a sensor's metric is baseline
  only now; a series that appears later counts from zero.

- **The CPU arm no longer fires after a sensor gap.** The usage delta was
  divided by the collector's interval even when it spanned several, so a
  steady 0.20-core service read as 0.80 cores after three reads without fresh
  data. It is divided by the real time between the two snapshots, and a read
  with no fresh snapshot is skipped.

- **Tracked operations are capped per service.** Every distinct route became
  a baseline key with no limit. At most `--red-baseline-max-operations`
  operations per service (default 100, `redMetrics.baseline.maxOperations`)
  are tracked one by one; the rest are judged together as
  `(other operations)`. `operation` is clipped to 128 bytes on the wire. The
  route matcher is unchanged; its comment no longer claims it bounds
  cardinality.

- **The RED frame cap keeps latency and baseline breaches.** With more than
  100 qualifying services the cut ranked by error count alone, so entries with
  no errors were dropped first, in map order. Breaches now rank first, then
  the error count, then namespace and service.

- **An unreadable gRPC status is unknown, not an error.** A sensor that
  starts after a connection opened never sees the HPACK table entries the
  server added, and long-lived connections outlive every sensor restart. A
  header block that ends the stream and names such an entry was reported as
  gRPC UNKNOWN, which counts as a server error: every successful trailers-only
  response, and every bodyless plain HTTP/2 response (a 304) on a cleartext
  connection, was an error until the connection recycled. Such a response is
  now typed gRPC only when something readable says so (a gRPC content-type or
  a `grpc-*` header, on the stream or earlier on the connection), and then
  carries no status code (the attribute is omitted), so it is counted as a
  call but not as an error; otherwise it is HTTP with its `:status`. A
  trailers-only error whose `grpc-status` is readable still reports its code
  (vendored OBI, `http2grpc_transform.go`).

- **A gRPC error on a connection older than the sensor is told apart from a
  success.** The decoder cannot read a table entry it never saw, but it can
  say which entry a block named, and an entry keeps that identity while later
  insertions move its index. Trailers that follow a response message are how a
  call succeeds, so after two such blocks in a row name the same entries,
  those entries are "status OK" for the connection. A trailers-only response
  that names all of them is OK (an empty stream); one that names other
  entries, outside those the response headers use, is an error and reports
  UNKNOWN. A status the server sends for the first time after the sensor
  attached arrives as a literal and keeps its code, on that response and on
  every repeat. Nothing is inferred before successful trailers were seen, or
  after a header block the sensor did not capture whole, so a connection that
  only ever answers trailers-only still reports no status (vendored OBI,
  `h2ResponseSlots`, `bhpack.SlotEvent`).

- **The fixed latency arm no longer learns a slowdown at start.** It uses the
  baseline arms' latency cold floor (`--red-baseline-lat-cold-floor-ms`) while
  it has fewer than `--red-latency-min-baseline-windows` readings: a p99 at or
  above it is reported and not added to the baseline.

- **`baseline` on the wire is always the median.** For a chronically
  breaching key the agent sent the higher reference it judged against (the
  90th percentile of its readings) in `baseline`, which the protocol documents
  as the median, so the core rendered "2000ms vs baseline 2000ms". The median
  is sent; the reference is logged.

- **A warn log step says it is one.** A `log-step` entry for the warn stream
  carried warn lines in `observed` and `baseline` with nothing to tell them
  from error lines. `ServiceLogMetrics` gains `stream` (`error` or `warn`,
  additive, `omitempty`; the core's mirror type needs it too).

- **promevents: the webhook requires a bearer token.** `/alertmanager` was
  open, so any pod that could reach the Service could post events into any
  mapped tenant with the bridge's own credentials. It needs
  `WEBHOOK_TOKEN_FILE` or `WEBHOOK_TOKEN` (compared in constant time;
  Alertmanager sends it through `http_config.authorization`) and refuses to
  start without one unless `WEBHOOK_INSECURE=true`. `deploy/promevents.yaml`
  mounts it from the Secret `triage-promevents-webhook`, which must exist.

- **promevents: a busy core is retried, not dropped.** Every 4xx was treated
  as permanent, but the core answers 429 when its queue is full, so those
  alerts were dropped and acknowledged. 429 and 408 are retried and then
  answered with 502, so Alertmanager retries.

- **promevents: a group retry resends only what failed.** One failed alert
  answers 502 for the whole group, and Alertmanager then sends all of it
  again. An alert already delivered is not sent again within `DEDUP_WINDOW`
  (default 2m, keep it under `repeat_interval`), by fingerprint, status and
  start time.

- **promevents: alerts are attributed to the workload, not the scraper.**
  `container` outranked `deployment`, so an alert from a scraped series was
  attributed to the exporter's container, and `service`/`job` attributed
  anything else to the scraped Service. The order is now `deployment`,
  `statefulset`, `daemonset`, `workload`, `service_name`, `job_name`, the
  owner derived from `pod`, `container`, then `service` and `job`, and those
  two are not used on an alert with no `namespace` label: it is left
  unattributed.

- **CI runs the tests.** The root job ran vet and build only. It now runs
  `go test -race ./...` and the vendored OBI HTTP/2 package's tests, and a
  `promevents` job vets, builds and tests that module.

- **Go services report RED metrics.** Every Go uprobe was refused on GKE
  COS 6.12 ("creating perf_uprobe PMU ... permission denied"), so OBI's Go
  tracer attached nothing and Go services (gRPC and net/http) reported no
  requests, errors or latency. The eBPF container needs `SYS_ADMIN`
  (`ebpf.extraCapabilities`); an unconfined AppArmor profile alone does not
  lift it. `ebpf.privileged: true` now renders a privileged container: the
  template had hardcoded `privileged: false`.

- **gRPC server errors are no longer read as OK.** A server's trailers
  arrive after the kernel already ended the span at the first DATA frame,
  labelled as request headers, and were decoded with the request HPACK
  decoder. Both decoders fell out of step: a later error's `grpc-status`,
  named through the table entry those trailers added, decoded as nothing and
  `:status 200` made it OK (payment failing 45% read as all OK), and request
  paths turned into `*`. Such blocks now go to the response decoder (vendored
  OBI, `readHTTP2HeaderEvent`). The kernel's HTTP/2 response capture grows
  from 64 to 256 bytes: a grpc-js trailers-only error with its
  `grpc-message` ran past 64, and the cut left the response decoder
  unreliable for the rest of the connection.


- **OBI's `log_level` applies.** The embedded instrumenter logs through the
  default slog logger and only OBI's own main applied `log_level`, so OBI's
  debug lines (why a probe did not attach) could never be turned on.

- **HTTP errors are counted from eBPF.** The OBI profile read the HTTP status
  from `http.status_code`; OBI exports `http.response.status_code`, so every
  request read as a success and no service ever showed an HTTP error.

- **A refused agent certificate no longer locks the agent out.** When the
  core closes a certificate session as revoked, expired or no longer
  approved, the agent drops the certificate and asks to join again with the
  same key, so it shows up as pending for approval. Before, it kept retrying
  the refused certificate at maximum backoff and logged "check token".

### Added

- **`triageagent.dev/triage: "false"` takes a workload out of triage.** On a
  workload's own metadata, its pod template, or its Namespace, the agent
  then sends the core nothing about it: no virtual app, no Warning events
  for its pods or owners, no log/RED/USE entries (OOM kills included), no
  topology nodes or edges. On a pod template it also keeps eBPF from
  instrumenting the pods; on a Namespace it removes the namespace from the
  eBPF and log-exporter scope. A Namespace `"false"` is not overridden by a
  workload `"true"`, the same rule as `triageagent.dev/virtual-app`.

- **promevents: Prometheus alerts into triage as events.** A separate
  module and Deployment (`promevents/`), the same pattern as `gcpevents`:
  Alertmanager's webhook in, one event per alert out to the core's events
  endpoint, with the workload resolved from the alert's labels, the tenant
  from its namespace, and auth by projected ServiceAccount token.

- **Agent join by admin-approved certificate request.** With
  `--join-ca-fingerprint` (chart `push.join.caFingerprint`) the agent checks
  the core's challenge against that CA fingerprint, sends a CSR for its
  cluster and tenants (`--join-tenant`, default `--target-tenant`), logs
  `join pending` with its key fingerprint until an admin runs `triagectl
  agent approve`, then opens every session with its certificate and renews
  it before expiry. No HMAC push token is needed in this mode. The key,
  certificate and saved core CA live in a Secret in the agent's namespace
  (`--join-secret`, chart default `push.join.storage: secret`), so a new pod
  needs no new approval; the chart creates it empty and grants `get` and
  `update` on that Secret only. `storage: emptyDir` or `pvc` keep them in
  `--join-state-dir` instead. Core side: core `docs/todo/agent-join.md`.

- **The controller reports each sensor's own health to the core** (ADR-0015
  phase 1). Every 60 seconds, when push is enabled, the controller sends a
  `sensor_state` frame carrying every sensor's goroutines, memory, gRPC send
  and ack timings and per-role gather stats, mapped from the sensor hub's
  own view (`sensorstream.Hub.Nodes()`). The frame carries every sensor's
  health, not any tenant's data, so it is sent unstamped once per
  connection, on the heartbeat tick, like the heartbeat frame itself.

### Changed

- **The Helm chart lives in this repo.** `charts/triage-agent` (0.21.0) is the
  install path. `make deploy` uses it by default.

## [v3.0.0] - 2026-09-20

This release is not compatible with a v2 install. Annotations, flags, the
module path and the eBPF engine changed. The push frame JSON is unchanged, so
a v3 agent talks to the same core.

### Changed

- **Annotation keys live under `triageagent.dev/`.** `triage/tenant` is
  `triageagent.dev/tenant`, `triage/virtual-app-identity` is
  `triageagent.dev/virtual-app-identity`, and so on for every key. The agent
  reads the new keys only. Namespaces and workloads annotated with the old
  keys are out of scope until they are re-annotated.
- **Applications come from virtual apps only.** The agent no longer reads or
  reconciles `app.k8s.io` Application objects and does not need that CRD. The
  Application API packages, the reconciler, the CRD file,
  `--stabilization-period`, `--concurrent-reconciles`, the shadow mode of
  `--virtual-apps` and the `triageagent.dev/applications` annotation are
  removed. `--virtual-apps` is `off` or `on`, default `on`.
- **One eBPF engine.** OBI is embedded in the agent binary and runs as
  `--mode ebpf`. `cmd/obiprobe` and the `beyla` value of `--red-source` are
  removed. Metric and label names are unchanged. The image for the eBPF
  DaemonSet is the agent image built with `-tags obi`.
- **Go module path is `github.com/triageagent-dev/agent`.**
- Chart 0.21.0: RBAC grants a fixed rule set taken from what the agent reads,
  with `rbac.extraWorkloadRules` for kinds added through
  `--workload-resources`. `ebpf.engine` is removed.

### Added

- Workloads are discovered through `--workload-resources`, and USE identity
  resolves through any controller kind.
- The workload name is the default virtual app identity.
- The agent resends the tenant snapshot when the core sends a resync frame.
- `gcpevents` decodes Cloud Monitoring alert notifications into triage events.

### Fixed

- `redsource`: duration histograms are keyed by the resolved workload
  identity, so p99 reaches `red_summary`.
- `nodeexporter`: the kubepods cgroup slice is discovered for a custom
  `--cgroup-root`.

## [v2.6.0] - 2026-09-05

### Security

- **The embedded eBPF instrumenter's Prometheus metrics no longer open a
  second, unauthenticated listener.** `ebpf.networkFlows.enabled` runs the
  eBPF DaemonSet pod with `hostNetwork: true`, which put the instrumenter's
  private Prometheus port on the *node's* loopback rather than the pod's —
  reachable, with no token, by any other host-network process or pod on that
  node. OBI's vendored Prometheus manager now hands its handler directly to
  this module (`third_party/obi/pkg/export/connector/prommgr.go`'s new
  `EmbedHTTP` hook) instead of opening a listener at all; the agent serves
  that handler from its own bearer-gated `:9464`, the same
  `metricsauth.Wrap` every other metrics port already uses. No port at the
  former private address is bound any more, on any interface.

### Changed

- **`--push-namespaces` (comma-separated string) replaced by repeatable
  `--target-namespaces`** (`--target-namespaces=a --target-namespaces=b`),
  and **`--tenant` renamed `--target-tenant`** — same semantics for both,
  no aliases, no deprecation shim. `push.ParseNamespaces` is unchanged and
  still used by `--log-exporter-include-namespaces` and the `OBI_PROBE_*` env
  vars; only the `--push-namespaces` flag's own string-splitting call site
  was removed. Chart: `push.namespaces`/`push.tenant` moved out of `push:`
  (now only about where telemetry goes — endpoint, token, clusterName,
  heartbeat) into a new `target:` block (`target.namespaces`, now a YAML
  list; `target.tenant`), which is about what the agent observes — the
  `triageagent.dev/tenant` namespace annotation is the other way to say the same
  thing. `target.namespaces` set with `target.tenant` empty still drops
  items from those namespaces for want of a tenant in cluster mode,
  unchanged. Chart version 0.14.0 -> 0.15.0.

- **Project renamed: `application-controller`/`app-controller` -> `triage-agent`.**
  The in-repo chart (`charts/app-controller/`, a stale 0.7.0 fork) was
  deleted rather than renamed; the chart that every cluster deploys from is
  the single source of truth.

  **Breaking for anyone consuming build artifacts by name:**
  - Binary: `bin/app-controller` -> `bin/triage-agent` (`make bin/triage-agent`,
    `make all`).
  - Container image renamed from `application-controller` to `triage-agent`
    (`KO_DOCKER_REPO`, `make ko-push`, CI's `image`/`release` jobs).
  - **GitHub Release asset filenames change**: `dist/app-controller-<os>-<arch>`
    -> `dist/triage-agent-<os>-<arch>`, and `checksums.txt` is keyed off the
    new prefix. Any script pinned to the old filenames (`app-controller-linux-amd64`,
    etc.) needs updating before the next tagged release.
  - `.ko.yaml` build id, `Dockerfile`'s `ENTRYPOINT`, and the `docker-build`/
    `ko-image` local targets all follow the same rename.
  - The Artifact Registry publish path for this project's `obi` spike images
    moves to its own `triage-agent` path, off a shared prefix.

- **`LeaderElectionID` renamed `"app-controller"` -> `"triage-agent"`**
  (`main.go`, only takes effect with `--enable-leader-election`, off by
  default). Verified against live deployments: every current install runs
  `replicaCount: 1` with leader election disabled, so no `Lease` named
  `app-controller` exists in any cluster today — this is a no-op in
  practice, not a live migration. (The `application-system` namespace on
  the legacy clusters belongs to the separate, unrelated legacy
  `kube-app-manager` controller, which uses an old-style ConfigMap lock,
  not a Lease — no name collision either way.) The only scenario this
  matters for is a future rollout with leader election turned on and
  `replicaCount > 1`: a rolling upgrade would briefly hold two active
  leaders (old pods still electing under `app-controller`, new pods under
  `triage-agent`) until every pod is on the new build — safe here since
  push mode is idempotent, and self-heals once the rollout completes.

### Fixed

- **`--target-namespaces` empty no longer means every namespace when a
  primary tenant is set.** `effectiveInScopeMode` returned true for every
  namespace in that case, so an agent given `--target-tenant` and no list
  observed the whole cluster and filed all of it under that tenant. Observed
  on a cluster, which was meant to watch `dev`, `stable` and `flux-system` and
  was also pushing `train`, `perf`, `sandbox`, `argo` and `sqlproxy`, all
  attributed to one tenant. An empty `--target-namespaces` list now
  means the `triageagent.dev/tenant`-annotated set, in BOTH cluster mode and
  single-tenant mode: not configured is not "everything". `--target-tenant`
  answers "which tenant does an in-scope namespace belong to" and never
  widens scope; an explicit `--target-namespaces` list still unions with the
  annotated set. In cluster mode, an in-scope namespace with no annotation
  has its items dropped and counted, not defaulted onto a tenant, unchanged.

  **This narrows scope for any deployment that relied on the old fail-open
  behavior.** A single-tenant agent (`--target-tenant` set) running with no
  `--target-namespaces` and no namespace annotations now observes nothing,
  where it previously observed and pushed the whole cluster under that
  tenant. Check for that combination before upgrading a deployment to this
  build. Every cluster in the fleet already carries an explicit list or
  annotations.

### Removed

- **Chart 0.7.0: the bundled Fluent Bit DaemonSet is gone from the chart.** The
  native log-exporter container (`logExporter.enabled`, ADR-0014) is now the only
  built-in source for the `log_metrics` signal family — `fluentbit-daemonset.yaml`,
  `fluentbit-configmap.yaml`, and the entire `fluentbit:` values block are deleted,
  along with the mutual-exclusivity guard that only existed to keep it from
  double-counting alongside the native exporter. External metrics sources are
  unchanged: any Prometheus endpoint emitting the same counter names — an existing
  Fluent Bit, a custom exporter — still plugs in by pointing `logMetrics.serviceName`
  (and `logMetrics.port`) at it; that guard now requires `logExporter.enabled=true`
  or an explicit `logMetrics.serviceName`. A release upgraded with `--reuse-values`
  carries its old `fluentbit.*` keys forward in the stored values — harmless, since
  no template reads them any more.

### Added

- **`--virtual-apps=off|shadow|on`: label-derived Application synthesis, in memory
  only (docs/todo/virtual-apps.md).** A CR-less cluster has no app-level readiness
  today — onboarding means authoring Application manifests. The push agent already
  watches every workload, so it can now synthesize the Application objects itself:
  new Deployment/StatefulSet/DaemonSet informers group workloads per namespace by
  `app.kubernetes.io/part-of` → `instance` → `name` (a workload with none of these
  is its own single-workload group), derive per-member readiness and a group
  version, and produce an in-memory `*appv1.Application` with the
  labels/conditions/componentsReady/objects shape the triage receiver already
  reads — no CRs are ever created or updated in the cluster. A group whose
  namespace/name collides with a real Application CR is always suppressed; the
  reconciled CR wins.

  Off by default (`virtualApps: "off"`, no extra informers, zero behavior change).
  `"shadow"` synthesizes and evaluates every CR-covered group against its real CR
  — a 60s `"virtual-apps shadow evaluation"` summary log line (ready/component-
  count/version agree-disagree counts), a `"virtual-apps shadow mismatch"` line per
  disagreeing field, and a debug-level `"virtual-apps would-push groups"` line
  listing the CR-less groups `"on"` would start pushing — but never pushes
  anything. `"on"` additionally merges non-suppressed synthesized apps into the
  snapshot and emits `app_delta` add/update/delete frames as groups appear,
  change, or vanish (~2s debounced). The wire format is unchanged — both payloads
  already carried `*appv1.Application`. Chart: `virtualApps` (default `"off"`),
  requires `push.enabled=true`.

- **Virtual apps: per-workload annotation control
  (docs/todo/virtual-apps.md).** Two annotations on the workload object's own
  `metadata.annotations` (Deployment/StatefulSet/DaemonSet — never the pod
  template) now steer grouping/synthesis per workload. `triageagent.dev/virtual-app:
  "false"` excludes a workload entirely — dropped at grouping time, never
  counted toward any group's members or readiness (any other value, including
  `"true"`, or the annotation's absence keeps it in). `triageagent.dev/virtual-app-group:
  <name>` is a group-membership key, not a display name: it outranks
  `part-of`/`instance`/`name` — same value ⇒ same vapp, namespace-scoped — so
  an owner can split a release-aggregate into per-service vapps without
  touching labels, or fold an unrelated shared resource (e.g. a `neo4j`
  Deployment several charts depend on) into one vapp by annotating each with
  the same group value. Exclusion wins when a workload carries both
  annotations. Shadow mode's 60s summary log line gains an `excluded` count.

- **Virtual apps: `triageagent.dev/domain`, `triageagent.dev/description`, `triageagent.dev/tier`
  metadata annotations (docs/todo/virtual-apps.md Phase 2 addendum).** Three
  more annotations on the workload object's own `metadata.annotations`
  (never the pod template) carry per-workload metadata onto the synthesized
  Application: `triageagent.dev/domain` (free text), `triageagent.dev/description` (free
  text), and `triageagent.dev/tier` (an integer 0-9 as a string — lower is more
  critical — parsed strictly; missing or unparsable is treated as "not
  declared", with the unparsable case additionally logged once at `V(1)`).
  Domain and description aggregate uniform-across-members-or-empty, the same
  rule `app.kubernetes.io/version` resolution already uses; tier aggregates
  as the MINIMUM across members that declare one (absent when none do — the
  most critical member speaks for the group). All three land on the
  synthesized Application's own `metadata.annotations`, and description
  additionally reaches `spec.descriptor.description` — this is the wire
  contract the triage hub reads, so annotating a real, hand-written
  Application CR with the same three keys works identically. A group that
  resolves none of the three gets none of the annotation keys at all
  (absent, never an empty string).

- **Virtual apps: namespace-level `triageagent.dev/*` annotation inheritance
  (docs/todo/virtual-apps.md namespace-inheritance addendum).** Operators can
  now annotate the NAMESPACE object itself, once, with `triageagent.dev/domain`,
  `triageagent.dev/description`, `triageagent.dev/tier` — the same three keys workloads and
  real Application CRs already carry — plus `triageagent.dev/virtual-app: "false"` to
  opt the WHOLE namespace out of virtual-app synthesis (a workload-level
  `triageagent.dev/virtual-app: "false"` keeps working independently in namespaces
  that are not excluded). Precedence is strict: workload/CR annotation >
  namespace annotation > absent. A new Namespace informer feeds the
  inheritance, applied at two points: per-workload, BEFORE group aggregation
  (so the existing uniform-or-empty / min-wins logic is untouched — a group
  whose members all inherit the same default resolves it naturally, and a
  member overriding it with its own conflicting value still disagrees as
  usual); and on every real Application CR pushed on the wire (snapshot and
  delta alike), filling any of the three annotations the CR does not already
  declare — independent of `--virtual-apps` mode, since a real CR's
  metadata benefits from this whether or not synthesis is even turned on. A
  namespace annotation change takes effect live: it re-arms the same debounce
  workload changes use, and re-pushes every real CR currently known in that
  namespace so its overlaid annotations catch up immediately.

- **The Application CRD is now served as `app.k8s.io/v1` (ADR-0012).** The schema
  has been stable since the fork — every addition since (push mode, log/RED/USE
  metrics, topology snapshots) landed in sibling packages and flags, never in
  `ApplicationSpec`/`ApplicationStatus` — so the beta label understated how settled
  the contract is. v1 is now the served + storage version and the one the controller
  reconciles.

  **Nothing breaks on upgrade.** v1beta1 stays served and is merely marked
  deprecated, so existing `Application` objects, hand-written manifests, and the
  triage agent's app-watcher (which resolves `app.k8s.io/v1beta1` through a
  hardcoded GVR) all keep working untouched; clients get an apiserver deprecation
  warning and migrate on their own schedule. Conversion is the apiextensions default
  `strategy: None` — no webhook — which is correct precisely because the two versions
  share one schema. That invariant is now enforced by tests
  (`api/v1.TestV1Beta1AndV1SchemasMatch`, `TestV1Beta1RoundTripsThroughV1`) rather
  than by reviewer memory: **`api/v1beta1` is frozen, and new fields go to `api/v1`
  only**, because a field present in one version and not the other is silently
  dropped for readers of the other. Removing v1beta1 is a later, separate change,
  gated on migrating stored objects to v1 first — the apiserver refuses to drop a
  version still listed in `status.storedVersions`. Sequence and the downstream
  consumer checklist are in `docs/todo/application-v1-graduation.md`.

  The push wire format is unaffected: `push/` now marshals `*v1.Application` while
  the triage receiver still unmarshals `*v1beta1.Application`, which stays compatible
  in both directions because the JSON field tags are identical and the receiver never
  reads `apiVersion` — objects from a typed controller-runtime cache carry an empty
  TypeMeta, so none reaches the wire (`TestAppFramesCarryNoAPIVersion`).

- **Cilium/Hubble as an unprivileged alternative RED + topology source
  (ADR-0013).** On clusters whose CNI is Cilium (self-managed Cilium, AKS ACNS —
  GKE-managed Dataplane V2 renames its metrics and is explicitly out of scope for
  v1), the platform's own Hubble agent already runs the privileged eBPF layer this
  chart's Beyla DaemonSet otherwise provides — so a new `hubble` profile scrapes
  Hubble's own `hubble_http_requests_total`/`_duration_seconds_bucket` metrics
  instead, producing the SAME `red_metrics`/`topology_snapshot` frames with ZERO
  elevated privilege in this chart on such clusters: no hostPID, no capabilities,
  no second DaemonSet. Select it with `redSource.profile: hubble` (chart) /
  `--red-source=hubble` (flag); requires `beyla.enabled=false` (the chart fails
  the render otherwise — running both would double-report the same signal) and,
  cluster-side, Hubble's `hubble.metrics` configured with `httpV2` and a
  `labelsContext` naming source AND destination namespace/workload labels (see
  `values.yaml`'s `redSource.hubble` comment for the exact Cilium Helm snippet).

  **This is a bigger refactor than a new source, not just an addition.** The
  scrape-based RED/topology collection in this repo is now ONE profile-driven
  engine (`redsource/`) instead of two separate packages: `redmetrics/` and
  `topology/` are deleted, and the default (`beyla`, unchanged flags/defaults/
  behavior) now runs on this same engine via `redsource.ProfileBeyla`. A
  `Profile` is pure data — metric names, label keys, an error predicate, a
  sample formatter, and which of two topology-edge-derivation shapes a vendor
  uses (one series labels both call ends, vs. separate client-side families
  decorated with unstructured address text) — so the engine itself never
  branches on a vendor's name, only on the data a Profile carries. Full test
  parity: the former `redmetrics`/`topology` test suites were ported onto the
  new engine (`TestBeyla_*`, `redsource/collector_test.go`) alongside an
  equivalent `TestHubble_*` suite, proving the shared engine reproduces the
  retired packages' exact gate/delta/cap behavior and correctly handles a
  structurally different vendor.

  **Known compatibility edge case:** the new `redSource.profile=beyla requires
  beyla.enabled=true` guard is broader than any guard that existed before —
  previously `redMetrics.enabled=true` alone (without `topology.enabled`) could
  legitimately point at a Beyla instance this chart does not manage
  (`beyla.enabled=false`, `redMetrics.serviceName` pointing elsewhere). That
  configuration now fails the render with no values-only workaround (setting
  `beyla.enabled=true` would deploy a redundant second DaemonSet instead). See
  ADR-0013 for the full reasoning and the deliberate, operator-accepted merge
  conflict this creates against a concurrent `redmetrics`/`topology` refactor
  in progress on `main`.

  **USE metrics no longer require Beyla.** The node-exporter (USE metrics)
  container's DaemonSet dependency on `beyla.enabled=true` — never technically
  necessary, since it only reads local cgroup files by pod UID — is gone:
  `beyla-daemonset.yaml`'s DaemonSet now renders whenever `beyla.enabled` OR
  `useMetrics.enabled` is true, with the beyla container, `hostPID: true`, and
  every beyla-only volume separately gated on `beyla.enabled` alone. A
  `useMetrics.enabled=true` + `beyla.enabled=false` render now ships a
  single-container pod with NO host PID namespace share at all — a real
  hardening win on top of the ADR-0009/AUDIT-finding-1 scoped-capabilities work,
  not just an unused toggle.

- **Native log exporter, retiring Fluent Bit for the log_metrics signal family
  (ADR-0014).** A fourth `--mode=log-exporter` runs as a third container on the
  Beyla DaemonSet pod (`logExporter.enabled`, chart-fixed Service `triage-log`,
  port 9466): a per-node container-log counter that tails
  `/var/log/containers/*.log` from each file's current end ("never re-read
  history"), classifies error/warn lines with regexes BYTE-IDENTICAL to Fluent
  Bit's own `fluentbit.pipeline.errorRegex`/`warnRegex` defaults, and exposes
  cumulative Prometheus counters under the EXACT SAME metric/label names Fluent
  Bit's `log_to_metrics` filter always produced
  (`log_metric_counter_log_{errors,warns,lines}_total{namespace,service}`).
  `logmetrics.Collector` — the scraper, delta/counter-reset math, gate, and
  `log_metrics` frame — needed **zero code changes**; the chart auto-points it
  at `triage-log` instead of `triage-fluentbit` whenever `logExporter.enabled`
  and `logMetrics.serviceName` is left unset.

  **Isolation and throughput were an explicit design bar, not an afterthought**:
  one goroutine per file (a hot/stuck file can never stall another's counters),
  a lock-free hot path (atomic counters resolved once per file, read via
  `Load()` by the `/metrics` handler with no lock ever held against a tailer),
  and a literal pre-filter in front of the (still linear-time, RE2) regex
  confirm. Security posture: `/var/log` mounted read-only into this container
  ONLY, `privileged: false`, every capability dropped with none added back,
  `readOnlyRootFilesystem: true` — no log line content ever leaves the
  process, counts only. No new RBAC: the log-exporter container's node-scoped
  pod informer reuses the `-beyla` ServiceAccount's existing `pods`
  get/list/watch grant.

  **Update (same day): error-line samples restored (ADR-0007's model, native).**
  A triage event previously read "service logged 18 error lines" with no
  sample text; ERROR lines now fingerprint (digit/hex/UUID/whitespace
  collapse, matching Fluent Bit's `fingerprint.lua` convention byte-for-byte)
  and attribute to their OWN permanent, monotonic Prometheus series
  (`sample="<fingerprint>"` label), capped at `logExporter.
  maxSamplesPerService` (default 8) DISTINCT templates per (namespace,
  service) with an unlabeled catch-all beyond the cap — never evicted or
  relabeled, since a Prometheus series' identity is its full label set and
  relabeling would reset the wrong counter and corrupt
  `logmetrics.Collector`'s delta math. `logmetrics.Collector`'s existing
  worst-offender sample selection (`pickSamples`, ADR-0007) picks the right
  one from this shape with **zero further code changes** — proven end to end
  against the exporter's real multi-series output
  (`logmetrics/logexporter_contract_test.go`). Measured **~2.6M lines/sec/core
  on the non-error path (0 allocs/op, hard-asserted via
  `testing.AllocsPerRun`) and ~1.6M lines/sec/core on the full realistic mix
  including sample fingerprinting** — both comfortably above the 500K/sec
  floor; the error-only path in isolation is ~197K lines/sec/core, not
  performance-gated since it's a small share of real log volume. Chart:
  `logExporter.sampleMaxLen` (160, matches `fluentbit.pipeline.sampleMaxLen`)
  and `logExporter.maxSamplesPerService` (8); `--log-metrics-sample-label`
  now defaults to `sample` on the logExporter-source path (previously
  omitted, since v1 carried no sample label at all).

  **Update (same day, again): JSON message extraction.** A live sample
  showed the counters-only gap wasn't the only one — a real error line came
  through as `{"actor":"worker","event":"sync","jobId":"#","label":
  "worker-a","level":"error","logging.googleapis.com/operation":{...}`,
  160 bytes entirely spent on envelope metadata with the actual message
  truncated away. Before fingerprinting, a JSON-object error line is now
  checked for `message`/`msg`/`error`/`err` (priority order, chart:
  `logExporter.sampleJsonFields`, flag: `--log-exporter-sample-json-fields`)
  — the first key with a non-empty string value wins, an OBJECT value tries
  its own `"message"` sub-key (`{"error":{"message":"..."}}`), and a
  top-level `"stack"` string's FIRST LINE is appended. Uses `encoding/json`
  — a real parser, not a substring scan, so an earlier key's string value
  that happens to contain the literal text `"message"` (or an escaped quote
  shaped like fake JSON) can never be mistaken for the actual key
  (`TestExtractErrorText_Table`, table-driven: pino/zap/GCP-envelope/
  nested-object/stack-append/substring-in-value/malformed-JSON/non-JSON
  cases). Malformed, truncated, or non-JSON content falls back to the raw
  line exactly as before — the sample is never lost, only ever improved.
  Extraction runs strictly before fingerprinting and only on the error
  path — the wire contract (`logmetrics/logexporter_contract_test.go`) and
  the non-error hot path (`TestNonErrorLinePath_ZeroAllocations`, still
  0 allocs/op) are both unaffected. Measured **~163K lines/sec/core for the
  error-only path** (down from ~197K pre-extraction, comfortably above the
  100K/sec floor set for this change) and **~1.5M lines/sec/core on the full
  mix, ~2.67M lines/sec/core on the non-error path** (both essentially
  unchanged from before).

  **Update (next day): logfmt extraction, plus a leading-noise strip pass.**
  JSON was only half of structured logging. A live logfmt sample —
  `ts=... caller=collector.go:# level=error msg="collector failed" ...
  err="dial tcp: lookup dev-ecosystem-postgres-api-b` — showed the exact
  same envelope-eats-the-budget failure for the `key=value` shape
  Prometheus's own ecosystem (and `log/slog`, `go-kit/log`, logrus's text
  formatter) emit by default. Non-JSON error content is now ALSO checked
  for logfmt structure, reusing the same configured field list
  (`--log-exporter-sample-json-fields`/`logExporter.sampleJsonFields` —
  names kept for compatibility; the classifier's internal field is now
  `sampleFields`, reflecting that it drives both shapes): the first
  `message`/`msg` field present and the first `error`/`err` field present
  are composed as `"<msg> | <err>"` (either alone if only one is present;
  identical values compose once, not duplicated). A real sequential
  tokenizer (`parseLogfmtPairs`) — never independent per-key searches — is
  what makes a decoy like `err="... msg=\"fake\" ..."` safe: a quoted
  value is consumed in full, escapes and all, before the scanner ever looks
  for the next key, so text inside one key's value can never be misread as
  another key=value pair (`TestExtractErrorText_
  LogfmtDecoyInsideQuotedValueIsNotExtracted`). JSON-shaped content still
  takes the JSON path exclusively — logfmt is a fallback for everything
  else, never a second attempt after JSON fails.

  A separate, unconditional pass — `stripLeadingNoise` — then mops up the
  residual case neither extractor can claim: a plain-text line with no
  structured envelope at all. Live sample: a Postgres log line whose
  fingerprinted form read `#-#-# #:#:#.# UTC [#] FATAL: role "root" does
  not exist` — the timestamp preamble was collapsed to placeholders but
  never removed, still eating budget ahead of the real `FATAL: ...` text.
  Running after `fingerprint` (after truncation too), it repeatedly trims
  leading tokens composed ENTIRELY of the fingerprint placeholder plus
  `[ ] ( ) : . , / + -`, or a bare timezone token from a tiny fixed set
  (`UTC`, `GMT`, `Z`) — stopping at the first token that fails both tests,
  and never returning empty even if the whole sample would otherwise strip
  away. It runs on every error line unconditionally; a JSON/logfmt
  extraction result's first token is always real text, so the pass is a
  provable no-op for those. No new flag — this pass has no configuration.
  Measured **~135K lines/sec/core for the error-only path** (down from
  round 3's ~163K — `parseLogfmtPairs`' tokenizer plus the now-unconditional
  strip scan account for the difference, still 1.35× above the shared
  100K/sec floor) and **~1.35M lines/sec/core on the full mix, ~2.53M
  lines/sec/core on the non-error path** (0 allocs/op, unchanged).

  **Update (round 5, operator-reported): sample labels now carry a raw
  exemplar of the first occurrence, not the masked fingerprint.** A triage
  event's sample text read `slot wait timed out after #ms` — the exact
  digit-run masking that bounds series cardinality was also destroying the
  one piece of diagnostic value (the real duration) the sample exists to
  show. Identity and display are now split: the slot registry is still
  keyed by the masked+truncated fingerprint exactly as before (never
  evicted, never relabeled, monotonic counters — the delta math still
  depends on it), but each `errorSlot` additionally captures, ONCE, at slot
  creation, a raw (unmasked) exemplar — the extracted error text of the
  first line that minted the slot, leading-noise-stripped and truncated the
  same way as before, just never masked — and it is this exemplar, not the
  fingerprint, that becomes the `sample` label. Later lines matching the
  same fingerprint only increment the counter; the exemplar is never
  updated, since label stability is series stability.
  `stripLeadingNoise` (stripnoise.go) now runs on RAW text instead of an
  already-masked string — it masks each LEADING token in isolation (via a
  helper factored out of `fingerprint()` itself, so identity masking and
  noise-detection masking can never drift apart) only to decide whether
  that token is noise, and returns the surviving RAW text unchanged; the
  identity pipeline still calls the same function on the already-masked
  fingerprint and gets byte-identical output to before this change, since
  masking an already-masked token is a no-op. A collision guard closes the
  one new failure mode this split introduces: two DIFFERENT fingerprints in
  the same (namespace, service) could truncate to the SAME raw exemplar
  (identical text up to `sampleMaxLen`, divergence only after the cut),
  which would otherwise mint two Prometheus series sharing one label set.
  On collision, the newer slot falls back to its own masked fingerprint as
  the label (unique by construction — it's the dedup key); on the
  vanishingly rare case that even that string is already in use as another
  slot's label, `" ·2"` is appended. **One-time effect on upgrade**:
  existing masked-fingerprint-labeled series are retired (the exporter
  process restarts on deploy, and the label value a new build computes for
  the same fingerprint is different from what an old build computed) —
  every error series starts fresh under its new exemplar label, which the
  collector's existing counter-reset handling already absorbs cleanly.
  Cross-node exemplars for the same underlying template may differ (each
  node's tailer captures whichever line happened to mint its own local
  slot first), so the hub's incident sample list may show a small number of
  near-duplicate-looking samples for what is really one template — cosmetic,
  not a correctness issue, since fingerprint identity (and therefore
  counting/absorption) is unaffected. No flag or chart changes — no new
  configuration surface, `sampleMaxLen`/`maxSamplesPerService` now bound
  both the identity fingerprint and the display exemplar.

  **`fluentbit.*` is now the legacy path** — fully functional, unchanged, not
  removed — retirement from the chart is a follow-up gated on fleet migration.
  The chart fails the render if `logExporter.enabled` and `fluentbit.enabled`
  are both true (one log source per cluster; both would double-count the same
  stream), and if `logMetrics.enabled` is set with neither source (nor an
  explicit `logMetrics.serviceName`) configured. See
  `docs/adr/0014-log-exporter-in-agent-ds.md` for the full capacity
  measurement methodology and the classification-parity requirements.


### Security

- **`google.golang.org/grpc` 1.83.0 → 1.83.1** (audit finding 44, HIGH):
  closes CVE-2026-84304. Bumped in both Go modules — the root `go.mod` and
  the vendored `third_party/obi/go.mod` (its own module, own `go.sum`) — via
  `go get ... && go mod tidy` in each; no other dependency moved in either
  module. No source change required.

- **Dependency bump closing four HIGH CVEs** (2026-08-21 re-audit): `golang.org/x/mod`
  0.35.0 → 0.40.0 (CVE-2026-56864, CVE-2026-56865), `golang.org/x/net` 0.55.0 → 0.58.0
  (CVE-2026-46600, re-rated UNKNOWN → HIGH since the previous audit), and
  `golang.org/x/text` 0.37.0 → 0.41.0 (CVE-2026-56852, open across both audits).
  `go mod tidy` carried `sync`, `sys`, `term` and `tools` along with them. All
  indirect dependencies; no source change was required and the full suite passes.
- **Corrected a security claim that was wrong in both the chart and ADR-0010.**
  Both stated the USE-metrics node-exporter container was "unprivileged, no
  hostPID". Its own securityContext is genuinely minimal (`privileged: false`,
  `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, read-only
  cgroup mount) — but `hostPID` is a **pod-level** setting and this container is a
  second container in Beyla's DaemonSet pod, so it shares the host PID namespace
  and the `-beyla` ServiceAccount whenever Beyla is enabled. The wrong half was
  the half a reviewer would rely on. No behaviour change; the isolation the old
  wording implied needs the standalone DaemonSet already tracked as a follow-up.

- **Beyla container: scoped capabilities instead of `privileged: true`**
  (AUDIT.md finding 1, top-rated finding of the 2026-08-21 re-audit).
  `privileged: true` granted every capability, host device access, and
  unconfined seccomp/AppArmor for a workload whose actual need is eBPF program
  load plus a handful of process-inspection capabilities. Default is now the
  unprivileged, scoped-capability securityContext Grafana documents for
  application mode on kernels >= 5.11 (GKE's COS is 6.x): `privileged: false`,
  `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`,
  `runAsUser: 0` (still required — the capabilities below are only honored for
  a root-UID process), `seccompProfile: RuntimeDefault`, and
  `capabilities: {drop: [ALL], add: [BPF, PERFMON, NET_RAW, SYS_PTRACE,
  DAC_READ_SEARCH, CHECKPOINT_RESTORE]}`. Two new escape hatches:
  `beyla.privileged: true` reverts to the old fully-privileged container for a
  kernel that can't load eBPF under the scoped set, and
  `beyla.extraCapabilities` appends documented per-kernel additions
  (`SYS_RESOURCE` for kernels older than 5.11, `SYS_ADMIN` for Go
  trace-context propagation on some kernels) without falling all the way back
  to `privileged: true`. `hostPID` is unaffected — it stays required for
  application-mode discovery and is not a container capability. Correspondingly
  hardened the node-exporter container on the same pod: added
  `capabilities: {drop: [ALL]}`, `seccompProfile: RuntimeDefault`, and
  `runAsNonRoot: true` (AUDIT.md finding 2's enforceable half — the image is
  chainguard's nonroot-by-default `static` base, verified against `.ko.yaml`,
  so this asserts existing behaviour rather than changing it). See
  ADR-0009's correction.

- **Beyla DaemonSet: `tolerations` now defaults to `[]` (behaviour change on
  upgrade).** Previously `[{operator: Exists}]`, which tolerates every taint —
  so a tracing DaemonSet landed on every node in the cluster including
  tainted and dedicated ones (control-plane, GPU pools, ...) with no operator
  opt-in, amplifying finding 1's blast radius by reach rather than privilege.
  A DaemonSet with this much host access has no business scheduling onto a
  node the operator deliberately tainted off-limits unless told to. The
  visible symptom on `helm upgrade`: the DaemonSet controller immediately
  deschedules the existing Beyla pods on every tainted node — pods
  disappearing from control-plane/dedicated pools right after the upgrade is
  this change working as intended, not a crash, and services on those nodes
  stop reporting RED metrics from that moment. Installs relying on the old
  blanket-scheduling behaviour must now set
  `beyla.tolerations: [{operator: Exists}]` (or a narrower set) explicitly;
  `beyla.nodeSelector` was already chart-rendered and remains the other half
  of scoping this DaemonSet to specific nodes.

- **Beyla ConfigMap: node-local Kubernetes metadata by default
  (`beyla.metaRestrictLocalNode: true`, `beyla.disableInformers: [node]`).**
  Beyla's own default runs a CLUSTER-WIDE pod-metadata informer in every
  DaemonSet pod. This chart's application-mode RED metrics only decorate with
  the local instrumented pod's identity, and topology edges are derived
  controller-side from address text, so `meta_restrict_local_node: true` plus
  disabling the node informer loses nothing the chart uses, and it removes
  N cluster-wide informers' worth of API-server load. Honest measurement
  note: this was initially expected to be the dominant per-pod memory share;
  measured before/after on the first busy cluster it was MARGINAL — Beyla's
  footprint scales with the instrumented process count on the node (tens of
  Mi on nodes running nothing instrumented, ~700Mi on the busiest), so
  `beyla.resources.limits.memory` must be sized per cluster and real
  reduction comes from narrowing the instrumented namespace set (or the
  `hubble` profile on Cilium clusters). Rendered via `hasKey` so an explicit
  `false` survives and a `--reuse-values` upgrade from a release predating
  the keys gets the new default.

- **Beyla ConfigMap: `discovery.instrument` no longer defaults straight to
  `"*"` when a namespace scope is inferable.** New precedence in
  `beyla-configmap.yaml`: `beyla.includeNamespaces` (existing, unchanged) wins
  if set; else `beyla.instrumentNamespaces` (new, plain namespace names); else,
  if `push.namespaces` is non-empty, the same namespace list is derived from
  it — we only push those namespaces, so instrumenting anything wider is pure
  risk with no observability benefit; else `"*"`, unchanged for chartless/dev
  setups with no push scope to infer from. No behaviour change for an install
  that already sets `push.namespaces` to fewer than "all" and leaves
  `includeNamespaces`/`instrumentNamespaces` unset — instrumentation scope now
  automatically tightens to match.

  Correction (2026-08-22, found live on the first cluster to exercise the
  derived branch): the two new arms originally rendered anchored `^ns$`
  entries on the claim that Beyla matches `k8s_namespace` as a regex. That is
  the DEPRECATED `discovery.services` block's semantics — `discovery.instrument`
  matches by GLOB (Grafana docs), so anchored entries match nothing and Beyla
  instrumented zero processes (RED `servicesScraped: 0`). Both arms now render
  plain names; a bare glob is an exact match, so the substring concern the
  anchors addressed does not exist under `instrument`.

- **All four scrape collectors (`redmetrics`, `usemetrics`, `topology`,
  `logmetrics`) now bound the scrape response they read** (AUDIT.md finding
  3). `io.ReadAll(resp.Body)` was unbounded, and the response was then parsed
  from a second in-memory copy — roughly double the response size at peak
  against a 256Mi container limit — for a scrape target resolved from a
  fixed-name Service's Endpoints and reachable unauthenticated over plaintext
  HTTP by any pod in the namespace carrying the right selector labels. Each
  collector now reads through `io.LimitReader(resp.Body, maxScrapeBodyBytes+1)`
  (8MiB) and treats a body over the limit as a scrape error (logged and
  skipped by the existing per-endpoint error path, same as any other scrape
  failure) rather than silently parsing a truncated body. Mirrors the
  `io.LimitReader` pattern `gcpevents/poll.go` and `gcpevents/client.go`
  already used. No shared helper package — each collector already
  deliberately avoids sharing code with its siblings (see ADR-0009's "mirrors
  logmetrics/ structurally... rather than sharing code"), so the constant and
  its comment are duplicated identically in each file instead.


- **Chart 0.3.0: the ClusterRole's cluster-wide read can now be narrowed, opt-in
  and off by default.** The role grants `get;list;watch` on
  `apiGroups: ["*"], resources: ["*"]`, which includes `secrets` in every
  namespace. That breadth is not accidental — `componentKinds` are arbitrary, an
  Application may legitimately name a `Secret`, `ConfigMap`, `Ingress`,
  `ServiceMonitor` or any CRD, and the set differs per installation, so no fixed
  list is correct for every install. RBAC has no deny rule either, so secrets
  cannot be subtracted from a wildcard. **The default is therefore unchanged**, and
  upgrading changes no permission. Setting
  `rbac.allowAllComponentKinds: false` replaces the wildcard with an enumerated set
  covering the kinds this controller computes status for plus the common Kubernetes
  and ecosystem kinds (`autoscaling`, `networking.k8s.io`,
  `rbac.authorization.k8s.io`, `monitoring.coreos.com`, Gateway API, `argoproj.io`,
  `kagent.dev`), with `rbac.extraComponentRules` for vendor CRDs and `secrets`
  isolated in its own rule so an install that never names a Secret can drop it
  entirely. Verify against live Applications before switching — an ungranted
  componentKind stops appearing in status with no error. Both keys are read via a
  `| default dict` local rather than `.Values.rbac.x`, because
  `helm upgrade --reuse-values` drops keys added after the installed release and a
  nil `.Values.rbac` would otherwise fail the whole template — the same failure
  mode that crash-looped Fluent Bit on `cluster-a` (0.2.1) and the controller on
  `cluster-b` (0.2.3). No controller image change.

### Fixed

- **`make manifests` / `make generate` work again.** `controller-gen` was pinned at
  v0.4.0 (2020) and panicked type-checking this module against
  `k8s.io/apimachinery v0.36`, so the committed CRD could not be regenerated at all;
  bumped to v0.21.0. `trivialVersions=true` is dropped from `CRD_OPTIONS` — it
  collapses a multi-version CRD into a single version, and the option no longer
  exists in current controller-gen — and `generate-go` now regenerates `./api/...`
  rather than just `./api/v1beta1/...`. The generated CRD keeps its license header
  via the Makefile instead of hand-maintenance.
- **Removed `conversion-gen` and `mockgen` from the build.** Neither was ever
  invoked (the repo has no `go:generate` directives), `conversion-gen@v0.18.9` no
  longer installs, and under `strategy: None` there are no conversion functions to
  generate — so `make generate`/`make e2e-test` no longer fail on a tool they do not
  use.

- **A `componentKind` that does not resolve is now reported instead of vanishing.**
  Such a kind is skipped, so the component never appears in status and no condition
  mentions it — the Application reads as complete while a declared kind is missing.
  The skip was logged at V(2) only, which `--zap-log-level=debug` does not show, so
  in practice it was invisible. It is now logged once per (Application,
  GroupKind) through the manager's base logger, naming the Application, the
  declared group, and the group it resolved to. The usual cause is a
  group/*version* in the `group` field: `group: v1, kind: PodDisruptionBudget`
  reduces to the core group, where no such kind exists, so it never matches — as do
  `Role`, `RoleBinding` and `PodSecurityPolicy` written the same way. Skipping
  stays non-fatal and is still not a reconcile error; one unresolvable kind must not
  take the resolvable ones with it, which is now covered by a test.

### Added

- **`gcpevents/` — a Cloud Run function bridging Google Cloud cost anomalies
  and Personalized Service Health into the triage agent as events on the same
  service graph the controller's Applications feed.** Two deployments from one
  module: a Pub/Sub-triggered function for Cloud Billing anomaly
  notifications, and a Cloud Scheduler-driven poller for service health, which
  also posts a readiness snapshot of currently-degraded services. Signals
  resolve against a generated catalog (`catalog_gen.go`, service data only)
  onto the canonical service-graph slug. Payloads are decoded by pattern
  rather than bound to a struct, because Google documents the field names in
  both camelCase and snake_case and spells amounts three ways. Permanent
  failures are acked, transient ones nacked. Its own Go module, with its own
  CI job. See
  [ADR-0008](docs/adr/0008-gcp-pubsub-event-bridge.md). No controller image or
  chart change.

### Changed

- **Chart-only (0.2.1/0.2.2): the error-sample fingerprint now extracts the
  message from structured JSON logs before truncating, scrubs URI
  credentials, and survives `helm upgrade --reuse-values`.** Plain head-
  truncation-then-normalize spent the whole `sampleMaxLen` budget on a
  GCP-style JSON log's envelope (`{"label":...,"logging.googleapis.com/
  operation":{...}`) and never reached `message`, so `sample` carried
  metadata instead of the error text (observed in production on `cluster-a`).
  `fingerprint.lua` now extracts `message`/`msg`/`error`/`err` string fields
  from JSON-shaped lines first (object-valued fields like
  `"error":{"code":5}` fall back to whole-head fingerprinting unchanged),
  unescapes `\n`/`\t`/`\"`/`\\`, then truncates last. A new URI-userinfo
  scrub (`user:***@host`) strips connection-string passwords out of the
  fingerprint regardless of source. Separately, `--reuse-values` drops chart
  defaults introduced after a release's last install, which rendered
  `emitterMemBufLimit` empty and crash-looped the Fluent Bit DaemonSet on
  `cluster-a`; the three `Emitter_Mem_Buf_Limit` template lines and `sampleMaxLen`
  now carry `| default` so a dropped value falls back to the chart's default
  instead of rendering empty (`sampleEnabled` intentionally excluded — nil
  there safely disables the feature). **0.2.2:** a `message`/`stack` value
  longer than the 2048-char search window has no closing quote within it, so
  the terminated extraction patterns above were silently falling back to the
  whole-head envelope for long stack traces; the filter now also tries
  `stack` (winston-style loggers) and, only when the window was actually cut
  (`#head == 2048`, so a short/empty value can't be mistaken for a cut one),
  an unterminated capture of whichever field opened but didn't close. No
  controller image change in either version. See
  [ADR-0007](docs/adr/0007-error-log-sample-fingerprint.md).

- **Chart-only (0.2.3): every remaining required scalar rendered from a
  `.Values.*` path now has a `| default`, so `helm upgrade --reuse-values`
  can no longer render an empty flag/config value for anything the binary or
  Fluent Bit requires.** 0.2.1 fixed `emitterMemBufLimit` after it
  crash-looped the Fluent Bit DaemonSet on `cluster-a`; today the same
  `--reuse-values` gap crash-looped the **controller** on `cluster-b` — its
  release predated `concurrentReconciles`, so a dropped default rendered
  `--concurrent-reconciles=` with no value and the binary exited on flag
  parse. Swept `deployment.yaml` (`--sync-period`, `--stabilization-period`,
  `--concurrent-reconciles`, `--push-heartbeat`, `--log-metrics-port`,
  `--log-metrics-interval-seconds`, `--log-metrics-error-threshold`, and the
  `metrics.bindAddress` arg), `fluentbit-configmap.yaml` (exporter `Port`,
  `errorRegex`/`warnRegex`, pipeline `serviceLabel`/`serviceLabelFallback`),
  and `fluentbit-daemonset.yaml` (Service/container port, image
  repository/tag). Deliberately **not** defaulted: any boolean
  (`sampleEnabled`, `*.enabled` — `| default` breaks an explicit `false`),
  `logMetrics.sampleLabel` and `logMetrics.serviceLabelFallback` (empty is a
  meaningful disable), `push.namespaces` (empty = all), auth/identity values
  (`push.endpoint`/`clusterName`/`tenant`/`token`/`existingSecret`, already
  `required`), `namespace` (empty = cluster-wide), and anything already
  gated on its own truthiness (a nil there correctly omits the arg, which
  was always the intended behavior). No controller image change.

### Removed

- **All Dockerfile-based image builds — this repo now builds every container
  image with ko.** `Dockerfile` (the plain image), `hack/Dockerfile.obiall`
  (the `-tags obi` "obi-single" image, hand-built from a manually staged
  `dist/` binary), and `hack/Dockerfile.obiprobe` (packaged the standalone
  `cmd/obiprobe` spike probe) are all deleted, along with the `docker-build`/
  `docker-push` Makefile targets and their now-unused `REGISTRY`/`IMAGE_NAME`/
  `IMAGE_TAG`/`ARCH`/`ALL_ARCH`/`CONTROLLER_IMG` variables. `hack/Dockerfile.obiprobe`
  specifically: dead spike scaffolding for this repo's build purposes — no
  live environment sets `beyla.engine: obi` (the chart's own
  comment calls that engine's default image "the spike build", vs. "the fleet
  standard" for `obi-single`), and the image it packaged was a one-off manual
  `docker buildx --push`, never wired into this repo's Makefile or CI.
  `cmd/obiprobe` itself is untouched (still builds, still exercised as a
  sanity check in `hack/build-obiprobe.sh`) — only its Dockerfile is gone.

  **New pair of ko build ids off the one main package**: `.ko.yaml` (build id
  `triage-agent`, unchanged) for the plain image, and a new `.ko.obi.yaml`
  (build id `triage-agent-obi`, same main package + `flags: [-tags, obi]`)
  for the `-tags obi` image that `--mode role "ebpf"` needs — selected via
  `KO_CONFIG_PATH` rather than a second `builds:` entry in `.ko.yaml`, since
  ko keys its whole `builds:` list by resolved import path and both variants
  resolve to the same one (`.`); two entries sharing an import path silently
  collide (last-one-wins) in ko's own config loader, so `KO_CONFIG_PATH` is
  what keeps the two from shadowing each other. New Makefile targets
  `ko-image-obi`/`ko-push-obi` mirror the existing `ko-image`/`ko-push` pair;
  `REGISTRY=<repo>` is now also accepted as an alias for `KO_DOCKER_REPO` on
  all four (wins if both are set), matching the core
  repo's `make push REGISTRY=...` convention. `make ko-push-obi` tags the obi
  image `$(VER)-obi`/`obi-latest` — it publishes to the same `KO_DOCKER_REPO`
  as the plain image, so it needs its own tag suffix to avoid colliding with
  the plain build's `$(VER)`/`latest`.

  **`hack/build-obiprobe.sh` restructured**: it no longer clones OBI or
  patches a `go.mod` replace directive (both were spike-only steps;
  `third_party/obi` is now a permanently vendored, checked-in copy of the
  OBI source, wired via an unconditional `replace` already in `go.mod`). It
  now does exactly two things, clearly separated: STEP 1 runs OBI's own
  `bpf2go` codegen via its pinned generator image (Docker — the only Docker
  use left anywhere in this repo's build, and only needed when bumping the
  vendored OBI version, never for a normal code change or any image build);
  STEP 2 is go-only sanity builds (this repo's main package `-tags obi`, plus
  `cmd/obiprobe`) proving the codegen actually unblocks both consumers — no
  image, no Docker. Building `-tags obi` for a real image is exclusively
  `make ko-image-obi`/`ko-push-obi` now.

### Fixed

- **`make bin/triage-agent` (and therefore `make all`) was broken on `main`.**
  The target ran `go build ... main.go` — single-file mode, which compiles
  only that one file and ignores the rest of the package
  (`apiversion.go`, `ebpf_obi.go`/`ebpf_stub.go`) — and failed with
  `undefined: runEBPF` plus three more undefined symbols from
  `apiversion.go`. Fixed to build the package (`go build ... .`); the same
  bug, and the same fix, applied to `.github/workflows/ci.yaml`'s `release`
  job, which built its multi-platform release binaries the identical
  single-file way and would have shipped the same broken binaries on the
  next tagged release.

## [1.5.0] - 2026-07-17

### Added

- **log_metrics frames now carry the erroring log line itself.** A new Lua filter
  in the chart's Fluent Bit pipeline fingerprints each error-classified log line
  (truncate to `fluentbit.pipeline.sampleMaxLen`, then collapse timestamps/hex
  ids/uuids to `#`) into a `sample` label on the error counter only, so retried
  instances of one error template collapse into a single metric series instead of
  fragmenting per timestamp/request-id. The collector picks, per (namespace,
  service) pair, the sample from whichever error series had the largest delta
  this window — the template actually firing now, not the oldest or highest-
  cumulative series — falling back to the first non-empty sample when nothing
  labeled moved. New chart knobs: `fluentbit.pipeline.sampleEnabled` (default
  `true`), `sampleMaxLen` (default `160`), `emitterMemBufLimit` (default `50M`,
  raised from Fluent Bit's 10M default since the sample label enlarges the error
  filter's emitted stream) and `logMetrics.sampleLabel` (default `"sample"`).
  `log_to_metrics` has no series expiry, so cardinality is bounded by
  fingerprinting rather than any eviction; if it still outgrows the pod's memory
  limit, the accepted failure mode is OOMKill + restart, which resets the
  counters — the collector's existing counter-reset delta handling already
  treats that correctly. No hub-side change required (`Sample` has existed on
  the wire since the `log_metrics` frame's v1 shape). See
  [ADR-0007](docs/adr/0007-error-log-sample-fingerprint.md).

## [1.4.6] - 2026-07-16

### Changed

- **Log-based metrics gate now qualifies a service on error OR warn count,
  not error count alone.** `logMetrics.errorThreshold` (default 10) is
  compared against both `ErrorCount` and `WarnCount` independently — a
  service with 0 errors but a warn-count spike now triggers a `log_metrics`
  frame on its own, whereas previously `WarnCount` was scraped and shipped
  but never gated on anything. See [ADR-0006](docs/adr/0006-log-based-metrics.md).

## [1.4.5] - 2026-07-14

### Fixed

- **Status computation no longer panics on sparse CRD conditions.**
  `getConditionOfType` cast `reason`/`status` with unchecked type assertions;
  a condition of type `Ready` without a `reason` field (it is `omitempty` in
  many schemas, including `appsv1.DeploymentCondition`) panicked the
  reconciler and crash-looped the controller. The function runs for **every
  component kind without a dedicated handler** via
  `statusFromStandardConditions`, so any custom resource could trigger it.
  Now uses comma-ok assertions; the unused `reason` return was dropped.
- **`deploymentStatus` no longer dereferences a nil `spec.replicas`** — it
  defaults to 1 like every other workload handler.
- **`spec.selector.matchExpressions` are honored when listing components.**
  Component listing used `client.MatchingLabels(selector.MatchLabels)`, which
  ignored `matchExpressions` entirely — an Application with a
  matchExpressions-only selector produced an empty label filter and claimed
  **every resource of each declared GVK in the namespace**. Listing now goes
  through `metav1.LabelSelectorAsSelector` (full selector semantics), matching
  what the dynamic-watch fan-out (`applicationsForComponent`) already did.
- **Hostname-only LoadBalancer ingress counts as provisioned.** AWS ELB/NLB
  populate `status.loadBalancer.ingress[].hostname` and leave `ip` empty;
  such Services were reported `InProgress` forever. An ingress entry now
  counts with either an IP or a hostname, and the mixed `||`/`&&` readiness
  expression was rewritten as explicit branches.
- **Application deleted mid-reconcile no longer surfaces as an error.** The
  status write treats `IsNotFound` as a no-op (common on churny namespaces
  with short-lived preview environments).

### Security

- **The push agent no longer sends its raw HMAC signing key over the wire.**
  The pre-upgrade `Authorization: Bearer` credential used to be the per-tenant
  key itself; it is now the handshake **signature** (HMAC-SHA256 over
  tenant/cluster/timestamp) accompanied by `X-Triage-Tenant`/`X-Triage-Cluster`/
  `X-Triage-Ts` headers, so the receiver can still verify pre-upgrade but the
  long-lived key never travels. Hello-frame verification remains the
  authoritative auth. The triage receiver ignores the Authorization header
  (hello-HMAC is its sole auth), so old and new agent/receiver combinations
  interoperate; receivers ≥ the paired `remoteagent` patch additionally verify
  the new headers pre-upgrade and reject mis-keyed clients with 401 before the
  WebSocket upgrade.
- Stale kubebuilder RBAC marker trimmed from `update;patch;watch` on `*/*` to
  read-only `get;list;watch`, matching the chart's ClusterRole (leftover from
  the removed owner-reference writing).

### Changed

- **Component reads are now served from the informer cache.** controller-runtime's
  default client bypasses the cache for unstructured reads, so every reconcile
  issued live `LIST` calls to the API server for each component GVK — while the
  dynamic component watches maintained informers whose data was never read.
  `client.CacheOptions.Unstructured: true` routes those reads through the cache.
  Trade-off: listing a non-workload kind (Secret/ConfigMap) now starts a
  cluster-wide informer for it; `TransformStripManagedFields` (below) offsets
  the memory cost.
- `managedFields` are stripped from every cached object
  (`cache.DefaultTransform`), typically cutting informer memory 30–40%.
- New `--concurrent-reconciles` flag (default 4); reconciles of distinct
  Applications run in parallel instead of serializing on one worker.
- In push mode the Event informer is scoped server-side to `type=Warning`
  (field selector) instead of caching every Event cluster-wide.
- Push connection robustness: read deadline + pong handler detect half-dead
  connections; the send-queue drop log is rate-limited to the first and every
  1000th drop per connection.
- **Toolchain: controller-runtime v0.19.3 → v0.24.1, k8s.io/* v0.31.3 →
  v0.36.0, Go 1.25 → 1.26.** No API changes required; CI follows via
  `go-version-file`. Makefile tool installs switched from legacy
  `GO111MODULE=on go get` (which mutates go.mod under modern Go — a
  `make generate` run downgraded controller-runtime to v0.6.5) to `go install`.

### Removed

- Dead code: `setOwnerRefForResources` (owner-ref writing was disabled long
  ago), the `StatusDisabled` constant, the reconciler's unused `Scheme` field,
  and `ObjectStatus.Link` population (`selfLink` was removed in Kubernetes
  1.24, so the value was always empty; the API field remains).

## [1.4.4] - 2026-07-13

### Fixed

- **Push mode: widened the outbound frame buffer (256 → 4096) to absorb
  reconnect resync bursts.** A reconnect replays the full informer cache
  before streaming live deltas, which can briefly emit thousands of frames on
  clusters with large event volume; the 256-slot buffer filled and dropped
  most of that backlog on `preview` (619 apps).

## [1.4.3] - 2026-07-12

### Fixed

- **Argo Rollout readiness no longer reports a down rollout as Ready (false
  negative).** `rolloutStatus` was phase-only since 1.2.0: it mapped
  `Progressing` and `Paused` straight to `Ready`. But `Progressing` is not
  exclusively a healthy scale-up/canary step — a Rollout whose new pods
  crash-loop sits in `Progressing` for the entire `progressDeadlineSeconds`
  window (and can stay there indefinitely if stuck) *before* Argo flips it to
  `Degraded`. Throughout that window the controller reported the Rollout — and
  therefore its Application's `Ready` condition — as `Ready`, so the Application
  CR never transitioned and downstream consumers (e.g. the triage agent) never
  saw the outage. Observed in production on `example-app`: the Rollout was
  down while the Application CR stayed `Ready 4/4`.

  The fix decides readiness on actual serving capacity — the same signal
  `deploymentStatus` uses — not on the phase:
  - `Degraded`/`Error` still report `InProgress` (authoritative failure).
  - Scaled-to-zero (`spec.replicas=0`) is still `Ready`.
  - For every other phase (`Healthy`/`Inactive`/`Progressing`/`Paused`/empty):
    an explicit `Available=False` condition → `InProgress`; otherwise
    `availableReplicas >= spec.replicas` → `Ready`, and `availableReplicas <
    spec.replicas` → `InProgress`. This keeps the scale-up/canary flap fix (a
    Progressing rollout still serving its full complement stays `Ready`) while
    catching the down/partially-available rollout.

  Regression coverage: the prior `rolloutStatus` unit specs were phase-only —
  they never set `availableReplicas` or an `Available` condition, so they
  encoded the very blind spot the bug lived in and could not catch it. New
  specs drive a `Progressing` rollout with `availableReplicas < desired` (down
  and partially-available), a stale-`Healthy` rollout with zero available, and
  an explicit `Available=False`, all asserting `InProgress`. They fail on the
  1.2.0..1.3.x code and pass on the fix.

  **Affected releases: `1.2.0` through `1.4.2` (and image tags `25171c3` /
  `1f3ab67` onward) carry this rollout false-negative.** A cluster running any
  of them can have a genuinely-down Argo Rollout reported healthy. Upgrade to
  `1.4.3` or later, or roll back to a pre-`25171c3` build (e.g. `d3d8790`) as an
  interim measure.

### Added

- Component status support for the Kubernetes Gateway API (`Gateway`,
  `HTTPRoute`) and kagent.dev (`Agent`, `ModelConfig`). These kinds report
  health via positive-polarity conditions (`Accepted`/`Programmed`/
  `ResolvedRefs`) rather than the workload `Ready`/`InProgress` convention:
  - `Gateway` is Ready when `Accepted=True` and `Programmed=True` (the
    deprecated `Ready` condition is ignored — `Programmed` is the health signal).
  - `HTTPRoute` is Ready when at least one attached parent has `Accepted=True`
    and `ResolvedRefs=True`; a parent with `Accepted=False` or `ResolvedRefs=False`
    (e.g. `BackendNotFound`) reports InProgress.
  - `Agent`/`ModelConfig` are Ready when `Accepted=True`.
  - All four drive the Application's `Ready` condition (added to `workloadKinds`)
    and get a real-time dynamic watch. The controller remains a read-only
    aggregator — it sets no `ownerReferences`, so removing an Application never
    cascades into deleting these resources.

- Version is now derived from `git describe` at build time; the standalone
  `VERSION`/`VERSION-DEV` files were dropped.

### Changed

- **BREAKING (push mode): the cluster-agent handshake is now HMAC-signed with a
  per-tenant key, replacing the plain bearer token.** The agent signs
  `(tenant, clusterName, timestamp)` with its tenant's key so the triage
  receiver can verify key possession and route pushed events to that tenant's
  service graph. `--push-token`/`--push-token-file` now carry the per-cluster
  HMAC signing **key** (used to sign; never sent on the wire) rather than a
  bearer token, and a new `--tenant` flag is **required** whenever
  `--push-endpoint` is set. The default endpoint help now points at
  `/events/ws`. Chart: new `push.tenant` (required when `push.enabled`);
  `push.token` is documented as the HMAC signing key and `clusterName` selects
  the receiver-side key. `push/hmac.go` is byte-identical to the triage
  receiver's `internal/remoteagent/hmac.go` (separate modules — keep in sync).
  Existing push deployments must set `push.tenant` and provision the matching
  per-cluster key on the receiver before upgrading.

## [1.4.1] - 2026-07-09

### Security

- Push mode now rejects a non-`wss://` `--push-endpoint` at startup so the bearer
  token is never sent over plaintext by accident. A plaintext `ws://` endpoint is
  allowed only with the explicit `--push-allow-plaintext` flag, which is separate
  from `--push-insecure-skip-verify` (TLS cert verification for `wss://`). Chart:
  `push.allowPlaintext` (default false). Addresses AUDIT finding 1.

### Changed

- `--push-namespaces` parsing trims whitespace and drops empty entries.
- Graceful reconnect: a dropped/refused connection logs at low level (no stack
  trace) and backs off; an auth-rejection close backs off hard.
- CI publishes a GitHub Release on `v*` tags (binaries + notes).
- README rewritten; removed dead fork-scaffold files.

## [1.4.0] - 2026-07-09

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Added

- **Push mode** (`push/` package, [ADR-0005](docs/adr/0005-outbound-push-mode.md)):
  when `--push-endpoint` is set, the controller dials a triage agent over an
  outbound WebSocket and streams its Application inventory + status deltas +
  Kubernetes Warning events. For clusters with no inbound API access. Opt-in and
  fully additive — no effect when the flag is unset (reconcile behavior unchanged).
  New flags: `--push-endpoint`, `--cluster-name`, `--push-token`,
  `--push-token-file`, `--push-namespaces`, `--push-heartbeat`,
  `--push-insecure-skip-verify`.
- **Helm chart** (`charts/kube-app-manager`): the recommended install path. Bundles
  the CRD, exposes push-mode and metrics toggles, and ships **without** the
  `kube-rbac-proxy` sidecar.

### Changed

- `--push-namespaces` parsing trims whitespace and drops empty entries, so
  `ops, dev` and `ops,dev` behave identically.
- The Helm chart disables metrics by default (`--metrics-addr=0`) and omits the
  scaffold `kube-rbac-proxy` sidecar and webhook service.

### Removed

- Legacy kustomize deploy machinery now that the Helm chart is the install path:
  `config/default`, `config/kube-app-manager`, `config/rbac`, `config/prometheus`,
  `config/crd` kustomizations + webhook/cainjection patches, `deploy/` (the
  all-in-one manifest), and the `docs/examples/wordpress` demo. Only the CRD source
  (`config/crd/bases/`) remains. Dropped the now-unused kustomize tool and the
  `generate-resources`/`set-image`/`deploy-wordpress` Make targets.

## [1.3.8] - 2026-06-30

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Fixed

- Reverted the `--enable-leader-election` default back to `false` (it was changed to
  `true` in 1.3.7). Enabling it by default required lease RBAC that the shipped
  single-replica deployment did not have, causing the manager to fail on startup.
  Leader election remains available via the flag for multi-replica deployments.

## [1.3.7] - 2026-06-30

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Updated the `golang.org/x/{net,oauth2,sys}` dependencies to their latest releases;
  this raises the minimum Go toolchain to 1.25 (`go.mod` and the Dockerfile builder).
- Added a hardened `securityContext` (pod and container) to the deployed manifest:
  `runAsNonRoot`, non-root uid/gid, `seccompProfile: RuntimeDefault`,
  `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, and dropped
  capabilities.
- Pinned the distroless and chainguard base images by digest for reproducible builds.
- `--metrics-addr` now defaults to `127.0.0.1:8080` and `--enable-leader-election`
  defaults to `true`.
- Aligned the AIO manifest image and `VERSION` with the current release.

### Fixed

- `make test` now installs envtest assets via `setup-envtest`, fixing the build on
  arm64 (Apple Silicon / linux-arm64).

## [1.3.6] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Log levels cleaned up:
  - **info** (default): `registered dynamic component watch` (with GVK) and errors only.
  - **debug** (`--zap-log-level=debug`): adds an `application status updated` line —
    `componentsReady: old -> new` — emitted only when an Application's status actually
    changes, not on every no-op reconcile.
  - **trace** (`--zap-log-level=2`): the framework's internal `Starting EventSource`
    chatter (moved here from debug; it can't name the kind, so it's noise).

## [1.3.5] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Dynamic component watches are now scoped to the workload kinds that drive the Ready
  condition (Deployment, StatefulSet, ReplicaSet, ReplicationController, DaemonSet, Job,
  CronJob, Rollout). Previously the controller started a cluster-wide informer for every
  declared `componentKind` — including Secret, ConfigMap, ServiceAccount, HPA,
  NetworkPolicy, PrometheusRule, ServiceMonitor — which cached unnecessary objects (e.g.
  every Secret) in memory and fired reconciles for changes that can't affect the
  Application's status. Non-workload components are still tracked in `ComponentList` via
  the cache resync; they just no longer get a real-time watch they don't need.

## [1.3.4] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Silenced the framework's `Starting EventSource` log flood. With one dynamic watch per
  component kind, controller-runtime emitted a `Starting EventSource` INFO line per watch
  — and because every dynamic source is an `*unstructured.Unstructured`, the line couldn't
  even name the kind. The controller now gates the framework's controller logger at V(1)
  (via `WithLogConstructor`), so that chatter is hidden at default INFO. Our own
  `registered dynamic component watch` line — which carries the GVK — is restored to INFO
  using the manager's ungated base logger, so you can still see exactly which kinds are
  watched.

## [1.3.3] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Moved the `registered dynamic component watch` and `NoMappingForGK — skipping` log
  lines from V(1) to V(2) (trace), so they stay silent even at `--zap-log-level=debug`
  and only appear at `--zap-log-level=2`.

## [1.3.2] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- Demoted the `registered dynamic component watch` and `NoMappingForGK — skipping` log
  lines to V(1) so they no longer flood the default INFO log. They are visible with
  `--zap-log-level=debug`.

## [1.3.1] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Changed

- **deploymentStatus and rolloutStatus revert to trusting Kubernetes' own status
  strictly.** 1.3.0 had added compensation for scale-up flaps — a Deployment was treated
  as serving when `availableReplicas>0` even if the `Available` condition was absent or
  `False`, and a Rollout with an empty `status.phase` fell back to `availableReplicas`.
  Investigation showed the `AppDegraded`-on-scale-up flap was caused by the workload's own
  rollout strategy (`maxUnavailable: 0`), which makes kube report `Available=False` on
  every scale-up by design; setting `maxUnavailable: 1` removes the flap with no controller
  change. The controller should not second-guess kube's availability verdict, so the
  compensation is removed: `deploymentStatus` is Ready only when `Available=True` (or
  scaled to zero) and `rolloutStatus` is phase-only again. The fix for such flaps belongs
  in the workload's `maxUnavailable`/rollout strategy.

### Added

- `--zap-log-level` (and the other zap flags) are now bound, so log verbosity is
  controllable at runtime (e.g. `--zap-log-level=debug`).

## [1.3.0] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Added

- **Real-time reconcile on component changes via dynamic watches.** The controller now
  registers a watch for each Application's `spec.componentKinds` at runtime (deduped per
  GVK), so a change to a matched component — a Deployment's status during a scale-up, a
  pod failing, a workload recovering — enqueues a reconcile of the owning Application
  within seconds, instead of waiting up to 120s for the cache resync (`--sync-period`).
  Component kinds are resolved dynamically from each Application's config (not a static
  list), so arbitrary CRD kinds such as Argo `Rollout` are watched the same way as
  built-in workloads. A healthy scale-up still produces no status change (see ADR-0003);
  the faster trigger matters for genuine degrades/recoveries. The resync remains as a
  backstop. See [ADR-0004](docs/adr/0004-dynamic-component-watches.md). Proven by an
  envtest that sets `SyncPeriod` to one hour and asserts a component-status change still
  reconciles the Application within seconds.

## [1.2.1] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Fixed

- **The actual prod scale-up flap: `deploymentStatus` gated "serving" on the
  `Available` *condition* alone, ignoring `availableReplicas`.** During an HPA scale-up
  kube updates `status.availableReplicas` first and writes the `Available` condition a
  beat later. In that window the Deployment had replicas serving (`availableReplicas>0`)
  but no `Available` condition yet, so the controller read it as not-available and
  reported `InProgress` — flapping the Application to degraded on every scale-up. This is
  the flap that survived 1.2.0 (1.2.0 fixed the replica-count and generation-skew gates
  but still trusted the condition exclusively). A Deployment is now treated as serving if
  the `Available` condition is `True` **or** `availableReplicas>0` and the condition is
  not explicitly `False`; a genuine `Available=False` still reports `InProgress`.
  The other workload kinds (StatefulSet, ReplicaSet, ReplicationController, DaemonSet)
  already decide on replica counters rather than the condition, so Deployment was the
  only affected kind.

  Crucially, this is covered by a new **envtest** spec (`scaleup_envtest_test.go`) that
  drives a real Deployment through a real apiserver with the `Available` condition
  absent — the prod state. It fails on the pre-fix code and passes on the fix. The prior
  unit tests missed the bug because they always set an `Available` condition explicitly,
  so the real "condition-not-yet-published" window was never exercised.

## [1.2.0] - 2026-06-25

> **⚠ Known bug (rollout false-negative):** a down Argo Rollout can be reported `Ready` — see the Unreleased `Fixed` entry. Introduced in 1.2.0; fixed post-1.4.1.

### Fixed

- **Workload readiness no longer flaps on HPA scale-up.** Scalable workloads are now
  reported `Ready` when they are *serving*, not when they have reached their full
  desired replica count. Previously an HPA scale-up (e.g. 2→3) flipped the workload —
  and therefore the Application's `Ready` condition — to `InProgress`/degraded for the
  ~90s the new pod needed to pass its readiness probe, which monitoring read as a false
  incident. Applies to Deployment, StatefulSet, ReplicaSet, ReplicationController, and
  DaemonSet (node-join). Scaled-to-zero still resolves to `Ready`, and genuine failures
  (nothing available / `ReplicaFailure` / unavailable pods) still report `InProgress`.
  See [ADR-0003](docs/adr/0003-scale-up-readiness-flap.md) (Accepted). _(live on image tag
  `25171c3`; `main` rebuilt as `1f3ab67`)_

- **Scale-up flap, part 2: removed the `ObservedGeneration` gate from the readiness
  predicate.** The first fix (`25171c3`) still required `status.observedGeneration ==
  metadata.generation` to report `Ready`. An HPA scale-up bumps `generation` instantly
  while the workload controller writes `observedGeneration` a beat later; in that window
  `Available` was still `True` but the generation clause was false, so the workload was
  reported `InProgress` and the Application was paged as degraded anyway (observed in production
  2026-06-25 on a build containing `25171c3`). The clause is removed from all five kinds;
  genuine failure is still caught by `Available=False` / `ReplicaFailure` / unavailable
  pods. Regression specs now reproduce the generation-skew window for every kind.

- **Argo Rollouts no longer flap on scale-up / canary steps.** `rolloutStatus` mapped
  `status.phase: Progressing` and `Paused` to `InProgress`, so a Rollout scaling up (HPA
  or replica bump) or stepping through a canary/blue-green rollout degraded its
  Application even while it kept serving (`Available` condition `True`) — the same false
  positive as the Deployment case, on the kind production actually runs on (~27 Rollouts).
  `Progressing` and `Paused` now map to `Ready`; only `Degraded`/`Error` report
  `InProgress`. Additionally, a **scaled-to-zero Rollout** (`spec.replicas=0`) is now
  `Ready` regardless of phase, so a parked Rollout with a `Degraded`/`InvalidSpec` phase
  (e.g. `example-parked-rollout`) doesn't degrade its app. Adds the first `rolloutStatus` unit
  specs (the kind previously had none). See [ADR-0003](docs/adr/0003-scale-up-readiness-flap.md).

### Changed

- Test suites are envtest-only: removed the `UseExistingCluster` option from both the
  `controllers` and `e2e` suites and added a `127.0.0.1` host guard so no test can run
  against a real cluster / the current kubeconfig.

### Internal

- Repaired the `controllers` envtest suite so it runs: `CreateController` now applies a
  unique `.Named(name)` (previously the name argument was unused, causing a
  controller-name collision on the second spec); `StartTestManager` no longer asserts on
  the manager's shutdown error; and the stale ownerReference spec was rewritten to assert
  the controller does **not** mutate owner references (mutation was disabled in an earlier
  change).
- Added `controllers/status_test.go` with unit coverage for all five scalable workload
  kinds.

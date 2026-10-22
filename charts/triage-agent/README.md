# triage-agent Helm chart

Installs the **triage agent**: the in-cluster agent that observes workloads
and streams their state to the triage core over an outbound WebSocket (no
inbound API access needed). It groups workloads into applications from
workload labels and annotations (virtual apps), watches Kubernetes Warning
events, and runs the per-node eBPF/exporter DaemonSet for RED golden signals,
USE saturation and log metrics. Workload owners steer everything with
`triageagent.dev/*` annotations.

## Install

```bash
helm install app charts/triage-agent -n triage --create-namespace
```

Applications are synthesized from workload labels and `triageagent.dev/*`
annotations. See [Virtual apps](#virtual-apps).

## Core connection

```bash
helm upgrade --install app charts/triage-agent -n triage \
  --set core.host=<core-host> \
  --set core.clusterName=<cluster> \
  --set 'target.namespaces={<namespace>}' \
  --set target.tenant=<tenant> \
  --set core.caFingerprint=<SHA256:... from `triagectl agent list`>
```

An agent authenticates to the core only by a join certificate: an admin-approved certificate request or a one-time agent key, pinned to the core CA by `core.caFingerprint` (`--join-ca-fingerprint`). The chart requires it when `core.host` is set, and the binary refuses to start without it whenever a core endpoint is set, in a pod and in Linux host mode alike.

After install the agent logs `join pending` with its key fingerprint; approve it on the core with `triagectl agent approve <cluster> --fingerprint <agent fingerprint>`. The key and certificate are kept in a Secret in the release namespace, so a new pod needs no new approval.

**With a one-time agent key** (core ADR-090) there is no approval step. The core mints the key and returns the install command with `core.host`, `core.caFingerprint`, `core.clusterName`, `core.tenants` and `core.agentKey=tak_...`. Set no `target.*` value: the agent runs in cluster mode and each namespace maps to a tenant by its `triageagent.dev/tenant` annotation. The chart writes the key into the join Secret (`core.storage: secret`), the core issues the certificate on the first join, and the agent then removes the key. A key works once, for that cluster name and those tenants.

An agent that watches no Kubernetes objects (`kubernetes.watch: false`, OTLP only) files every frame under one tenant. With no `target.tenant` the chart takes it from `core.tenants` when that holds exactly one entry.

The core sends each agent a daily cap on Kubernetes events per tenant. The agent holds its events to it and counts what it drops. App deltas and metric batches are never held back.

Upgrading an install that used `push.token` or `push.existingSecret`: both values are gone. Read the core CA fingerprint from `triagectl agent list` on the core, set `core.caFingerprint`, upgrade, then approve the join as above. The old token Secret can be deleted afterwards.

## Virtual apps

`virtualApps` groups workloads into applications with no CRs and no manifests,
purely from labels and annotations: `"off"` does nothing, `"on"` synthesizes
and pushes the result. Requires `core.host` when set explicitly. Left
unset, the chart's own default, the container gets no `--virtual-apps` flag
and the binary's own default applies, which is `"on"`.

```bash
helm upgrade --install app charts/triage-agent -n triage --reuse-values \
  --set virtualApps=on
```

Workload owners override the grouping per workload or per namespace with
`triageagent.dev/virtual-app`, `triageagent.dev/domain`,
`triageagent.dev/description` and `triageagent.dev/tier` annotations.

## Excluding a workload from triage

`triageagent.dev/triage: "false"` takes a workload out of triage entirely.
The agent then sends the core nothing about it: no virtual app, no Kubernetes
Warning events for its pods, ReplicaSets or owners, no log, RED or USE entries
(including OOM kills and memory pressure), and no topology nodes or edges.
Use it for test-traffic tools such as a load generator whose own restarts are
expected.

Set it on any of:

- the workload's own `metadata.annotations` (Deployment, StatefulSet,
  DaemonSet, Argo Rollout, Job, CronJob);
- its pod template's annotations, which also keeps eBPF from instrumenting
  the pods, exactly like `triageagent.dev/instrument: "false"`;
- the Namespace, which covers every workload in it and also stops the eBPF
  and log-exporter roles from instrumenting or tailing it. A workload
  annotated `"true"` does not override a Namespace `"false"`, the same rule
  as `triageagent.dev/virtual-app`.

Only the exact value `"false"` excludes. Changes apply live: annotating a
running workload drops its signals from the next informer update on, and
removes its virtual app from the core.

```bash
kubectl -n <namespace> annotate deployment load-generator triageagent.dev/triage=false
```

## Log-based metrics

Ships a native log-exporter container (`logExporter.enabled`, a third container on
the agent DaemonSet, see [ADR-0014](../../docs/adr/0014-log-exporter-in-agent-ds.md))
that tails container logs and counts error/warn lines per (namespace, service),
exposing them as Prometheus counters. The controller scrapes that container,
computes per-interval deltas, and forwards services whose error delta OR warn delta
crosses `errorThreshold` (either counter independently qualifies) to triage as
`log_metrics` frames over the push connection — see
[ADR-0006](../../docs/adr/0006-log-based-metrics.md). Requires `core.host`.

```bash
helm upgrade --install app charts/triage-agent -n triage --reuse-values \
  --set logExporter.enabled=true \
  --set logMetrics.enabled=true
```

`logExporter.enabled=true` auto-wires the scraper at the bundled exporter's Service.

### External metrics sources

Any Prometheus endpoint emitting the same counter names — an existing Fluent Bit, a
custom exporter — plugs in instead of the native exporter: set only
`logMetrics.enabled=true` and point `logMetrics.serviceName`/`logMetrics.port` at it.

## OTLP receiver

For clusters that already run OpenTelemetry, or that cannot run the eBPF
DaemonSet, the controller accepts OTLP metrics over gRPC from SDKs and
Collectors (`otlp.enabled`, port 4317 on the `<fullname>-sensors` Service,
[ADR-0017](../../docs/adr/0017-otlp-receiver.md)). It feeds the same
collectors as the DaemonSet, routed by metric name:

| Metric names | Collector |
|---|---|
| `http.server.request.duration`, `rpc.server.call.duration` and the client histograms | RED, SIG and L7 topology |
| `obi.network.flow.bytes` | L4 topology |
| `use.*` | USE |
| `log.errors`, `log.warns`, `log.lines` | Logs-based metrics |

Points need `service.name` and `k8s.namespace.name` (resource or point
attributes) and cumulative temporality. Older conventions
(`http.server.duration` in ms, `http.status_code`, numeric
`rpc.grpc.status_code`) are converted. The workload comes from
`k8s.owner.name`, or from the `k8s.deployment.name` family that the
Collector's `k8sattributes` processor adds.

The receiver serves the controller's own certificate; copy `ca.crt` from the
`<fullname>-sensor-ca` ConfigMap to the sender. Senders authenticate with a
client certificate that chains to `otlp.clientCASecret` (mutual TLS), or a
projected ServiceAccount token, audience `triage-agent-otlp`, of an account
in `otlp.allowedServiceAccounts`. Set at least one.

A Collector in another cluster authenticates with a client certificate whose
URI SANs name its tenants, `spiffe://<domain>/tenant/<name>` (one or more).
Its data is placed in those tenants: the resource's `triage.tenant` attribute
picks one when the certificate names several. A tenant must also be one of
`core.tenants`, so adding one is a join tenant plus the core's approval;
the certificate cannot widen it. Its namespaces need not exist in this cluster
(see ADR-0017's amendment for the details).

A sender is bound to its identity: a token sender may publish only for the
namespace of its ServiceAccount (points claiming another
`k8s.namespace.name` are refused and counted per sender; a missing one is
filled in), and a client certificate with a SPIFFE URI SAN
`spiffe://<domain>/ns/<namespace>/sa/<name>` likewise. Accounts listed in
`otlp.forwarders` (also in `allowedServiceAccounts`), and certificates
without a SPIFFE namespace, may publish for any namespace: use that for a
Collector gateway that adds the namespace with `k8sattributes`.

```bash
helm upgrade --install app charts/triage-agent -n triage --reuse-values \
  --set otlp.enabled=true \
  --set otlp.clientCASecret=otel-client-ca \
  --set redMetrics.enabled=true --set topology.enabled=true
```

Collector exporter, mutual TLS:

```yaml
exporters:
  otlp/triage:
    endpoint: <fullname>-sensors.<namespace>.svc:4317
    tls:
      ca_file: /etc/otel/triage/ca.crt      # from <fullname>-sensor-ca
      cert_file: /etc/otel/certs/client.crt  # chains to otlp.clientCASecret
      key_file: /etc/otel/certs/client.key
```

Token instead of a client certificate: mount a projected token with
audience `triage-agent-otlp` and use the `bearertokenauth` extension with
its `filename`.

## Local TUI

`ui.listen` (for example `127.0.0.1:8088`) serves the agent's TUI on a loopback
address ([ADR-0019](../../docs/adr/0019-local-tui.md)); reach it with
`kubectl port-forward deploy/<fullname> 8088:8088`. Needs `core.host`. Every
replica shows the leader's view when the replica gossip is on.

For a login, put an admin key (at least 16 bytes) in a Secret and name it:

```bash
kubectl -n <ns> create secret generic triage-agent-ui \
  --from-literal=admin-key="$(openssl rand -base64 33)"
helm upgrade triage-agent charts/triage-agent --reuse-values \
  --set ui.listen=127.0.0.1:8088 --set ui.adminKey.existingSecret=triage-agent-ui
```

The key is mounted as a file and read on each login, so editing the Secret
rotates it and ends every session. Without `ui.adminKey.existingSecret` the TUI
has no login and the loopback bind is its only guard.

## Monitoring API

`api.listen` (for example `:9480`) serves Prometheus `/metrics`, the discovery
document `/.well-known/triage-agent` and the read-only JSON API
([ADR-0020](../../docs/adr/0020-monitoring-api.md)). It is off by default and
never unauthenticated: the chart refuses `api.listen` without
`api.tokens.existingSecret`.

```bash
printf 'prom * %s\ngrafana-shop shop %s\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" > tokens
kubectl -n <ns> create secret generic triage-agent-api --from-file=tokens=tokens
helm upgrade triage-agent charts/triage-agent --reuse-values \
  --set api.listen=:9480 --set api.tokens.existingSecret=triage-agent-api
```

A line is `name tenant token`; tenant `*` is unrestricted, any other tenant limits
the token to it (the agent forces the `tenant` query and hides other tenants,
the node-wide sensors and the core address). Scrape with
`Authorization: Bearer <token>` (Prometheus `authorization.credentials`). Only
the leader replica exports signal series, so scrape the `<fullname>-api`
Service. Set `api.tls.existingSecret` (a `kubernetes.io/tls` Secret) to serve
TLS; plain HTTP sends the tokens in the clear. `api.serviceMonitor.enabled` adds a
ServiceMonitor that reads the scrape token from `api.serviceMonitor.tokenSecret`.

## eBPF privilege

The DaemonSet runs every sensor role in one container, so they share one
security context. `ebpf.privileged` picks one of two modes.

| | `ebpf.privileged: false` (default) | `ebpf.privileged: true` |
|---|---|---|
| `privileged` | `false` | `true` |
| Capabilities | drop `ALL`; add `BPF`, `PERFMON`, `NET_RAW`, `SYS_PTRACE`, `DAC_READ_SEARCH`, `CHECKPOINT_RESTORE`, plus `ebpf.extraCapabilities` | all |
| Privilege escalation | blocked | not restricted |
| Seccomp | `RuntimeDefault` | not set |
| Root filesystem | read-only | read-only |
| User | 0 | 0 |

Both modes run with `hostPID: true`, which OBI needs to hook other
containers' processes.

When to widen the default (ADR-0016):

- `ebpf.extraCapabilities: [SYS_ADMIN]`: Go services on a kernel that refuses
  Go uprobes without it. On GKE COS 6.12 every Go uprobe fails with
  "opening perf event: permission denied" and Go services report no RED.
- `ebpf.extraCapabilities: [SYS_RESOURCE]`: kernels before 5.11.
- `ebpf.privileged: true`: only when the kernel cannot load OBI's programs
  under the default plus the capabilities above, with the cluster owner's
  agreement.

Try the default first and add one capability at a time.

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `registry.invalid/example-shared-registry/triage-agent` (resolves to no registry; set your own) | Controller image |
| `image.tag` | `""` (→ `appVersion`) | Image tag |
| `replicaCount` | `1` | Replicas. Above 1, leader election is on and a PodDisruptionBudget keeps one replica up; with the sensor stream, replicas gossip what their sensors send (ADR-0015 phase 4) |
| `namespace` | `""` | Restrict cache to one namespace; empty = cluster-wide |
| `syncPeriod` | `120` | Manager cache resync seconds |
| `workloadResources` | `[]` | GVRs (`<group>/<version>/<resource>`) virtual-app synthesis watches; one `--workload-resources` flag per entry. Empty uses the binary's built-ins (`apps/v1/deployments`, `apps/v1/statefulsets`, `apps/v1/daemonsets`, `argoproj.io/v1alpha1/rollouts`); a non-empty list replaces them entirely |
| `leaderElection.enabled` | `false` | Enable leader election with one replica; always on above 1 |
| `metrics.enabled` | `false` | Serve the metrics endpoint |
| `metrics.bindAddress` | `127.0.0.1:8080` | Metrics bind address when enabled |
| `ui.listen` | `""` | Serve the local TUI on this loopback address (e.g. `127.0.0.1:8088`); without `core.host` it runs standalone |
| `ui.maxEvents` | `100` | How many of the newest events the TUI and the API keep and show (10 to 5000) |
| `ui.adminKey.existingSecret` | `""` | Secret holding the TUI's admin key (key `ui.adminKey.secretKey`, default `admin-key`); set = login required |
| `api.listen` | `""` | Serve the monitoring API on this address (e.g. `:9480`); needs `api.tokens.existingSecret` or `api.serviceAccountAuth`; without `core.host` it runs standalone |
| `api.tokens.existingSecret` | `""` | Secret with the API tokens, one `name tenant token` per line (key `api.tokens.secretKey`, default `tokens`) |
| `api.tls.existingSecret` | `""` | `kubernetes.io/tls` Secret for the API; empty serves plain HTTP |
| `api.serviceMonitor.*` | off | `enabled`, `interval`, `tokenSecret.{name,key}`: a ServiceMonitor scraping `/metrics` |
| `core.host` | `""` | Triage core hostname; set turns the connection on (`push.*`, the old name of `core.*`, is still read with `push.enabled=true`) |
| `core.agentKey` | `""` | One-time agent key from the core's install command; the join is approved at once (needs `core.storage: secret`) |
| `core.endpoint` | `""` | Full WebSocket URL, overrides `core.host` |
| `core.clusterName` | `""` | Cluster name stamped into events (required with `core.host`) |
| `target.tenant` | `""` | Tenant for namespaces in scope that carry no `triageagent.dev/tenant` annotation; annotated namespaces always use their annotation's tenant. Declared in the hello. Empty means annotated namespaces only (cluster mode); a key install leaves it empty |
| `target.namespaces` | `[]` | Namespaces to observe. Empty means the `triageagent.dev/tenant`-annotated set, not every namespace; an explicit list unions with the annotated set |
| `core.heartbeatSeconds` | `20` | Heartbeat interval |
| `core.insecureSkipVerify` | `false` | Skip TLS cert verify for `wss://` (dev only) |
| `core.allowPlaintext` | `false` | Allow plaintext `ws://` endpoint (traffic unencrypted) |
| `core.caFingerprint` | `""` | Core agent CA fingerprint, from the install command the core gives; required with `core.host` (the agent joins by certificate) |
| `core.tenants` | `[]` | Tenants the join asks for; empty means `target.tenant` alone. A cluster-mode agent lists every tenant it serves. A single entry is also the tenant of an agent with `kubernetes.watch: false` |
| `core.storage` | `secret` | Where the agent keeps its key, certificate and the saved core CA: `secret`, `emptyDir` (a new join for each pod) or `pvc` |
| `core.existingClaim` | `""` | PersistentVolumeClaim for `core.storage: pvc` |
| `virtualApps` | `""` (binary default: `on`) | Label-derived app synthesis: `off` or `on` (requires `core.host` when set explicitly) |
| `logMetrics.enabled` | `true` | Log-metrics collector; runs with `core.host` and a source (`logExporter.enabled` or `otlp.enabled`) |
| `logMetrics.serviceName` | `""` | Service fronting the metrics source's exporter pods; empty auto-wires to `logExporter`'s Service when `logExporter.enabled=true`, else set explicitly for an external source |
| `logMetrics.port` | `2021` | Prometheus exporter port |
| `logMetrics.intervalSeconds` | `60` | Scrape + gate-evaluation interval |
| `logMetrics.errorThreshold` | `10` | Minimum error-count OR warn-count delta per interval to report a service (either counter independently qualifies) |
| `logMetrics.errorMetric` / `warnMetric` / `totalMetric` | `log_metric_counter_log_{errors,warns,lines}_total` | Prometheus counter family names the source emits |
| `logMetrics.namespaceLabel` / `serviceLabel` / `serviceLabelFallback` | `namespace` / `service` / `service_fallback` | Label keys identifying each sample |
| `logMetrics.sampleLabel` | `sample` | Label key on error-counter series holding the fingerprinted log line (ADR-0007); empty disables `Sample` population |
| `logExporter.enabled` | `false` | Deploy the native log-exporter container (third container on the agent DaemonSet, ADR-0014) |
| `logExporter.port` | `9466` | Exporter port (must match `logMetrics.port` if scraping it) |
| `logExporter.includeNamespaces` | `[]` | Namespaces to tail; empty falls back to `target.namespaces`, then all namespaces |
| `logExporter.errorRegex` / `warnRegex` | see values.yaml | Regex matched against each log line to classify it |
| `logExporter.sampleMaxLen` | `160` | Fingerprint truncation length (collector additionally caps the wire field at 256) |
| `logExporter.maxSamplesPerService` | `8` | Cap on distinct error fingerprints tracked per (namespace, service) before falling into an unlabeled catch-all series |
| `logExporter.sampleJsonFields` | `[message, msg, error, err]` | JSON/logfmt field priority list for extracting the sample text |
| `serviceAccount.create` | `true` | Create SA + RBAC |
| `kubernetes.watch` | `true` | `false` runs the agent on OTLP and sensor metrics alone (ADR-0021): no Namespace, Service, Ingress, workload or Event informers, no virtual apps, and no ClusterRole or ClusterRoleBinding, so it installs with namespace-scoped rights. Needs `core.host` and one tenant, `target.tenant` or `core.tenants` with exactly one entry (every frame files under that tenant). An empty `target.namespaces` then means every namespace the authenticated senders report |
| `ebpf.enabled` | `false` | Deploy the eBPF DaemonSet (one agent container running the ebpf role, plus the node-exporter/log-exporter roles when `useMetrics.nodeExporter`/`logExporter.enabled` are also set), using the same image as the controller (`image.repository`:`image.tag`) and the same agent binary, with `--mode` repeated per role |
| `ebpf.excludeWorkloads` | `[]` | `k8s_owner_name` glob excludes rendered into the OBI discovery config (e.g. `["*synthetic*"]`) |
| `ebpf.excludePaths` | `[]` | `exe_path` glob excludes (e.g. `["*ms-playwright*"]`); prefer the `triageagent.dev/instrument` pod annotation, this is the coarse fallback |
| `ebpf.metaRestrictLocalNode` | unset | OBI's Kubernetes metadata informer scope. Unset follows `ebpf.networkFlows.enabled`: node-local when flows are off, cluster-wide when flows are on (flow peers need pod identity beyond the local node). Set `true`/`false` to override either default. A cluster-wide informer's memory cost scales with total cluster pod count — size `ebpf.resources.limits.memory` accordingly |
| `ebpf.excludeOtelInstrumentedServices` | `false` | Instrument a process that already ships its own OTel SDK too (OBI's `discovery.exclude_otel_instrumented_services`). This chart's metrics endpoint is scraped only by the agent itself, so covering that process here does not duplicate anything in the customer's own telemetry backend. Set `true` to restore OBI's own default: that process then gets no RED metrics, only a `target_info` entry |
| `resources` | `requests: 50m/64Mi`, `limits: 200m/512Mi` | Controller container CPU/memory requests/limits |

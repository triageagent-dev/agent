*Specification · agent repository · v3.3.0 · 0a7ba07*

# Triage Agent

The **collection and aggregation layer** of Triage: an agent on **Kubernetes** clusters, **Linux** hosts and **cloud provider connectors**. The agent discovers **virtual apps** from workloads, measures them with **eBPF** , **cgroup** and **logs-based metrics** as **USE** , **RED** , **SIG** and **LOG** frameworks, and streams the result to the **core** over one **outbound secure WebSocket (WSS)**. It sends signals only; drift, issues and root cause are decided in the **core**.

_Diagram: Agent architecture: kernel and host sources feed three sensor roles on each node, which stream to sensorstream; push sends frames to the core._

Figure 1. Components and boundaries.

## 01 Architecture

Data crosses three boundaries: from kernel and host files into the sensor roles, from the node into `sensorstream` over gRPC, and from the cluster to the core over a secure WebSocket (WSS) the agent opens. Nothing outside the cluster can open a connection into it.

The sensor roles send one OTLP snapshot per role every `--sensor-tick`, default 15 s. `sensorstream` keeps the latest per node and serves it to the collectors through `metricset.Source`. Collectors are `manager.Runnable`s that run only on the leader, and only the leader holds the connection to the core. The controller can run several replicas: every replica takes sensor streams and gossips what its own sensors send to the others over mutual TLS (headless Service `<fullname>-peers`, port 9471, pinned to the same sensor certificate), keeping the newest snapshot per node. The leader therefore sees every node whichever replica a sensor connected to, and a newly elected leader already holds the state.

The agent is one program, run in two shapes, and it ships as two images (below). The **DaemonSet** hosts the **sensor roles** ; the **controller Deployment** hosts everything that talks to the **Kubernetes API** or the **core**. A namespace is observed only when it is annotated with a tenant or listed in `--target-namespaces`, and every frame leaves stamped with that tenant.

**Images.** `agent` is built without OBI: it links no OBI package, needs no generated eBPF bindings, is about 40 percent smaller (54 MB against 93 MB for linux/amd64), and fails fast on `--mode role ebpf`. It runs the controller and the OTLP role, so an agent that only reads OTLP needs this image alone. `agent-obi` is built with the OBI instrumenter and runs the eBPF role of the DaemonSet (`image.ebpf.repository`, default `<image.repository>-obi`). Both are published with the same tags: `v<major>.<minor>.<commits since the last release>-<sha>` for every build of `main`, and `v<X.Y.Z>`, `X.Y.Z`, `X.Y` and `X` for a release. The chart is published to `oci://ghcr.io/<owner>/charts/triage-agent` on a release.

Collectors are off by default. Virtual apps and Kubernetes Warning events flow as soon as the agent is connected to a core (`core.host`); RED, USE, log and topology collectors each need their own flag and the sensor stream.

Out of scope for the agent: comparing the observed topology with the declared graph, opening issues, and any model call. The core does those.

### High availability

The controller runs as several replicas behind one leader. Leader election is on whenever `replicaCount` is above 1, through a Lease named `triage-agent` in the agent's namespace. Every replica serves the sensor stream and the OTLP receiver; only the leader runs the collectors and holds the connection to the core.

Concern| Behaviour  
---|---  
Intake| Sensors and OTLP senders reach any replica through the `<fullname>-sensors` Service; losing a replica drops only the streams it held, which reconnect to another within the 1 to 30 s backoff  
Shared state| Replicas gossip every sensor snapshot and OTLP source over mutual TLS on 9471, newest wins, so a new leader already holds the latest data from every node and sender  
Failover| controller-runtime defaults: a 15 s lease, renewed every 10 s, retried every 2 s. The lease is not released on shutdown, so a new leader takes over when it expires: within about 15 s for a crash or a rollout alike  
Core link| The new leader opens the connection with the same join certificate, kept in `--join-secret`, so it needs no new key and no new approval, and resends each tenant's virtual-app snapshot  
Baselines| Held in the leader's memory and not gossiped. After a failover every service is cold until it has the minimum windows again; the cold floors keep reporting a clear breach in that time  
Disruptions| A PodDisruptionBudget with `maxUnavailable: 1` when replicas are above 1, so a drain takes one controller at a time  
Placement| No anti-affinity by default; set `affinity` to spread replicas across nodes or zones  
  
A single replica runs without leader election and without a PodDisruptionBudget; a restart is a gap in intake and frames until the pod is back.

### Linux host mode

With `--mode=host` the agent runs as one systemd service on a VM or bare-metal host, with no Kubernetes API. The host reports the same frames as a cluster, so the core and the protocol are unchanged: a host is a cluster of one with one synthetic namespace. There is no controller Deployment, no DaemonSet, no sensor stream and no listener unless `--ui-listen` or `--api-listen` is set (section 11); the sensor roles run in the agent's own process and the collectors read them directly.

In a cluster| On a host  
---|---  
Cluster| The OS host name; `--cluster-name` overrides it. A host installed with a one-time key uses the name the core minted  
Namespace| One synthetic namespace, `--host-namespace`, default `host`. Hosts running the same services share one value, so their services merge in the core graph  
Tenant| `--target-tenant`, required; tenant mode only  
Virtual app| A systemd unit selected by `--host-units` globs, default every enabled service except system units; Ready when `ActiveState=active` and `SubState=running`, through the same flap damping  
USE| The unit's cgroup under `system.slice`, with the same readers, PSI included; limits from the unit's own `cpu.max` and `memory.max`  
LOG| journald by `_SYSTEMD_UNIT`, through `journalctl --follow`; priority 0 to 3 is an error, 4 a warning, then the same regexes and redaction  
RED| OBI with Kubernetes off, one discovery entry per unit selecting every PID in its cgroup; series are stamped with the unit and the host namespace from `/proc/<pid>/cgroup`. A new PID in a unit restarts the instrumenter, a short RED gap  
Topology| A callee is named only by `server.address`, so every edge goes to an external host, even another unit; L4 flows carry addresses only  
Identity| The same join certificate, by one-time key or admin approval (section 06); `--join-ca-fingerprint` is required. Key and certificate kept in `--join-state-dir`  
HA| None: one agent per host, and a second one would duplicate every frame  
  
Install is a binary or a deb or rpm package with `triage-agent.service`, which reads its flags from `/etc/triage-agent/env` and runs as root with the same six capabilities as the DaemonSet. `install.sh`, published with each release, installs the package from the command the core returns for a host key: it writes the env file, puts the key in `enroll.key` in the join state directory (mode 0600) and starts the service. The `hello` carries the capability `host`. The acceptance steps have not yet been run on a real host.

## 02 Scope and tenants

Each item the agent sees, whether a workload, an event or a metric series, is resolved to a tenant through its namespace before it is queued. An annotated namespace always goes to its annotation's tenant. Tenant mode adds a primary tenant for namespaces listed in `--target-namespaces`; cluster mode works from annotations alone.

_Diagram: Tenant resolution in the two modes. Tenant mode: namespaces from the list and annotations; annotated ones go to their annotation's tenant, listed-only ones to the primary tenant. Cluster mode: annotated namespaces only, each to its annotation's tenant. A Linux host takes its tenant from --target-tenant._

Figure 2. Tenant resolution. An annotated namespace always goes to its annotation's tenant. Items opted out with `triageagent.dev/triage: "false"` are never sent. A Linux host has no namespaces, so `--target-tenant` sets its tenant. In cluster mode a namespace listed in `--target-namespaces` without the annotation has no tenant; its items are skipped and counted.

| Tenant mode| Cluster mode  
---|---|---  
Selected by| `--target-tenant` set| `--target-tenant` empty; `--cluster-name` is the identity  
Authentication| Per cluster, by join certificate; the hello names the primary tenant| Per cluster, by join certificate; the hello names no tenant  
Frame stamp| Primary tenant unstamped; other annotated tenants stamped| Every frame stamped  
Scope| `--target-namespaces` plus annotated namespaces| Annotated namespaces  
Listed, not annotated| Primary tenant| Skipped and counted  
Covers| Namespaces from the list and annotations; plain Linux hosts (`--mode=host`)| Annotated namespaces only  
  
An agent installed with a one-time key (section 06) runs in cluster mode. The key fixes the tenants it may serve, the chart sets no `target.*` value, and the core drops anything outside the key's bindings. An agent that watches no Kubernetes objects (`kubernetes.watch: false`) has no annotation to read, so every frame files under one tenant, `--target-tenant`. The chart takes it from `core.tenants` when that holds exactly one entry and `target.tenant` is empty; with several entries, set `target.tenant`.

### Annotations

Key| On| Effect  
---|---|---  
triageagent.dev/tenant| Namespace| Puts the namespace in scope and names its tenant  
triageagent.dev/triage: "false"| Workload, pod template, namespace| No virtual app, events, metrics or topology for it  
triageagent.dev/instrument| Pod| Excludes the pod from eBPF instrumentation  
triageagent.dev/virtual-app, -group, -identity| Workload, namespace| Names and groups the virtual app; namespace values are inherited  
triageagent.dev/tier, weight, domain, description| Workload, namespace| Describes it; `weight` is 0.1 to 10  
  
## 03 Objects and signals

### Objects

Objects the chart installs in the agent namespace.

Object| Kind| Purpose  
---|---|---  
triage-agent| Deployment| Controller; one or more replicas, the leader pushes to the core  
<fullname>-ebpf| DaemonSet| Sensor roles; `hostPID`; `hostNetwork` only with network flows  
<fullname>-peers| Service, headless| Port 9471, gossip between controller replicas, mutual TLS  
PodDisruptionBudget| PodDisruptionBudget| Keeps a controller replica up during voluntary disruptions, when replicas are above 1  
<fullname>-sensors| Service, ClusterIP| Port 9470  
<fullname>-otlp| Deployment and Service, ClusterIP| The OTLP sensor role, when `otlp.sensor.enabled`: two replicas by default, OTLP on 4317. With `otlp.insecure` the sensors serve plain gRPC; when `allowFrom` lists namespaces the chart also adds a NetworkPolicy that admits only those, otherwise it renders none  
OBI config| ConfigMap| Mounted as a file; not read through the API  
<fullname>-sensor-ca| ConfigMap, created empty| Controller writes `ca.crt`; sensors pin it  
<fullname>-sensor-tls| Secret, created empty| Controller certificate and key  
<fullname>-join| Secret| Ed25519 key, issued certificate, core CA, and the one-time agent key until the core has issued the certificate  
  
### Watched API types

The controller follows these Kubernetes API types through informers. All access is get, list and watch.

API type| Default| Used for| Ready when  
---|---|---|---  
apps/v1 deployments, statefulsets| Yes| Virtual-app discovery| `readyReplicas` ≥ `spec.replicas`  
apps/v1 daemonsets| Yes| Virtual-app discovery| `numberReady` ≥ `desiredNumberScheduled`  
argoproj.io/v1alpha1 rollouts| Yes| Virtual-app discovery| `readyReplicas` ≥ `spec.replicas`  
kagent Agent (AI agents)| As its Deployment| The kagent controller runs each Agent as a Deployment; the Agent resource itself is not watched| As a Deployment  
Any namespaced workload kind| Opt-in| Virtual-app discovery for custom types and vendor CRDs| A `Ready` or `Available` condition  
apps/v1 replicasets, batch/v1 jobs, cronjobs| Yes| Owner walk from a pod to the workload that names it|   
v1 pods| Yes| Pod to workload identity for resource metrics|   
v1 namespaces| Yes| Scope and tenant annotations|   
v1 services, networking.k8s.io/v1 ingresses| Yes| Service index and entry points|   
v1 events| Yes| Warning events, filtered server-side to `type=Warning`|   
  
Custom types are added with `workloadResources` (flag `--workload-resources`), one `<group>/<version>/<resource>` per entry, and a matching read rule in `rbac.extraWorkloadRules`. A non-empty list replaces the defaults, so keep the ones still needed. A type the API server does not serve is skipped at startup with one log line.
    
    
    workloadResources:
      - apps/v1/deployments
      - apps/v1/statefulsets
      - apps/v1/daemonsets
      - argoproj.io/v1alpha1/rollouts
      - helm.toolkit.fluxcd.io/v2/helmreleases   # Flux HelmRelease
    rbac:
      extraWorkloadRules:
        - apiGroups: ["helm.toolkit.fluxcd.io"]
          resources: ["helmreleases"]
          verbs: ["get", "list", "watch"]

### Signals

Five sources feed the agent. RED and topology come from OBI (OpenTelemetry eBPF Instrumentation, vendored at `third_party/obi`), which chooses its own hooks; the agent loads no eBPF program of its own.

Source| Read by| Method| Yields  
---|---|---|---  
Kernel hooks| ebpf| OBI uprobes and kprobes for HTTP and gRPC; socket filter for L4 when `ebpf.networkFlows.enabled`| RED per service; L7 and L4 edges  
cgroup v2, PSI| node-exporter| Host files, read only| USE per container  
Container log files| logs-based metrics| Tails from file end, polls every 250 ms; partial lines over 1 MiB dropped and counted| `log.errors`, `log.warns`, `log.lines`  
Kubernetes API| controller| Informers on namespaces and workloads; events filtered server-side to `type=Warning`| Virtual apps, Warning events  
OpenTelemetry SDKs and Collectors| controller| OTLP/gRPC push to port 4317, when `otlp.enabled`; routed by metric name| RED, SIG and L7 topology from semantic-convention histograms or span metrics; USE, with PSI, from kubelet cAdvisor or `use.*`; LOG from `log.*`  
  
### OTLP receiver

A cluster that already runs OpenTelemetry, or cannot run the DaemonSet, pushes OTLP metrics to the controller. The receiver keeps each resource's newest cumulative point per series and hands it to sensorstream, so the same collectors, baseline gates and replica gossip apply. Metric names route it: the HTTP, gRPC and database histograms and `obi.network.flow.bytes` go to RED and topology, `use.*` to USE, `log.errors`, `log.warns` and `log.lines` to LOG. Where a sensor and an OTLP sender report the same service or pod, the sensor's numbers are used and the OTLP ones dropped, so nothing is counted twice.

A Collector that has only traces, kubelet access and logs covers all three frameworks without the DaemonSet. The receiver translates two inputs it would otherwise ignore:

Collector input| Translated to| Notes  
---|---|---  
Traces through the `spanmetrics` connector| HTTP and gRPC server histograms (RED, SIG), client histograms (L7 edges)| Read from `traces.span.metrics.duration` by `span.kind` and `rpc.system`; milliseconds become seconds; a span without a status code counts as 200 or OK, an error span as 500 or UNKNOWN; internal spans and the calls counter are dropped. Needs `add_resource_attributes: true` so the service, not the Collector, owns the series  
Kubelet `/metrics/cadvisor` through the `prometheus` receiver| `use.*` per pod, node cores and memory| Pod-level cgroup series only. PSI is the rate of `container_pressure_*_waiting_seconds` as a percentage, the same meaning as the DaemonSet's `some avg`; it needs Kubernetes 1.34 or later and a ClusterRole with `get` on `nodes/metrics`  
Logs through the `count` connector| LOG| Count by severity into `log.errors`, `log.warns` and `log.lines`  
  
Points need `service.name` and `k8s.namespace.name` and cumulative temporality. Older conventions are converted: `http.server.duration` in milliseconds, `http.status_code`, a numeric `rpc.grpc.status_code`. The workload comes from `k8s.owner.name` or the `k8s.deployment.name` family that the `k8sattributes` processor adds. Delta sums and histograms are added into a running total per series; exponential histograms are refused and reported in the response. Limits (`otlp.limits`): 2000 resources in all, 5000 series each, 500 resources per sender, 4 MiB per request; a resource silent for 10 minutes is dropped.

The receiver serves the controller's certificate, which senders pin from `-sensor-ca`. A sender authenticates with a client certificate that chains to `otlp.clientCASecret`, which is the OTel exporter's own `tls.cert_file` and `key_file`, or with a projected ServiceAccount token, audience `triage-agent-otlp`, of an account in `otlp.allowedServiceAccounts`, checked by TokenReview.
    
    
    exporters:
      otlp/triage:
        endpoint: <fullname>-sensors.<namespace>.svc:4317
        tls:
          ca_file: /etc/otel/triage/ca.crt       # from -sensor-ca
          cert_file: /etc/otel/certs/client.crt  # chains to otlp.clientCASecret
          key_file: /etc/otel/certs/client.key

### OTLP sensor role

With `otlp.sensor.enabled` the receiver runs in its own Deployment, `--mode=otlp` of the same image, and the controller keeps no OTLP listener. A Collector then sends **raw** telemetry: no `spanmetrics`, `count`, `deltatocumulative` or `filter` component is needed, only `k8sattributes`, which has to run on the sender because it reads the API server, and an OTLP exporter. The pods transform what they receive:

Raw input| Made into| Notes  
---|---|---  
Server and consumer spans| HTTP and gRPC server duration histograms (RED, SIG)| gRPC is recognised from `rpc.system`, `rpc.system.name` or `rpc.grpc.status_code`. Internal spans are ignored  
Client and producer spans| Client duration histograms (L7 edges)| Same attributes as the spanmetrics path  
Log records| `log.errors`, `log.warns`, `log.lines` per namespace and service| From `severity_number`; counts only, no error sample  
Delta sums and histograms| Cumulative series| Added into the resource's running total; a reset or an out-of-order point never lowers a counter  
Metrics no collector reads| Dropped at intake| Before they take a slot against the limits, so an unfiltered Collector does not exhaust them. Counted as `unread` in the pod log and the ledger  
  
A service that reports its own request duration metric and also sends spans would be counted twice. The RED collector reads sources in the order sensors, native OTLP, then OTLP derived from spans, and skips a derived series whose service and metric a native source already reported. A pod's derived resources carry `triage.otlp.replica`, so the shares of one service that different pods saw stay separate sources that the collectors sum.

The receiver accepts exports compressed with gzip, zstd or snappy, the codecs a Collector's OTLP exporter can use (gzip by default); a gRPC server refuses a codec nobody registered, so the receiver registers all three itself.

Each pod keeps its sources in memory and sends the changed ones to the controller over the sensor stream every `otlp.sensor.tick`. It authenticates to the controller with the controller's serving certificate as its client certificate, so an OTLP-only install needs no ClusterRole. The controller applies the joined tenants and the workspace scope again to every source. A sender sees `UNAVAILABLE` only when no pod is ready, and retries from its own queue; a lost pod resets its counters and the next snapshot restores the series.

**Collectors inside the cluster.** A Collector with neither a client certificate nor a token for the agent can use `otlp.insecure`: plain gRPC, no sender check, every point filed under the agent's one tenant. With `allowFrom`, a list of namespaces, the chart renders a default-deny NetworkPolicy that admits only those; without it no NetworkPolicy is rendered and the port is open to every pod that can reach the Service. A point that names a namespace outside the tenant's scope is refused, and the limits apply per remote address. It excludes `ownCA`, `clientCASecret`, `allowedServiceAccounts` and `forwarders`, and the cluster's network plugin must enforce NetworkPolicy.

**Namespace and workload.** A resource needs `k8s.namespace.name`. The core refuses a whole frame over one entry with no namespace or name, so the push path holds such an entry back by name instead; it is listed under _Held back from the core_ in the local TUI and counted in `triage_agent_frames_malformed_dropped_total`. A common cause is `k8sattributes` associating pods by `container.id` first: some SDKs report a container id that is not the pod's, and the processor uses the first rule whose attribute is present, with no fallback. Associate by connection IP or pod UID instead.

**Topology without a Service index.** An agent with `kubernetes.watch=false` cannot resolve a callee's Service address. A client span whose server address names a service the agent has read (`name`, `name:port`, `name.namespace` or `name.namespace.svc`) becomes an edge to that service; any other address stays external.
    
    
    exporters:
      otlp_grpc/triage:
        endpoint: <fullname>-otlp.<namespace>.svc:4317
        tls:
          insecure: true    # otlp.insecure, in-cluster only

Virtual apps come from workloads only, synthesized by workload name or by label when a namespace opts in. The workload kinds and their readiness rules are under Watched API types above.

**Sandboxed runtimes.** eBPF works on the host kernel, so sandboxes limit it. The agent cannot run under gVisor, Kata or user-namespaced pods, which have no host kernel or host PID namespace to probe. Workloads that run under them are mostly invisible to the traffic role: their traffic passes through the sandbox's own kernel, so RED, SIG and L7 topology are missing and at most L4 flows at the sandbox's interface are seen.

## 04 Data flow and frames

Gated frames carry only services that crossed their baseline in the window. Summary frames carry every service, ungated, so the core can see quiet services too.

_Diagram: Pipeline from sensor roles through sensorstream and the collectors to push and the core, with timings, and a snapshot request from the core on graph load._

Figure 3. Pipeline with its cadences. Reconnect backoff is 1 s to 30 s on both links.

Frame| Content| Enabled by  
---|---|---  
snapshot, app_delta| Virtual-app status| Core configured  
k8s_event| Kubernetes Warning events| Core configured  
red_metrics / red_summary| RED from OBI, gated / every service| `redMetrics.enabled`  
use_metrics / use_summary| USE from cgroup v2 and PSI, gated / every service| `useMetrics.enabled`  
log_metrics| Error and warn deltas per service, one sample line| `--log-metrics-enabled`  
topology_snapshot| Observed L7 calls and optional L4 flows| `--topology-enabled`  
sensor_state| Sensor health every 60 s, unstamped, not tenant data. Lists each OTLP source with its last push and per-framework counts: RED and LOG data points, USE pods| Any collector  
limits (inbound)| The core's daily event cap for each tenant the agent serves (core policy `budgets.agentEventsDaily`). The agent holds its Kubernetes events to it with one token bucket per tenant, refilled evenly over 24 hours and holding one hour's share, so a storm is cut within minutes. App deltas and metric batches carry state and are never held back; the core caps the events it derives from them. Drops are counted per tenant and logged on the first and every thousandth. The frame also carries `namespaces`, the namespaces each tenant takes from this agent (absent or empty means every namespace): the OTLP receiver refuses, and the push path drops and counts as `outOfScopeDropped`, anything outside it. An older core sends no limits and nothing changes| Always  
resync (inbound)| Snapshot request on graph load. Core asks the agent to resend one tenant's virtual-app snapshot after that tenant's service graph reloads. The agent answers with `snapshot` frames and an end marker; no metrics or topology are resent| Always  
  
### Wire formats

Inside the cluster the agent speaks native OpenTelemetry. OBI emits OTel metrics under the semantic conventions, and the sensor roles forward them unchanged as OTLP `ResourceMetrics` with cumulative temporality. No custom metric model sits between OBI and the collectors.

OTel metric| Type| Used for  
---|---|---  
http.server.request.duration| Histogram| RED and SIG for HTTP; status from `http.response.status_code`  
rpc.server.call.duration| Histogram| RED and SIG for gRPC; status from `rpc.response.status_code`  
http.client.request.duration, rpc.client.call.duration, db.client.operation.duration| Histogram| L7 topology edges  
obi.network.flow.bytes| Counter| L4 topology edges  
k8s.namespace.name, service.name| Resource attributes| Identity of every series  
  
The sensor stream wraps those metrics in a small protobuf envelope. Each tick a sensor sends one `Snapshot` per role and one `SensorState`; the controller acknowledges each message by sequence number.
    
    
    service SensorService { rpc Stream(stream StreamRequest) returns (stream StreamResponse); }
    
    message StreamRequest { uint64 seq; Timestamp time; oneof { Hello | Snapshot | SensorState } }
    message Snapshot {
      string role = 1;
      repeated opentelemetry.proto.metrics.v1.ResourceMetrics resource_metrics = 2;
    }
    message StreamResponse { Ack { uint64 seq } }

The collectors turn the cumulative histograms into per-window request count, error count and p99 per service, then gate them. A latency breach names the slowest operation of the window in its sample, and a window whose p99 bucket holds a single request is neither judged nor learned, so one long-lived stream closing does not read as the service's latency. What leaves the cluster is one JSON frame per WebSocket text message: a shared envelope with exactly one payload field named after the kind. The `red_metrics` example below uses illustrative values.
    
    
    { "v": 1, "kind": "red_metrics", "cluster": "<cluster>", "tenant": "<tenant>",
      "redMetrics": {
        "windowStart": 1759834800, "windowSec": 60,
        "services": [{
          "namespace": "shop", "service": "checkout",
          "requestCount": 1840, "errorCount": 41,
          "sample": "GET /api/orders -> 502 x41",
          "p99Ms": 812, "baselineP99Ms": 140, "latencyBreach": true,
          "breach": "op-latency", "operation": "POST /api/orders",
          "observed": 812, "baseline": 140, "evidenceSec": 180
        }]
      } }

The payload fields are `hello`, `snapshot`, `appDelta`, `k8sEvent`, `logMetrics`, `redMetrics`, `redSummary`, `useMetrics`, `useSummary`, `topologySnapshot` and `sensorState`. Field names are a pinned contract with the core and must stay byte-identical on both sides; fields added later are optional.

### Wire caps

Cap| Value  
---|---  
Services per metrics frame| 100, larger frames chunked  
Sample text| 256 bytes  
Operation name| 128 bytes  
Topology| 200 nodes, 500 edges  
External address| 128 bytes  
  
## 05 Log samples

A `log_metrics` frame carries one sample line per service so the core can see what the errors say. The fingerprint that groups errors is masked; the sample that leaves the cluster is the line with secrets and email addresses replaced by `***`, then cut to length.

_Diagram: An error line is extracted and stripped, then splits: a masked fingerprint used for identity only, and a raw exemplar that is cut to 256 bytes and sent to the core without redaction._

Figure 4. Masking applies to the fingerprint; redaction applies to the exemplar. The collector picks the sample of the error series with the largest positive delta in the window. Defaults: 160 bytes and 8 fingerprints per service.

Control| Status  
---|---  
Length cap, 160 B exporter / 256 B wire| present  
Slot cap, 8 fingerprints per service; overflow counted with no sample| present  
Fingerprint masking| identity only  
Namespace scope and opt-out| present  
Secret and PII redaction of the sample: URI credentials, JWTs, Bearer and Basic credentials, secret-named keys, AWS key IDs, email addresses| present  
URI credential scrub (`user:***@host`)| present  
Prompt guardrails before a model call| core-side  
  
## 06 Connections and trust

Both links are encrypted and authenticated in both directions, and node roles open no port; the controller's own optional TUI and monitoring listeners are in section 11. Between the controller and the core, each side proves itself with a certificate inside the WebSocket: the core first, by signing a challenge nonce with its agent CA, then the agent, with its Ed25519 join certificate and a signature over the same nonce. The join certificate is the agent's only credential for the core. This is mutual certificate authentication over TLS 1.3, carried in the WebSocket rather than as TLS client certificates.

The core's challenge is bound to the connection. On every join-mode dial the agent sends a fresh 32-byte random nonce in the `X-Triage-Agent-Nonce` upgrade header (base64), and a core that supports binding echoes it in the challenge and signs it together with its own nonce, so a challenge recorded on one connection fails on any other. A challenge without the echo is replayable and is always refused. A core that also binds the TLS channel sets `channelBound` and signs 32 bytes of TLS exporter keying material (label `EXPORTER-triage-agent-join`, no context) as a third line of the message, so a relay that terminates TLS itself cannot forward the challenge; the agent derives the same material from its side of the connection and refuses a mismatch. Without `channelBound` the agent accepts the nonce-bound signature over verified `wss://` or plaintext `ws://`, and refuses it when `--core-insecure-skip-verify` is set. A certificate returned on join is checked before it is saved: it must chain to the trusted CA, be currently valid, and name exactly the requested cluster and tenants; a rotation cross certificate must also be a CA certificate with the certificate-signing key usage.

### Joining the core

There is no shared token, no signed hello and no per-cluster secret. The agent makes an Ed25519 key on first start and gets a certificate for it from the core. `--join-ca-fingerprint` (chart `core.caFingerprint`, from `triagectl agent list` on the core) pins the core's agent CA. It is required whenever `--triage-endpoint` is set, in a pod and in host mode, and the agent refuses to start without it. The key, certificate and saved core CA are kept in the Secret named by `--join-secret`, or in `--join-state-dir`, so a new pod or a restart needs no new approval.

Way to join| Flow  
---|---  
One-time agent key (core ADR-090)| The core admin or cabinet mints a key for one agent, `tak_<id>_<secret>`, of kind cluster, host or otlp, with its bindings (a tenant and namespaces). The core also mints the cluster name. The chart writes the key into the join Secret (`core.agentKey`, needs `core.storage: secret`); on a host `install.sh` writes it to `enroll.key` in the join state directory. On its first connect the agent spends the key, the core issues the certificate at once, and the agent removes the key from the store. A key the core refuses stays in the store. A key works once, for that cluster name and those tenants only  
Admin approval| With no key the agent logs `join pending` with its key fingerprint. An admin runs `triagectl agent approve <cluster> --fingerprint <fingerprint>` on the core  
Tenants| The join names the cluster and its tenants (`--join-tenant`, chart `core.tenants`; default `--target-tenant`). The certificate must name exactly those. They are also the only tenants an OTLP client certificate may name  
Storage| Chart `core.storage`: `secret` (default, a Secret the chart creates empty), `emptyDir` (a new key and a new join for each pod) or `pvc` (`core.existingClaim`)  
  
Between a sensor and the controller, one-way TLS plus a token gives mutual authentication with nothing extra to run. The controller holds a self-signed ECDSA P-256 certificate that each sensor pins from the `-sensor-ca` ConfigMap, so the sensor knows it reaches the real controller. The sensor presents no client certificate. It sends its projected ServiceAccount token, audience `triage-agent-sensors`, and the controller checks it with a **TokenReview** , a call to the Kubernetes API server that verifies the token's signature, expiry, audience and bound pod and returns the ServiceAccount and node it was issued for. The controller needs only `create` on `tokenreviews` for this. The token must belong to the `-ebpf` ServiceAccount, and the node named in the token must match the node in the sensor's hello. The kubelet issues the token per pod, valid for one hour and rotated automatically, so there is no client CA, no per-node certificate and no shared client key. A compromised node cannot send data as another node.

_Diagram: Trust on each link: sensors pin the controller certificate and present a ServiceAccount token; the core and the controller prove themselves to each other with certificates over a challenge nonce._

Figure 5. Authentication on each link.

  * **Listeners.** Only the controller listens: 9470 for the sensor stream when `--sensor-listen` is set, and 4317 for OTLP when `--otlp-listen` is set. Metrics on 8080 are off by default (`--metrics-addr=0`). No pprof in the repository.
  * **Keepalive.** 30 s with a 10 s timeout on both ends of the gRPC stream.
  * **Certificate life.** The sensor certificate is valid for 10 years and regenerated 30 days before expiry. The join certificate renews with less than a third of its life left.

## 07 RBAC

RBAC is read-only on workloads. The agent has no cluster-wide Secret or ConfigMap access and updates three named objects in its own namespace, each by `resourceNames`.

Role| Grants  
---|---  
ClusterRole, controller| get, list, watch on namespaces, events, pods, services, ingresses, deployments, statefulsets, daemonsets, replicasets, jobs, cronjobs, rollouts; get on endpoints. No Secrets, ConfigMaps or wildcards  
ClusterRole, ebpf| get, list, watch on pods, services, nodes, namespaces and workload kinds; pod informer limited to its node  
Role -sensor-stream| get, update on `-sensor-tls` and `-sensor-ca`  
Role -join| get, update on `-join`  
Role leader-election| Leases; create and patch events  
ClusterRole -sensor-tokenreview| create tokenreviews  
ClusterRole -api-review (with `api.serviceAccountAuth`)| create tokenreviews and subjectaccessreviews, for the monitoring API's ServiceAccount authentication  
ClusterRole -api-reader (with `api.serviceAccountAuth`)| get on `agent-api` in `triageagent.dev`: the permission an API caller needs. Bound by the operator, not by the chart, cluster-wide or per tenant namespace  
  
### Process privilege

Both workloads run the same image, whose default user is `nonroot` (UID 65532, Chainguard `static` base). The controller keeps that user and needs no host access. The DaemonSet overrides it with `runAsUser: 0` in its pod spec, in both modes below. It runs all sensor roles in one container, `agent`, so the traffic, resource and logs-based metrics roles share one security context. The chart offers two privilege modes for it, selected by `ebpf.privileged`.

| Unprivileged (default)| Privileged fallback  
---|---|---  
ebpf.privileged| `false`| `true`  
privileged| false| true  
Capabilities| Drop `ALL`; add `BPF`, `PERFMON`, `NET_RAW`, `SYS_PTRACE`, `DAC_READ_SEARCH`, `CHECKPOINT_RESTORE`, plus `ebpf.extraCapabilities`| All  
Privilege escalation| Blocked| Not restricted  
Seccomp| `RuntimeDefault`| Not set  
Root filesystem| Read-only| Read-only  
User| 0 (root), overrides the image's `nonroot`| 0 (root), overrides the image's `nonroot`  
Use when| Kernels 5.11 and later, which load eBPF under this capability set| Older or unusual kernels that cannot load eBPF under the capability set  
  
**Root scope.** UID 0 is what makes the six capabilities effective: Kubernetes has no field for ambient capabilities, and file capabilities on the binary are blocked by `no_new_privs`. Root here is not full privilege; it is UID 0 with every other capability dropped, `privileged: false`, `no_new_privs`, seccomp and a read-only root filesystem. `hostPID` already requires the `privileged` Pod Security level, with or without root. Comparable eBPF agents such as Beyla, Cilium, Falco and Tetragon also run as root, most with wider capabilities or full privilege.

### Extra capabilities

Capability| Needed for  
---|---  
SYS_RESOURCE| Kernels before 5.11, to lift BPF verifier instruction and memory limits  
SYS_ADMIN| Go services on GKE COS 6.12: without it every Go uprobe fails with permission denied and Go services report no RED; the sensor status points to this capability only when the failing executable is Go. Some kernels also need it for Go trace-context propagation  
  
Both widen the container beyond the audited default; the chart advises trying without them first.

### Host access

Item| When| Why  
---|---|---  
hostPID| Traffic metrics on| OBI hooks other containers' processes; a namespace share has no capability substitute  
hostNetwork| `ebpf.networkFlows.enabled`| The socket filter captures L4 flows in the host network namespace  
/sys/fs/bpf| Always| BPF filesystem for OBI's maps and programs  
/sys/fs/cgroup, read-only| Resource metrics on| cgroup v2 and PSI files  
/var/log, read-only| Logs-based metrics on| Container log files  
  
## 08 Topology and drift

With `--topology-enabled`, the RED collector builds a `topology_snapshot` from OBI's client-side metrics and, when enabled, L4 flows. Counts accumulate across RED ticks and flush every 600 s as one full snapshot, so a lost frame heals on the next flush. The snapshot keeps the top 500 edges by calls and the first 200 nodes, and is skipped when every scrape in the tick failed.

_Diagram: Sample topology snapshot drawn as a graph: eleven workloads, three external hosts, and one observed edge from checkout to cache that is not in the declared graph._

Figure 6. A sample `topology_snapshot` drawn as a graph. Nodes are workloads, never pods, sized by traffic. Each edge is one caller, callee and protocol, carrying the call count for the window, or bytes for an L4 flow. A destination outside scope is sent as an external host. The core flags an observed edge that the declared graph lacks, here checkout to cache, as drift.

The agent has no drift comparator. The core compares the snapshot with the declared service graph. A snapshot request does not resend topology; the next flush carries it.

## 09 Connectors

Each connector is its own Go module, deployed apart from the agent, that turns a provider's signals into events for the core events API.

Connector| Runs as| Input| Auth to core| Delivery  
---|---|---|---|---  
gcpevents| Cloud Run function| Billing cost anomalies and Monitoring alerts via Pub/Sub push; Service Health polled every 15 min| Bearer from Secret Manager, `X-Tenant`| 3 attempts, 4xx dropped  
promevents| Deployment, port 8080| Alertmanager webhook `POST /alertmanager`| Projected SA token (audience `triage-core`, 1 h) or static token, `X-Tenant` per alert| 15 s timeout, 3 attempts, 4xx except 408 and 429 dropped, duplicates suppressed 2 min  
  
`promevents` runs non-root with a read-only root filesystem and all capabilities dropped; its Role in the core namespace grants verb `ingest` on named tenants only.

## 10 Configuration

### Requirements

Item| Requirement  
---|---  
Kernel| 5.8 or later, 4.18 on RHEL-based distros; OBI refuses to start below that. The unprivileged default is documented for 5.11 and later; 5.8 to 5.10 need `SYS_RESOURCE`  
BTF| Kernel built with `CONFIG_DEBUG_INFO_BTF`; OBI refuses to start without it  
cgroup| cgroup v2, for resource metrics  
Linux host| systemd 240 or later (D-Bus for unit discovery, `journalctl` for logs), cgroup v2, and the kernel and BTF above. Root, with the six capabilities the unit file grants  
Controller resources| Requests 50m CPU, 64Mi memory; limits 200m CPU, 512Mi memory  
DaemonSet resources| Requests 50m CPU, 128Mi memory; limits 300m CPU, 700Mi memory. Memory grows with the number of instrumented pods; CPU peaks near 130m during the attach burst after a restart  
  
### Flags and values

Flag or value| Default| Meaning  
---|---|---  
--triage-endpoint / core.host, core.endpoint| empty| Core WebSocket URL. The chart builds `wss://<core-host>/events/ws` from `core.host`; `core.endpoint` is a full-URL override. Empty runs without a core  
--cluster-name / core.clusterName| empty; host mode the host name| Cluster identifier; required with an endpoint. For a key install, the name the core minted  
--join-ca-fingerprint / core.caFingerprint| empty| Core agent CA fingerprint; required with an endpoint  
--join-tenant / core.tenants| \--target-tenant| Tenants the join asks for, repeatable; for an OTLP-only agent with one entry, also its tenant  
core.agentKey| empty| One-time agent key, written to the join Secret; on a host, `enroll.key` in the join state directory  
--join-secret, --join-state-dir / core.storage, core.existingClaim| Secret (chart); `/data/join`| Where the key, certificate and core CA are kept  
--core-heartbeat / core.heartbeatSeconds| 20| Heartbeat interval in seconds  
--core-insecure-skip-verify / core.insecureSkipVerify| false| Skip TLS verification of a `wss://` endpoint; dev only  
--kubernetes / kubernetes.watch| true| `false` runs on OTLP and sensor data alone, with no informers and no cluster-wide rights; needs one tenant (see section 02)  
--target-tenant| empty| Primary tenant; empty selects cluster mode. Required in host mode  
--target-namespaces| empty| Static scope list beside annotated namespaces  
--workload-resources| Deploy, STS, DS, Rollout| Workload kinds watched  
--virtual-apps| on| Synthesize virtual apps  
--sensor-listen| empty| Sensor stream address; empty disables it and its collectors  
otlp.enabled| false| OTLP/gRPC receiver on 4317; turns on the sensor stream without the DaemonSet  
otlp.clientCASecret| empty| Secret with `ca.crt` for mutual TLS  
image.ebpf.repository / tag| empty / `image.tag`| Image of the eBPF DaemonSet, built with OBI. Empty uses `<image.repository>-obi`. The controller and the OTLP role always use `image.repository`  
otlp.sensor.enabled| false| Runs the OTLP receiver and its transformers as `--mode=otlp` pods; senders dial `<fullname>-otlp`. Enabling it reissues the serving certificate once  
otlp.sensor.replicas / tick| 2 / 10s| Pods of the role, and how often each sends its changed sources to the controller  
otlp.insecure.enabled / allowFrom| false / empty| Plain gRPC for in-cluster Collectors; `allowFrom` lists the namespaces the NetworkPolicy admits; empty renders no NetworkPolicy  
otlp.limits.*| 2000 / 5000 / 500 / 250000 / 4 MiB| `maxResources`, `maxPointsPerResource`, `maxResourcesPerSender`, `maxSeries` (series held in all, what the memory is sized by), `maxRequestBytes`; a series no export updated for 10 minutes is dropped  
otlp.allowedServiceAccounts| empty| `namespace/name` or `namespace/*` allowed to send with a token  
--sensor-tick| 15s| Sensor send interval  
--log-metrics-enabled| false| Log collector; needs push  
--log-metrics-interval-seconds| 60| Window  
--log-metrics-error-threshold| 10| Error or warn delta that qualifies a window  
--log-baseline| true| Median gate: ×3, ≥ 5 windows  
--log-exporter-sample-max-len| 160| Bytes kept for fingerprint and sample  
--log-exporter-max-samples-per-service| 8| Distinct fingerprints per service  
--topology-enabled| false| Topology snapshots  
--topology-interval-seconds| 600| Snapshot flush  
--core-allow-plaintext / core.allowPlaintext| false| Allow `ws://` beyond localhost and .svc  
--metrics-addr| 0| Self-metrics listener; 0 disables  
--ui-listen / ui.listen| empty| Local TUI on a loopback address; works without a core (standalone)  
--ui-max-events / ui.maxEvents| 100| Newest events per tenant the TUI and API keep, 10 to 5000; older ones are dropped, the counters keep counting  
--ui-admin-key-file / ui.adminKey.existingSecret| empty| Admin key for the TUI login, read from a mounted Secret; empty means no login  
--ui-admin-key, TRIAGE_AGENT_UI_ADMIN_KEY| empty| Host mode only: the key as a flag or environment variable  
--api-listen / api.listen| empty| Monitoring API address; needs `--api-tokens-file` or `--api-sa-auth`; works without a core (standalone)  
--api-tokens-file / api.tokens.existingSecret| empty| API tokens, one `name tenant token` per line; optional with `--api-sa-auth`  
--api-sa-auth / api.serviceAccountAuth| false| Cluster mode: also accept ServiceAccount tokens (audience `triage-agent-api`) decided by RBAC  
--api-mcp / api.mcp| false| Serve the MCP endpoint `POST /mcp` (read-only tools)  
--api-docs / api.docs| false| Serve the OpenAPI spec and the Swagger page  
--api-tls-cert-file, --api-tls-key-file / api.tls.existingSecret| empty| Serve the API over TLS; empty serves plain HTTP  
ebpf.privileged| false| Fully privileged fallback for older kernels  
ebpf.networkFlows.enabled| false| L4 flows; DaemonSet on `hostNetwork`  
replicaCount| 1; production runs 3| Controller replicas; above 1 turns on leader election, replica gossip and a PodDisruptionBudget  
  
## 11 Local TUI and API

Two optional listeners let people and tools read what the agent sees. Both are off by default and read the same state, with a core or standalone: a `push.Observer` receives a read-only copy of every frame the pusher queues, per resolved tenant, so what they show is what the core is sent (ADR-0019, ADR-0020).

### TUI

`--ui-listen` serves the core's TUI, embedded in the binary, with two panels: **app** (virtual apps with traffic, the dependency graph and a treemap) and **sensors** (sources on the left, the events they raise on the right, and above them what the collectors did with the selected source's data, see _Signal path_ below). `T` or `N` picks the tenant (one is always selected), `?` shows the flags in effect and the agent's statistics, `s` cycles app state, `/` filters. The address must be loopback and requests must carry a loopback `Host`; the way in is `kubectl port-forward` or an SSH tunnel. Only GET is served, apart from login and logout.

Item| Behaviour  
---|---  
Login| With an admin key (at least 16 bytes) every `/api/` call needs a session; the static files stay public so the login page loads. In a cluster the key is a file mounted from a Secret and read on each use, so editing the Secret rotates it. In host mode it may also come from `--ui-admin-key` or `TRIAGE_AGENT_UI_ADMIN_KEY`. Without a key there is no login and the loopback bind is the only guard  
Session| Cookie, `HttpOnly`, `SameSite=Strict`, 12 h, signed with a key derived from the admin key: every replica accepts it and rotating the key ends all sessions. Login and logout refuse a foreign `Origin`; failed logins back off from 1 s to a minute, even for the right key  
Replicas| Only the leader's pusher and collectors run. The leader publishes its state (the store snapshot, newest 300 events, apps, link state) to its peers every 5 s as a `ui` mesh frame, and followers serve it, so every replica shows the same view, up to 5 s behind. Event ids continue from the leader's across a failover  
Events| The newest `--ui-max-events` events are kept **per tenant** , so a tenant that raises many (one latency window a minute) cannot push a quiet tenant's out. The snapshot gossiped to followers is bounded over all tenants  
Tenants| Every service, edge, workload and event carries the tenant its frame was routed to; `?tenant=` on any endpoint filters. A callee in another tenant shows as `other tenant` without its name. Node-wide sensors appear in every tenant's view. The picker is a view filter, not isolation  
  
### Monitoring API

`--api-listen` serves Prometheus `/metrics`, a discovery document `/.well-known/triage-agent` and the read-only JSON API (`/api/v1/status, tenants, apps, services, graph, sources, events, stream`) for Prometheus, Grafana and other tools. It binds any address, serves TLS when a certificate is given and plain HTTP otherwise, and **never runs unauthenticated** : it refuses to start without a credential source, `--api-tokens-file` or, in cluster mode, `--api-sa-auth`. Two further parts are **off by default** : an MCP endpoint (`--api-mcp`) and an OpenAPI description with a Swagger page (`--api-docs`).

Item| Behaviour  
---|---  
Tokens| A Secret file of `name tenant token` lines, tokens at least 24 bytes, sent as `Authorization: Bearer`. Compared by hash against every entry in constant time; reloaded when the file changes, and a broken reload keeps the loaded tokens. Tenant `*` is unrestricted  
ServiceAccount auth| In cluster mode `--api-sa-auth` (chart `api.serviceAccountAuth`) also accepts a ServiceAccount token of audience `triage-agent-api` (`kubectl create token <sa> --audience triage-agent-api`). The agent runs a TokenReview, then a SubjectAccessReview for `get` on `agent-api.triageagent.dev`. A ClusterRoleBinding of the chart's `-api-reader` role is unrestricted; a RoleBinding in a namespace binds the caller to that namespace's `triageagent.dev/tenant`, and a namespace without the annotation is refused. A token that is not a JWT of a plausible size is refused without a review; reviews run one per token at a time and eight at once; accepted outcomes are cached up to 60 s (never past the token's expiry), refused ones 30 s, in a separate cache. The token file stays for callers outside the cluster and for host mode, where the flag is refused. Serve `api.tls` with it  
MCP| `POST /mcp` speaks the Model Context Protocol over streamable HTTP (JSON-RPC 2.0, protocol 2025-06-18, one JSON answer per call, no sessions, no batches). Seven read-only tools mirror the JSON API: `agent_status`, `list_tenants`, `list_apps`, `list_services`, `get_graph`, `list_sources`, `get_events`. Same bearer tokens and tenant scope; a tenant-bound caller's `tenant` argument is overridden by its own. A request whose `Origin` host differs from `Host` is refused; the body is limited to 64 KiB and one answer to 2 MiB. The only POST the API accepts  
OpenAPI and Swagger| `/api/v1/openapi.json` describes the paths (the MCP path only when it is on), `/api/v1/docs` serves a Swagger UI page and `/api/v1/docs.js` its init script. They are static and need no token, so a browser can load them; the calls the page makes need one (Authorize). Served with a CSP, `X-Frame-Options: DENY`, and the two swagger-ui files from cdnjs pinned by SRI hash, so the browser needs internet access  
Tenant scope| A token tied to a tenant can read that tenant only, enforced by the agent: the `tenant` query is forced to it, a request for another tenant is 403, the tenant list shows only it, and the token sees neither the node-wide sensors nor the core address and cluster-wide frame counts  
Metrics| Per service `triage_agent_service_{requests_per_second, error_ratio, latency_p99_seconds, cpu_pressure_ratio, memory_pressure_ratio, io_pressure_ratio, oom_kills_window, log_error_lines_window, log_warn_lines_window}`; `triage_agent_app_ready`; `triage_agent_edge_{calls,bytes}_per_second`; `triage_agent_source_up`; `triage_agent_events_total`; for an unrestricted token the link (`core_link_connected`, `frames_sent_total`, dropped) and sensor nodes. Labels stop at tenant, namespace and service. Values are the last window's, not samples. `triage_agent_signal_path_total{role,source,outcome}` (unrestricted tokens only) counts what each collector did with each source's data  
Signal path| Every collector (RED, USE, logs) and the OTLP receiver counts, per sensor node or OTLP source: `ticks` (reads), `no_snapshot`, `points`, `services`, `filtered` (outside the namespace scope) and `unattributed` (no workload). Per role (source `*`): services read, `gated` (read but below every gate), `capped` (cut by a frame's size cap) and `sent` (handed to the push sender). They appear in the sources API as `ledger` and `roleLedger` and under the selected source in the TUI, so an empty event list can be told from data that was read and held back. The units follow each collector (a service in the logs and RED roles, a pod in USE), the counters restart from zero on a leader change, and a restricted token sees none of them  
Leader only| Only the leader exports signal series; a follower answers `/metrics` with `triage_agent_info` alone, so Prometheus sees each series once however many replicas it scrapes. The JSON API is served by every replica from the gossiped view. A leader change leaves a gap of seconds and restarts the `events_total` counters  
Chart| Service `<fullname>-api`; `api.tls.existingSecret` for TLS; `api.mcp` and `api.docs` (both false) and `api.serviceAccountAuth`; an optional ServiceMonitor that reads the scrape token from its own Secret. The chart refuses `api.listen` without tokens or `api.serviceAccountAuth`  
  
## 12 OBI note

`third_party/obi` is an owned fork of OBI v0.12.2 with 11 local commits, including eBPF C changes, and is not tracked upstream; Apache-2.0 attribution stays in `NOTICE`. Its Go offsets table, used for binaries without debug info, covers Go up to 1.26.4 and grpc-go up to 1.81.1. Refresh it with OBI's tracker when new Go or grpc-go releases land, and keep scanning its `go.mod` for CVEs.

## 13 FAQ

What is the difference between the eBPF and OTLP modes?

Both feed the same collectors, gates and frames; they differ in where the numbers come from. In eBPF mode the DaemonSet reads them on each node: OBI hooks see every HTTP and gRPC call without code changes, cgroup files give USE and PSI, container log files give LOG, and a socket filter adds L4 flows. In OTLP mode the triage DaemonSet does not run; an OpenTelemetry Collector pushes metrics to the controller on port 4317. RED and L7 edges come from semantic-convention histograms or span metrics, LOG from the count connector, and USE with PSI from each node's kubelet, which reads the cgroup pressure files itself and serves them on `/metrics/cadvisor`. The Collector still has to reach every node: as a DaemonSet each pod scrapes its own node's kubelet, or one Deployment discovers the nodes through the Kubernetes API and scrapes them all. The two can run side by side; where both report the same service or pod, the sensor's numbers are used. With the OTLP sensor role the Collector sends raw traces, logs and metrics and the agent makes the histograms and counts itself (section 03).

| eBPF| OTLP  
---|---|---  
On each node| Triage DaemonSet with `hostPID`, root, six capabilities, reading `/sys/fs/cgroup`| The kubelet's own `/metrics/cadvisor`, scraped by the Collector (a DaemonSet, or one Deployment for all nodes) with `get` on `nodes/metrics`  
Coverage| Every process on the node, any language| Only what is traced or scraped  
Service graph| L7 calls and L4 flows| L7 edges from client spans that name the server; no L4  
Needs| Kernel 5.8+, BTF, cgroup v2| A Collector with `k8sattributes` and an OTLP exporter, plus a kubelet cAdvisor scrape for USE; with the controller's own listener it also needs spanmetrics and the count connector. Kubernetes 1.34+ for PSI  
Sandboxed runtimes| Mostly invisible| Seen if instrumented  
  
Measured side by side on one cluster, OTLP alone matched eBPF for USE and for RED on traced services, and kept about a tenth of the graph's edges once L4 flows were gone.

Can the agent run outside a cluster in OTLP-only mode?

Yes, as long as it can reach a Kubernetes API. The controller loads a kubeconfig when it is not in a pod, so it runs on a laptop or a VM against any cluster it can reach. Set `POD_NAMESPACE` to the namespace that holds its Secrets, turn leader election off for a single copy, and give the kubeconfig identity the agent's RBAC, including `create` on TokenReviews when senders use tokens. Senders must reach that machine on 4317 and verify it by name, so add that name to the serving certificate with `--sensor-extra-names`; a Collector next to it is the simple setup, and every point still needs `k8s.namespace.name` to land in a tenant. A host with no Kubernetes API at all runs the Linux host mode (see the last question).

What sensitive information leaves the cluster?

Only the frames in section 04, over the one outbound WebSocket connection to the core. They carry:

  * names: cluster, namespace, workload, service, and pod names in USE and events;
  * aggregates per window: request, error and log counts, p99 latency, CPU, memory and PSI percentages;
  * operation names: HTTP route templates, RPC methods and the status codes seen;
  * topology edges between workloads, and the host name of a destination outside scope;
  * the message of Kubernetes Warning events;
  * one log line per service, redacted for secrets and email addresses and cut to 160 bytes (section 05);
  * sensor health and the OTLP source list; a tenant sees only its own OTLP sources, and role error text only reaches the all-tenant view.

Request and response bodies, headers, raw logs, traces, environment variables and Secrets never leave. Namespaces without a tenant and objects opted out with `triageagent.dev/triage: "false"` are not sent at all.

Why are connectors needed?

The agent sees what runs in the cluster. Some failures start outside it: a cost anomaly or a Service Health incident in the cloud provider, an alert from an existing Prometheus rule, a security finding. Connectors bring those into the same tenants and issues as events, so a cloud outage and the errors it causes in a service are correlated in one place. Each connector is a small separate deployment that posts to the core events API with its own credentials (section 09), so the agent itself needs no cloud permissions.

How can a Linux host be instrumented?

With `--mode=host`, the agent runs as one systemd service on a VM or bare-metal host, with no Kubernetes API. The host is a cluster of one: `--cluster-name` (default the host name) names it, `--target-tenant` sets the tenant since a host has no namespaces (figure 2), and every selected systemd unit is a virtual app in one synthetic namespace (`--host-namespace`, default `host`). USE comes from each unit's cgroup, LOG from journald and log files, and RED from OBI in a build with the `obi` tag, which the release binaries are. The agent joins like a cluster, by its join certificate only: with a one-time key from the core, `install.sh` does it in one command and the core issues the certificate at once; without a key, the agent prints a fingerprint and an admin runs `triagectl agent approve`. The key and certificate stay in `/var/lib/triage-agent/join`. The release carries the binaries, the unit file with the same six capabilities as the DaemonSet and a deb and rpm package; the README has the steps. Host containers (Docker, Podman) are not covered yet. Alerts from a host's existing Prometheus still arrive through `promevents`.

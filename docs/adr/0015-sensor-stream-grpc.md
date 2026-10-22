# ADR-0015: Sensor Stream to the Controller over gRPC

- **Status:** Accepted. Phase 1 implemented 2026-09-26, deployed to a test
  cluster 2026-09-26. Phase 4 (HA: replica gossip) implemented 2026-10-07.
- **Date:** 2026-09-26
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0006 (log-based metrics), ADR-0009 (RED metrics), ADR-0010
  (USE metrics), ADR-0014 (log exporter in the agent DaemonSet), core
  `docs/todo/agent-join.md` (the controller's certificate session to the core)

## Context

The eBPF DaemonSet (the sensors) serves Prometheus `/metrics` on 9464
(RED), 9465 (USE) and 9466 (logs). The controller finds the sensor pods
through each Service's Endpoints, scrapes every pod, diffs cumulative
counters against the previous scrape and sends one windowed frame
(`red_metrics`, `use_metrics`, `log_metrics`) to the core.

This shape comes from the first sensors, which were third-party exporters
(Beyla, Fluent Bit, node-exporter) that only offered a Prometheus endpoint.
The sensors are now this repo's own binary in `--mode` roles, and the
endpoint stayed so the scrapers needed no change (ADR-0014).

What it costs:

- **Lag.** `intervalSeconds` defaults to 60 for all three, and a delta needs
  two scrapes, so a spike reaches the core 60 to 120 seconds after it starts.
- **Exposure.** Three ports on the pod network serve per-service request,
  error, latency and log counts. They are gated only by a shared bearer token
  (`metricsAuth`, chart `<fullname>-metrics-token`), and serve without auth
  when the token file is missing (`metricsauth/auth.go`).
- **Fixed windows.** The collectors report every service over its gate once
  per window, whether or not anything changed since the last one.

## Decision

### 1. Sensors stream to the controller over gRPC

Each sensor pod dials the controller and holds one bidirectional gRPC stream.
The controller serves it on a cluster-internal Service; the sensors need no
discovery and the controller needs no Endpoints lookup.

- **Auth.** The sensor presents a projected ServiceAccount token with its own
  audience. The controller checks it with a TokenReview when the stream opens
  and accepts only the sensors' ServiceAccount.
- **Transport security.** Self-managed TLS. On first start the controller
  generates a key and a self-signed certificate, keeps them in a chart-made
  Secret (`<fullname>-sensor-tls`, get and update on that one Secret only,
  like the join Secret) so a new pod keeps the same identity, and publishes
  the certificate to a ConfigMap (`<fullname>-sensor-ca`). The sensors mount
  that ConfigMap and pin it. No external CA and no cert-manager.
- **Messages.** A hello (node, sensor version, roles), then per-tick deltas:
  per (namespace, service) request, error and latency-bucket counts, cgroup
  and PSI readings, log error/warn/total counts with samples. Each message
  carries a sequence number so the controller can see a gap.
- **Tick.** A longer regular tick (a flag, default to be measured, on the
  order of 10 seconds), plus an immediate send when a service's counters
  jump past a large-jump threshold within the tick, so a sharp change does
  not wait for the next one.
- **Sensor state.** Every sensor also reports its own state on the stream:
  eBPF programs loaded, events dropped, ring buffer fill, send lag, CPU and
  memory. The controller keeps it per node and forwards it to the core, so
  the core knows when a node's data is missing or degraded rather than
  reading silence as health.
- **Backpressure.** HTTP/2 flow control. A sensor keeps a ring buffer and
  replays it after a reconnect.

### 2. No sensor data reachable from outside

The sensors open no listening port on the pod network. A process's own
health and self-metrics stay local to the pod (localhost or a probe path)
and are never an as-is export of the sensor data. The Prometheus scraper
code stays in the controller for one case: an external OpenTelemetry or
Prometheus source on an install that has no sensors of its own.

### 3. The controller keeps state and decides what is real

The controller merges every node's deltas into per-service state: a service
on four nodes is one row. Detection is by thresholds on that state, not by
fixed windows:

- rates and ratios over a short rolling horizon (error rate, latency
  percentiles, PSI, restarts and OOM kills);
- a service trips when a threshold holds for a set time and clears after a
  longer one (hysteresis), so a single bad tick does not reach the core;
- the core hears about a transition (degraded, recovered, and what changed)
  when it happens, plus a slow periodic summary for the baseline.

Thresholds are controller flags, rendered from chart values, the same way
`--red-metrics-*` and `--use-metrics-*` are set today.

### 4. The core protocol changes with it

The core is free to change. The controller advertises the new frames in its
hello; the core treats a transition as an event on the service. The windowed
frames are kept only for a core that does not advertise the new ones.

### 5. HA later

Phase 1 runs one controller replica, which is the chart default today. Later,
with leader election on (`--enable-leader-election`, already in the binary),
every replica accepts sensor streams, replicas share state by gossip, and
only the leader sends to the core.

## Phases

1. **gRPC transport and sensor state.** Sensors stream to the controller
   over self-managed TLS; the controller still builds the same 60 second
   windows, so the core's metric frames do not change. Sensors report their
   own state from the start, and a `sensor_state` frame carries it to the
   core, which gives the measurements the tick and buffer defaults come
   from. Removes the three sensor ports, their Services, the metrics token
   and the Endpoints scrape for the sensors.
2. **State and thresholds.** The controller keeps per-service state from the
   stream and detects by threshold flags with hysteresis; the tick lengthens
   and large jumps send at once.
3. **Transition frames.** New core frame kinds, advertised in the hello. The
   windowed frames remain for older cores.
4. **HA.** Streams to every replica, gossip, leader sends.

Each removal in phase 1 (ports 9464, 9465, 9466, the `triage-ebpf` and
related Services, `metricsAuth` and `<fullname>-metrics-token`, the
controller's Endpoints scrape of the sensors) is named to the user before it
goes.

## Phase 1 implementation

- **Wire.** `proto/triageagent/sensor/v1/sensor.proto`, generated with
  `buf generate` into `sensorstream/sensorpb` (buf.lock pins the OTLP protos
  from `buf.build/opentelemetry/opentelemetry`). A `Snapshot` carries one
  role's metrics as OTLP `ResourceMetrics` with cumulative temporality; the
  collectors still diff against the previous snapshot, as phase 1 intends.
- **One metric standard.** OTLP end to end; nothing in the agent speaks
  Prometheus. OBI exports through its in-process `MetricsConsumer`
  (`cmd/triage-agent/ebpf_metrics.go`), with its Prometheus exporter off.
  node-exporter and log-exporter build OTLP directly (`metricset.Builder`)
  with OTel-style names and attributes (`k8s.namespace.name`,
  `service.name`, `k8s.pod.name`). The controller reads everything through
  `metricset.Set` (`metricset.FromOTLP`). controller-runtime still links
  `prometheus/client_golang` for its own metrics; the agent serves none.
- **Hub.** `sensorstream.Hub` on `--sensor-listen` (chart port 9470,
  Service `<fullname>-sensors`) keeps each node's latest snapshot per role
  and its `SensorState`, and acks each state message.
- **Self-managed TLS.** `sensorstream.EnsureServingCert` generates an ECDSA
  P-256 self-signed certificate for the Service DNS names, keeps it in
  `<fullname>-sensor-tls`, and publishes it to `<fullname>-sensor-ca`, which
  the sensors mount and pin (TLS 1.3).
- **Auth.** A projected token with audience `triage-agent-sensors`, checked
  once per stream with a TokenReview; only the `<fullname>-ebpf`
  ServiceAccount is accepted, and when the API server names the token's node
  the hello must name the same node.
- **Sensor state.** Per role: collect time, metric and data point counts,
  errors. Per process: goroutines, heap, CPU seconds, reconnects, send
  errors, send time, ack round trip, tick bytes, connect time. The hub keeps
  it per node, and the controller sends `sensor_state` to the core every 60
  seconds, mapped from `sensorstream.Hub.Nodes()`
  (`cmd/triage-agent/sensor_hub.go`). It carries the sensors' own health, not
  any tenant's data, so it is a session-level frame like heartbeat: sent
  unstamped once per connection, on the heartbeat tick (`push/connworker.go`).
- **Removed.** The three sensor ports (9464, 9465, 9466) and their Services
  (`triage-ebpf`, `triage-node`, `triage-log`); `metricsauth` and
  `<fullname>-metrics-token`; the Endpoints scrape in all three collectors;
  the Hubble RED profile; scraping an external Prometheus log source; and
  the flags and chart values that configured those scrapes (service names,
  ports, metric names, label keys, `--red-source`, `--hubble-*`,
  `redSource`, `metricsAuth`).
- **One replica.** The chart refused `replicaCount` above 1 while the sensor
  stream was on, until phase 4 (below).

## Phase 4 implementation

Phases 2 and 3 are not done; phase 4 came first because with more than one
replica the leader missed every node whose sensor the Service had sent to
another replica.

- **Every replica takes streams.** Unchanged from phase 1: `Hub` is not
  leader-gated. The leader alone runs the collectors and the push, as
  before.
- **Gossip.** `internal/mesh`: one WebSocket per peer over mutual TLS, on
  `--mesh-listen` (chart port 9471, `sensorStream.meshPort`). The peer
  transport follows the core's `internal/gossip` mesh. Peers come from the
  DNS answer of the headless Service `<fullname>-peers`
  (`--mesh-service`, not-ready pods included), redialed every 15 seconds.
  Every replica dials every peer and sends its own records on the
  connections it dialed; a received record is applied and not forwarded,
  so one hop reaches the mesh.
- **One record.** `NodeSnapshot{Node, Seq, At, Snapshot}`: one message a
  sensor sent (hello, one role's snapshot, or state; no body means the
  stream closed), its sequence number, and when the replica holding the
  stream received it. A replica sends each message its own sensors send,
  and on connecting to a peer replays the latest of each kind for every
  sensor connected to it.
- **Merge.** `Hub.Merge` keeps the newest record per node and kind, by `At`
  and then `Seq` (a sensor's sequence restarts with its stream, so `Seq`
  alone orders nothing across replicas). A node whose sensor holds its
  stream to this replica ignores peer records: its own stream is the live
  view, whatever a peer's clock says. A node a peer reports counts as
  connected until a close record or `--sensor-stale` without a record.
  Gaps stay per replica.
- **Failover.** Every replica holds every node, so a newly elected leader
  reads the same targets on its first tick. The chart renders the mesh
  with one replica too, so a rollout's new pod takes the old one's state.
- **Auth.** No new credential: every replica loads the same certificate
  from `<fullname>-sensor-tls` and pins it, both as server and as client,
  as the sensors pin it. A pod without that Secret cannot join.
- **Chart.** Above one replica, leader election is always on and a
  PodDisruptionBudget (`maxUnavailable: 1`) keeps one replica up. No RBAC
  change: discovery is DNS only.

## Open questions

- **Tick length and ring buffer size.** Set from measurements on a live
  cluster, not guessed: message size and rate per node per tick, and how
  long a controller restart takes from stream loss to a new stream. Phase 1
  reports both through the sensor state above, so the defaults are chosen
  from what the core sees.
- **Large-jump threshold.** Relative to the service's own recent rate, or an
  absolute count; also a flag either way.

## Consequences

- Detection lag falls from 60 to 120 seconds to the hysteresis time.
- Nothing on the pod network serves sensor data; the shared metrics token
  goes away.
- The controller holds more state and becomes the one place detection logic
  lives, which is where HA work lands later.
- `google.golang.org/grpc` becomes a direct dependency (it is indirect today,
  through OpenTelemetry).

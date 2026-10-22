# Linux host mode

Status: Implemented 2026-10-08 on branch `feat/linux-host`, ADR-0018. The
per-phase acceptance checks on a Linux host are still to be run; the steps are
in the ADR's `## Acceptance` section.

Run the agent on a plain Linux host (VM or bare metal) with no Kubernetes
API, as one systemd service, reporting the same frames a cluster agent
sends: virtual apps, USE with PSI, LOG, RED and topology. The core needs no
change; a host is a cluster of one with one synthetic namespace.

## Model

| Kubernetes agent | Host agent |
|---|---|
| cluster (`--cluster-name`) | host name, overridable with `--cluster-name` |
| namespace | one synthetic namespace, `--host-namespace` (default `host`); a fleet of hosts running the same services shares one value so their services merge in the core graph |
| tenant from namespace annotation | `--target-tenant` only (tenant mode); cluster mode is not offered |
| workload / virtual app | systemd unit (`*.service`), selected by `--host-units` globs |
| pod | the unit's main cgroup (`/sys/fs/cgroup/system.slice/<unit>`) |
| container logs (`/var/log/containers`) | journald, by `_SYSTEMD_UNIT` |
| Ready | unit `ActiveState=active` and `SubState=running`, through the existing flap damping |
| join identity in a Secret | file store, `--join-state-dir` (already exists: push/join_store.go) |

One process, no controller-runtime manager, no gRPC sensor stream, no
listener. Sensor roles feed the collectors in-process; the leader-only logic
runs unconditionally (one agent per host, no HA).

## What depends on Kubernetes today

From a code survey (file:line on branch `feat/otlp-receiver`); each needs a
host replacement or a seam:

1. `push.New` and `Pusher.Start` take a `manager.Manager`: cache, informer
   sync, leader election, Event and Namespace informers
   (push/pusher.go:335-354, 419, 428, 494, 525).
2. Collectors are manager Runnables (cmd/triage-agent/main.go:590, 637, 708);
   `usemetrics` resolves identity with `mgr.GetClient()`
   (usemetrics/collector.go:296; usemetrics/identity.go:41). `redsource` and
   `logmetrics` only take `mgr` as a parameter.
3. `nodeexporter` finds cgroups from a node-scoped pod informer (pod UID and
   QoS to cgroup path: nodeexporter/exporter.go:110-130, 206;
   nodeexporter/cgroup.go:41, 66-116). Its file readers take a plain dir and
   are reusable (cgroup.go:129, 183-380; nodecapacity.go:25, 74).
4. `logexporter` tails `/var/log/containers/*.log`, names from the file name,
   service from a pod lister (logexporter/exporter.go:95, 155-172;
   discovery.go:26, 90). Classifier and redaction are reusable
   (classify.go:23-24; redact.go:18-39; metrics.go:144). No journald support.
5. The OBI config is built with `k8s_namespace` selectors,
   `kubernetes.enable: true` and the k8s name resolver
   (cmd/triage-agent/ebpf_config.go:58-102). Without the k8s decorator OBI
   names a service from `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` or
   the executable (third_party/obi/.../exec/file.go:200-237) and sets no
   `k8s.namespace.name`; the RED profile keys on it
   (redsource/profiles.go:44, 51) and drops series without a destination
   identity (redsource/deltas.go:134-148). OBI can select processes by
   `exe_path`, `open_ports`, `cmd_args`, `target_pids`
   (third_party/obi/pkg/appolly/services/attr_glob.go:89-114) but has no
   systemd selector, and excludes systemd paths by default
   (third_party/obi/pkg/obi/config.go:346).
6. Virtual apps come from `workloadInfo` through the package-private
   `applyWorkload` (push/virtualapps.go:141, 638, 900); no exported source
   interface.
7. `--mode` rejects `controller` next to sensor roles
   (cmd/triage-agent/sensor_roles.go:84), and the sensor roles call
   `ctrl.GetConfigOrDie()` (sensor_roles.go:50, 71).
8. Every payload item carries `namespace` and is routed per namespace
   (push/protocol.go:259 and siblings; push/tenant_routing.go:69-103). With an
   empty static namespace list the scope is empty
   (push/namespace_scope.go:163-169), so host mode sets the static list to
   the synthetic namespace.

## Phases

Each phase ends with tests green, a commit, and the acceptance check run on a
test VM. Do not push or deploy unless asked.

### 0. ADR and spike

- Write `docs/adr/0018-linux-host-mode.md` from this file (decisions in the
  Model table, the open questions below with the answers the spike finds).
- Spike OBI on a VM without Kubernetes: a pinned config with
  `kubernetes.enable: false`, discovery by `exe_path`/`open_ports`, and
  confirm which attributes reach the metrics (does a process carry its PID,
  or can each `instrument` entry set a `name`?). This decides how RED is
  attributed to a unit in phase 5.

### 1. Host runtime skeleton

- New mode `--mode=host` (cmd/triage-agent/host.go): no manager, no
  kubeconfig. Flags: `--host-namespace`, `--host-units`, `--cgroup-root`,
  `--target-tenant`, push and join flags, collector flags as today.
- In-process source: an adapter from `sensorstream.RoleSource` (`Collect()`,
  sensorstream/client.go:50) to `metricset.TimedSource`, about 50 lines; the
  collectors read it as they read the hub.
- Run collectors without a manager: they are `Start(ctx) error`; replace the
  `mgr` uses with narrow interfaces (an identity resolver for `usemetrics`).
- Push without a manager: split the transport (connection, join, tenant
  workers, frame queues) from the Kubernetes inputs, and add a host
  constructor that takes an app source and no informers. Keep
  `push.New` byte-identical for the cluster path.
- Acceptance: on a VM, the agent joins with `--join-ca-fingerprint`, an
  admin approves it with `triagectl agent approve`, and the core shows the
  session with the host name and tenant; `hello` carries the synthetic
  namespace.

### 2. Units as virtual apps

- Unit discovery through systemd D-Bus (`github.com/coreos/go-systemd/v22/dbus`,
  pure Go): list `*.service` units matching `--host-units`, subscribe to
  property changes, and poll every 10 s as a fallback. Default selection:
  every enabled service except a built-in denylist (`systemd-*`, `dbus`,
  `ssh*`, `cron`, `getty@*`, `polkit`, `udisks2`, `snapd*`, `triage-agent`).
- An exported app-source seam in `push` that feeds `applyWorkload`: one unit
  becomes one workload, Ready from `ActiveState`/`SubState`, restarts from
  `NRestarts` as a change.
- Opt-out per unit by a drop-in environment marker is not needed in phase 2;
  `--host-units` covers it.
- Acceptance: `snapshot` lists the selected units; stopping one sends an
  `app_delta` after the hysteresis.

### 3. USE from unit cgroups

- A host collect loop in `nodeexporter` that walks
  `<cgroup-root>/system.slice/<unit>` for the selected units and calls the
  existing readers; labels `k8s.namespace.name=<host-namespace>`, service and
  pod both the unit name (without `.service`).
- Limits from the cgroup's own `cpu.max` and `memory.max`; no request
  fallback, so an unlimited unit reports node-relative usage only.
- Acceptance: `use_summary` lists each unit with CPU, memory and PSI; a
  `stress-ng` unit raises CPU PSI above the threshold and sends
  `use_metrics`.

### 4. LOG from journald

- A journald reader in `logexporter`: `journalctl --follow --output=json
  --since=now` with one `_SYSTEMD_UNIT=` match per selected unit, restarted
  with backoff; no cgo. Attribute by `_SYSTEMD_UNIT`. Classify by `PRIORITY`
  (0-3 error, 4 warn) and fall back to the existing regexes for `MESSAGE`;
  redaction and sampling unchanged.
- Optional plain-file tails for services that log to files:
  `--host-log-files=<unit>=<glob>`.
- Acceptance: a unit logging errors shows in `log_metrics` with a redacted
  sample.

### 5. RED and topology from OBI

- Host OBI config: `kubernetes.enable: false`, no k8s resolver, discovery
  from the selected units' main PIDs (`target_pids` refreshed on unit
  changes, or `exe_path` from `ExecStart`, per the phase 0 spike).
- Attribution: stamp each series with `k8s.namespace.name=<host-namespace>`
  and `service.name=<unit>`, mapping process to unit through
  `/proc/<pid>/cgroup`. Keep redsource keyed on namespace and service as
  today.
- Topology: client spans to another host's unit resolve only by address;
  send them as external hosts until a fleet name map exists. L4 flows with
  `ebpf.networkFlows` as in the cluster.
- Acceptance: a curl loop against an nginx unit shows in `red_summary`; a
  unit that calls another shows an edge in `topology_snapshot`.

### 6. Packaging and docs

- Release binaries for linux/amd64 and arm64 built with `-tags obi`, and a
  unit file `triage-agent.service`: `User=root`,
  `CapabilityBoundingSet=` and `AmbientCapabilities=` with BPF, PERFMON,
  NET_RAW, SYS_PTRACE, DAC_READ_SEARCH, CHECKPOINT_RESTORE (same six as the
  DaemonSet), `NoNewPrivileges=yes`, `ProtectSystem=strict`,
  `ProtectHome=yes`, `ReadWritePaths=` the join state dir,
  `EnvironmentFile=/etc/triage-agent/env`.
- deb and rpm packages with nfpm, optional.
- Update docs/spec.html: the Linux host box loses "soon", FAQ answer 5, the
  tenant-mode table, requirements (kernel and BTF as today, systemd 240+,
  cgroup v2). README quickstart for a host.

## Open questions

- Host containers (Docker, Podman) are out of scope; their cgroups are under
  `system.slice/docker-<id>.scope`. Revisit after phase 5.
- Whether the core should show a host differently from a cluster of one
  (a `hello` capability flag `host` costs nothing and keeps the option open;
  add it in phase 1).
- OTLP receiver on the host for local SDKs: possible later on 127.0.0.1 with
  the synthetic namespace as the default; not in this plan.

## Rules for whoever implements this

- Build from a clean worktree on a new branch `feat/linux-host` off
  `feat/otlp-receiver`; stage only your own hunks.
- Conventional commits, no Co-Authored-By trailer. No cluster, company, tenant
  or user names in code, docs, tests or commits.
- `go test ./...` and `make lint` green at every commit; new code gets unit
  tests next to it (cgroup and journald readers against fixtures).
- Keep the cluster path unchanged: the chart renders identically, and
  `push.New` behaves as before.

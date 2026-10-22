# ADR-0018: Linux Host Mode

- **Status:** Accepted. Phases 0 to 6 implemented 2026-10-08 (phase 0 answered
  the OBI question from the source, not a VM run). The manual acceptance steps
  under `## Acceptance` have not yet been run on a Linux host.
- **Date:** 2026-10-08
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (push mode), ADR-0009 (RED metrics), ADR-0010 (USE
  metrics), ADR-0006/0014 (log-based metrics), ADR-0011/0013 (topology),
  ADR-0015 (sensor stream), ADR-0017 (OTLP receiver)

## Context

The agent is a Kubernetes controller: virtual apps come from workload
objects, Warning events from the Event API, the sensors run on a
DaemonSet and stream to the controller, and the push transport sits on a
controller-runtime manager for its cache, informers and leader election.

A plain Linux host (a VM or bare metal) running services as systemd units
has none of that. It has the same questions: which services run, are they
up, what are their RED, USE and LOG signals, who calls whom. The core
answers them for a cluster from the frames the agent sends; it should not
need to know whether a host or a cluster sent them.

## Decision

The agent gains `--mode=host`: one process, one host, no Kubernetes API,
no controller-runtime manager, no sensor stream listener, no replica
gossip. It reports the same frames a cluster agent sends. A host is a
cluster of one with one synthetic namespace.

| Kubernetes agent | Host agent |
|---|---|
| cluster (`--cluster-name`) | the OS host name; `--cluster-name` overrides it |
| namespace | one synthetic namespace, `--host-namespace` (default `host`); a fleet of hosts running the same services shares one value so their services merge in the core graph |
| tenant from a namespace annotation | `--target-tenant` only (tenant mode); cluster mode is not offered |
| workload / virtual app | systemd unit (`*.service`), selected by `--host-units` globs |
| pod | the unit's main cgroup (`<cgroup-root>/system.slice/<unit>`) |
| container logs (`/var/log/containers`) | journald, by `_SYSTEMD_UNIT` |
| Ready | unit `ActiveState=active` and `SubState=running`, through the existing flap damping |
| join identity in a Secret | file store, `--join-state-dir` |
| sensors on a DaemonSet, streamed over gRPC | sensor roles in-process, read by the collectors through `sensorstream.Local` |
| leader-only loops | run unconditionally: one agent per host, no HA |

Phase 1 ships the skeleton:

- **Transport without Kubernetes.** `push.NewHost` builds a `Pusher` with
  no manager, cache, informers or leader election. Its namespace scope is
  the static list `[host-namespace]`, its tenant routing is tenant mode
  with `--target-tenant`, and `Start` runs the connection loop only: hello,
  the (empty, for now) snapshot sequence, heartbeat and `sensor_state`.
  `push.New` and the cluster path are unchanged.
- **Hello.** The namespace list carries the synthetic namespace, and the
  capabilities carry `host` next to the collector capabilities. The core
  stores `HelloPayload.Capabilities` on the session as sent and validates
  no entry (`internal/remoteagent/receiver.go`, `registerSession`), so an
  older core tolerates the new string; nothing on the core side gates on
  it yet. This keeps the option of showing a host differently from a
  cluster of one without a protocol change.
- **In-process sensors.** `sensorstream.Local` runs the sensor roles in
  the agent's own process on the sensor tick and serves their latest sets
  as a `metricset.TimedSource`, so the log, USE and RED collectors read it
  exactly as they read the hub. Its `Nodes()` feeds the same `sensor_state`
  frame. Phase 1 registers no role; phases 3 to 5 add theirs.
- **Collectors without a manager.** The collectors never used the manager
  beyond its client: `redsource.New` and `logmetrics.New` lose the
  parameter; `usemetrics.New` takes an `IdentityResolver`, with the
  owner-walking `OwnerIdentityResolver` on the cluster path and a static
  resolver on the host that names every pod of the synthetic namespace by
  its own name (one unit is one pod is one service).
- **Flags.** `--host-namespace`, `--host-units` (globs, validated and kept
  for phase 2), `--cgroup-root`, `--target-tenant` (required), the push and
  join flags and the collector flags as in the cluster. `host` is refused
  next to any other `--mode` value.

### OBI without Kubernetes

Phase 5 attributes RED and topology to a unit. The plan asked a spike to
decide how, on a VM. No VM was available, so the answer below comes from
reading the vendored OBI source (`third_party/obi`), not from a run; the
acceptance steps at the end confirm it on a host.

- **Discovery.** A `discovery.instrument` entry selects by `exe_path`,
  `open_ports`, `cmd_args`, `target_pids` and `languages`
  (`pkg/appolly/services/attr_glob.go`); the `k8s_*` keys are the only
  Kubernetes-bound selectors. There is no systemd selector.
  `excluded_linux_system_paths` (default `/lib/systemd/`,
  `/usr/lib/systemd/`, `/usr/libexec/`, `/sbin/`, `/usr/sbin/`) only skips
  language detection under those prefixes; `exe_path` and `open_ports`
  still match them.
- **Naming.** An entry can set `name` and `namespace` (deprecated but
  working, `pkg/appolly/discover/typer.go`); they become `service.name`
  and `service.namespace` of every process it matches, the last matching
  entry winning. The process's own `OTEL_SERVICE_NAME`, then `service.name`
  in `OTEL_RESOURCE_ATTRIBUTES`, override the entry
  (`pkg/appolly/discover/exec/file.go`); with nothing set the executable
  basename is used. No setting supplies a default namespace.
- **Attributes.** With `attributes.kubernetes.enable: false` the k8s label
  group is off (`pkg/export/attributes/attr_defs.go`: `appKubeAttributes`
  follows `GroupKubernetes`), so no `k8s.*` attribute is set and a series
  keeps `service.name` and `service.namespace` (reported as metric
  attributes by default, empty unless an entry or the environment sets
  them). The resource carries `service.instance.id`, composed as
  `<hostname>:<pid>` (`pkg/internal/traces/read_decorator.go`). The PID
  reaches the collectors only inside that id, so a PID-to-unit map needs
  the `/proc/<pid>/cgroup` lookup the plan names, or a `name` per entry.

So phase 5 can select each unit's processes by PID (`target_pids`, refreshed
on unit changes) or by `exe_path` from `ExecStart`, pin `name: <unit>` and
`namespace: <host-namespace>` on the entry, and map `k8s.namespace.name`
from `service.namespace` for the RED profile, which keys on it. A service
that sets `OTEL_SERVICE_NAME` itself keeps its own name, as in a cluster.

### Phase 5: RED and topology from OBI

- **Discovery.** One `discovery.instrument` entry per selected unit with at
  least one process: `target_pids` is every PID in the unit's cgroup and its
  sub-cgroups (`<cgroup-root>/system.slice/<unit>.service/**/cgroup.procs`),
  so a master and its workers are all selected. The entry pins
  `name: <unit>` and `namespace: <host-namespace>`. `kubernetes.enable` is
  false, which also strips the k8s name resolver. Selection by PID, not by
  `exe_path` from `ExecStart`, because two units can run one binary and
  `exe_path` would name both by the last matching entry. The default
  exclusions (OBI and otelcol binaries) stay; the systemd path exclusion
  only affects language detection, which is wrong for daemons that live in
  `/usr/sbin` and `/usr/libexec`, so `excluded_linux_system_paths` is
  emptied.
- **Refresh.** OBI reads `target_pids` once at start and does not re-evaluate
  it for new processes, so the runner restarts the embedded instrumenter with
  a new config when a selected unit gets a PID the running config lacks. The
  cgroups are polled on the discoverer's period (10 s) and a PID counts after
  two consecutive polls, so a short-lived child does not cause a restart; a
  vanished PID or a unit that left the selection needs none, because OBI
  drops an exited process itself. The cost is a re-attach of every uprobe on
  a change and a short RED gap; the metrics store keeps serving across it.
  OBI also has a runtime PID selector (`instrumenter.WithDynamicPIDSelector`)
  that would need no restart, but passing one starts the network and stats
  agents whatever the config says; that is untested here and left as a later
  option. A failing instrumenter (no BTF, missing capability) is logged and
  retried every 30 s, never returned, so it cannot take the host's USE and
  LOG down with it.
- **Attribution.** The host's RED `RoleSource` wraps the OBI metrics store
  and stamps each resource `k8s.namespace.name=<host-namespace>` and
  `service.name=<unit>`, on the resource and on data points that carry those
  keys. The unit comes from the PID in `service.instance.id`
  (`<hostname>:<pid>`) through `/proc/<pid>/cgroup`, which also covers a
  process that sets `OTEL_SERVICE_NAME`, or else from the pinned
  `service.namespace`/`service.name`. A resource that matches neither, such
  as the network flow resource, passes unchanged. `redsource` and the
  cluster path are untouched.
- **Topology.** A client span names its callee by `server.address` only. With
  no cluster index the callee is an external host, including a unit on the
  same host or on another one; no fleet name map exists yet. With
  `OBI_NETWORK_FLOWS=true` the L4 flow metric is on as in the cluster, but
  without Kubernetes metadata a flow carries addresses and no owner names,
  so it yields no unit edge.
- **Build.** The role needs the `obi` build tag, like the cluster role
  (`ebpf_host_obi.go`); without it `ebpf_host_stub.go` logs "built without
  -tags obi" and registers nothing. It runs when `--red-metrics` or
  `--topology` is on.

## Consequences

- A host reports with no change to the core or the protocol: the same
  frames, the same tenant pin, the same join approval.
- One agent per host and no HA: the leader-only loops run unconditionally,
  and a second agent on the same host would duplicate every frame.
- `--mode=host` is a separate entry point (`cmd/triage-agent/host.go`)
  next to the controller and the sensor roles; the chart does not render
  it and is unchanged.
- The `hello` frame still lists `applications` and `k8s_events` first, as
  every agent does; a host sends no Kubernetes events. The core does not
  gate on either, and `host` is what tells a reader which agent it is.
- Until phases 3 to 5 the host sends
  no metrics: the session shows the host name, tenant, namespace and its
  units. Each phase fills one row of the table above.
- The `usemetrics` constructor changed shape (`IdentityResolver` in place
  of the manager); every other collector constructor only lost an unused
  parameter.

## Acceptance

Manual, on a Linux host with systemd and outbound access to a core that
has agent join enabled. Not run on a VM for this phase.

1. Build `bin/triage-agent` (plain `go build` is enough for phase 1) and
   copy it to the host.
2. Run, as any user that can write the join state directory:

   ```sh
   ./triage-agent --mode=host \
     --triage-endpoint=wss://core.example.internal/events/ws \
     --target-tenant=acme \
     --join-ca-fingerprint='SHA256:...' \
     --join-state-dir=/var/lib/triage-agent/join
   ```

   Expected log lines: `triage-agent starting ... mode=host`, `host mode
   enabled` with `cluster=<hostname>` and `namespaces=[host]`, then `join
   pending: approve with triagectl agent approve <hostname> --fingerprint
   ...`.
3. On an admin machine: `triagectl agent approve <hostname> --fingerprint
   <printed fingerprint>`. The agent logs `agent certificate issued` and
   reconnects.
4. On the core, `triagectl agent list` (or the UI's agent view) shows the
   session with the host name as the cluster, tenant `acme`, namespaces
   `host`, and capabilities including `host` and `sensor_state`.
5. `--cluster-name=web-01` in place of the default changes the cluster
   the core shows; `--mode=host --mode=ebpf` exits with an error naming
   the conflict; omitting `--target-tenant` exits with an error.
6. The HMAC path also works: `--push-token-file=/etc/triage-agent/token`
   in place of the join flags. Outside Kubernetes the token file is allowed;
   in a pod the agent joins by certificate only (2026-10 decision).

### Phase 2: units as virtual apps

Manual, on the same host after the phase 1 steps. Not run on a VM for this
phase.

1. Run with a glob for two test services, for example `--host-units='nginx*'
   --host-units=redis`, or with no `--host-units` to take every enabled
   service except the built-in denylist (`systemd-*`, `dbus`, `ssh*`, `cron`,
   `getty@*`, `polkit`, `udisks2`, `snapd*`, `triage-agent`).
2. On the core, `snapshot` for the tenant lists one app per selected unit in
   the namespace `host`, named by the unit without `.service`, with
   `componentsReady` `1/1` for each unit that is `active (running)`.
3. `systemctl stop nginx`. After the core's hysteresis the app turns not
   ready: an `app_delta` with op `update` and `0/1` arrives. `systemctl
   start nginx` brings it back to `1/1`.
4. `systemctl restart nginx` raises `NRestarts` only for a unit that
   restarts itself (`Restart=always` after a crash, for example): `kill -9
   <MainPID>` and the app's description changes to `restarts: 1`, sent as an
   `app_delta` update.
5. `systemctl disable --now` a unit selected by default (no globs) removes
   its app: an `app_delta` with op `delete` arrives.
6. With the agent started while D-Bus is unreachable, it logs `app source
   ended; restarting` every 5 s and still sends `hello` and heartbeat.

### Phase 3: USE from unit cgroups

Needs a host with cgroup v2 (`stat -fc %T /sys/fs/cgroup` prints
`cgroup2fs`) and PSI (`cat /proc/pressure/cpu` works), on top of steps 1 to 4
above, with `--target-tenant` set and `--use-metrics` left at its default.

1. Install `stress-ng` and a limited unit, `/etc/systemd/system/stress-ng.service`:

   ```ini
   [Unit]
   Description=CPU stress for the triage agent USE acceptance

   [Service]
   ExecStart=/usr/bin/stress-ng --cpu 0 --timeout 0
   CPUQuota=100%
   MemoryMax=512M
   ```

   Then `systemctl daemon-reload && systemctl start stress-ng`. Also leave a
   second service running with no `CPUQuota` or `MemoryMax` (any idle one,
   such as `nginx`).
2. Restart the agent with `--host-units='stress-ng.service,nginx.service'`
   (or none, to select every service outside the built-in denylist).
3. Check the cgroup files the agent reads:
   `cat /sys/fs/cgroup/system.slice/stress-ng.service/{cpu.max,memory.max,cpu.pressure}`
   shows `100000 100000`, `536870912` and a `some avg60=` line.
4. After a minute or two, ask the core for `use_summary` on the tenant. It
   lists `stress-ng` and `nginx` as services of namespace `host`, each with CPU,
   memory and PSI. `stress-ng` shows a CPU limit of 1 core, CPU
   utilisation near 100 percent and a non-zero CPU PSI. `nginx` shows usage
   against the node only (no limit, no limit percentage).
5. Raise the pressure: `systemctl set-property stress-ng CPUQuota=25%`. CPU
   PSI of `stress-ng` rises above the core's threshold and the agent sends
   `use_metrics` for it (the core's log or `triagectl` shows the frame).
6. `systemctl stop stress-ng` removes the unit from the next sample without
   an error in the agent log.

### Phase 4: LOG from journald

Needs a host with systemd and a unit that logs errors. `app-demo.service`
below is any such unit; run the agent as root (or a user in the
`systemd-journal` group) so `journalctl` can read the unit's journal.

1. Run the phase 1 command with `--host-units=app-demo.service` and
   `--log-metrics-enabled`. The followed units are the ones systemd
   discovery selects, so a glob works here too.
2. Make the unit log an error and a warning, for example by having it run
   `logger -p user.err -t app-demo 'db connect failed password=hunter2'`
   under the unit, or `systemd-cat -p err echo 'db connect failed
   password=hunter2'` inside a oneshot copy of it.
3. On the host, `journalctl -u app-demo.service -n 5 -o json` shows the
   entries with `PRIORITY` and `_SYSTEMD_UNIT=app-demo.service`; the agent
   starts `journalctl --follow --output=json --since=now` with one
   `_SYSTEMD_UNIT=` match per selected unit (visible with `ps`).
4. After the next sensor tick and the logmetrics window, the core's
   `log_metrics` for namespace `host`, service `app-demo` shows the error
   count and a `sample` of `db connect failed password=***`, never
   `hunter2`.
5. `systemctl restart systemd-journald` (the follower exits and is
   restarted after the backoff) and changing `--host-units` followed by an
   agent restart both keep counting; lines logged while the follower was
   down are not counted (the follower starts at `--since=now`).
6. For a service that writes a file, add
   `--host-log-files=app-demo=/var/log/app-demo/*.log` and append an
   `ERROR ...` line to a matching file: it is counted under the same
   service name.

### Phase 5: RED and topology from OBI

Needs a binary built with `-tags obi`, root or the capabilities of the
packaged unit (BPF, PERFMON, NET_RAW, SYS_PTRACE, DAC_READ_SEARCH,
CHECKPOINT_RESTORE), a kernel with BTF, and `--target-tenant` set with the RED
and topology collectors left at their defaults. Not run on a VM for this
phase.

1. Install `nginx` and a second service that calls it, for example
   `/etc/systemd/system/caller.service` running
   `/bin/sh -c 'while true; do curl -s http://127.0.0.1/ >/dev/null; sleep 0.2; done'`
   (`systemctl daemon-reload && systemctl start caller`). Start the agent with
   `--host-units='nginx.service,caller.service'`. The log shows `host OBI
   starting` with `units=2`; a binary built without the tag logs `built
   without -tags obi` instead and sends no RED.
2. Check what OBI follows: `cat /sys/fs/cgroup/system.slice/nginx.service/cgroup.procs`
   lists the master and the workers, and they are the `processes` count in
   the log line.
3. Drive load from another machine (or the host):
   `for i in $(seq 1 2000); do curl -s http://<host>/ >/dev/null; done`, and
   `curl -s http://<host>/missing` a few times for 404s.
4. After one or two sensor ticks, ask the core for `red_summary` on the
   tenant. It lists `nginx` in namespace `host` with the request rate and
   latency percentiles; nothing is listed under a `<hostname>:<pid>` name or
   an empty namespace.
5. `systemctl restart nginx`. Within about 20 s the agent logs `host OBI
   selection changed, restarting` and the new nginx processes show again in
   `red_summary`; the gap is the restart plus the polls.
6. `topology_snapshot` for the tenant shows an edge from `caller` to an
   external destination (`127.0.0.1`): a unit that calls another resolves by
   address only, so even a same-host callee is external until a fleet name
   map exists. To see the 5xx path, make nginx return 502 (a `proxy_pass` to
   a closed port) and `red_metrics` is sent once the error gates trip.
7. With `OBI_NETWORK_FLOWS=true` in `/etc/triage-agent/env` the agent also
   starts the flow capture (`network: socket_filter`); the log shows no
   error. Flow series carry no unit names on a host, so no extra edge
   appears.

### Phase 6: packaging

Needs a host with systemd 240+, cgroup v2 and a kernel with BTF, and a core
with agent join enabled. Use the deb on Debian or Ubuntu, the rpm on a
RHEL-family host, for the host's architecture.

1. Install the package (`dpkg -i triage-agent_<version>_amd64.deb` or `rpm -i
   triage-agent-<version>.x86_64.rpm`). `/usr/bin/triage-agent`,
   `/usr/lib/systemd/system/triage-agent.service`, `/etc/triage-agent/env`
   (mode 0600) and `/var/lib/triage-agent/join` exist, and `systemctl cat
   triage-agent` shows the unit (the postinstall ran `daemon-reload`). The
   service is not enabled or started yet.
2. Edit `/etc/triage-agent/env`: `TA_ENDPOINT`, `TA_TENANT` and
   `TA_JOIN_CA_FINGERPRINT`. Reinstalling the package keeps the edited file.
3. `systemctl enable --now triage-agent`. `systemctl show triage-agent -p
   User -p NoNewPrivileges -p ProtectSystem -p ProtectHome -p
   CapabilityBoundingSet -p AmbientCapabilities` reports root, `yes`,
   `strict`, `yes` and exactly the six capabilities. `systemd-analyze
   security triage-agent` lists no unexpected exposure beyond them.
4. `journalctl -u triage-agent` shows `triage-agent starting ... mode=host`
   and `join pending: approve with triagectl agent approve ...`. On an admin
   machine, `triagectl agent approve <hostname> --fingerprint <fingerprint>`;
   the agent logs `agent certificate issued`.
5. `systemctl status triage-agent` is `active (running)` and
   `journalctl -u triage-agent -p warning` has no entries beyond start-up.
   `triagectl agent list` shows the host under tenant `acme`.
6. `systemctl restart triage-agent` rejoins without a new approval: the key
   is in `/var/lib/triage-agent/join`, the only path the unit may write.
   Setting `TA_JOIN_STATE_DIR=--join-state-dir=/root/x` and restarting makes
   the agent exit with a read-only file system error, which shows
   `ProtectSystem=strict` is in force; undo it afterwards.
7. On the release page, `triage-agent_linux_amd64`, `triage-agent_linux_arm64`
   and `triage-agent_linux_sha256sums.txt` are present and the checksums
   match; `triage-agent_linux_amd64 --help` runs with the `--mode` flag
   listed.

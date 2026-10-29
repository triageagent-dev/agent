[![CI](https://github.com/triageagent-dev/agent/actions/workflows/ci.yaml/badge.svg)](https://github.com/triageagent-dev/agent/actions/workflows/ci.yaml)

# Triage Agent

Triage Agent is the per-cluster agent of the Triage System. It ships as three parts:

- A controller Deployment, one per cluster, multi-tenant by namespace annotation.
- A per-node DaemonSet running the eBPF instrumentation, the node exporter and the log exporter.
- Cloud connectors: one package per provider that turns provider signals into events for the core. Google Cloud is implemented in `gcpevents/` (cost anomalies, Service Health).

## What it does

- Groups workloads into applications: virtual apps synthesized by workload name (or, opted in per namespace, by label).
- Watches Kubernetes Warning events.
- Collects RED metrics (rate, errors, duration) and USE metrics (utilization, saturation, errors) with an eBPF DaemonSet, per the RED and USE signal frameworks.
- Counts log errors per workload.
- Builds the service topology from observed L7 calls and optional L4 network flows.
- Streams everything to the core over one outbound WebSocket.
- Bridges cloud provider signals into the same event stream through per-provider connectors.

## What it sends

| Frame kind | Content | Enabled by |
|---|---|---|
| `snapshot`, `app_delta` | Application status: virtual apps synthesized by workload name (or label, per namespace) | Always, once the core connection is on |
| `k8s_event` | Kubernetes Warning events only | Always, once the core connection is on |
| `red_metrics` (gated), `red_summary` (ungated, every service) | RED metrics | `redMetrics.enabled` |
| `use_metrics` (gated), `use_summary` (ungated, every service) | USE metrics | `useMetrics.enabled` |
| `log_metrics` | Log error/warn counts per workload | `logMetrics.enabled` |
| `topology_snapshot` | Service topology: L7 calls plus optional L4 network flows | `topology.enabled` |

## What it receives

| Frame kind | Effect |
|---|---|
| `resync` | Resend one tenant's full snapshot on the live connection, without reconnecting — sent by the core when that tenant's service graph reloads |

## Install

```bash
helm upgrade --install triage-agent \
  oci://registry.invalid/example-shared-registry/charts/triage-agent \
  -n triage --create-namespace \
  --set core.host=<core-host> \
  --set core.clusterName=<cluster> \
  --set core.caFingerprint=<SHA256:... from `triagectl agent list`>
```

An agent authenticates to the core only by a join certificate: an admin-approved certificate request or a one-time agent key, pinned to the core CA by `core.caFingerprint` (`--join-ca-fingerprint`). The chart requires it when `core.host` is set, and the binary refuses to start without it whenever a core endpoint is set, in a pod and in Linux host mode alike.

After install the agent logs `join pending` with its key fingerprint; approve it on the core with `triagectl agent approve <cluster> --fingerprint <agent fingerprint>`. The key and certificate are kept in a Secret in the release namespace, so a new pod needs no new approval.

**With a one-time agent key** (core ADR-090) there is no approval step. The core returns the whole command when a key is minted (`POST /v2/tenants/{id}/keys`): `core.host`, `core.caFingerprint`, `core.clusterName` and `core.tenants` as the key fixes them, and `core.agentKey=tak_...`. The agent runs in cluster mode with no `target.*` values: annotate each namespace with `triageagent.dev/tenant=<tenant ID or name>` (the core prints those lines too), and the core drops anything outside the key's bindings. The chart writes the key into the join Secret, the core issues the certificate on the first join, and the agent then removes the key. A key works once, for that cluster name and those tenants only.

Upgrading an install that used `push.token` or `push.existingSecret`: both values are gone. Read the core CA fingerprint from `triagectl agent list` on the core, set `core.caFingerprint`, upgrade, then approve the join as above. The old token Secret can be deleted afterwards.

| Value | Meaning |
|---|---|
| `core.host` | Triage core hostname; setting it turns the connection on. `push.*` is the old name of `core.*`, still read with `push.enabled=true` |
| `core.agentKey` | One-time agent key from the command the core prints; the join is approved at once |
| `core.caFingerprint` | Core agent CA fingerprint; required with `core.host`. The agent joins by certificate |
| `core.clusterName` | Cluster identifier stamped into pushed events and frames |
| `ebpf.enabled` | Deploy the eBPF DaemonSet, this binary's own `--mode=ebpf` role, embedding OBI directly |
| `redMetrics.enabled` | `red_metrics`/`red_summary`; on by default, runs with `ebpf.enabled` or `otlp.enabled` |
| `topology.enabled` | `topology_snapshot`; on by default, runs with `ebpf.enabled` or `otlp.enabled` |
| `useMetrics.enabled` | `use_metrics`/`use_summary`; on by default, runs with `useMetrics.nodeExporter` or `otlp.enabled` |
| `logExporter.enabled` | Deploy the native log-exporter container feeding `log_metrics` |
| `ebpf.networkFlows.enabled` | Add L4 network-flow topology edges |
| `image.tag` | Controller image tag, empty defaults to `v<Chart.appVersion>` |

Scope observation by annotating namespaces with `triageagent.dev/tenant`, or by setting `--target-namespaces`. Annotate a workload, its pod template, or a namespace with `triageagent.dev/triage: "false"` to take it out of triage entirely: the agent sends no events, metrics, topology or virtual app for it (see the chart README's "Excluding a workload from triage"). `ebpf.networkFlows.enabled` sets `hostNetwork: true` on the DaemonSet. `--workload-resources` (repeatable, `group/version/resource`) sets which namespaced workload kinds virtual-app synthesis discovers; omit it for the default list (Deployments, StatefulSets, DaemonSets, Argo Rollouts).

## Install on a Linux host

**With a one-time agent key** (core ADR-090), one command does all of the steps below and needs no approval. The core returns it, filled in, when a host key is minted:

```bash
curl -fsSL https://github.com/triageagent-dev/agent/releases/latest/download/install.sh | sudo sh -s -- \
  --core <core-host> --ca SHA256:<fp> --tenant <tenant-id> --name <cluster-name> --key tak_<id>_<secret>
```

It installs the release's deb or rpm after checking its SHA-256, writes `/etc/triage-agent/env`, puts the key in `/var/lib/triage-agent/join/enroll.key` (0600) and starts the service; the agent removes the key once its certificate is issued.

`--mode=host` runs the agent as one systemd service on a VM or bare-metal host, with no Kubernetes API. The host is a cluster of one; each selected systemd unit is a virtual app, with USE from its cgroup, LOG from journald and RED from OBI (the release binaries are built with `-tags obi`). It needs a 5.8+ kernel with BTF, cgroup v2 and systemd 240+. See `docs/spec.html` and `docs/adr/0018-linux-host-mode.md`.

```bash
# 1. Download the binary for your architecture (amd64 or arm64) and check it.
ARCH=amd64
BASE=https://github.com/triageagent-dev/agent/releases/latest/download
curl -fLO "$BASE/triage-agent_linux_$ARCH"
curl -fLO "$BASE/triage-agent_linux_sha256sums.txt"
sha256sum --ignore-missing -c triage-agent_linux_sha256sums.txt
sudo install -m 0755 "triage-agent_linux_$ARCH" /usr/bin/triage-agent

# 2. Install the unit and the environment file (from a checkout of this repo,
#    or use the deb/rpm from the release, which does steps 1 and 2).
sudo install -m 0644 packaging/systemd/triage-agent.service /etc/systemd/system/
sudo install -d -m 0755 /etc/triage-agent
sudo install -m 0600 packaging/systemd/env.example /etc/triage-agent/env
sudo systemctl daemon-reload

# 3. Set the endpoint, the tenant and the core's agent CA fingerprint.
sudoedit /etc/triage-agent/env     # TA_ENDPOINT, TA_TENANT, TA_JOIN_CA_FINGERPRINT

# 4. Start it. The agent logs "join pending" with its own fingerprint.
sudo systemctl enable --now triage-agent
journalctl -u triage-agent -f
```

On an admin machine, approve the host with the fingerprint from the log, then check on the host and the core:

```bash
triagectl agent approve <host-name> --fingerprint <fingerprint-from-the-log>
systemctl status triage-agent      # active (running)
journalctl -u triage-agent -n 20   # "agent certificate issued", no errors
triagectl agent list               # the host, tenant, namespace "host", capability "host"
```

The core's agent CA fingerprint comes from `triagectl agent list`. `/etc/triage-agent/env` documents every host flag; the unit reads them from there and runs `--mode=host` itself. The key and certificate stay in `/var/lib/triage-agent/join`, so a restart needs no new approval.

## Local TUI and monitoring API

Both are off by default.

**TUI.** `--ui-listen 127.0.0.1:8088` (chart `ui.listen`) serves a browser TUI built like the core's: panel 1 `app` shows the virtual apps with their traffic, a dependency graph and a treemap; panel 2 `sensors` shows the sources (Kubernetes watch or host units, eBPF, node-exporter and log-exporter sensors, OTLP senders) beside the events they raise. `T` or `N` picks the tenant, `?` shows the flags in effect and the agent's statistics, `/` filters, `s` cycles app state. It shows what the agent sends to the core. With no `--triage-endpoint` (chart `core.host` unset) and the TUI or API on, the agent runs standalone: same view, nothing sent. With replicas every replica shows the leader's view. It binds loopback only: reach it with `kubectl port-forward deploy/<name> 8088:8088` (host mode: an SSH tunnel). With an admin key it asks for a login: in a cluster `--ui-admin-key-file` from a Secret (chart `ui.adminKey.existingSecret`), in host mode also `TRIAGE_AGENT_UI_ADMIN_KEY` or `--ui-admin-key`. See [ADR-0019](docs/adr/0019-local-tui.md).

**Monitoring API.** `--api-listen :9480` with `--api-tokens-file` (chart `api.*`) serves Prometheus `/metrics` of the derived signals, `/.well-known/triage-agent` and the read-only JSON API behind bearer tokens, for Prometheus and Grafana. Each token line is `name tenant token`; a token tied to a tenant can read that tenant only. It will not start without tokens, and only the leader exports signal series. See [ADR-0020](docs/adr/0020-monitoring-api.md).

## Verifying a release

Every file attached to a release (binaries, deb and rpm packages, the SHA-256 sum files) is signed keyless with cosign, and has a `<file>.sigstore.json` bundle next to it. The signing identity is the release workflow of this repository. Download a file and its bundle, then:

```sh
cosign verify-blob \
  --bundle triage-agent_linux_amd64.sigstore.json \
  --certificate-identity-regexp '^https://github.com/triageagent-dev/agent/\.github/workflows/ci\.yaml@refs/tags/v.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  triage-agent_linux_amd64
```

The container image is signed by digest and carries a build-provenance attestation:

```sh
cosign verify ghcr.io/triageagent-dev/agent@sha256:<digest> \
  --certificate-identity-regexp '^https://github.com/triageagent-dev/agent/\.github/workflows/ci\.yaml@.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/triageagent-dev/agent@sha256:<digest> --repo triageagent-dev/agent
```

## Development

Security facts (ports, capabilities, token handling) are in `PASSPORT.md`.

```bash
make bin/triage-agent                 # build the controller binary
make test                             # unit tests (no cluster needed)
make lint                             # golangci-lint
make obi-generate                     # regenerate the vendored eBPF bindings (docker required)
make ko-push KO_DOCKER_REPO=...       # build and push the image `agent` (no OBI); ko-push-obi builds `agent-obi` (-tags obi)
make packages                         # host binaries (-tags obi) and deb/rpm in dist/ (needs nfpm)
```

Two images come off the same binary: `agent` (no OBI) runs the controller and the otlp role; `agent-obi` (`-tags obi`) runs the eBPF DaemonSet, which the chart names as `image.ebpf.repository` (default `<image.repository>-obi`).
See `docs/` for ADRs and todos, and `docs/spec.html` for the agent specification.

## License

Apache-2.0, see `LICENSE` and `NOTICE`.

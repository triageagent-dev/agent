# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the per-cluster agent of the Triage Agent project — a Kubernetes controller that derives "virtual apps" from workload labels and annotations (`triageagent.dev/virtual-app`, `virtual-app-group`, `virtual-app-identity`, `tier`, `weight`, `domain`, `description`, `triage`, namespace key `triageagent.dev/tenant`), runs an OBI eBPF instrumenter plus log and resource metrics collectors, and pushes all of it to the core over an outbound WebSocket.

The only remote is `origin`, https://github.com/triageagent-dev/agent. Do
not add another remote or push without being asked.

## Terminology: core vs agent

The system has exactly two components. Use these words in prose, docs, commit
messages and UI strings:

- **agent** — THIS repo. The per-cluster push agent: it derives virtual apps
  from workload labels and annotations and streams their state, Kubernetes
  Warning events and eBPF RED/USE metrics to the core over an outbound
  WebSocket. Repo
  `triageagent-dev/agent`, chart
  `charts/triage-agent` in this repo, release `triage-agent`.
- **core** — the central triage controller in the sibling repo
  `~/code/triageagent-dev/core` (`github.com/triageagent-dev/core`), chart
  `charts/triage-core`, release `triage`. Never call it "the hub" in prose.

Since the rename, `triage-agent` names this repo/chart/release — but some older
ADRs and todos use it informally for the core too. Existing context markers
("Companion (hub side):", "(core repo)") disambiguate these, so the
prose is left as written; fix an instance only when it is genuinely ambiguous in
context, never by blind sweep.

## Commands

```bash
# Build
make bin/triage-agent           # build binary (just go build, no codegen)
make all                         # fmt, vet, lint, test, build

# Container image — ko only, no Dockerfile anywhere in this repo
make ko-image                    # build with -tags obi, local, no push
make ko-push                     # build with -tags obi, build + push

# Run locally against current kubeconfig
./bin/triage-agent

# Test (no cluster or downloaded test binaries needed)
make test                        # go test on every package with test files, -coverprofile cover.out

# Single test
go test -v ./push/... -run "TestName"

# Code quality
make lint                        # golangci-lint
make fmt                         # go fmt
make vet                         # go vet
```

## Architecture

### Main Entry (`main.go`)

Sets up the `controller-runtime` manager and starts it. Flags: `--metrics-addr` (`0` disables), `--enable-leader-election`, `--namespace`, `--sync-period`, plus the push-mode flags (`--triage-endpoint`, `--cluster-name`, `--join-ca-fingerprint`, `--target-namespaces`, `--core-heartbeat`, `--core-insecure-skip-verify`).

### Push mode (`push/`)

Opt-in outbound-WebSocket streaming of Applications + Kubernetes Warning events to a triage agent, for clusters with no inbound API access (ADR-0005). `push.New()` returns nil unless `--triage-endpoint` is set, so it is a no-op by default. `push.Pusher` is a `manager.Runnable` that reuses the manager cache/informers — it does **not** touch `controllers/`. The wire frames (`push/protocol.go`) mirror the triage receiver's `internal/remoteagent/protocol.go` (separate modules; keep them in sync).

### Sensor stream (`sensorstream/`, `metricset/`, `proto/`)

The eBPF DaemonSet's roles (`ebpf`, `node-exporter`, `log-exporter`) are sensors: none opens a port. Each sensor pod keeps one gRPC stream to the controller (`triageagent.sensor.v1.SensorService/Stream`, `proto/triageagent/sensor/v1/sensor.proto`, generated with `make proto`) and every `--sensor-tick` sends each role's metrics as OTLP `ResourceMetrics` plus its own `SensorState` (ADR-0015). The controller (`--sensor-listen`, Service `<fullname>-sensors`, port 9470) keeps a self-signed certificate in the `-sensor-tls` Secret and publishes it to the `-sensor-ca` ConfigMap for the sensors to pin, and accepts only the `-ebpf` ServiceAccount's projected token (audience `triage-agent-sensors`) through a TokenReview. `sensorstream.Hub` implements `metricset.Source`; the collectors below read the latest snapshot per node from it. Every replica takes sensor streams; with `--mesh-listen` the replicas gossip what their own sensors send (`internal/mesh`, Service `<fullname>-peers`, port 9471, mutual TLS pinned to the same `-sensor-tls` certificate) and `Hub.Merge` keeps the newest record per node, so the leader sees every node (ADR-0015 phase 4). Every 60 seconds, when push is also enabled, the controller maps `Hub.Nodes()` to a `sensor_state` push frame and sends it (`cmd/triage-agent/sensor_hub.go`'s `sensorStatusLoop`, ADR-0015 phase 1). This frame carries the sensors' own health, not tenant data, so `push/connworker.go` writes the latest one unstamped once per connection on the heartbeat tick, like the heartbeat.

### Log-based metrics (`logmetrics/`)

Reads the `log-exporter` role's counters from the sensor stream, gates on error/warn deltas per (namespace, service) and forwards qualifying windows as `log_metrics` frames over the push connection above (ADR-0006, sample population ADR-0007, exporter ADR-0014). `logmetrics.New()` returns nil unless `--log-metrics-enabled` is set AND push mode itself is enabled (the `Sender` it forwards through). `logmetrics.Collector` is a `manager.Runnable`, leader-gated. Chart: `logExporter.*` runs the role on the DaemonSet, `logMetrics.*` configures the collector.

### eBPF RED metrics (`redsource/`)

Reads the embedded OBI instrumenter's OTLP metrics (`ebpf` role) from the sensor stream, gates on error-count AND error-rate deltas per (namespace, service) and forwards qualifying windows as `red_metrics` frames over the same push connection (ADR-0009). `redsource.New()` has the identical nil-disabled/no-sender contract as `logmetrics.New()`; the same collector also derives the `topology_snapshot` frame (ADR-0011/ADR-0013) from OBI's client-side metrics and network flows, see `redsource/profiles.go`'s `ProfileOBI`. Chart: `ebpf.*` deploys the DaemonSet (`hostPID: true`: application mode needs the shared PID namespace to hook every other container's process; `hostNetwork` only with `ebpf.networkFlows.enabled`), `redMetrics.*`/`topology.*` configure the collector.

### Host mode (`cmd/triage-agent/host.go`)

`--mode=host` (ADR-0018) runs the agent on a plain Linux host: no kubeconfig, no controller-runtime manager, no sensor listener, no mesh. `push.NewHost` is the Pusher without informers or leader election (static scope `[--host-namespace]`, tenant mode with `--target-tenant`, hello capability `host`); `sensorstream.Local` runs the sensor roles in-process and serves them as a `metricset.TimedSource`, so the RED/USE/LOG collectors read them as they read the hub; `usemetrics.StaticIdentityResolver` names every pod of the synthetic namespace by itself. `hostRoles` is where later phases register their roles. Cluster name defaults to the OS host name.

### Local TUI (`agentui/`)

`--ui-listen 127.0.0.1:8088` (chart `ui.listen`; cluster and host mode) serves the browser TUI embedded from `agentui/web/` plus a read-only JSON API and an SSE event stream (ADR-0019). Loopback bind and Host check are enforced in code; with `--ui-admin-key-file` (a Secret mounted by the chart; host mode also `--ui-admin-key`/`TRIAGE_AGENT_UI_ADMIN_KEY`) the API needs an `agentui/auth.go` session, otherwise there is no login. `agentui.Store` implements `push.Observer`; `Pusher.SetObserver` taps every queued frame (`enqueueOK`, per resolved tenant) and `SendSensorState`, so what the TUI shows is what the core is sent. Apps come from `Pusher.Apps()`. With `--mesh-listen`, `agentui.Replica` has the leader publish its Store snapshot, apps and link state as a `ui` mesh frame every 5 s and the followers serve it, so every replica shows the same view. Panel 1 `app`: apps, traffic, graph. Panel 2 `src`: sources left, events right.

`--api-listen` (chart `api.*`, ADR-0020) adds `agentui.APIServer`: `/metrics`, `/.well-known/triage-agent` and the JSON API behind bearer tokens from `--api-tokens-file` (`name tenant token`; a tenant-bound token has its `tenant` query forced and sees no node-wide sensors). It refuses to start without tokens; only the leader exports signal series.

### Deploy

`charts/triage-agent` (Helm, in this repo) is the install path — toggles push mode + metrics, ships its own RBAC, and has no `kube-rbac-proxy` sidecar. No CRD or `config/` directory ships from this repository.

### Container images

**ko only. There is no Dockerfile anywhere in this repo.** One image comes
off the main package (`.ko.yaml`, `make ko-image`/`ko-push`), built with
`-tags obi` so it can run `--mode role "ebpf"` (see
`ebpf_obi.go`/`ebpf_stub.go`: the untagged stub exists only for the plain
`go vet`/`go test` runs, not for a shipped image). Both the controller
Deployment and the eBPF DaemonSet in `charts/triage-agent` run this one
image. The build additionally requires
`third_party/obi/pkg/**/*_bpfel.{go,o}` to already be generated (`make
obi-generate`, Docker required, wraps `third_party/obi`'s own
`docker-generate` target; `hack/build-obi.sh` runs the same target
locally). It is a prerequisite, never a build step itself, but it is not
only-when-bumping-OBI-version: `go vet ./...` and `go test ./...` on the
untagged build reach the generated files too, because
`cmd/triage-agent/ebpf_config_test.go` and `ebpf_metrics_test.go` import
`go.opentelemetry.io/obi/pkg/obi` / `.../pkg/export/connector`
unconditionally. CI runs `make obi-generate` before `go vet`/`go build` for
this reason (cached on the vendored tree's git hash, see CI below). Details:
`docs/release.md`, `docs/todo/obi-embed.md`.

### Testing

Test files build a `fake.NewClientBuilder()` Kubernetes client; no cluster and
no downloaded test binaries are needed. `make test` runs `go test` on every
package that has test files.

### CI (`./github/workflows/ci.yaml`)

- **Every push/PR to `main`**: `make obi-generate` (skipped on a cache hit
  keyed on `git rev-parse HEAD:third_party/obi` — see `test` job) + `go vet`
  + `go build`, then `go test -race` for the root module, `promevents` and
  the vendored HTTP/2 decoder (`third_party/obi/pkg/ebpf/common`).
- **Every push to `main` (not a PR)**: the `image` job builds and pushes the
  image to `ghcr.io/<owner>/<repo>` with `ko build` (multi-arch,
  `linux/amd64`+`linux/arm64`), tagged with the short sha (+ `latest` on the
  default branch). It restores the same cached eBPF bindings as the `test`
  job (`.ko.yaml` always builds with `-tags obi`, which needs them) rather
  than regenerating them.
- **Tag `v*`**: the `image` job additionally tags `X.Y.Z`, `X.Y`, `X`; the
  `release` job builds multi-arch binaries (`linux/amd64`, `linux/arm64`,
  `darwin/amd64`, `darwin/arm64`) + creates the GitHub Release with binaries
  and SHA256 checksums

To release: `git tag v0.x.y && git push origin v0.x.y`

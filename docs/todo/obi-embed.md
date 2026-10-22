# Spike: embed OpenTelemetry eBPF Instrumentation (OBI) as a library

- **Status**: Spike complete, 2026-08-24, branch `spike/obi-embed`. Not implemented, not deployed.
- **Related**: `docs/adr/0009-red-metrics-beyla.md` (Alternative 1: "Embed OBI directly
  into the controller binary ... Deferred to a later phase"), `docs/adr/0013-red-source-profiles.md`
  (the profile-driven scrape engine this would replace or coexist with), triageagent-dev/core
  `docs/todo/ebpf-telemetry-source.md` (the consolidation plan this is Phase 2 of).
- **Update, image-build consolidation (2026-08-27)**: `hack/Dockerfile.obiall`
  and `hack/Dockerfile.obiprobe` are removed — every container image in this
  repo is now built by ko, no Dockerfile anywhere. `ebpf_obi.go`/`ebpf_stub.go`
  (the `--mode role "ebpf"` embedding this doc recommends below) and
  `third_party/obi` (a vendored, checked-in copy of the OBI source, replacing
  the external-clone step round 2 below describes) already exist on `main` —
  this doc's own "Status" line above and the historical narrative below predate
  that, and are kept as the point-in-time spike record, not a live status.
  The `-tags obi` build of this repo's own main package is now `.ko.obi.yaml`
  (`make ko-image-obi`/`ko-push-obi`); the standalone `cmd/obiprobe` probe
  this doc's round 2 built as a separate image still builds (as a sanity
  check in the restructured `hack/build-obiprobe.sh`) but is no longer
  packaged into an image — the "obi-single" engine (triageagent-dev/charts'
  `charts/triage-agent` values.yaml, `ebpf.engine`) documents itself as "the
  fleet standard" against the standalone probe's "obi" engine, which its own
  comment calls "the spike build". `hack/build-obiprobe.sh` no longer clones
  OBI or patches a `go.mod` replace directive — both are unconditional now
  that `third_party/obi` is vendored — it only runs the codegen prerequisite
  (Docker, OBI-version-bump-only) plus go-only sanity builds. See
  `docs/release.md` for the current build commands.
- **Update, single-image consolidation (2026-09-27)**: `.ko.obi.yaml` is
  gone. `.ko.yaml` itself now carries `-tags obi`, so `make ko-image`/`ko-push`
  build the one image the controller Deployment and the eBPF DaemonSet both
  run; `ko-image-obi`/`ko-push-obi`/`OBI_VER` are removed from the Makefile,
  and so is the chart's `ebpf.image.repository`/`.tag`/`.pullPolicy` and the
  `triage-agent.ebpfImage` helper (the DaemonSet now renders
  `image.repository`/`image.tag` directly). See `docs/release.md` for the
  current build commands.

## Goal

Check whether `go.opentelemetry.io/obi` (OBI, the donated Beyla core; module
`go.opentelemetry.io/obi@v0.12.2`) can run embedded in our own Go binary as a
library, replacing the separate `grafana/beyla` container in the DaemonSet.

Round 1 of this spike found that the published Go module ships no
pre-generated eBPF bytecode and stopped at that verdict. Round 2 (this
version of the doc) went further: cloned OBI at the pinned tag, ran its own
`bpf2go` code generation inside its own generator container, and got a real
working `cmd/obiprobe` binary that builds for both linux/amd64 and
linux/arm64, packaged into a pushed container image. The build-toolchain cost
described below is no longer a hypothesis — it is the exact, now-executed set
of steps, captured in `hack/build-obiprobe.sh` for anyone to reproduce.

## Verdict

**Technically works. The generator step is a real, adoptable build stage, not
a blocker — but it is a genuine new piece of build infrastructure this repo
would own, and the dependency footprint is still large.** `bpf2go` generation
via `ghcr.io/open-telemetry/obi-generator:0.2.15` completed cleanly in under a
minute against a stock Docker Desktop install, produced correct per-arch Go
files for both `amd64` and `arm64`, and the resulting `cmd/obiprobe` binary
cross-compiles for both linux architectures and runs as a real container
image. Nothing about the eBPF/privilege model changes this verdict — the
open question is purely whether owning that generator step in our own build
pipeline forever (one more Docker stage, one more pinned upstream image, one
re-run per OBI version bump) is worth removing the separate `grafana/beyla`
image. Recommend an operator decision at this point, not further spiking:
the technical unknowns from round 1 are now resolved.

## What was measured (round 2)

### 1. Code generation via OBI's own generator image

Docker Desktop was not running at the start of this round (`docker info`
failed) and was started for this spike (`open -a Docker`, then polled until
ready — took under a minute). Docker Desktop's engine on this host is native
`linux/arm64`; `ghcr.io/open-telemetry/obi-generator:0.2.15` publishes both
`linux/amd64` and `linux/arm64`, so no QEMU emulation was needed to run the
generator itself (bpf2go cross-compiles the eBPF C to the requested
`-target` architectures regardless of the generator container's own host
arch — that is the whole point of it being a cross-compiler).

```
$ git clone --depth 1 --branch v0.12.2 \
    https://github.com/open-telemetry/opentelemetry-ebpf-instrumentation.git obi-src
$ docker pull ghcr.io/open-telemetry/obi-generator:0.2.15
$ cd obi-src && make docker-generate
### Generating files in Docker...
(completes in well under a minute; idempotent — a second run prints
"make: Nothing to be done for 'generate'.")
```

`make docker-generate` (from OBI's own `Makefile`) runs exactly:

```makefile
docker run --rm -u "$(id -u):$(id -g)" \
  -v "$(CURDIR):/src:z" \
  -v "$git_common_dir:/src/.gitrepo:ro,z" \
  -e GIT_DIR="/src/.gitrepo..." \
  -w /src \
  ghcr.io/open-telemetry/obi-generator:0.2.15 \
  make generate
```

— mounts the checkout in, runs `make generate` (`go generate ./...` with
`BPF_CLANG`/`BPF_CFLAGS`/`BPF2GO` set) inside the container, as the host
user (not root) so generated files aren't root-owned on the host. Confirmed
output, per-arch, little-endian (`_bpfel`) files under `pkg/`:

```
pkg/ebpf/common/bpf_arm64_bpfel.go
pkg/ebpf/common/bpf_x86_bpfel.go
pkg/internal/statsolly/ebpf/stats_arm64_bpfel.go
pkg/internal/statsolly/ebpf/stats_x86_bpfel.go
pkg/internal/netolly/ebpf/{net,netsk}_{arm64,x86}_bpfel.go
pkg/internal/rdns/ebpf/xdp/bpf_{arm64,x86}_bpfel.go
pkg/internal/ebpf/{logger,watcher,tpinjector,generictracer,gotracer,gpuevent,logenricher}/bpf_{arm64,x86}_bpfel.go
... (more; every eBPF-backed package gets its own pair)
```

Both `amd64` and `arm64` targets are little-endian, so `bpf2go`'s
`-target amd64,arm64` flag produces exactly two files per package
(`_x86_bpfel.go`, `_arm64_bpfel.go`) — no `_bpfeb` (big-endian) variants
needed for these two targets. Each `.go` file `//go:embed`s its matching
compiled `.o` (the actual eBPF bytecode).

Sanity build, straight from the generated checkout, no changes to this
repo yet:

```
$ cd obi-src && GOOS=linux GOARCH=amd64 go build ./pkg/instrumenter/... ./pkg/obi/... ./cmd/obi/...
$ cd obi-src && GOOS=linux GOARCH=arm64 go build ./pkg/instrumenter/... ./pkg/obi/... ./cmd/obi/...
```

Both exit clean — OBI's own binary target builds for both architectures from
the generated tree.

### 2. Wiring this repo to the generated checkout

A `replace` directive in `go.mod` points at the generated OBI checkout:

```
replace go.opentelemetry.io/obi => /path/to/obi-src
```

then `go mod tidy`. This is spike-only and does not resolve for anyone else
without first running `hack/build-obiprobe.sh`, which reproduces the whole
chain (clone, `docker-generate`, patch the `replace` line, build) from
scratch. **This must be removed before this code leaves spike status** — see
the comment on the `replace` line itself in `go.mod`.

### 3. `cmd/obiprobe` now builds for both real targets

```
$ GOOS=linux GOARCH=amd64 go build -o dist/obiprobe-linux-amd64 ./cmd/obiprobe
$ GOOS=linux GOARCH=arm64 go build -o dist/obiprobe-linux-arm64 ./cmd/obiprobe
$
```

(no output — both succeed; this is the exact command that produced 32 lines
of `undefined: Bpf...` errors in round 1). `go build ./...` and `go vet ./...`
on darwin (host OS) still pass cleanly — `cmd/obiprobe` is unchanged
structurally from round 1 (`main.go` linux-gated, `config.go` OS-agnostic,
`main_stub.go` the non-linux placeholder); the only code fix this round was a
leftover `strings` import dropped when `main.go` was split, caught the moment
the linux build actually got far enough to reach that line.

### 4. Binary sizes

| Build | amd64 | arm64 |
|---|---:|---:|
| `cmd/obiprobe`, unstripped (debug info) | 126,570,328 bytes (~120.7 MiB) | 119,739,853 bytes (~114.2 MiB) |
| `cmd/obiprobe`, `-ldflags="-s -w"` (what's shipped) | 93,311,138 bytes (~89.0 MiB) | 87,687,330 bytes (~83.6 MiB) |
| This repo's own controller (`.`, unstripped, for reference) | 47,974,900 bytes (~45.8 MiB) | not built this round |

The stripped `obiprobe` binary is ~1.9x the size of our own controller
binary alone — expected, since it statically links the full eBPF
instrumentation engine plus the OTel Collector export machinery `pkg/obi`
pulls in (see dependency footprint below), most of which our use case
(application-mode discovery + Prometheus export only) doesn't exercise.

### 5. Container image

`hack/Dockerfile.obiprobe`: `gcr.io/distroless/static-debian12` base (the
binary is fully static — confirmed via `file`, no cgo), no `USER` directive
(obiprobe needs `runAsUser: 0` at runtime for its eBPF capabilities to be
honored — same reasoning as `beyla-daemonset.yaml`'s own container — so the
image should not fight the pod securityContext by baking in non-root).

Local build (`--load`, no push, sanity check): amd64 image = 95,425,116 bytes
(~95.4 MB), consistent with the 89 MiB stripped binary plus the thin
distroless base.

**Pushed image**: `registry.invalid/example-shared-registry/sre/application:obiprobe-spike-3636987`
(`sre/application-obiprobe` does not exist as an established image path in
this Artifact Registry project — `sre/application` does, so per the spike
instructions this used a new tag on the existing image path rather than a
new one). Multi-platform push (`--platform linux/amd64,linux/arm64`) needed a
`docker-container`-driver buildx builder — the default Docker Desktop
builder uses the `docker` driver, which cannot export a multi-platform
manifest at all (`ERROR: Multi-platform build is not supported for the
docker driver`); `docker buildx create --driver docker-container --use`
fixed this.

A true multi-arch push (`--platform linux/amd64,linux/arm64 --push`) then hit
a transient registry auth timeout after ~63s of layer upload
(`failed to authorize: DeadlineExceeded`) — a token-refresh hiccup mid-push,
not a build or replace-directive problem. Re-running single-platform
(`--platform linux/amd64`) succeeded after ~533s of layer upload (slow
uplink, not an error). **What is actually live in the registry today is
linux/amd64 only** — confirmed via `docker manifest inspect`:

```
$ docker manifest inspect registry.invalid/example-shared-registry/sre/application:obiprobe-spike-3636987
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    { "digest": "sha256:a55a45a34fa2fd704917315e8adb7472eb00406415668bbdeedffecd295da4da",
      "platform": {"architecture": "amd64", "os": "linux"} },
    { "digest": "sha256:a1c7badc2435c93c6f80d9637aaad14e3cc6d16ef50ccf74b83fd7e2bf4df539",
      "platform": {"architecture": "unknown", "os": "unknown"} }
  ]
}
```

(the second entry is buildx's default SBOM/provenance attestation, not a
second platform image). An arm64 manifest was never pushed in this round —
`dist/obiprobe-linux-arm64` exists locally and builds cleanly (see sizes
above), but re-running the multi-platform push was not repeated after the
single-arch one succeeded. Re-running
`docker buildx build --platform linux/amd64,linux/arm64 ... --push` with the
same builder would add the arm64 manifest to the same tag.

### 6. Dependency footprint — unchanged from round 1

The `replace` directive swaps the source location, not the module's own
declared dependencies, so the numbers from round 1 stand (re-measured after
`go mod tidy` with the replace in place; the 2-line `go.sum` difference below
is noise from the tidy re-run, not from the replace itself):

| Metric | Before OBI | After OBI | Delta |
|---|---:|---:|---:|
| `go.mod` require lines | 65 | 220 | +155 |
| `go.sum` lines | 193 | 557 | +364 |
| `go list -deps ./...` unique packages (GOOS=linux) | 821 | 1516 | +695 (+85%) |

`pkg/obi.Config` still pulls in most of the OTel Collector's
receiver/exporter/extension/pipeline framework, because it is designed to
also drive OTLP export, which we would never use for the Prometheus-only
path this project needs.

## Config parity check — unchanged from round 1

Still holds; nothing about running the generator changes what fields
`obi.Config` accepts. Full field-by-field table is in the round-1 findings
below this section stayed accurate and is kept for reference:

| Our chart setting | OBI v0.12.2 field | Match? |
|---|---|---|
| `discovery.instrument[].k8s_namespace` | `services.DiscoveryConfig.Instrument` (`GlobDefinitionCriteria`, key `AttrNamespace = "k8s_namespace"`) | Yes — same key, same glob-not-regex semantics our chart's own comment documents. |
| `discovery.exclude_instrument[].k8s_namespace` | `services.DiscoveryConfig.ExcludeInstrument` | Yes. |
| `attributes.kubernetes.enable: true` | `transform.KubernetesDecorator.Enable`, type `kubeflags.EnableFlag` (`string`, `oneof=true false autodetect`) | Yes, empirically verified: a YAML bool unmarshals into a string-kind field under `gopkg.in/yaml.v3` (isolated test, `obiquickcheck`) — `err == nil`, `Enable == "true"`. |
| `attributes.kubernetes.meta_restrict_local_node` | `transform.KubernetesDecorator.MetaRestrictLocalNode` (`bool`) | Yes. |
| `attributes.kubernetes.disable_informers` | `transform.KubernetesDecorator.DisableInformers` (`[]string`) | Yes. |
| `routes.unmatched: wildcard` | `transform.RoutesConfig.Unmatch` (`oneof=unset path wildcard heuristic low-cardinality`) | Yes. |
| `prometheus_export.port` / `.path` | `prom.PrometheusConfig.Port` / `.Path` | Yes. |

Metric/label names our `redsource.ProfileBeyla` already expects
(`http_server_request_duration_seconds_count`, `k8s_namespace_name`,
`service_name`, `http_response_status_code`, `http_request_method`,
`http_route`, the four `*_client_duration_seconds_count` families,
`server_address`) are OBI's own OTel-semconv names, not a Grafana layer —
switching to embedded OBI changes no metric name or label key our collector
already parses.

Gaps, unchanged from round 1: `BEYLA_CONFIG_PATH` is a `grafana/beyla`
wrapper-binary convention (OBI's own `cmd/obi` reads `OTEL_EBPF_CONFIG_PATH`
or `--config`) — irrelevant to embedding, since a direct `obi.LoadConfig`
call never goes through either env var. The v2 config schema
(`internal/config/schema`, `internal/config/convert`) is not publicly
importable; `obi.LoadConfig` only parses the v1 shape our chart already
speaks, which is what this spike used throughout.

## Recommended embedding shape

Unchanged recommendation from round 1: a `--mode=ebpf` flag on the existing
`triage-agent` binary, mirroring `--mode=node-exporter` /
`--mode=log-exporter` already on the same DaemonSet — one image, in-process
`instrumenter.Run`, unchanged `red_metrics` wire contract to triage.

`hack/obiprobe-spike.yaml` is a standalone DaemonSet (separate image, not
this flag) built for THIS spike specifically to let the lead compare a real
running embedded-OBI pod against the existing beyla container on one node,
without touching the real chart. It carries the same scoped-capability
`securityContext` as `beyla-daemonset.yaml` (`CAP_BPF`, `CAP_PERFMON`,
`NET_RAW`, `SYS_PTRACE`, `DAC_READ_SEARCH`, `CHECKPOINT_RESTORE`,
`runAsUser: 0`, `privileged: false`), `hostPID: true`, and a bpffs mount, on
port 9964 (9464 is beyla's), instrumenting `acme,qdrant-cluster`, with
`kubernetes.io/hostname` pinned to a single node — **the node name is a
placeholder (`TODO-PIN-NODE-NAME`) and this manifest is not applied by this
branch.**

That standalone-DaemonSet shape is not the production recommendation — it
exists only so this spike could produce something the lead can actually run.
The trade-off called out in round 1 stands: embedding moves eBPF load
privileges onto our own image/ServiceAccount instead of a narrowly-scoped
separate `beyla` container, and that is a real security-posture cost to
weigh, not a formality, before any of this becomes a real chart change.

## Maintenance trade-offs — sharpened after actually running the generator

- **The generator step is fast and reliable, not fragile.** `make
  docker-generate` finished in under a minute on a stock Docker Desktop
  install with no special setup beyond a running daemon. This lowers the bar
  from round 1's more cautious framing — it is not a brittle, hand-tuned
  toolchain we'd be reverse-engineering; it is one `docker run` with a
  pinned, versioned image.
- **It is still a new permanent build stage.** Our Dockerfile would need a
  `FROM ghcr.io/open-telemetry/obi-generator:<tag> AS obigen` stage (or
  equivalent CI step) that clones/vendors OBI's source and runs `make
  generate` before our own `go build` — this repo does not have an
  equivalent stage today for anything else it depends on.
- **Version bumps mean re-running generation, not just `go get -u`.** Each
  OBI version bump requires re-pulling (or re-pinning) the matching
  generator image tag and re-running `make docker-generate` against the new
  tag — a real, recurring step, though a cheap one based on this run.
- **Image count**: embedding removes the separate `grafana/beyla` image pull
  and its fixed-Service-name/two-releases-collide caveat (ADR-0009).
- **Dependency surface**: unchanged from round 1 — `go.sum` roughly triples,
  package-level import graph grows ~85%, all from OTel Collector export
  machinery we would not use.
- **Kernel/support matrix**: Grafana currently tests Beyla application mode
  across kernel versions and publishes compatibility notes; embedding OBI
  directly makes that our own verification burden.

## Open questions

1. Does OBI publish a release artifact with the generated bytecode already
   baked in, avoiding the generator-image build stage entirely? Not checked
   against OBI's GitHub releases page in this round — worth doing before any
   real implementation, now that we know the generator-image path works and
   this would only be an optimization on top of a working baseline.
2. Is `pkg/instrumenter.Run`'s dependency on the full OTel Collector
   extension/receiver/pipeline framework avoidable via a narrower entry
   point? Not investigated.
3. ~~Scrape-over-loopback (simplest, no `redsource/` changes) vs. a deeper
   in-process metrics handoff — still open, still recommend the former.~~
   Resolved in favor of the in-process handoff: `ebpf.networkFlows.enabled`
   runs the DaemonSet pod with `hostNetwork: true`, which turns the
   scrape-over-loopback design's private port into a listener on the
   *node's* loopback — reachable, unauthenticated, by any other
   host-network process or pod on that node. `third_party/obi/pkg/export/
   connector/prommgr.go`'s `EmbedHTTP` hook now lets the agent take OBI's
   Prometheus handler directly and serve it from its own bearer-gated
   `:9464`, so OBI's manager opens no listener of its own in this binary —
   see `ebpf_metrics.go` and `ebpf_obi.go`'s `runEBPF`.
4. OBI's v1-config deprecation timeline, if any — still open.
5. The multi-platform image push in this round needed a `docker-container`
   buildx builder instead of Docker Desktop's default `docker`-driver one;
   worth confirming whatever CI builds this repo's real images already uses
   a compatible builder/driver, since this repo's existing single-arch
   `Dockerfile` build has apparently never hit this.

## Files touched by this spike

Round 1: `cmd/obiprobe/{main.go,config.go,main_stub.go}` (~115 lines), this
file, `go.mod`/`go.sum` (`go get go.opentelemetry.io/obi@v0.12.2` +
`go mod tidy`).

Round 2: `go.mod` (spike `replace` directive — remove before this leaves
spike status), `.gitignore` (`/dist/`, build script output), this file
(rewritten), plus:

- `hack/build-obiprobe.sh` — reproduces the whole chain: clone OBI at the
  pinned tag, run `docker-generate` against the pinned generator image,
  patch the `replace` directive, build both architectures, verify darwin
  still builds/vets clean.
- `hack/Dockerfile.obiprobe` — packages the prebuilt `dist/` binaries into a
  minimal distroless image (built from artifacts, does not run `go build`
  itself — that needs the generator step's output first).
- `hack/obiprobe-spike.yaml` — standalone DaemonSet manifest for the lead to
  apply to one node manually. Not applied by this branch.

No chart, deployment, or existing-package changes beyond `cmd/obiprobe`
itself and the spike-only `go.mod` replace. The `hack/obiprobe-spike.yaml`
manifest was not applied to any cluster by this spike.

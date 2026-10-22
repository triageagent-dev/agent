# Releasing

Git tags are the only source of truth. There is no `VERSION` file to maintain.

## Version derivation

`make version` reports exactly what a build will stamp into the binary and use
for image tags:

```bash
$ make version
Version:    v2.3.0-09d351f
Commit:     09d351f
Build Date: 2026-08-21
Registry:   registry.invalid/example-shared-registry/triage-agent
```

- **On a tag**: the tag itself, `v` stripped (`v2.3.0` → `2.3.0`).
- **Off a tag**: the last tag's major.minor, the patch position replaced by the
  commit count since that tag, plus the short sha — `v2.3.14-09d351f`. Consecutive
  builds off one branch stay distinguishable and ordered.
- **No tags yet** (this repo's current state): `BASE_VERSION` in the Makefile
  seeds the series. Keep it in step with the chart's `appVersion`, and ahead of
  whatever is already deployed.
- **Dirty tree**: a `-dirty` suffix is appended. An image built from uncommitted
  work does not correspond to the sha it names, so the tag says so.

Override for a one-off build with `VER=...`.

These values reach the binary through `-ldflags` (`main.version`,
`main.gitCommit`, `main.buildDate`) and are printed on the first line of the
controller's log:

```
triage-agent starting {"version":"v2.3.0-09d351f","commit":"09d351f","buildDate":"2026-08-21","mode":"controller"}
```

They are also reported to the triage agent in the push hello frame. A build with
no `-ldflags` reports `dev` / `unknown`.

## Cutting a release

```bash
make release-tag RELEASE_TAG=v2.4.0
```

Creates an annotated tag and pushes it to `$(RELEASE_REMOTE)` (default `origin`),
which triggers the CI release. The target rejects anything that is not an explicit
`vX.Y.Z`. To undo:

```bash
make delete-release-tag RELEASE_TAG=v2.4.0
```

Remember this repo has two remotes (see CLAUDE.md) — `make release-tag` pushes the
tag to one. Push it to the other with `git push private v2.4.0` if both should
carry it.

## What CI does

`.github/workflows/ci.yaml`:

| Trigger | Jobs |
|---------|------|
| every push / PR | `test` (`make obi-generate` — cached, see below —, vet, build), `gcpevents` (vet, build, `go test ./...`) |
| push to a branch (not PR) | `image` — multi-arch `ko build` to `ghcr.io/<owner>/<repo>`, tagged with the short sha, plus `latest` on the default branch |
| tag `v*` | `image` additionally tags `X.Y.Z`, `X.Y` and `X`; `release` builds `linux/{amd64,arm64}` + `darwin/{amd64,arm64}` binaries with SHA256 checksums and creates the GitHub Release |

The `image` job invokes `ko` directly rather than through `make ko-push`, so it
must export `VERSION` / `GIT_COMMIT` / `BUILD_DATE` itself — `.ko.yaml` templates
all three and ko **fails the build** if any is missing (`template: map has no entry
for key "VERSION"`). Any new ko call site needs them too.

`.ko.yaml` builds with `-tags obi`, so the `image` job needs the generated eBPF
bindings too. It restores them from the same cache the `test` job populates
(keyed on `git rev-parse HEAD:third_party/obi`) instead of regenerating them
itself. `go vet` (and any local `go test`/`make test`) needs them regardless of
build tag: `cmd/triage-agent`'s untagged `ebpf_config_test.go`/`ebpf_metrics_test.go`
import OBI packages that reach the generated bindings transitively.

## Local image builds

There is no Dockerfile anywhere in this repo — ko builds every image, straight
from the one main package, built with `-tags obi`:

```bash
make ko-image                                    # build locally, no push
make ko-push                                     # build and push
KO_DOCKER_REPO=<registry>/<repo> make ko-push    # override the target registry
REGISTRY=<registry>/<repo> make ko-push          # same override, alternate knob name
```

`make ko-push` defaults to GHCR. Deployments that pull from somewhere else need
the override — the acme-gke deployment, for instance, pulls from
`registry.invalid/example-shared-registry/triage-agent`.

`--mode role "ebpf"` only works in a binary built with `-tags obi`
(`ebpf_obi.go`/`ebpf_stub.go`, docs/todo/obi-embed.md); this is why the
image is always built that way, and both the controller Deployment and the
eBPF DaemonSet run it. The build requires `third_party/obi/pkg/**/*_bpfel.{go,o}`
to already be generated — a Docker-only prerequisite (`make obi-generate`;
`hack/build-obi.sh` runs the same target locally), never a build step
itself. Without them the build fails loudly at compile time; it never
silently falls back to the stub. CI's `test` job runs `make obi-generate`
automatically before `go vet`/`go build`, caching the output on
`git rev-parse HEAD:third_party/obi` so it only actually invokes Docker when
the vendored tree changes; the `image` job restores that same cache before
its `ko build` — see `.github/workflows/ci.yaml`.

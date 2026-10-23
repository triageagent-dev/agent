# Development Guide

## Prerequisites

- [Go](https://golang.org/dl/) v1.24+
- [ko](https://ko.build) — container image builds (the only image build path in this repo — no Dockerfile)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) + cluster access (optional, for deploy targets)
- Docker, only for `hack/build-obi.sh`'s codegen step (OBI version bumps), never for building an image

Install build tools:
```bash
make install-tools
```

## Workflow

### Build

```bash
make bin/triage-agent   # binary
make ko-image            # OCI image `agent`, built without OBI (local layout, no push)
make ko-push             # build and push it to $KO_DOCKER_REPO
make ko-push-obi         # the image with OBI for the eBPF DaemonSet, to $KO_DOCKER_REPO-obi (needs make obi-generate)
```

There is no Dockerfile in this repo — ko builds every image, always with
`-tags obi`, so the controller Deployment and the eBPF DaemonSet run the
same one. The build additionally needs `third_party/obi/pkg/**/*_bpfel.{go,o}`
generated first (Docker, one-time per OBI version bump, via
`hack/build-obi.sh`); see [release.md](release.md#local-image-builds) and
[obi-embed.md](todo/obi-embed.md).

### Test (no cluster required)

```bash
make test        # unit tests
```

Run a single test:
```bash
go test -v ./push/... -run "TestName"
```

### Code quality

```bash
make fmt         # go fmt
make vet         # go vet
make lint        # golangci-lint
```

## Deploy to cluster

```bash
make deploy
make undeploy
```

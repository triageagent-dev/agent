# Copyright 2026 Denys Vasyliev.
# SPDX-License-Identifier: Apache-2.0
#
# Makefile for triage-agent

# Version/build stamps. Same derivation as the sibling triageagent-dev/core repo,
# so a build here identifies itself the same way one there does.
#
# Plain `git describe` is not enough on its own: this repo carries no tags yet, so
# it degrades to a bare short sha ("f3c70cb"), which is not a version and cannot be
# ordered against anything. Instead: use an exact tag when we are on one, otherwise
# auto-increment the patch position with the commit count and append the sha —
#   on a tag:   2.3.0
#   otherwise:  v2.3.14-f3c70cb
# so consecutive builds off the same branch are distinguishable and ordered.
# BASE_VERSION seeds the series until the first real tag exists; keep it in step
# with the chart's appVersion. It must stay AHEAD of what is deployed — acme-gke
# ran v2.2.0-f3c70cb while the chart still said appVersion 2.0.0, so the two had
# already drifted; 2.3.0 re-syncs them above the highest shipped build. Override
# for a one-off build with VER=... .
BUILD_DATE ?= $(shell date -u '+%Y-%m-%d')
# The -dirty suffix matters: an image built from an uncommitted tree does NOT
# correspond to the sha it names, and a tag that quietly claims otherwise is worse
# than an ugly one. Same marker the old `git describe --dirty` produced.
GIT_COMMIT ?= $(shell echo "$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)$$(test -n "$$(git status --porcelain 2>/dev/null)" && echo -dirty)")
LAST_TAG ?= $(shell git describe --tags --abbrev=0 2>/dev/null)
COMMITS_SINCE_TAG ?= $(shell git rev-list $(LAST_TAG)..HEAD --count 2>/dev/null || echo "0")
BASE_VERSION ?= $(shell if [ -n "$(LAST_TAG)" ]; then echo $(LAST_TAG) | sed 's/^v//'; else echo "2.3.0"; fi)

# Every build carries the sha, including one made exactly on a tag. The old
# on-a-tag branch emitted a bare "2.5.0" with no sha and no "v", so an image
# name told you nothing about which commit produced it, and two images built
# from one commit under different tags looked like different releases.
NEXT_VERSION := $(shell echo $(BASE_VERSION) | awk -F. '{print $$1"."$$2"."$(COMMITS_SINCE_TAG)}')
VER ?= v$(NEXT_VERSION)-$(GIT_COMMIT)

# Stamped into the binary by every build path (bin/triage-agent, CI release
# binaries) and, via the VERSION/GIT_COMMIT/BUILD_DATE env vars, by .ko.yaml.
# Without these a build reports version "dev" and two images are indistinguishable.
LDFLAGS := \
	-s -w \
	-X 'main.version=$(VER)' \
	-X 'main.gitCommit=$(GIT_COMMIT)' \
	-X 'main.buildDate=$(BUILD_DATE)'
RELEASE_REMOTE ?= origin
RELEASE_TAG ?= $(VER)

# Directories.
TOOLS_DIR := $(shell pwd)/hack/tools
TOOLBIN := $(TOOLS_DIR)/bin

COVER_FILE ?= cover.out

.DEFAULT_GOAL := help

.PHONY: version
version: ## Show the version/commit/date this build stamps into binary and image
	@echo "Version:    $(VER)"
	@echo "Commit:     $(GIT_COMMIT)"
	@echo "Build Date: $(BUILD_DATE)"
	@echo "Registry:   $(KO_DOCKER_REPO)"

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	  /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: all
all: fix vet fmt test lint misspell tidy bin/triage-agent ## Run full build pipeline


## --------------------------------------
## Tooling Binaries
## --------------------------------------

# v1.23.6 (2020) cannot read Go 1.26 export data and failed on every package with
# "could not load export data", so `make lint` and `make all` were dead.
$(TOOLBIN)/golangci-lint:
	GOBIN=$(TOOLBIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0

$(TOOLBIN)/kind:
	GOBIN=$(TOOLBIN) go install sigs.k8s.io/kind@v0.9.0


$(TOOLBIN)/misspell:
	GOBIN=$(TOOLBIN) go install github.com/client9/misspell/cmd/misspell@v0.3.4

.PHONY: install-tools
install-tools: ## Install all tool binaries into hack/tools/bin
install-tools: \
	$(TOOLBIN)/golangci-lint \
	$(TOOLBIN)/misspell \
	$(TOOLBIN)/kind

## --------------------------------------
## Tests
## --------------------------------------

# Packages `make test` covers: every package with test files, so push/ and
# the metrics collectors run alongside everything else. Packages with no test
# files are excluded — a -coverprofile run synthesizes their empty profile
# with `covdata`, which the module-based toolchains under GOTOOLCHAIN=auto do
# not ship (`go: no such tool "covdata"`), so one such package fails the
# whole run. Selecting on test files rather than naming a package keeps this
# from going stale when one gains or loses tests. `go build ./...` and
# `go vet ./...` still compile every package.
TEST_PKGS ?= $(shell go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)

# Regenerate the sensor stream gRPC code (docs/adr/0015-sensor-stream-grpc.md)
# from proto/. Needs buf, protoc-gen-go and protoc-gen-go-grpc on PATH:
#   go install github.com/bufbuild/buf/cmd/buf@v1.57.2
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
.PHONY: proto
proto: ## Lint and regenerate sensorstream/sensorpb from proto/
	cd proto && buf lint --path triageagent && buf generate --path triageagent

# Run tests
.PHONY: test
test: ## Run unit tests
	go test -v $(TEST_PKGS) -coverprofile $(COVER_FILE)

## --------------------------------------
## Build and run
## --------------------------------------

# Build triage-agent binary. LDFLAGS is what makes one build distinguishable
# from another at runtime (startup log + push hello frame); without it the binary
# reports version "dev".
#
# Builds the PACKAGE (`./cmd/triage-agent`), not `main.go` alone — main.go is
# one of several files in that package (ebpf_obi.go/ebpf_stub.go among them);
# naming just main.go here used to compile only that one file and fail with
# "undefined: runEBPF" the moment `go build` reached anything the other files
# define. `$(wildcard cmd/triage-agent/*.go)` tracks every source file in
# that package for staleness, not just main.go.
bin/triage-agent: $(wildcard cmd/triage-agent/*.go) ## Build the controller binary
	go build -ldflags "$(LDFLAGS)" -o bin/triage-agent ./cmd/triage-agent

# Run against the configured Kubernetes cluster in ~/.kube/config
.PHONY: runbg
runbg: bin/triage-agent ## Run controller in background, logs to triage-agent.log
	bin/triage-agent --metrics-addr ":8083" >& triage-agent.log & echo $$! > triage-agent.pid

# Run against the configured Kubernetes cluster in ~/.kube/config
.PHONY: run
run: bin/triage-agent ## Run controller against current kubeconfig
	bin/triage-agent

# Debug using the configured Kubernetes cluster in ~/.kube/config
.PHONY: debug
debug: fmt vet ## Debug controller with dlv
	dlv debug ./cmd/triage-agent


## --------------------------------------
## Code maintenance
## --------------------------------------

.PHONY: fmt
fmt: ## Run go fmt
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: fix
fix: ## Run go fix
	go fix ./...


.PHONY: tidy
tidy: ## Run go mod tidy
	go mod tidy

# The obi collector is linux-only (`//go:build obi && linux`). Linting the host
# platform leaves those files out of the build, so the helpers their callers use
# are reported unused, and anything wrong INSIDE them is never analysed. Lint
# what ships.
.PHONY: lint
lint: $(TOOLBIN)/golangci-lint obi-verify ## Run golangci-lint and verify the vendored OBI checksum
	GOOS=linux $(TOOLBIN)/golangci-lint run --build-tags obi ./...
	./hack/lint-comment-style.sh

.PHONY: obi-checksum
obi-checksum: ## Rewrite third_party/obi/CHECKSUM after a deliberate change to the fork
	./hack/obi-checksum.sh --write

.PHONY: obi-verify
obi-verify: ## Fail if third_party/obi differs from third_party/obi/CHECKSUM
	./hack/obi-checksum.sh

.PHONY: misspell
misspell: $(TOOLBIN)/misspell
	$(TOOLBIN)/misspell ./**

.PHONY: misspell-fix
misspell-fix: $(TOOLBIN)/misspell
	$(TOOLBIN)/misspell -w ./**


## --------------------------------------
## Deploy
## --------------------------------------

# The agent chart lives in this repo.
HELM_CHART ?= charts/triage-agent
HELM_RELEASE ?= triage-agent
HELM_NAMESPACE ?= triage

# Deploy controller via the Helm chart to the cluster in ~/.kube/config.
.PHONY: deploy
deploy: ## Deploy controller to current cluster (Helm)
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART) -n $(HELM_NAMESPACE) --create-namespace

.PHONY: undeploy
undeploy: ## Remove the controller release from current cluster
	helm uninstall $(HELM_RELEASE) -n $(HELM_NAMESPACE)

## --------------------------------------
## OBI codegen
## --------------------------------------
# third_party/obi is a vendored, checked-in copy of OBI's source; its
# bpf2go-generated Go bindings (*_bpfel.{go,o}, third_party/obi/.gitignore)
# are NOT committed, so a fresh checkout starts without them. Any build that
# reaches pkg/ebpf/common or its siblings — not just the `-tags obi` main
# package, but also cmd/triage-agent's untagged _test.go files that import
# go.opentelemetry.io/obi/pkg/obi (ebpf_config_test.go, ebpf_metrics_test.go)
# — fails with "undefined: BpfXxxT" until this has run once. Docker-only,
# reproducible via the pinned ghcr.io/open-telemetry/obi-generator image
# (third_party/obi/Makefile's GEN_IMG); no clang/llvm/bpf2go install needed
# on the host, all of that lives inside the generator image. Idempotent —
# third_party/obi's own Makefile tracks the generated *_bpfel.go.d files and
# no-ops on a second run with no source changes. Used by CI (cached on the
# vendored tree's git hash, see .github/workflows/ci.yaml) and by
# hack/build-obi.sh's step 1.
.PHONY: obi-generate
obi-generate: ## Regenerate third_party/obi's bpf2go eBPF bindings (docker required)
	@if ! docker info >/dev/null 2>&1; then \
		echo "ERROR: no working docker daemon (Docker Desktop or colima)." >&2; \
		echo "This step cannot proceed without it — the generator image needs" >&2; \
		echo "a real container runtime, not just the docker CLI." >&2; \
		exit 1; \
	fi
	$(MAKE) -C third_party/obi docker-generate

## --------------------------------------
## ko
## --------------------------------------
# ko is the ONLY way any container image in this repo gets built — there is
# no Dockerfile anywhere in this tree. One image comes off the one main
# package (.ko.yaml, build id `triage-agent`), built with `-tags obi`: the
# controller Deployment and the eBPF DaemonSet both run it. `-tags obi` is
# what makes `--mode role ebpf` real (ebpf_stub.go fails fast without it).
# Requires third_party/obi/pkg/**/*_bpfel.{go,o} to already be generated
# first via `make obi-generate` — a docker-only prerequisite, never a build
# step. See hack/build-obi.sh and docs/todo/obi-embed.md.
#
# REGISTRY is an optional alias for KO_DOCKER_REPO (wins if both are set) —
# same knob name as the sibling triageagent-dev/core repo's `make push REGISTRY=...`,
# kept here for muscle memory. Override KO_DOCKER_REPO directly if you'd
# rather not set REGISTRY at all; either reaches the same variable.
#
# Makefile.local is gitignored and included first, so a machine can set
# KO_DOCKER_REPO once without the internal registry entering the repo.
-include Makefile.local
# Resolves to nothing real — set KO_DOCKER_REPO or REGISTRY (or Makefile.local)
# to an actual registry before pushing.
KO_DOCKER_REPO ?= registry.invalid/example-shared-registry/triage-agent
ifdef REGISTRY
KO_DOCKER_REPO := $(REGISTRY)
endif

.PHONY: ko-image
ko-image: ## Build the container image locally using ko (no push)
	VERSION=$(VER) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) KO_DOCKER_REPO=ko.local ko build \
		--oci-layout-path=bin/images/triage-agent \
		./cmd/triage-agent

.PHONY: ko-push
ko-push: ## Build and push the container image using ko
	VERSION=$(VER) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) KO_DOCKER_REPO=$(KO_DOCKER_REPO) ko build \
		--tags=$(VER) \
		--bare \
		--image-label org.opencontainers.image.version=$(VER) \
		./cmd/triage-agent

# The image without OBI (hack/ko-otlp/.ko.yaml): for the controller and the OTLP
# role, which never run the eBPF role. The binary still imports OBI packages, so
# the generated eBPF bindings are needed to compile it (`make obi-generate`).
# Published to <KO_DOCKER_REPO>-otlp.
.PHONY: ko-image-otlp
ko-image-otlp: ## Build the image without OBI locally using ko (no push)
	KO_CONFIG_PATH=hack/ko-otlp VERSION=$(VER) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) KO_DOCKER_REPO=ko.local ko build \
		--oci-layout-path=bin/images/triage-agent-otlp \
		./cmd/triage-agent

.PHONY: ko-push-otlp
ko-push-otlp: ## Build and push the image without OBI using ko, to <KO_DOCKER_REPO>-otlp
	KO_CONFIG_PATH=hack/ko-otlp VERSION=$(VER) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) KO_DOCKER_REPO=$(KO_DOCKER_REPO)-otlp ko build \
		--tags=$(VER) \
		--bare \
		--image-label org.opencontainers.image.version=$(VER) \
		./cmd/triage-agent

## --------------------------------------
## Host packages
## --------------------------------------
# Linux host mode (docs/adr/0018-linux-host-mode.md): static binaries built
# with -tags obi for linux/amd64 and linux/arm64, then a deb and an rpm of
# each with nfpm (https://nfpm.goreleaser.com, `go install
# github.com/goreleaser/nfpm/v2/cmd/nfpm@latest`). The -tags obi build needs
# the generated eBPF bindings first (`make obi-generate`). Output lands in
# dist/. Version stamping is the one bin/triage-agent uses.
HOST_ARCHES ?= amd64 arm64
# deb and rpm versions cannot carry the leading "v".
PKG_VERSION ?= $(patsubst v%,%,$(VER))

.PHONY: host-binaries
host-binaries: ## Build dist/triage-agent_linux_{amd64,arm64} with -tags obi (needs make obi-generate)
	mkdir -p dist
	for arch in $(HOST_ARCHES); do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch go build -tags obi -ldflags "$(LDFLAGS)" \
			-o dist/triage-agent_linux_$$arch ./cmd/triage-agent || exit 1; \
	done
	cd dist && { sha256sum triage-agent_linux_* 2>/dev/null || shasum -a 256 triage-agent_linux_*; } > triage-agent_linux_sha256sums.txt

.PHONY: packages
packages: host-binaries ## Build the deb and rpm packages of the host agent with nfpm
	@command -v nfpm >/dev/null 2>&1 || { echo "ERROR: nfpm not found; go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest" >&2; exit 1; }
	for arch in $(HOST_ARCHES); do \
		for fmt in deb rpm; do \
			ARCH=$$arch VERSION=$(PKG_VERSION) BINARY=dist/triage-agent_linux_$$arch \
				nfpm package --config packaging/nfpm.yaml --packager $$fmt --target dist/ || exit 1; \
		done; \
	done

.PHONY: clean
clean: ## Remove build artifacts and tool binaries
	go clean --cache
	rm -f $(COVER_FILE)
	rm -f $(TOOLBIN)/goimports
	rm -f $(TOOLBIN)/golangci-lint
	rm -f $(TOOLBIN)/misspell
	rm -f $(TOOLBIN)/kind


## --------------------------------------
## Releasing
## --------------------------------------
# Git tags are the source of truth. To cut a release, tag a commit on main and push
# the tag — CI (.github/workflows/ci.yaml) builds the multi-arch binaries + image and
# publishes the GitHub Release. Pass the version explicitly:
#   make release-tag RELEASE_TAG=v1.5.0

.PHONY: release-tag
release-tag: ## Tag the current commit and push it (RELEASE_TAG=vX.Y.Z), triggering the CI release
	@case "$(RELEASE_TAG)" in \
		v[0-9]*.[0-9]*.[0-9]*) ;; \
		*) echo "RELEASE_TAG must be an explicit semver tag, e.g. make release-tag RELEASE_TAG=v1.5.0 (got '$(RELEASE_TAG)')"; exit 1 ;; \
	esac
	git tag -a $(RELEASE_TAG) -m "Release $(RELEASE_TAG)"
	git push $(RELEASE_REMOTE) $(RELEASE_TAG)

.PHONY: delete-release-tag
delete-release-tag: ## Delete a release tag locally and on the remote (RELEASE_TAG=vX.Y.Z)
	git tag --delete $(RELEASE_TAG)
	git push $(RELEASE_REMOTE) :refs/tags/$(RELEASE_TAG)

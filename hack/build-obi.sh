#!/usr/bin/env bash
# Regenerates the OBI eBPF bytecode under third_party/obi (bpf2go output —
# see third_party/obi/.gitignore) and sanity-builds the thing that consumes
# it: this repo's own main package (cmd/triage-agent) built `-tags obi`. See
# docs/todo/obi-embed.md.
#
# ============================================================================
# Docker is required for STEP 1 (codegen) ONLY, and only when bumping the
# vendored OBI version — third_party/obi is a checked-in copy of OBI's
# source, and its generated *_bpfel.{go,o} files are gitignored, so a fresh
# checkout starts without them and needs this once. Nothing past STEP 1 uses
# Docker. In particular this script builds NO container image: the one image
# this repo ships is built by `make ko-image`/`ko-push` (.ko.yaml, built with
# -tags obi) — ko + go only, no Dockerfile anywhere in this tree.
# ============================================================================
#
# Usage:
#   hack/build-obi.sh
#
# Requires: git, go, a working docker daemon (Docker Desktop or colima) that
# can pull ghcr.io/open-telemetry/obi-generator — the exact image/tag is
# pinned in third_party/obi/Makefile's own GEN_IMG, not here; bumping the
# vendored OBI version re-pins it there, not in this script.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OBI_SRC="${REPO_ROOT}/third_party/obi"
DIST_DIR="${REPO_ROOT}/dist"

echo "== repo root: ${REPO_ROOT}"

if [ ! -d "${OBI_SRC}" ]; then
	echo "ERROR: ${OBI_SRC} not found. third_party/obi is a vendored, checked-in" >&2
	echo "copy of the OBI source — this script does not clone it from upstream." >&2
	exit 1
fi

## ---------------------------------------------------------------------------
## STEP 1 (docker): codegen. The only docker use in this repo's whole build.
## ---------------------------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
	echo "ERROR: no working docker daemon (Docker Desktop or colima). This step" >&2
	echo "cannot proceed without it — the generator image needs a real container" >&2
	echo "runtime, not just the docker CLI." >&2
	exit 1
fi

echo "== running OBI's own code generation (bpf2go, via 'make obi-generate' — third_party/obi/Makefile's docker-generate target, also used by CI's ci.yaml)"
(cd "${REPO_ROOT}" && make obi-generate)

bpfel_count=$(find "${OBI_SRC}" -iname '*_bpfel.go' | wc -l | tr -d ' ')
echo "== generated ${bpfel_count} *_bpfel.go files under third_party/obi/pkg (gitignored — third_party/obi/.gitignore; do not git add them)"

## ---------------------------------------------------------------------------
## STEP 2 (go only, no docker, no image): sanity build. Proves the codegen
## above actually unblocks the -tags obi consumer. `make ko-image` /
## `ko-push` are the real, shipped way to build the image — the build below
## is a local correctness check, and its dist/ output is never published
## (gitignored, see .gitignore's /dist/ entry).
## ---------------------------------------------------------------------------
mkdir -p "${DIST_DIR}"

echo "== sanity build: cmd/triage-agent, -tags obi (linux/amd64 + linux/arm64)"
(cd "${REPO_ROOT}" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags obi -ldflags="-s -w" -o "${DIST_DIR}/triage-agent-obi-linux-amd64" ./cmd/triage-agent)
(cd "${REPO_ROOT}" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags obi -ldflags="-s -w" -o "${DIST_DIR}/triage-agent-obi-linux-arm64" ./cmd/triage-agent)

echo "== darwin sanity: go build ./... and go vet ./... must still pass (the obi path is linux-gated + a darwin stub)"
(cd "${REPO_ROOT}" && go build ./... && go vet ./...)

echo "== done"
ls -la "${DIST_DIR}"
file "${DIST_DIR}"/*-linux-* 2>/dev/null || true

cat <<EOF

Codegen is complete; the dist/ binary above is a local sanity check only
(gitignored, never published). To build the real, shipped image:
  make ko-image   # local, no push
  make ko-push    # build and push

git status should now show third_party/obi/pkg/**/*_bpfel.{go,o,go.d} as
untracked — they stay gitignored (third_party/obi/.gitignore). Do not
'git add' them.
EOF

# Repo layout cleanup — binary under cmd/, no stray files at the root

Requested 2026-09-05: the repo root carries eighteen `package main` Go files
(a 1021-line `main.go`, the `ebpf_*` role files, flag types, the namespace
tenant watch, apiversion, podnamespace) plus local build leftovers. A reader
cannot tell the binary from the library packages.

## Target layout

```
cmd/triage-agent/      package main: main.go, modeflag.go, namespaceflag.go,
                       podnamespace.go, apiversion*.go, namespace_tenant_watch*.go,
                       ebpf_obi.go (obi && linux), ebpf_stub.go, ebpf_config.go,
                       ebpf_metrics.go, ebpf_reload.go and their tests
cmd/obiprobe/          unchanged (the two-container `ebpf.engine: obi` path)
push/ redsource/ usemetrics/ nodeexporter/ logexporter/ logmetrics/
metricsauth/ gcpevents/ controllers/ api/   library packages, unchanged
third_party/obi/       vendored, two local patches, unchanged
config/crd/ e2e/ hack/ docs/                 only what a Makefile target,
                                             workflow or doc still uses
```

Root after the move: `go.mod`, `go.sum`, `Makefile`, `.ko.yaml`,
`.golangci.yml`, `.gitignore`, `README.md`, `CHANGELOG.md`,
`CLAUDE.md`, `PASSPORT.md`, `SECURITY.md`, `CONTRIBUTING.md`,
`CODE_OF_CONDUCT.md`, `LICENSE`, `NOTICE`, directories.

## Rules

- Pure move for Go: `git mv`, package stays `main`, no logic changes. The
  import path of the binary becomes `github.com/triageagent-dev/agent/cmd/triage-agent`;
  the module path stays (NOTICE).
- Every build entry point follows: `Makefile` (`go build`, `ko build`
  targets), `.ko.yaml` (`main`/build paths), `.golangci.yml` path rules,
  `hack/lint-comment-style.sh` globs,
  GitHub workflows under `.github/`, `go:generate` directives, Dockerfiles if
  any. `grep -rn "\./main.go\|ebpf_obi.go"` across
  the repo (docs included) finds the references to update.
- Deletion needs proof of no use: a candidate is removed only when no
  Makefile target, script, workflow, `go:generate`, test or doc references
  it (`hack/boilerplate.go.txt`, `hack/tools/*`, `hack/obiprobe-spike.yaml`,
  `hack/build-obiprobe.sh` are the candidates; `cmd/obiprobe` stays while the
  chart still offers `ebpf.engine: obi`). Local untracked leftovers
  (`cover.out`, `bin/app-controller`, `bin/triage-agent`, `dist/`,
  `Makefile.local`) are deleted from disk and must be covered by
  `.gitignore`.
- Docs follow the paths: README layout section, PASSPORT facts (file
  paths, counts), `docs/todo/*.md` and `docs/adr/*.md` that name a moved
  file, CLAUDE.md build notes.

## Verification

`make fmt && make lint && make test`; `GOOS=linux go build -tags obi ./...`
and `GOOS=linux go vet -tags obi ./...`; `ko build --push=false` (or the
Makefile's equivalent) resolves the new import path for both ko configs; the
root contains no `*.go`; `git status` clean after the commits. A live
build+deploy of the moved binary to acme-gke follows the merge of this change.

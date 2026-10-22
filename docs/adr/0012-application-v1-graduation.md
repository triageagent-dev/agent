# ADR-0012: Graduate the Application CRD to app.k8s.io/v1

- **Status:** Superseded. Applications come from virtual apps only; the agent has no Application custom resource.
- **Date:** 2026-08-21
- **Deciders:** Denis (Principal SRE)
- **Related:** docs/todo/application-v1-graduation.md (migration plan and the
  downstream checklist for ~/code/triageagent-dev/core)

## Context

The `Application` CRD has served a single version, `app.k8s.io/v1beta1`, since
the resource was introduced. The schema has been
stable for the entire life of this controller — the additions since then
(push mode, log/RED/USE metrics, topology snapshots) all landed in sibling
packages and flags, none of them in `ApplicationSpec` or `ApplicationStatus`.
A beta version that nothing has needed to change is a version that should be
GA, and the beta label misrepresents how settled the contract is to anyone
reading a manifest.

Two constraints shape how it can move:

1. **Live objects exist.** Every `Application` currently stored in every cluster
   is stored as v1beta1. Renaming the version in place makes them unreadable.
2. **A downstream consumer reads it by hardcoded GVR.** The triage agent's
   `internal/appwatcher` resolves `app.k8s.io/v1beta1` through a dynamic client,
   and the triage chart renders its own Application CR at that apiVersion. That
   is a separate Go module in a separate repo with a separate release cadence, so
   it cannot be changed atomically with this one.

Both rule out a rename. The question was only whether v1beta1 stays *served*.

There is also a precondition that had to be dealt with first: `make manifests`
and `make generate` were already broken. `controller-gen` was pinned at v0.4.0
(2020) and panics type-checking this module, which now builds against
`k8s.io/apimachinery v0.36`. The CRD in the repo had not been regenerated in a
long time and could not be.

## Decision

Ship a **two-version CRD** with no schema change between the versions:

```yaml
versions:
- name: v1
  served: true
  storage: true
- name: v1beta1
  served: true
  storage: false
  deprecated: true
  deprecationWarning: "app.k8s.io/v1beta1 Application is deprecated; use app.k8s.io/v1"
```

Conversion is left unset, which is `strategy: None` — the apiextensions default.
The apiserver then serves a stored object under whichever version the client
asks for, rewriting `apiVersion` and nothing else. **That is only correct while
the two schemas are identical**, which is exactly the case for a beta-to-GA
graduation with no field changes, and it is why no conversion webhook is needed.

`api/v1` is a copy of the v1beta1 types, not an alias of them: controller-gen
derives each version's OpenAPI schema from real type declarations in the
version's own package, so aliases would produce no v1beta1 schema at all.
`api/v1beta1` is now **frozen** — new fields go to `api/v1` only.

The controller, `main.go`, `push/` and the e2e suite all move to the v1 Go type.
Both versions stay registered in the manager's scheme so it can still decode an
object handed to it as v1beta1.

Supporting changes this forced:

- `controller-gen` v0.4.0 → v0.21.0, and `trivialVersions=true` dropped from
  `CRD_OPTIONS` (it collapses a multi-version CRD into one version, and the
  option no longer exists in current controller-gen).
- `generate-go` regenerates `./api/...` rather than `./api/v1beta1/...`.
- `conversion-gen` and `mockgen` removed from the build. Neither was ever
  invoked — the repo has no `go:generate` directives — `conversion-gen@v0.18.9`
  no longer installs at all, and under `strategy: None` there are by definition
  no conversion functions to generate.

## Consequences

**Nothing downstream breaks on this release.** v1beta1 stays served, so existing
CRs, the triage app-watcher, the triage chart's own Application, and any hand-written
manifest keep working untouched. Clients see a deprecation warning; they migrate on
their own schedule. The downstream checklist is in the todo doc.

**`strategy: None` is a standing constraint, not a one-time choice.** If v1 ever
gains a field v1beta1 does not have, serving both versions off one stored object
silently drops that field for anyone reading v1beta1. Two tests in `api/v1` enforce
the invariant rather than leaving it to reviewer memory:
`TestV1Beta1AndV1SchemasMatch` walks both `Application` types and fails on any
difference in field name, JSON tag or type, and `TestV1Beta1RoundTripsThroughV1`
round-trips a fully populated object between the two. If either fails, the fix is
to mirror the change into both versions or to replace `strategy: None` with a
conversion webhook — not to relax the test.

**The push wire format is unaffected.** `push/` now marshals `*v1.Application`
while the triage receiver still unmarshals `*v1beta1.Application`, and that stays
compatible because the JSON field tags are identical (the test above) and the
receiver never reads `apiVersion` — it unmarshals into its own struct, and objects
read from a typed controller-runtime cache carry an empty TypeMeta, so no
apiVersion reaches the wire at all (`TestAppFramesCarryNoAPIVersion`).

**Removing v1beta1 is a later, separate change**, and it is gated on migrating
stored objects to v1 first — the apiserver refuses to drop a version still listed
in the CRD's `status.storedVersions`. Sequence is in the todo doc.

**We own this API.** This repo is the upstream for `app.k8s.io` as we ship it —
the versions are ours to cut, and graduating to v1 needs no sign-off from anyone
else. The `api-approved.kubernetes.io` annotation keeps its original value because
the apiserver requires the annotation to be present and well-formed for a
`*.k8s.io` group and does not check what it references; it is a formality that
keeps the CRD installable, not a claim of external review.

**`make lint` remains broken, unchanged by this work.** `golangci-lint@v1.23.6`
(2020) cannot read Go 1.26 export data; it fails identically on a clean tree. CI
runs `go vet` + `go build` + `go test`, all of which pass. Bumping the linter is
its own change, since a v1→v2 jump will surface a backlog of findings.

# Application CRD v1 graduation

Status: Superseded. Applications come from virtual apps only; the agent has no Application custom resource.
ADR: docs/adr/0012-application-v1-graduation.md

Graduate `app.k8s.io/v1beta1 Application` to `app.k8s.io/v1`, keeping v1beta1
served through a deprecation window.

## Shape

Two-version CRD, no schema change between them:

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
conversion:
  strategy: None
```

`strategy: None` is correct only because the two schemas are byte-identical —
this is a pure beta-to-GA graduation with no field changes. The apiserver then
serves a stored object under whichever version the client asks for, rewriting
only `apiVersion`. No conversion webhook, no `conversion-gen`, no hub/spoke
conversion functions. **If v1 ever gains a field v1beta1 does not have, this
stops being safe and a conversion webhook becomes mandatory.**

## Work

- [x] Bump `controller-gen` v0.4.0 -> v0.21.0. The pinned v0.4.0 panics
      type-checking this module (it predates `k8s.io/apimachinery v0.36`), so
      `make manifests` / `make generate` were already broken before this work.
- [x] Drop `trivialVersions=true` from `CRD_OPTIONS` — it collapses a
      multi-version CRD into one version, and the option no longer exists in
      current controller-gen.
- [x] `generate-go`: `paths=./api/v1beta1/...` -> `paths=./api/...`.
- [x] New `api/v1` package: `groupversion_info.go`, `application_types.go`
      (a frozen copy of the v1beta1 types plus `+kubebuilder:storageversion`),
      generated deepcopy, tests.
- [x] Mark v1beta1 `+kubebuilder:deprecatedversion:warning=...`.
- [x] Switch the controller, `main.go`, `push/` and the e2e suite to the v1 Go
      type. Both versions stay registered in the scheme.
- [x] Regenerate the CRD, sync into `charts/triage-agent/crds/`.
- [x] Drift guard test: a v1beta1 Application round-trips through JSON into a v1
      Application unchanged. This is what keeps `strategy: None` honest.
- [x] Docs: README, quickstart, develop, PASSPORT, CLAUDE.md, chart README +
      Chart.yaml, CHANGELOG, ADR-0012.

## Verified on a live cluster (acme-gke, 2026-08-21)

Deployed to release `triage-agent` / ns `triage` on the acme-gke cluster, which
held **40 Applications all stored as v1beta1** — the real test of `strategy: None`
against data that predates the graduation.

- CRD applied first (server-side). `status.storedVersions` went `["v1beta1"]` ->
  `["v1beta1","v1"]`; v1 served+storage, v1beta1 served+deprecated.
- All 40 objects read successfully under BOTH versions.
- A full `spec`+`status` diff of the same object fetched as v1 and as v1beta1 is
  byte-identical — conversion is lossless in practice, not just in the unit test.
- The controller (v2.3.0-09d351f) came up clean on the v1 watch and every
  Application's `componentsReady` was unchanged against the pre-upgrade baseline.
- An Application created as v1 reconciled normally and read back as v1beta1.
- A v1beta1 client now receives the apiserver warning
  `app.k8s.io/v1beta1 Application is deprecated; use app.k8s.io/v1`; v1 clients get
  none. k9s renders the CRD as `v1,v1beta1!`, the `!` being its deprecation marker.

**Ordering note for any future cluster:** the CRD must be applied BEFORE the new
controller rolls out. The v2.3.0 controller watches `app.k8s.io/v1` and fails with
`no matches for app.k8s.io/v1` against a v1beta1-only CRD.

## Downstream (NOT done here — separate change in ~/code/triageagent-dev/core)

Nothing downstream breaks on this release: v1beta1 stays served, so every
existing client keeps working unchanged. These are the migration points for when
that repo moves:

- `internal/appwatcher/watcher.go:277` — `applicationGVR` hardcodes
  `Version: "v1beta1"` for the dynamic client. This is the one that actually
  matters; it is the live read path.
- `cmd/appgen/manifest.go:18` — `apiVersion = "app.k8s.io/v1beta1"` in generated
  manifests.
- `charts/triageagent/templates/application.yaml:2` — the chart's own
  Application CR.
- `internal/remoteagent/protocol.go` — receiver side of the push wire format.
  **No change needed**: controller-runtime clears `TypeMeta` on typed objects, so
  `apiVersion` never appears on the wire, and the JSON field tags are identical
  across the two versions. The frames stay compatible in both directions.
- Doc/comment references in `api/v1alpha1/triageagentconfig_types.go`,
  `charts/triageagent/values.yaml`, `clusterrole.yaml` (RBAC itself is
  version-agnostic — `apiGroups: ["app.k8s.io"]` covers both).

## Removing v1beta1 (a later, separate change)

Do not do this until all of the above have shipped. The sequence:

1. Confirm no client reads v1beta1 — apiserver deprecation warnings surface in
   `kube-apiserver` logs and in `kubectl` output.
2. Migrate stored objects to v1: `kubectl get applications -A -o yaml | kubectl replace -f -`
   (or `kube-storage-version-migrator`), then confirm
   `status.storedVersions == ["v1"]` on the CRD.
3. Only then set `served: false` on v1beta1, ship, and delete `api/v1beta1` in a
   following release.

Skipping step 2 leaves `storedVersions` containing `v1beta1`; the apiserver
refuses to drop a version still listed there.

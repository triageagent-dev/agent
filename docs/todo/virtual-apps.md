# Virtual apps — label-derived Application synthesis in the push agent

Status: PLANNED (2026-08-23)
Implementation landed 2026-08-23 on `feature/virtual-apps` (off `beyla-hardening`),
acme-gke validation pending — status stays PLANNED until the shadow-mode rollout
below is actually run against acme-gke and reviewed with the operator.
Target: feature branch off `beyla-hardening` (acme-gke runs that line; main merge pending).
Consumer contract: triageagent-dev/core `internal/remoteagent` (snapshot/app_delta frames carry
`applicationsv1beta1.Application`; receiver replays through DeltaFIFO into the same
appevents.Emitter the in-cluster informer uses — ZERO hub changes needed).

## Problem

A CR-less cluster has no app-level readiness: onboarding a tenant today means
authoring Application manifests + running the reconciler. The agent already
watches the cluster and pushes everything else (k8s events, RED/USE/log
metrics, eBPF topology); it can synthesize the Application objects itself from
workload state grouped by labels — no manifests, no CRs, no customer YAML.

## Design

### Grouping and synthesis (in-memory only — no CRs are ever written)

- Informers on the configured `--workload-resources` list (default
  Deployments, StatefulSets, DaemonSets, Argo Rollouts — see "Workload
  resources" below), scoped to `--target-namespaces`.
- Group per namespace by label priority: `app.kubernetes.io/part-of` →
  `app.kubernetes.io/instance` → `app.kubernetes.io/name`; a workload with
  none of these becomes its own single-workload group named after itself.
- Per group, synthesize one `applicationsv1beta1.Application`:
  - `metadata.name` = group name; `metadata.namespace` = the group's ns;
    `metadata.labels["app.kubernetes.io/name"]` = group name (the receiver's
    `AppName()` canonical-name contract) plus a marker label
    `app.kubernetes.io/managed-by: virtual-apps`.
  - `spec.descriptor.version`: `app.kubernetes.io/version` label when uniform
    across members; else the first container's image tag when uniform; else "".
  - `status.conditions[Ready]`: True iff every member is ready — see
    "Workload resources" below for the current, kind-generic readiness rule
    (desired/ready field resolution, and the scaled-to-zero fix).
  - `status.componentsReady` = "n/m"; `status.objects` = one entry per member
    (kind, name, per-member ready status) — the fields the emitter actually
    reads (Conditions, ComponentsReady, Objects).
- CR wins: a group whose ns/name collides with a real Application CR in the
  cache is SUPPRESSED from pushing — the reconciled CR is authoritative.
- Debounce group recompute (~2s coalesce) so a rollout's pod churn doesn't
  storm app_delta frames; flap damping stays receiver-side (hysteresis),
  unchanged.

### Modes: `--virtual-apps=off|shadow|on` (default off)

- **shadow** — synthesize + evaluate, never push. For every group covered by a
  real CR, diff the vapp against the CR: Ready agreement, component set/count,
  version. Periodic (60s) summary log line (agree/disagree counts per field)
  plus per-group detail on mismatch, and the list of CR-less groups that `on`
  would start pushing. This is the acceptance instrument: the existing
  Application CRs are the ground truth for the synthesizer.
- **on** — snapshot list = real CRs + non-suppressed vapps merged
  (`listAppsFn`); group state changes emit app_delta. CR-covered groups still
  suppressed.

### Chart

`virtualApps: "off"` value rendering the flag on the agent container.

## Rollout / evaluation on acme-gke

1. Deploy `shadow` to acme-gke (`triage-agent`, ns `triage`) — zero behavior
   change on the wire; read the eval summary in agent logs.
2. Review agreement numbers + the would-push list with the operator.
3. Flip to `on`: CR-less groups begin reporting; acme services gain live
   readiness in triage panel 1 (rows move ○ unknown → ●/●) with no manifest
   deployed.

## Out of scope (v1)

- Distributing the tenant graph to the agent (hub stays the only graph owner;
  agent pushes everything it groups, hub resolves — same contract as topology
  snapshots).
- CronJobs/Jobs, plain ReplicaSets, and custom-selector grouping.
- Hub-side `source: labels` marker surfacing (cosmetic follow-up).

## Per-workload annotation control (addendum, 2026-08-24)

Two annotations on the WORKLOAD OBJECT's own `metadata.annotations`
(Deployment/StatefulSet/DaemonSet — deliberately never the pod template, since
these control how the workload object itself participates in
grouping/synthesis, not anything about the pods it manages):

- **`triageagent.dev/virtual-app: "false"`** — excludes the workload from
  grouping/synthesis entirely. This is an opt-out, not a gate: any other value
  (including `"true"`) or the annotation's absence keeps the workload in. An
  excluded workload is dropped at grouping time and never counts toward any
  group's members or readiness — a group whose only members are excluded
  simply does not exist, and a still-nonready excluded member cannot drag down
  a group it would otherwise have belonged to.

- **`triageagent.dev/virtual-app-group: <name>`** — a GROUP MEMBERSHIP key, not a
  display name. It outranks `part-of`/`instance`/`name` — the highest
  grouping priority in `groupNameFor` — so an owner can split a
  release-aggregate (e.g. `instance=prod-ecosystem`) into per-service vapps
  without touching labels. The rule is simply: **same group value ⇒ same
  vapp (namespace-scoped)**. That cuts both ways — it also lets unrelated
  workloads fold into one shared vapp: a `neo4j` Deployment used by several
  charts in one namespace, each annotated `triageagent.dev/virtual-app-group: neo4j`,
  synthesizes as a single shared `neo4j` vapp instead of each chart's own
  release-scoped group silently claiming it.

Interplay: a workload can carry both annotations at once (e.g. a chart that
sets a shared group value but also wants it excluded from synthesis this
environment). Exclusion always wins — the group annotation can never resurrect
an excluded workload into a group of its own or anyone else's.

Shadow mode's periodic (60s) summary line gains an `excluded` field: the count
of workloads currently carrying `triageagent.dev/virtual-app: "false"`, computed from
the flat workload set before grouping. This is visibility only — shadow mode
never pushes regardless.

Implementation: `push/virtualapps.go` — `workloadInfo.Excluded` /
`workloadInfo.GroupOverride`, `workloadAnnotations()`, consulted first in
`groupNameFor()` and filtered in `groupWorkloads()`; `countExcluded()` feeds
the shadow-eval summary. Tests in `push/virtualapps_test.go`.

## Metadata annotations (Phase 2, addendum, 2026-08-24)

Three more annotations on the WORKLOAD OBJECT's own `metadata.annotations`
(same placement rule as the two control annotations above — Deployment/
StatefulSet/DaemonSet, never the pod template) carry per-workload metadata
into the synthesized Application rather than controlling grouping/synthesis
itself:

- **`triageagent.dev/domain`** — service domain (free-text string).
- **`triageagent.dev/description`** — a human description of the workload.
- **`triageagent.dev/tier`** — a criticality tier, an integer 0-9 as a string, parsed
  strictly (`strconv.Atoi` + range check). Lower is more critical. Missing or
  unparsable (non-integer, or outside `[0,9]`) is treated identically to "not
  declared" — the workload just carries no tier — except that the unparsable
  case additionally logs once at `V(1)` so a typo'd value isn't silently
  invisible.

**Aggregation per group** (`synthesizeApplication`):

- **domain, description**: uniform-across-members-or-empty, the same rule
  `resolveGroupVersion` applies to `app.kubernetes.io/version` — every member
  that declares one must agree, or the group value is empty. A member that
  doesn't declare the annotation at all counts as disagreeing (not as
  abstaining), same as a version-label holdout breaks version uniformity.
- **tier**: the MINIMUM across members that declare one — lower is more
  critical, so the most critical member speaks for the whole group. No member
  declares → absent (never a fabricated `0`; declaring `0` and declaring
  nothing are different states, both trivially distinguished by an
  `int`+`bool` pair, not a raw `int` defaulting to zero).

**Carried on the synthesized Application:**

- `description` → **both** `spec.descriptor.description` (the Application
  CRD's own free-text field) **and** `metadata.annotations["triageagent.dev/description"]`.
- `domain` → `metadata.annotations["triageagent.dev/domain"]`.
- `tier` → `metadata.annotations["triageagent.dev/tier"]` (string form of the resolved
  minimum).

A group that resolves none of the three (no member declared any of them, or
every declared value conflicted) gets **none** of the three annotation keys on
the synthesized Application — absent, never present with an empty-string
value. A real, un-synthesized Application CR is unaffected either way.

**This is the wire contract.** The triage hub reads `triageagent.dev/domain`/
`triageagent.dev/description`/`triageagent.dev/tier` straight off the Application object's
`metadata.annotations` — the same three keys work identically whether the
Application came from this synthesizer or is a real, reconciled CR a team
wrote and annotated by hand. Hub-side graph fill-in (using these three fields
to enrich a tenant's service-graph node — domain grouping, tier-weighted
blast radius, a description surfaced in the UI) is the **companion change**,
tracked in the `triageagent-dev/core` repo, not here.

Implementation: `push/virtualapps.go` — `workloadInfo.Domain`/`Description`/
`Tier`/`HasTier`/`TierInvalid`, extended `workloadAnnotations()`,
`resolveGroupDomain()`/`resolveGroupDescription()`/`resolveGroupTier()`
(mirroring `resolveGroupVersion`'s uniform-value pattern), and the
`metadata.annotations`/`spec.descriptor.description` assembly in
`synthesizeApplication()`. Tests in `push/virtualapps_test.go`.

## Namespace inheritance (addendum, 2026-08-24)

Operators can annotate the **NAMESPACE object** itself, once, with the same
three keys above — `triageagent.dev/domain`, `triageagent.dev/description`, `triageagent.dev/tier` — plus
`triageagent.dev/virtual-app: "false"`. Every workload in that namespace and every real
Application CR pushed on the wire inherits any of the three metadata fields it
does not declare itself. **Precedence is strict**: workload/CR annotation >
namespace annotation > absent — a namespace default only ever fills a gap,
never overrides a value already declared closer to the object.

- **`triageagent.dev/virtual-app: "false"` on the NAMESPACE** means something different
  from the same annotation on a workload: it excludes the **WHOLE namespace**
  from vapp synthesis, regardless of any individual workload's own
  annotation. The per-workload annotation keeps working independently in
  namespaces that are NOT excluded — the two are ORed together
  (`workload.Excluded || namespace.Excluded`), never merged into one flag.

- **Workload inheritance** is applied at the `workloadInfo` level, in
  `virtualAppsState.workloadsSnapshot()`, **before** group aggregation runs —
  so `resolveGroupDomain`/`resolveGroupDescription`/`resolveGroupTier`'s
  existing uniform-or-empty / min-wins logic is completely untouched: a group
  whose members all inherit the same namespace default resolves that value
  naturally, exactly as if every member had declared it itself, and a member
  that overrides the default with its own conflicting value still triggers
  the usual disagreement (empty result) against the other members' inherited
  value.

- **The CR overlay** (real Application CRs, not synthesized vapps) is
  independent of `--virtual-apps` mode entirely — it applies to every real CR
  pushed on the wire regardless of whether virtual-app synthesis is even
  turned on. `Pusher.overlayNamespaceDefaults` is the ONE helper both the
  snapshot path (`sendSnapshot`) and the delta path (`onApp`) call before a CR
  reaches the wire: it fills any of the three `triageagent.dev/*` annotations ABSENT on
  the CR from its namespace's defaults and never overwrites one the CR
  already carries (present, even as an empty string, is never touched). It
  only ever touches `metadata.annotations` — unlike the per-workload
  synthesis path, it does NOT also populate `spec.descriptor.description`,
  since a real CR's `spec` is the customer's own and this overlay is
  metadata-only.

- **Live updates** (bullet 4): a Namespace informer (`Pusher.
  registerNamespaceInformer`, cluster-scoped, registered once for the
  Pusher's whole lifetime like the virtual-app workload informers,
  independent of `--virtual-apps` mode) feeds an `nsDefaultsStore` keyed by
  namespace name. On every add/update, `Pusher.onNamespace`:
  1. Re-parses the namespace's `triageagent.dev/*` annotations (`namespaceAnnotations`,
     reusing `workloadAnnotations`'s exact parsing rules) and stores them.
  2. If virtual-app synthesis is enabled, arms the SAME debounce timer
     workload changes already use (`virtualAppsState.scheduleRecompute`) —
     a burst of workload churn and namespace annotation changes together
     still coalesces into one recompute.
  3. Re-lists real Application CRs in that namespace and re-enqueues each as
     an `app_delta` update (`Pusher.repushAppsInNamespace`) — the simplest
     correct way to make an already-known CR's overlaid annotations catch up
     immediately, rather than waiting for that CR's own next unrelated event.
     A no-op when push is disconnected or the namespace is out of
     `--target-namespaces` scope.

  On delete, the namespace's defaults are simply removed from the store —
  a workload or CR in that (now-defunct) namespace stops inheriting.

- **Tier parsing** reuses the exact same strict 0-9 parser as the per-workload
  annotation (`workloadAnnotations`, via `namespaceAnnotations`). An invalid
  namespace `triageagent.dev/tier` value logs once at `V(1)` per informer event
  (add/update) and is otherwise treated as absent — same behavior as an
  invalid workload-level tier.

Implementation: `push/namespace_defaults.go` — `namespaceDefaults`,
`namespaceAnnotations()`, `nsDefaultsStore`,
`applyNamespaceDefaultsToWorkload()`, `applyNamespaceDefaultsToApp()`;
`push/virtualapps.go` — `virtualAppsState.nsDefaultsFn`, the overlay call in
`workloadsSnapshot()`, `scheduleRecomputeLocked()`/`scheduleRecompute()`;
`push/pusher.go` — `Pusher.nsDefaults`, `registerNamespaceInformer()`,
`onNamespace()`, `repushAppsInNamespace()`, `overlayNamespaceDefaults()`, and
the overlay call sites in `sendSnapshot()`/`onApp()`. Tests in
`push/namespace_defaults_test.go`, `push/virtualapps_test.go`, and
`push/pusher_test.go`.

## Namespace-scope-by-annotation (addendum, 2026-08-25)

"Automatic enablement": today an agent only watches the namespaces named in
its static `--target-namespaces` list (`Pusher.inScope`, `virtualAppsState.
inScope` — exact string match, empty list = all). This addendum adds a
DYNAMIC, live-updating source of additional in-scope namespaces on top of
that list: a Namespace object carrying `triageagent.dev/tenant: "<tenant>"`, where
`<tenant>` equals this agent's own configured `Options.Tenant` (the same
value stamped into the hello frame and used to select the triage service
graph), is in scope automatically — no operator edit to `--target-namespaces`,
no restart. Built for a single agent that should pick up a new namespace the
moment it's labeled for its tenant, rather than needing a chart/flag change
and a rollout.

**The scope rule** (`effectiveInScope`, `push/namespace_scope.go`) —
read this before touching `inScope` anywhere:

```
effective scope = union(static list, dynamic set)   — when static is non-empty
effective scope = the dynamic set ALONE             — when static is empty,
                                                        in both modes
```

An empty static `--target-namespaces` list means the `triageagent.dev/tenant`-annotated
set, in both modes. A primary tenant decides which tenant an in-scope namespace
is filed under; it does not widen scope. With no list and no annotation the
agent observes nothing. A non-empty static list unions with the dynamic set.

Both `Pusher.inScope` and `virtualAppsState.inScope` delegate to the same
`effectiveInScope` helper against the SAME `*dynamicScopeSet` instance (shared,
not copied, wired once in `Pusher.New`) — so a namespace's scope membership is
identical on both paths at every instant.

**Maintenance reuses the existing Namespace informer** (`registerNamespaceInformer`
/ `Pusher.onNamespace`, unchanged wiring from the namespace-inheritance
addendum above): on every add/update, `onNamespace` now also evaluates
`tenantScopeMatch(ns, opts.Tenant)` and calls `addDynamicScope`/
`dropDynamicScope`, both transition-gated (a redundant re-sync of an
already-scoped, or already-unscoped, namespace does nothing) and nil-safe (a
bare `*Pusher` struct literal predating this feature has no `dynamicScope`
wired and behaves exactly as before). On delete, the namespace unconditionally
leaves dynamic scope the same way its `nsDefaults` entry is removed.

**The crux: backfilling already-cached workloads.** `onDeployment`/
`onStatefulSet`/`onDaemonSet` check `inScope` only at INFORMER EVENT time. A
workload that was already steady (Ready, no further churn) before its
namespace entered scope sits in the informer cache but fires no new event —
without an explicit backfill it would never enter `virtualAppsState.
workloads`, and the annotation would only ever work for that workload's NEXT
event (which may never come). `addDynamicScope` calls
`virtualAppsState.backfillNamespace(ns)`, which lists every
Deployment/StatefulSet/DaemonSet already resident in the cache for that one
namespace (`listCachedDeployments`/`listCachedStatefulSets`/
`listCachedDaemonSets`, overridable via `listDeploymentsFn`/
`listStatefulSetsFn`/`listDaemonSetsFn` for tests — same injectable-function
convention as `listAppsFn`/`listRealAppsFn` elsewhere in this package) and
feeds each one through `applyWorkload` directly, bypassing the normal
`inScope` gate since the caller has already established the namespace is in
scope. It ends with an immediate `recomputeNow` (not the debounced
`scheduleRecompute`) so the backfilled workloads' vapp — and, mode "on", its
`OpAdd` delta — appear without waiting out `virtualAppsDebounce`. Real
Application CRs in the newly-scoped namespace need no equivalent backfill
mechanism: `onNamespace`'s existing `repushAppsInNamespace(ns.Name)` call
already re-lists and re-enqueues every CR in that namespace, and now succeeds
because `inScope` has already flipped to true by the time it runs.

**Removal actually evicts, it doesn't just stop re-admitting.** A namespace
leaving dynamic scope (annotation removed, changed to a different tenant, or
the Namespace itself deleted) calls `dropDynamicScope`, which — ONLY when
`staticInScope(ns, opts.Namespaces)` is false afterward (the namespace may
still be covered by the static list even though its dynamic membership just
ended) — deletes both the real Application CRs (`emitAppDeletesForNamespace`,
one `OpDelete` app_delta per CR the last listing found in that namespace) and
the virtual apps (`virtualAppsState.evictNamespaceAndRecompute(ns)`) that
namespace contributed. The vapp eviction drops every workload belonging to
that namespace from the flat `workloads` map (`evictNamespace`) and forces an
immediate recompute: the group they formed vanishes from `v.current`, and
`emitDeltas` (mode "on") emits its own `OpDelete` exactly like any other
group disappearing. Without this explicit eviction, a workload that had
already been backfilled into `v.workloads` would sit there forever —
`onDeployment` et al. only fire on a NEW event, and a steady workload that
lost scope generates none. A no-op recompute is skipped entirely when
eviction removed nothing, so an annotation flip on a namespace that never
contributed a workload (or a namespace that's ALSO covered by the static
list, so nothing actually left scope) does no redundant work.

**The post-removal probe must never fall back to "assume still in scope."**
`staticInScope(ns, static)` answers "is ns in the static
`--target-namespaces` list", full stop — deliberately NOT
`effectiveInScope(ns, static, nil)`. `effectiveInScope`'s nil-dynamic-set
branch returns `true` when the static list is empty, because a nil dynamic
set there means "the annotation watcher was never wired" (bare struct
literals in tests) — permissive on purpose, so those particular callers keep
exact pre-feature behavior. `dropDynamicScope` is never one of those callers:
it has already loaded ns's own tenant out of a real, non-nil `dynamicScope` a
few lines above, and is asking what scope looks like once THAT ONE mapping is
gone. `staticInScope` exists so this probe cannot take
`effectiveInScope`'s permissive branch — taking it here, with the SHIPPING
configuration (no `--target-namespaces` flag at all, so `opts.Namespaces` is
empty), makes every dynamically-scoped namespace permanently in scope: the
annotation coming off never evicts anything, and the namespace's real
Application CRs and virtual apps linger in the hub with no path to ever being
reaped short of a process restart with a static list.

Implementation: `push/namespace_scope.go` — `annotationTenant`,
`dynamicScopeSet`, `effectiveInScope()`, `staticInScope()`,
`Pusher.addDynamicScope()`/`dropDynamicScope()`; `push/virtualapps.go` —
`virtualAppsState.dynamicScope`/`cache`/`listDeploymentsFn`/
`listStatefulSetsFn`/`listDaemonSetsFn`, `inScope()` (rewritten to
`effectiveInScope`), `backfillNamespace()`, `listCachedDeployments()`/
`listCachedStatefulSets()`/`listCachedDaemonSets()`, `evictNamespace()`,
`evictNamespaceAndRecompute()`; `push/pusher.go` — `Pusher.dynamicScope`,
`inScope()` (rewritten to `effectiveInScope`), the wiring in `New()`, and the
`onNamespace()` additions. Tests in `push/namespace_scope_test.go`,
`push/virtualapps_test.go`, and `push/pusher_test.go`.

### Scoped-cache limitation + runtime-factory fallback (2026-08-25)

**Bug (live-verified on the ops cluster):** the addendum above works cleanly
only when the manager cache is cluster-wide (`--namespace=""`). When
`--namespace` scopes the cache (`main.go` builds `cacheOpts.DefaultNamespaces
= {--namespace} ∪ {--target-namespaces}`), a namespace pulled into DYNAMIC
scope by a `triageagent.dev/tenant` annotation but NOT in that static set breaks two
ways: `backfillNamespace`'s manager-cache list fails with `unknown namespace
for the cache` (the exact error observed), and — more fundamentally —
`registerInformers`' Deployment/StatefulSet/DaemonSet informers, which are
manager-cache informers, **never deliver any event for it at all**, not
delayed, never. `controller-runtime` cannot add a namespace to an already
namespace-scoped cache at runtime. The Namespace informer itself is
unaffected (`Namespace` is cluster-scoped), which is why the annotation is
still seen and `dynamicScope`/`inScope` still flip correctly — only the
workload side silently goes dark.

**Fix: a runtime, per-namespace client-go informer factory
(`push/dynamic_informers.go`) as the fallback for exactly this case.**

- **Coverage detection.** `push.Options.CacheNamespaces` carries `main.go`'s
  `cacheOpts.DefaultNamespaces` keys (nil/empty = cluster-wide cache, the
  unaffected case). `namespace_scope.go`'s `cacheCovers(ns, cacheNamespaces)`
  answers a deliberately different question from `effectiveInScope`:
  `effectiveInScope` asks "should this agent push this namespace at all";
  `cacheCovers` asks "does the manager cache already know about it at the
  informer level". `virtualAppsState.cacheCovers(ns)` wraps it with the
  instance's own `cacheNamespaces` field (wired once by `Pusher.New`, same
  "set directly" convention as `nsDefaultsFn`/`dynamicScope`).
- **Factory lifecycle.** `dynamicInformerManager` (`push/dynamic_informers.go`)
  keeps `map[namespace]*dynamicNamespaceFactory` under a mutex — a namespace
  gets at most one `informers.NewSharedInformerFactoryWithOptions(clientset,
  resync, informers.WithNamespace(ns))`, with Deployment/StatefulSet/DaemonSet
  event handlers wired to the SAME `virtualAppsState.onDeployment`/
  `onStatefulSet`/`onDaemonSet` callbacks the manager-cache-backed informers
  use (`dynamicWorkloadHandlers`), so a workload discovered this way feeds
  synthesis through the identical `inScope`-gated path — never a parallel one
  that could drift. `ensure(ctx, ns)` is idempotent (annotation flap = no-op,
  keyed by the map, checked again after `WaitForCacheSync` to close a
  same-namespace-twice race defensively) and starts the factory with its own
  `stopCh`; `stop(ns)` closes that channel and forgets the entry, also
  idempotent. `virtualAppsState.backfillNamespace` calls `ensure` before
  listing when `!cacheCovers(namespace)`, then lists from the factory's own
  typed listers (`listDeployments`/`listStatefulSets`/`listDaemonSets` —
  the "backfill FROM THIS FACTORY'S LISTERS" step, since the manager-cache
  list would just fail again) rather than relying on the `AddFunc` calls that
  fire during the initial sync (which are real and harmless if they also
  fire — `applyWorkload`/`emitDeltas` are idempotent upserts — but timing
  relative to `WaitForCacheSync`'s return is not a contract worth depending
  on). `virtualAppsState.evictNamespaceAndRecompute` calls `dynInformers.stop`
  unconditionally, before the existing eviction/recompute, so a namespace
  whose annotation flaps off before any workload ever settled still has its
  informer goroutines torn down rather than leaking.
- **Clientset.** `push.Options.Clientset kubernetes.Interface` is a plain
  client-go interface — `informers.NewSharedInformerFactoryWithOptions` does
  not accept a controller-runtime cache/client. `main.go` builds it from the
  manager's own `rest.Config` (`kubernetes.NewForConfig(mgr.GetConfig())`),
  so it authenticates as the same ServiceAccount as everything else in the
  process. Nil is a documented-safe degrade throughout
  (`dynamicInformerManager` methods return a wrapped `errNoClientset` instead
  of touching a nil interface): a clientset build failure in `main.go`, or a
  test that only ever exercises `Options{}` directly, gets one logged error
  per affected namespace instead of a crash.
- **RBAC finding.** The controller's `ClusterRole` (`charts/triage-agent/
  templates/rbac.yaml`) is bound via a `ClusterRoleBinding` (cluster-wide
  regardless of `--namespace`), and both its `allowAllComponentKinds` branches
  already grant cluster-wide `get/list/watch` on `apps/v1`
  deployments/statefulsets/daemonsets (the default branch via `apiGroups:
  ["*"], resources: ["*"]`; the narrowed branch explicitly). So RBAC was
  never the blocker for the workload side of this bug — it's purely the
  manager cache's namespace scoping. One real gap found and fixed: the
  narrowed branch had no rule for the core `""` group's `namespaces`
  resource at all, which the pre-existing Namespace informer
  (`registerNamespaceInformer`, namespace-inheritance addendum) also depends
  on — added `get/list/watch` on `namespaces` next to the existing `events`
  rule.
- **Idempotency/races.** `dynamicScopeSet` (unchanged) already gates
  `addDynamicScope`/`dropDynamicScope` on genuine transitions; `ensure`/`stop`
  add their own independent idempotency at the factory-map level so an
  annotation flap can never leak a goroutine or register two factories for
  one namespace, and `stop` on a namespace that never had one (always
  cache-covered, or never dynamically scoped) is a no-op.
- **Tests** (`push/dynamic_informers_test.go`, fake clientset via
  `k8s.io/client-go/kubernetes/fake`): `ensure` idempotency and initial-sync
  listing (including proof a factory is genuinely namespace-scoped — a
  same-named Deployment in a different namespace is invisible to it), `stop`
  idempotency + stop-channel closure, the no-clientset degrade path, and an
  end-to-end `virtualAppsState` pair — `TestVirtualAppsState_
  BackfillNamespace_UncoveredNamespaceUsesRuntimeFactory` (annotate → factory
  delivers a pre-existing steady Deployment → vapp appears → evict → factory
  stopped + vapp gone) and `TestVirtualAppsState_BackfillNamespace_
  CoveredNamespaceNeverStartsFactory` (a namespace already in
  `cacheNamespaces` must never touch `dynInformers` at all).

Implementation: `push/dynamic_informers.go` (new) —
`dynamicWorkloadHandlers`, `dynamicNamespaceFactory`, `dynamicInformerManager`
+ `ensure`/`stop`/`running`/`listDeployments`/`listStatefulSets`/
`listDaemonSets`; `push/namespace_scope.go` — `cacheCovers()`;
`push/virtualapps.go` — `virtualAppsState.cacheNamespaces`/`dynInformers`,
`cacheCovers()`, `backfillNamespace()`/`listCachedDeployments()`/
`listCachedStatefulSets()`/`listCachedDaemonSets()`/
`evictNamespaceAndRecompute()` updated to route through the fallback;
`push/pusher.go` — `Options.CacheNamespaces`/`Clientset`, the wiring in
`New()`; `main.go` — `cacheNamespaces` capture alongside `cacheOpts`, the
`pushClientset` build; `charts/triage-agent/templates/rbac.yaml` — added
`namespaces` get/list/watch. Not yet observed live against a real
scoped-cache deployment — see the scope checklist below.

### Empty-static-list eviction guard bug (fix, 2026-09-03)

**Bug (live-verified on acme-gke):** removing the `triageagent.dev/tenant` annotation
from namespace `qdrant-cluster` logged `namespace left dynamic scope`, but
its virtual app `qdrant` was still present in the hub roughly an hour later,
showing stale signals. `dropDynamicScope`'s post-removal probe was written as
`effectiveInScope(ns, opts.Namespaces, nil)` — passing `nil` for "what will
the dynamic set look like after the mapping I'm about to remove is gone",
rather than for its actual meaning, "the annotation watcher was never
wired". With no `--target-namespaces` flag (`opts.Namespaces` empty — the
shipping configuration on every agent that takes no `--target-namespaces`
argument, acme-gke included), that hit `effectiveInScopeMode`'s permissive
`dynamic == nil` branch and returned `true` unconditionally, so
`!effectiveInScope(...)` was always `false` and the entire cleanup branch —
both the real-CR `OpDelete`s and `virtualAppsState.evictNamespaceAndRecompute`
— was dead code for every agent running the shipping configuration. The
mapping was removed and the informers stopped regardless, so the namespace's
apps were orphaned in the hub with no path to ever be reaped.

**Fix:** `staticInScope(ns, static)` (`push/namespace_scope.go`) answers the
static-list-only question with no dynamic fallback at all, and
`dropDynamicScope` calls it instead of `effectiveInScope(ns, opts.Namespaces,
nil)`. `effectiveInScope`/`effectiveInScopeMode` themselves are unchanged —
their permissive `dynamic == nil` branch is still correct for every other
caller (`Pusher.inScope`, `virtualAppsState.inScope`), which always pass
their own live, non-nil `dynamicScope`.

Tests: `push/namespace_scope_test.go` —
`TestPusher_DropDynamicScope_EmptyStaticNamespaces_DeletesRealAppAndVirtualApp`
(the bug, reproduced against a connected `tenantMux` and a capturing vapp
`enqueue` — not a bypassed test double) plus the neighbouring cases (static
list still covers ns → no delete; static list excludes ns → delete anyway;
ns never dynamically scoped → no-op); `push/pusher_test.go` —
`TestPusher_OnNamespace_DynamicScope_EmptyStaticAlsoEvictsOnRemoval`
(rewritten from `..._LegacyEmptyStaticNeverEvicts`, which had asserted the
bug's behavior as correct).

## Scope checklist (status of record, 2026-08-24)

"Complete" claims cite this list, item by item. Live = observed on acme-gke
(tenant acme) against hub v2.8.13-9a5e419, agent v2.3.0-39afff0.

Verified live:
- [x] vapp synthesis, CR-wins suppression, mode on (18 vapps on acme-aks, 5 apps acme)
- [x] `triageagent.dev/virtual-app-group` — split neo4j sts pair merged to one app 2/2
- [x] `triageagent.dev/domain`/`/description`/`/tier` travel on vapps AND real CRs; hub parses
- [x] catalog-wins precedence (qdrant kept catalog tier 2 over annotation tier 1)
- [x] namespace inheritance — ns-annotated acme; unannotated, uncataloged
      `annotation-demo` deployment auto-onboarded with domain=rag, tier=2,
      ns description (auto-created node, fill-in path). CR re-push on ns change observed.

Built + unit-tested, NOT observed live:
- [ ] `triageagent.dev/virtual-app` exclude (workload- and ns-level)
- [ ] gossip mirror of domain/tier across hub replicas (round-trip test only)
- [ ] group aggregation conflict arms (uniform-or-empty divergence) beyond tests
- [ ] namespace-scope-by-annotation (`triageagent.dev/tenant`) — union rule, cached-workload
      backfill on add, eviction on remove (including with an empty static
      list — see "Empty-static-list eviction guard bug" above); unit tests
      only (`push/namespace_scope_test.go`, `push/virtualapps_test.go`,
      `push/pusher_test.go`), not yet observed against a live cluster
- [ ] scoped-manager-cache runtime-informer-factory fallback (`push/
      dynamic_informers.go`) — unit tests only (fake clientset,
      `push/dynamic_informers_test.go`), not yet observed against a live
      cluster running with `--namespace` set

Open:
- [ ] emitDeltas debug log line (vapp pushes invisible in agent logs)
- [ ] initial recompute before first snapshot (~1s race, deltas self-heal)
- [ ] merge feature/virtual-apps → main; push both repos (user gates)
- [ ] cluster-c (prod) agent still on v2.4.4
- [ ] OBI eBPF embed spike — see docs/todo/obi-embed.md (spike/obi-embed branch)

## Cluster-mode push (tenant-less agent, addendum, 2026-08-27)

Multi-tenant push (`push/tenant_routing.go`, protocol v3, the per-frame
`Tenant` envelope) let one agent serve its configured PRIMARY tenant
(`--target-tenant`/`Options.Tenant`) plus any number of SECONDARY tenants named by a
`triageagent.dev/tenant` namespace annotation, all over one WebSocket connection. This
addendum removes the primary requirement entirely: **`--target-tenant` is now
optional**. An agent run with `--cluster-name` set and `--target-tenant` empty is
identified only by its cluster name — a "cluster-scoped" agent whose every
frame's tenant is derived purely from namespace annotations, with no implicit
default tenant at all.

**Wire contract:**

- **Cluster-mode = `Options.Tenant == ""`.** The hello frame declares an
  empty tenant (`HelloPayload.Tenant == ""`) and is HMAC-signed exactly as
  before — `tenant=""` is simply the signing input to the existing
  `(tenant,cluster,ts)` canonical string (`push/hmac.go`); no new fields, no
  new signing scheme. The pre-upgrade `X-Triage-Tenant` header is likewise
  sent empty. The core accepts this only in per-cluster-token mode with a
  non-empty tenant pin set configured for that cluster; an OLDER core
  rejects the empty hello at handshake — fail-closed, connection refused,
  never a misfile. **This is a hard core-first rollout constraint**, on top
  of (not instead of) the existing multi-tenant one (per-frame `Tenant`
  needs core v2.9.5+): do not roll a tenant-less agent out to a cluster
  whose core does not yet support per-cluster-token, multi-tenant-pin
  handshakes.
- **Every frame stamps its real tenant explicitly.** `wireTenantFor`'s
  "empty means primary" convention applies only when `Options.Tenant != ""`;
  in cluster-mode a frame's wire `Tenant` field is always its resolved
  tenant's own name, verbatim — the core drops any unstamped frame it
  receives in cluster-mode, so the agent must never send one. This applies
  to the (re)connect snapshot sequences too: there is no primary sequence to
  send first (`connWorker.runOnce` skips it when `Options.Tenant == ""`) —
  only one stamped sequence per tenant currently holding a
  dynamically-scoped namespace (`activeSecondaryTenants`, which returns
  every such tenant in this mode, not just non-primary ones).
- **An unmapped namespace has no tenant, full stop.** With no primary to
  fall back to, `tenantForNamespace` resolving to `""` now means "nothing to
  route this to" rather than "route it to primary" — every multi-item split
  path (`filterAppsForTenant`, `splitFrameByTenant`'s app_delta/k8s_event
  branches, `splitByTenant`'s generic log/red/use-metrics + topology path)
  drops such an item instead of inventing a destination for it. Drops are
  counted and rate-limit-logged (`Pusher.noTenantDropped`,
  `recordNoTenantDrop` — first occurrence + every 1000th), never silent and
  never flooding the log.

**Config:** `--target-tenant` (chart: `target.tenant`) is optional wherever
`--push-endpoint`/`push.enabled` is set — `--cluster-name`/`push.clusterName`
remains the only required identity flag. The chart's `required` guard on
`target.tenant` is removed; the `--target-tenant` arg is rendered only when
`target.tenant` is non-empty.

Implementation: `push/tenant_routing.go` (`resolveTenant`,
`recordNoTenantDrop`, `fallbackTenantFrame`, cluster-mode branches of
`wireTenantFor`/`splitFrameByTenant`/`splitByTenant`/`splitPayloadByTenant`/
`splitTopologyByTenant`), `push/connworker.go` (`runOnce`'s primary-sequence
guard), `push/pusher.go` (`Options.Tenant` doc, `Pusher.noTenantDropped`),
`main.go` (the `--target-tenant` flag help text and the removed required-check),
`charts/triage-agent/templates/deployment.yaml` (conditional `--target-tenant`
arg). Tests in `push/pusher_test.go` (`TestClusterMode_*`,
`TestSplitFrameByTenant_ClusterMode_*`). HMAC signing itself
(`push/hmac.go`) is UNCHANGED — cluster-mode is purely a routing/stamping
change layered on top of the existing signed-hello handshake.

## Core-triggered resync (addendum, 2026-09-08)

A new hub→agent frame, `{"v":1,"kind":"resync","tenant":"<tenant>"}`, lets
the core ask this agent to resend one tenant's full snapshot without forcing
a reconnect — sent to every live session already holding that tenant, when
its service graph is reloaded on the core side. `tenant` names which
tenant; empty means the session's handshake tenant (`Options.Tenant`),
which only resolves to something in single-tenant/primary mode — cluster-mode
has no primary to fall back to, so an unstamped resync there is dropped.

Handling lives in `push/connworker.go`'s `readLoop` (decodes every hub frame
now, not just `pong`) and `Pusher.handleResyncFrame` (`push/tenant_routing.go`):
it builds the named tenant's snapshot sequence the same way the (re)connect
path does (`snapshotFramesForTenant`) and routes it through that tenant's
per-tenant outbound queue (`tenant_worker.go`) so the write is serialized
with everything else `connWorker.writeLoop` puts on the wire — this handler
never touches the connection directly. A resync for tenant X on a
multi-tenant connection (primary + secondary tenants, or several tenants in
cluster-mode) resends only X's sequence; every other tenant's state is
untouched. A resync arriving while the previous one for the same tenant is
still queued or being drained is coalesced into a no-op
(`tenantMux.tryStartResync`/`clearResyncPending`) rather than queuing a
second full sequence. Any other/unrecognized hub→agent frame kind stays
ignored at `V(2)`, as before this addendum.

Tests: `push/resync_test.go`.

## Namespace annotation `triageagent.dev/applications`

A NAMESPACE object annotated `triageagent.dev/applications: "off"` (exact string
match; any other value, or its absence, is unaffected) drops every real
Application CR in that namespace from the push agent entirely:

- The CRs are not pushed on the wire — excluded from the snapshot listing
  and from delta updates. An add/update is dropped as if the CR did not
  exist; a delete still passes through, since that is what the transition
  below uses to clean already-pushed CRs off the wire.
- The CRs no longer suppress virtual-app synthesis for the workloads they
  would otherwise cover — those workloads are synthesized as virtual apps
  instead, exactly as if no CR existed for them.
- The namespace-defaults overlay (domain/description/tier/weight) still
  applies to the resulting virtual apps unchanged.

This is orthogonal to `triageagent.dev/virtual-app: "false"`: that annotation opts
workloads OUT of synthesis (namespace- or workload-scoped); `triageagent.dev/applications: "off"`
drops the REAL CRs instead, which is what lets those same workloads be
synthesized in the first place. The two annotations can be combined or used
independently, and neither implies the other.

**Transitions**, driven by the same namespace informer that backs the
inheritance and namespace-scope-by-annotation addenda above:

- Added to an already-observed namespace: the namespace's real Application
  CRs are deleted off the wire (the same delete-emission path a namespace
  leaving scope uses), and virtual-app synthesis recomputes so their
  workloads' vapps get pushed instead.
- Removed: the namespace's real Application CRs are re-listed and pushed as
  adds, and suppression applies again from there.

Works in both push modes — primary mode with `--target-namespaces` and
cluster mode with `triageagent.dev/tenant` namespace annotations. One INFO line is
logged per transition per namespace, never per CR.

Implementation: `push/namespace_defaults.go` (`annotationApplications`,
`namespaceDefaults.ApplicationsOff`, `namespaceAnnotations`); `push/pusher.go`
(`Pusher.applicationsOff`, the exclusion in `listApps`, the add/update guard
in `onApp`, the transition handling in `onNamespace`,
`repushAppsInNamespace`'s op parameter); `push/virtualapps.go`
(`virtualAppsState.applicationsOff`, the exclusion in `listRealApps`, which
also keeps shadow evaluation from crediting an excluded CR). Tests in
`push/namespace_defaults_test.go`, `push/dynamic_apps_test.go`, and
`push/applications_off_test.go`.

## Namespace annotation `triageagent.dev/virtual-app-identity`

A NAMESPACE object annotated `triageagent.dev/virtual-app-identity` selects which
chain `GroupNameFor` uses to name every workload's virtual app in that
namespace:

- `workload` or absent (the default): identity is the workload's own name
  (Deployment/StatefulSet/DaemonSet `metadata.name`) — labels play no part.
  `triageagent.dev/virtual-app-group` on a workload still wins outright either way: it
  is an explicit per-workload instruction, not a mode.
- `labels`: opts into the part-of/instance/name chain instead.
- Any other value (case-sensitive exact match) is treated as the default
  (`workload`), with one WARN per namespace naming the bad value.

Precedence, strictly: `triageagent.dev/virtual-app-group` (per-workload) >
namespace's `triageagent.dev/virtual-app-identity` mode > the label chain.

The default names every virtual app after its own workload — a Helm release
that stamps one `app.kubernetes.io/instance` label on dozens of workloads
does not fold them into a single virtual app; each gets its own. A namespace
that wants the label-derived aggregate back (grouping several workloads
sharing `part-of`/`instance`/`name` into one vapp) opts in with `labels`.
Either way, two workloads of different kinds sharing one name (a Deployment
and a StatefulSet) still fold into one group.

Every pipeline that names a workload's identity follows the same mode:
virtual-app synthesis (`GroupNameFor`), the identity index
(`rebuildIdentityIndex`/`IdentityFor`/`WorkloadForSelector` — what RED, USE,
and topology all read), and the USE-metrics collector's own identity
resolution (`usemetrics/identity.go`), which reads the mode through
`Pusher.IdentityModeFor`. There is exactly one implementation of the chain;
every caller passes the namespace's mode into it rather than re-deriving the
precedence.

**Transitions**, driven by the same namespace informer that backs the
inheritance, namespace-scope-by-annotation, and applications-off addenda
above: adding, changing, or removing the annotation on an already-observed
namespace recomputes virtual-app synthesis under the new mode. A workload
whose vapp name changes is a rename — the existing delta path expresses that
as a delete of the old name plus an add of the new one, exactly like any
other group appearing or disappearing (`virtualAppsState.emitDeltas`). The
identity index is rebuilt in the same recompute pass, so RED/USE/topology
never disagree with the vapps live at any given moment. One INFO line is
logged per transition per namespace, never per workload — its `from`/`to`
fields always print `workload` or `labels`, never an empty string, even
though the underlying zero value ("", not yet observed or no annotation) and
the explicit `workload` string behave identically.

Works in both push modes — primary mode with `--target-namespaces` and
cluster mode with `triageagent.dev/tenant` namespace annotations.

Implementation: `push/virtualapps.go` (`IdentityModeWorkload`/
`IdentityModeLabels`, `WorkloadIdentity.Mode`, `GroupNameFor`,
`workloadInfo.IdentityMode`); `push/namespace_defaults.go`
(`annotationVirtualAppIdentity`, `namespaceDefaults.IdentityMode`/
`IdentityModeInvalid`, `parseIdentityMode`, `identityModeDisplay`,
`applyNamespaceDefaultsToWorkload`); `push/pusher.go`
(`Pusher.IdentityModeFor`, the transition detection in `onNamespace`);
`usemetrics/identity.go` (`Collector.identityModeFor`, `workloadGroupName`)
via `usemetrics.Options.IdentityModeFor`. Tests in
`push/namespace_defaults_test.go`, `push/virtualapps_test.go`, and
`push/virtual_app_identity_test.go`.

## Workload resources (addendum, 2026-09-07)

Grouping/synthesis and the identity index read a configurable list of
namespaced GVRs discovered through one generic path, so a CRD-owned workload — Argo Rollouts, kagent
Agents, Sandboxes, any operator-managed kind — participates in virtual-app
synthesis, readiness, and identity resolution exactly like a Deployment.

**Flag**: `--workload-resources`, repeatable, value form
`group/version/resource` (e.g. `apps/v1/deployments`); never a comma list.
Default when omitted entirely: `apps/v1/deployments`, `apps/v1/statefulsets`,
`apps/v1/daemonsets`, `argoproj.io/v1alpha1/rollouts`. Giving the flag even
once REPLACES that default list — it does not add to it. The core API
group's `/v1/...` form is not accepted; every resource here is namespaced
under a named group. A malformed value is rejected at startup with a clear
error.

**Discovery gate**: each configured resource is checked against API
discovery exactly once, at startup (`ServerResourcesForGroupVersion`,
grouped so several resources under one GroupVersion cost one call). A
resource the server does not serve — the whole GroupVersion absent, or
present but missing that resource — is skipped with one INFO line at
startup, never per namespace: a cluster without Argo installed is otherwise
unaffected. The surviving resources carry their discovered Kind forward, so
no informer/lister callback ever re-derives it from the object (a List
response's items are not guaranteed to carry TypeMeta).

**Mechanism**: one unstructured informer per configured resource, on both
paths that used to run three typed ones — the manager-cache path
(`virtualAppsState.registerInformers`, via an `unstructured.Unstructured`
carrying just the resource's GVK, which the cache resolves through its
RESTMapper like any CRD) and the out-of-cache per-namespace runtime factory
(`dynamicInformerManager`, via
`dynamicinformer.NewFilteredDynamicSharedInformerFactory` per (namespace,
resource)). One adapter, `workloadFromUnstructured(obj, kind)`, replaces the
three typed adapters — `kind` is the resource's discovered Kind, passed by
the caller.

**Generic readiness** (replaces the old per-kind Deployment/StatefulSet/
DaemonSet functions — the generic rule cannot key off
generation/observedGeneration, since not every configured kind sets them
meaningfully):

- desired: `spec.replicas` when present; else `status.desiredNumberScheduled`
  (DaemonSet's own field name, kept as a field-presence fallback rather than
  a kind check, so any other kind using the same convention is covered
  too); else `1` for a kind with no scaling concept at all.
- ready: `status.readyReplicas`, else `status.numberReady`, else
  `status.availableReplicas`, else `1` when `status.conditions[]` carries a
  `Ready` or `Available` entry with `status: "True"`, else `0`.

**Scaled to zero**: a workload whose desired count resolves to `0` is
excluded from grouping/synthesis entirely (the same mechanism
`triageagent.dev/virtual-app: "false"` uses) rather than being read as ready: `readyReplicas == replicas == 0`
is not readiness. A group whose members are all scaled to zero disappears the
same way any group does when its last member leaves: the existing delta
path emits the delete.

Real Application CR suppression (`crComponentCoverage`) matches a CR's
`status.componentList` entries by `(Kind, Name)` with no kind allowlist, so
a CR that already accounts for a Rollout (or any other configured kind)
suppresses it exactly as it would a Deployment.

Implementation: `push/workload_resources.go` (`DefaultWorkloadResources`,
`ParseWorkloadResource`/`ParseWorkloadResourceList`,
`resolveWorkloadResources`); `push/virtualapps.go`
(`workloadFromUnstructured`, `workloadDesiredReady`, `onWorkload`,
`registerInformers`, `backfillNamespace`/`listCachedWorkloads`,
`crComponentCoverage`); `push/dynamic_informers.go`
(`dynamicWorkloadHandlers.onWorkload`, `dynamicInformerManager.ensure`/
`listWorkloads`); `push/pusher.go` (`Options.WorkloadResources`/
`Options.Discovery`); `cmd/triage-agent/main.go` (`--workload-resources`,
wired from the same discovery client that resolves the served Application
API version). Tests in `push/workload_resources_test.go`,
`push/virtualapps_test.go`, `push/dynamic_informers_test.go`,
`push/dynamic_apps_test.go`, and `push/identity_test.go`.

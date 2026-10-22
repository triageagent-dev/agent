# ADR-0004: Real-Time Reconcile via Dynamic Component Watches

- **Date:** 2026-06-25
- **Status:** Superseded. Applications come from virtual apps only; the agent has no Application custom resource.

## Context

The controller watched only the `Application` object (`For(&Application{})`). A change
to a matched component — e.g. a Deployment's `.status` updating during a scale-up, a pod
failing, or a workload recovering — did **not** enqueue a reconcile of the owning
Application. Those changes were only picked up by the cache **resync** (`--sync-period`,
default 120s).

Consequence: a genuine degrade or recovery of a component could sit unreported in the
Application's status for **up to 120 seconds**. The component informers were already
running (the `List` calls in `fetchComponentListResources` start them, keeping the cache
fresh in real time), but nothing connected those informer events to the Application's
reconcile queue.

The set of component kinds is **not known at startup**: each Application declares its own
in `spec.componentKinds`, resolved at runtime via the RESTMapper. So a static
`.Watches(&Deployment{}, ...)` list in `SetupWithManager` cannot express "watch whatever
kinds the Applications actually declare."

## Decision

Register component watches **dynamically at runtime**, keyed off each Application's
`spec.componentKinds`:

1. `SetupWithManager` uses `builder.Build(r)` (not `.Complete(r)`) so the controller keeps
   a handle to the `controller.Controller` and the manager `cache.Cache`.
2. `Reconcile` calls `ensureComponentWatches`, which for each component GVK (resolved via
   the RESTMapper) registers — **once per GVK**, deduped via a `sync.Map` —
   `c.Watch(source.Kind(cache, <unstructured with GVK>, handler.EnqueueRequestsFromMapFunc(r.applicationsForComponent)))`.
3. `applicationsForComponent` maps a changed component to reconcile requests for every
   Application **in the component's namespace** whose label selector matches the
   component's labels.

One watch per kind serves every Application using that kind: the watch is
namespace-agnostic and the map function re-evaluates selectors per event. Using
`*unstructured.Unstructured` with the GVK set means arbitrary CRD kinds (e.g. Argo
`Rollout`) are watched the same way as built-in kinds. The existing `*/*`
`get;list;watch` RBAC marker already authorizes watching arbitrary component kinds.

`ensureComponentWatches` no-ops when the controller/cache handles are nil (a reconciler
constructed without `SetupWithManager`, as some unit tests do) — status aggregation still
works, only the real-time trigger is skipped.

## Consequences

- A real component status change (degrade, recovery, scale-to-zero) is reflected in the
  Application status **within seconds** instead of up to 120s.
- **A healthy scale-up (e.g. 2→3) still produces no status change** — see ADR-0003: the
  readiness predicate already reports a serving workload as `Ready` throughout a scale-up,
  so the faster trigger has nothing new to write. The watch matters for cases where the
  component status *genuinely* changes, not for the scale-up no-op.
- More frequent reconciles (one per component status write rather than one per resync).
  Reconcile is cheap and idempotent, and `--stabilization-period` still debounces the
  Ready→NotReady→Ready edge, so rapid events do not flap the written status.
- Informers for watched kinds run for the process lifetime once started (no teardown).
- The `--sync-period` resync remains as a backstop for anything a watch misses.

## Tests

`controllers/dynamic_watch_test.go` starts a **real manager with `SyncPeriod` set to one
hour** (so the resync cannot be the trigger), creates an Application and a matching
Deployment, mutates **only** the Deployment's status, and asserts the Application's
component status flips within seconds. Because the resync is an hour away, a reconcile in
that window can only have come from the dynamic component watch — this proves the watch
fires, end to end, rather than asserting it in isolation.

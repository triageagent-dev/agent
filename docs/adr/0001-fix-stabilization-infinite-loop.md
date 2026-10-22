# ADR-0001: Fix Infinite Stabilization Loop on Ready Transition

- **Date:** 2026-04-20
- **Status:** Superseded. Applications come from virtual apps only; the agent has no Application custom resource.
- **Commit:** `6006345`

## Context

Applications in steady-state (all components healthy) were permanently stuck
reporting `componentsReady: 2/3` or similar with `Ready=False`, even though the
underlying Kubernetes resources were fully available.

### Root Cause

The `Reconcile` loop in `controllers/application_controller.go` included a
stabilization guard intended to prevent flapping — if the computed status
transitioned from `NotReady → Ready`, it would requeue after `StabilizationPeriod`
(default 30s) before writing the new status.

The bug: the status was **never written during the wait**. On the next reconcile
the stored status was still `Ready=False`, so `isTransitioningToReady` returned
`true` again, triggering another 30s requeue — indefinitely. The Application could
never reach `Ready` in steady state after the controller restarted.

This was confirmed by:
1. Scaling down the cluster controller and running the binary locally with
   `--stabilization-period=0`, which immediately resolved affected Applications.
2. Checking `lastTransitionTime` and `lastUpdateTime` on the Ready condition —
   both were frozen at the time of the controller's last restart, confirming the
   status write was never reaching `updateApplicationStatus`.

### Affected Cluster

- Cluster: `example-workload` (and others) in `ops` ns
- Controller version before fix: image tag `29d6f03`
- GKE server version: `v1.35.2-gke.1485000`

## Decision

Track stabilization using the `lastTransitionTime` of the stored `Ready=False`
condition rather than a blind requeue. If the application has already been
not-ready for longer than `StabilizationPeriod`, write immediately. Otherwise
requeue for only the **remaining** time — so the guard fires exactly once.

```go
if r.StabilizationPeriod > 0 && isTransitioningToReady(newApplicationStatus, &app.Status) {
    notReadySince := notReadySince(&app.Status)
    waited := time.Since(notReadySince)
    if waited < r.StabilizationPeriod {
        return ctrl.Result{RequeueAfter: r.StabilizationPeriod - waited}, nil
    }
}
```

The `notReadySince` helper reads `lastTransitionTime` from the current `Ready`
condition, falling back to `time.Now()` (fires immediately) if not found.

## Consequences

- Applications that have been `NotReady` longer than `StabilizationPeriod` will
  transition to `Ready` on the next reconcile without any extra delay.
- The stabilization guard still works correctly for genuine flapping: a brief
  recovery (< 30s) still waits before writing.
- No change to the public API or CRD schema.

## Deployment

```
crane copy ghcr.io/OWNER/REPO:6006345 \
  REGISTRY/application:6006345

kubectl set image deployment/kube-app-manager-controller \
  kube-app-manager=REGISTRY/application:6006345 \
  -n triage
```

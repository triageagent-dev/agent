# ADR-0003: Don't Flap Workload Readiness on HPA Scale-Up

- **Date:** 2026-06-24
- **Status:** Superseded. Applications come from virtual apps only; the agent has no Application custom resource.
- **Commit:** `25171c3`

## Context

An Application was being paged as **degraded** every time its HPA scaled a
Deployment up. Example status during an autoscale event:

```
Components:
  Kind: Deployment   Name: example-service   Status: InProgress
  Kind: Service      ...                                          Status: Ready
  Kind: Ingress      ...                                          Status: Ready
Components Ready: 2/3
```

### Root Cause

The per-kind status functions in `controllers/status.go` required the workload to
be at its **full desired replica count** before reporting `Ready` — e.g.
`deploymentStatus` required `ReadyReplicas == AvailableReplicas == spec.replicas`,
and `stsStatus` / `replicasetStatus` / `replicationControllerStatus` likewise.

When the HPA scales 2→3, `spec.replicas` jumps to 3 immediately, but the new pod
needs its readiness-probe window (observed: up to **90s**) before it counts as
ready. During that window `ReadyReplicas (2) != spec.replicas (3)`, so the function
returned `InProgress`. That flipped the Application's `Ready` condition to `False`
(`2/3`), and monitoring read the condition transition as a degraded incident — even
though the existing replicas were serving fine and the app was only *adding*
capacity.

This is a false positive: **scaling up is not degradation.** Kubernetes itself does
not consider the workload unavailable during a healthy scale-up (a Deployment's
`Available` condition stays `True`).

Note: this controller emits **no Kubernetes events** (there is no `Recorder` in the
code). The signal monitoring keys on is the Application's `Ready` condition
transitioning to `False`.

## Decision

Report scalable workloads as `Ready` when they are **serving** (their currently
running replicas are healthy), not when they have reached the full *desired* count.
Each kind uses the signal Kubernetes already publishes for "available":

| Kind | Ready when | Notes |
|------|-----------|-------|
| **Deployment** | `Available` condition `True` and no `ReplicaFailure` | `Available` respects `maxUnavailable`; stays True through a healthy scale-up |
| **StatefulSet** | `ReadyReplicas == CurrentReplicas` (and `CurrentReplicas > 0`) | STS has no `Available` condition; `CurrentReplicas` is what the controller has actually rolled, so this means "all running pods are ready" |
| **ReplicaSet** | `AvailableReplicas > 0` and no `ReplicaFailure` | |
| **ReplicationController** | `AvailableReplicas > 0` | no conditions in RC status |
| **DaemonSet** | `NumberUnavailable == 0` | the DS controller's own counter respects `maxUnavailable`, so a node join doesn't flap |
| **Rollout** (Argo) | `phase` ∈ {`Healthy`,`Inactive`,`Progressing`,`Paused`}; or `spec.replicas==0` | added by the 2026-06-25 amendment below; `Progressing`/`Paused` are serving states, not failures |

All preserve the **scaled-to-zero → Ready** case, and still report `InProgress` on
genuine failure (nothing available / `ReplicaFailure=True` / unavailable pods).

### Amendment (2026-06-25): the `ObservedGeneration` gate was a residual flap

The original fix (commit `25171c3`) still gated `Ready` on
`Status.ObservedGeneration == Generation` for every kind. That re-introduced the
**exact** flap it set out to remove, just through a different field:

An HPA scale-up changes `spec.replicas`, which bumps `metadata.generation`
**immediately**. The workload controller writes `status.observedGeneration` only a
moment later. In that window `generation` is ahead of `observedGeneration` while the
`Available` condition is still `True` — and the `ObservedGeneration == Generation`
clause evaluated to false, so the workload was reported `InProgress` and the
Application was paged as degraded (`AppDegraded` for `example-service`, observed in production
productionon a build that *did* contain `25171c3`).

The generation gate was meant to avoid trusting *stale* status, but for a scale-up
the previous `Available=True` was and remains correct — the gate was rejecting good
status, not stale status. **The `ObservedGeneration == Generation` clause is removed
from the Ready predicate of all five kinds.** Genuine failure is already signalled by
`Available=False` / `ReplicaFailure=True` / unavailable pods, which still gate
`InProgress` regardless of generation skew.

### Amendment (2026-06-25): the same flap exists for Argo Rollouts

The original ADR covered the five built-in workload kinds but **not** Argo Rollouts,
which is what production actually runs on (≈27 Rollouts vs a handful of Deployments).
`rolloutStatus` mapped `status.phase` directly:

```
Healthy / Inactive                          -> Ready
Degraded / Progressing / Paused / Error     -> InProgress
phase missing/empty                         -> InProgress
```

A Rollout scaling up (HPA or a replica bump) — or stepping through a canary /
blue-green rollout — sits in `phase: Progressing` (and `Paused` at a healthy pause)
while the new pod starts, even though its `Available` condition stays `True` ("Rollout
has minimum availability"). Mapping `Progressing`/`Paused` to `InProgress` is the
*exact* scale-up false positive this ADR exists to remove, just on a different kind.

**`Progressing` and `Paused` now map to `Ready`.** Only `Degraded`/`Error` (a real
failure) report `InProgress`; an empty phase still reports `InProgress`.

**Scaled-to-zero Rollouts are `Ready` regardless of phase.** A Rollout with
`spec.replicas=0` runs nothing, so a `Degraded`/`InvalidSpec` phase on it is noise
nobody acts on. Real example: `example-parked-rollout` sits at
`DESIRED 0` with `phase: Degraded` (`InvalidSpec` — missing `strategy.canary`). It must
not degrade its Application while parked; the error only becomes actionable if it is
scaled back up, at which point the phase reflects it on a non-zero replica count. This
mirrors the scaled-to-zero → `Ready` treatment of the other kinds — with the deliberate
difference that for a Rollout the zero check comes *first*, so a parked-but-broken
Rollout reads `Ready`, not `InProgress`.

Note: `Error` and `Unknown` were in the original switch but Argo Rollouts never emits
them as `phase` values; they are kept defensively (`Error` → `InProgress`).

### Amendment (2026-06-25, v1.2.1): Deployment trusted the `Available` *condition* alone

Even after the above amendments, production kept paging on Deployment scale-ups. The
remaining bug: `deploymentStatus` decided "is this serving?" purely from
`Conditions[Available]`, ignoring `status.availableReplicas`.

During an HPA scale-up the kube Deployment controller updates the **replica counters
first** (`availableReplicas` already reflects the existing serving pods) and writes the
`Available` **condition a beat later**. In that window:

```
status.availableReplicas = 4   (serving)
status.conditions[Available] = <absent>
```

`deploymentStatus` saw no `Available` condition → `available=false` → `InProgress`, and
the Application flapped to degraded on every scale-up — exactly the incident this ADR
set out to kill, on the kind production runs most.

**A Deployment is now treated as serving if `Available` is `True`, OR
`availableReplicas>0` and the condition is not explicitly `False`.** A genuine
`Available=False` (below minAvailable) still reports `InProgress`. The other workload
kinds already decide on replica counters, not the condition, so Deployment was the only
affected kind.

Why the earlier "all green" test runs missed it: every *unit* test built the Deployment
struct with an `Available` condition already set, so the condition-not-yet-published
window never occurred in tests. This amendment adds an **envtest** spec
(`controllers/scaleup_envtest_test.go`) that creates a real Deployment in a real
apiserver with the `Available` condition **absent** and asserts the Application status
the controller actually writes. It fails on the pre-fix code and passes on the fix.

### Behavior matrix

| Scenario | Before | After |
|----------|--------|-------|
| Scale-up 2→3, existing pods healthy | `InProgress` (false incident) | **Ready** |
| Scale-up while `observedGeneration` lags `generation` | `InProgress` (false incident) | **Ready** |
| Rollout scaling up / canary step (`phase: Progressing`) | `InProgress` (false incident) | **Ready** |
| Rollout paused while serving (`phase: Paused`) | `InProgress` (false incident) | **Ready** |
| Rollout scaled to zero with `Degraded`/`InvalidSpec` phase | `InProgress` | **Ready** |
| All replicas down / can't meet minAvailable | `InProgress` | `InProgress` |
| `ReplicaFailure=True` | `InProgress` | `InProgress` |
| Rollout `phase: Degraded`/`Error` at non-zero replicas | `InProgress` | `InProgress` |
| Scaled to zero | `Ready` | `Ready` |

## Alternatives Considered

- **Tune the controller flags** (`--sync-period`, `--stabilization-period`): these
  affect *when* status is re-read and flap-damping on the Ready→NotReady→Ready edge,
  but the readiness *predicate* was the bug — the workload genuinely reported
  `InProgress`. Tuning would only shorten or mask the false-degraded window, not
  remove it.
- **Move the 90s tolerance into a `startupProbe`** on the workload: a good
  complementary fix (it stops kubelet `Unhealthy` events during boot), but it lives
  in each app's pod spec, not in this controller, and does not change that a
  scale-up legitimately lags `ReadyReplicas`.

## Consequences

- An Application no longer transitions to `NotReady`/degraded while an HPA adds
  capacity to any scalable workload (Deployment, StatefulSet, ReplicaSet,
  ReplicationController, DaemonSet node-join).
- Slightly less strict: an Application reports `Ready` while a workload is below its
  full desired count, as long as it is serving and not failing. This matches
  Kubernetes' own "Available" semantics and is the intended trade-off.
- No change to the public API or CRD schema.

## Tests

`controllers/status_test.go` — unit specs for all five built-in kinds, each covering a
scale-up regression, a broken case, and scaled-to-zero. Per the 2026-06-25 amendment,
every kind also has a spec that reproduces the **generation-skew window**
(`generation` ahead of `observedGeneration` while `Available=True`) and asserts the
workload stays `Ready` — this is the case the original specs missed, because the test
helper hardcoded `ObservedGeneration = Generation` and so never exercised the lag.

The second 2026-06-25 amendment adds a `rolloutStatus` spec block (there were **no**
Rollout tests before — the reason that kind shipped untested): every phase branch,
`Progressing`/`Paused` → `Ready`, `Degraded`/`Error` → `InProgress`, the missing-phase
fallback, and the scaled-to-zero-ignores-`Degraded` case (`example-parked-rollout`).

The pre-existing `controllers` envtest suite was also repaired so it runs
(controller-name collision, manager-shutdown assertion, and a stale ownerReference spec
that tested removed owner-ref mutation).

## Deployment

```
crane copy ghcr.io/OWNER/REPO:25171c3 \
  REGISTRY/application:25171c3

kubectl set image deployment/kube-app-manager-controller \
  kube-app-manager=REGISTRY/application:25171c3 \
  -n triage
```

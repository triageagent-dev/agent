# ADR-0005: Outbound Push Mode (stream Applications + events to triage)

- **Status:** Implemented (auth superseded — see note below)
- **Date:** 2026-07-09
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0004 (dynamic component watches); triage-agent ADR-029
  (remote-agent WS event source — the receiving end)

> **Note (2026-07-14):** the plain bearer-token auth described below was
> replaced by a per-tenant HMAC handshake — see CHANGELOG "BREAKING (push
> mode)" entry. `--push-token-file` now carries the HMAC **signing key**, never sent on the wire, and a new
> `--tenant` / `push.tenant` flag is required whenever push mode is enabled.
> The wire protocol, frame kinds, and reconnect/backoff behavior below are
> otherwise unchanged. See `docs/quickstart.md` and the top-level README for
> current usage.

> **Note (2026-08-28):** `--push-namespaces` (a comma-separated string) was
> replaced by the repeatable `--target-namespaces` flag, and `--tenant` was
> renamed `--target-tenant` — same semantics, see the CHANGELOG. Chart-side,
> `push.namespaces`/`push.tenant` moved out of `push:` (which is now only
> about where telemetry goes) into a new `target:` block (`target.namespaces`,
> a list; `target.tenant`), which is about what the agent observes. The flag
> table below still names the pre-rename flags.

## Context

This controller (`github.com/triageagent-dev/agent`) runs *inside* a cluster and already
computes the full picture the triage agent wants: the list of Applications, their
aggregated `Ready`/`InProgress` status, and their component detail
(`Status.Objects`) — updated in near-real-time via dynamic component watches
(ADR-0004).

The triage agent normally learns this by running informers *into* each cluster over
the GKE API + Workload Identity. That **pull** model requires triage to reach the
cluster's API server. **Closed clusters** — behind NAT/firewall, in a customer
tenancy, on-prem — are unreachable that way and stay invisible to triage.

We already hold the exact state to report, and we can make an **outbound**
connection even when nothing inbound is allowed.

## Decision

Add an optional **push mode** to `kube-app-manager`. When
`--push-endpoint=wss://…` is set, the controller — in addition to its normal
reconcile loop — dials an outbound WebSocket to the triage agent and streams:

1. a **snapshot** of all Applications on connect,
2. **per-Application deltas** (add/update/delete) as they happen,
3. **Kubernetes Warning events** in the watched namespace(s),
4. heartbeats, with automatic reconnect + backoff.

The controller sends **raw Application objects and raw events** — it does *not*
classify degradations or build triage events. All interpretation happens on the
triage side (its `appwatcher` classifier), so this controller stays a thin,
faithful reporter and the two ends can't drift.

```
kube-app-manager (this repo, in closed cluster)
  ├─ ApplicationReconciler        (unchanged — computes status)
  └─ NEW push mode (--push-endpoint)
       ├─ watch Application CRs (reuse manager cache/informer)
       ├─ watch corev1 Events (Warning) in namespace(s)
       ├─ dial wss://triage/v1/cluster-agent/ws  (Bearer token)
       ├─ send hello → snapshot → snapshot_end
       ├─ stream app_delta / k8s_event
       └─ heartbeat; reconnect w/ backoff; re-snapshot on reconnect
```

### Wire protocol (v1)

Newline-delimited JSON, one frame per line, defined by triage ADR-029. This repo
implements the **agent→hub** frames plus reading `pong`:

| kind          | payload |
|---------------|---------|
| `hello`       | `{v:1, cluster, agentVersion, namespaces[], capabilities[]}` — first frame after upgrade |
| `snapshot`    | `{apps: [Application,...]}` — chunked if large |
| `snapshot_end`| end-of-inventory marker → hub reconciles removals |
| `app_delta`   | `{op: add\|update\|delete, app, old?}` |
| `k8s_event`   | `{event: corev1.Event}` — Warning only |
| `heartbeat`   | liveness ping; expect `pong` back |

`cluster` is set from a new `--cluster-name` flag (defaults to the value triage uses
as the source segment: `app/<cluster>/<ns>/<name>`).

### Auth & transport

- **Outbound WSS only** — no inbound listener added to this controller. Works
  through NAT/proxy (respects `HTTPS_PROXY`).
- **Bearer token** from `--push-token-file`, sent as `Authorization: Bearer` on the upgrade request. The
  token is the triage `triageagent-api-tokens` value.
- TLS verification on by default; `--push-insecure-skip-verify` only for local dev.

### Flags (new)

| flag | meaning |
|------|---------|
| `--push-endpoint` | `wss://host/v1/cluster-agent/ws`; empty = push mode off (default) |
| `--cluster-name` | cluster identifier stamped into triage source paths |
| `--push-token-file` | Bearer token file (agents outside Kubernetes only) |
| `--push-namespaces` | comma-list to watch; empty = all (respects `--namespace`) |
| `--push-heartbeat` | heartbeat interval (default 20s) |
| `--push-insecure-skip-verify` | dev only |

Push mode is **off by default** — a plain `kube-app-manager` behaves exactly as
today. Leader election (if enabled) gates the pusher so only one replica streams.

## Consequences

**Positive**
- One controller you may already run becomes the bridge for closed clusters — no
  bespoke forwarder, no inbound exposure, no Workload-Identity trust to establish.
- Raw-object protocol keeps this side dumb and stable; triage owns all semantics.
- Reuses the existing manager cache/informer for Applications — no extra API load
  beyond the events watch.

**Negative / risks**
- New runtime dependency on a WebSocket client library (`gorilla/websocket`,
  matching triage) and outbound network egress from the controller.
- Must implement robust reconnect/backoff + re-snapshot; a silently dead connection
  makes the cluster blind on the triage side (mitigated by heartbeats + triage-side
  staleness detection).
- Streaming full Application objects has a size cost for large fleets; snapshot is
  chunked and deltas are per-object.
- Push should run on **one** replica only — gate behind leader election to avoid
  duplicate streams.

**Neutral**
- Reconcile behavior, status computation, CRDs, and existing flags are unchanged.
  Push mode is purely additive and opt-in.

## Alternatives considered

1. **Do nothing; require inbound API reachability (tunnel/VPN).** Restores the
   existing triage pull model at the network layer. Heavier per-cluster infra;
   rejected as the default, may coexist.
2. **Separate forwarder binary.** A second pod watching Application CRs read-only.
   Rejected: this controller already holds the state; a second watcher duplicates
   its work and its deployment.
3. **HTTP POST batches instead of WS.** Simpler but no live stream / clean
   snapshot-on-connect / reconnect catch-up. The triage side keeps its HTTP
   `/events` path for point alerts; this feature is specifically the live inventory
   stream.

## Implementation

Tracked in `docs/todo/remote-agent-push.md`. Receiving end: triage-agent
`docs/todo/remote-agent-ws-source.md` (ADR-029).

# TODO: Outbound Push Mode (application controller / agent side)

**Status:** Implemented and shipped (auth reworked to per-tenant HMAC post-launch —
see the note in [ADR-0005](../adr/0005-outbound-push-mode.md) and the CHANGELOG).
The step-by-step plan below is kept for history; for current usage see the
top-level README and `docs/quickstart.md`.
**ADR:** [ADR-0005](../adr/0005-outbound-push-mode.md)
**Companion (hub side):** triage-agent → `docs/todo/remote-agent-ws-source.md` (ADR-029)

Add an opt-in push mode to `triage-agent`: dial an outbound WebSocket to the
triage agent and stream Application snapshot + deltas + k8s Warning events. Off by
default; existing behavior unchanged when `--push-endpoint` is empty.

## Design constraints
- **Additive & opt-in.** No push flags set → controller behaves exactly as today.
- **Raw objects on the wire.** Send Application/Event objects as-is; do NOT classify
  degradations here — triage's `appwatcher` owns all semantics (ADR-029).
- **Reuse the manager cache/informer** for Applications (already synced); add one
  informer for `corev1.Event` (Warning) in the watched namespace(s).
- **Outbound only.** No new inbound listener. Respect `HTTPS_PROXY`.
- **One streamer.** Gate the pusher behind leader election when enabled.

## Steps (each a separate commit)

### 1. Wire protocol package
- [ ] New `push/protocol` (or `internal/push`): frame structs + NDJSON encode/decode
      matching triage ADR-029 (`hello`/`snapshot`/`snapshot_end`/`app_delta`/
      `k8s_event`/`heartbeat`/`pong`). `v:1`.
- [ ] Add `github.com/gorilla/websocket` to `go.mod` (match triage's version line).
- [ ] Unit tests: frame round-trip.

### 2. Pusher runnable
- [ ] New `push.Pusher` implementing `manager.Runnable` +
      `manager.LeaderElectionRunnable` (`NeedLeaderElection()=true`).
- [ ] Dial loop: connect → send `hello` → send `snapshot` (chunked) → `snapshot_end`
      → stream deltas + events → heartbeat. On error: close, backoff (exp, capped),
      reconnect, **re-snapshot**.
- [ ] Bearer auth: read token from `--push-token-file` (preferred) or `--push-token`;
      set `Authorization: Bearer` on the upgrade request header.
- [ ] TLS: verify by default; `--push-insecure-skip-verify` for dev only.

### 3. Sources
- [ ] Applications: subscribe to the manager cache informer for
      `app.k8s.io/v1beta1 Application` (already watched). On add/update/delete →
      enqueue `app_delta`. On (re)connect → list current set → `snapshot`.
- [ ] Events: informer for `corev1.Event` filtered to `type=Warning` in the watched
      namespace(s) → `k8s_event` frames.
- [ ] Bound an outbound send queue; drop-oldest with a warn log if the socket is slow
      (never block the informer handlers).

### 4. Flags + main.go wiring
- [ ] Add flags per ADR-0005: `--push-endpoint`, `--cluster-name`, `--push-token`,
      `--push-token-file`, `--push-namespaces`, `--push-heartbeat`,
      `--push-insecure-skip-verify`.
- [ ] In `main.go`: if `--push-endpoint != ""`, construct `push.Pusher` and
      `mgr.Add(pusher)`. No-op otherwise.
- [ ] Validate: `--push-endpoint` set requires `--cluster-name` and a token source.

### 5. Deploy artifacts
- [ ] Example overlay in `deploy/` (or `config/`) showing push mode: endpoint,
      cluster name, token Secret mount, RBAC for reading `events` (add to the
      controller Role/ClusterRole).
- [ ] README section: "Reporting to triage (push mode)".

### 6. Tests
- [ ] Envtest: create/update/delete an Application → assert the pusher emits the
      right `app_delta` frames (use a fake WS server / in-memory conn).
- [ ] Reconnect: drop the server mid-stream → pusher backs off, reconnects,
      re-sends snapshot.
- [ ] Auth header present; TLS-verify default on.
- [ ] `make all` (fmt, vet, lint, test) green before every commit.

## Out of scope (v1)
- Receiving commands from triage (hub→agent) beyond `pong`.
- Streaming metrics/logs — Applications + Warning events only in v1.
- mTLS / cert pinning (Bearer over WSS is v1).

## Open questions
- [ ] Token rotation: file-watch `--push-token-file` for hot reload, or reconnect on
      401? (Lean: file-watch + reconnect on auth failure.)
- [ ] Snapshot chunk size / max frame — pick a default (e.g. 50 apps/frame) and make
      it a constant; revisit if large fleets hit WS message limits.
- [ ] Should `--cluster-name` default from a downward-API env (cluster metadata) when
      unset, or be strictly required? (ADR: required when push on.)

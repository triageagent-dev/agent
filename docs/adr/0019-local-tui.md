# ADR-0019: Local TUI

- **Status:** Accepted. Backend and tests implemented 2026-10-10; the web
  frontend is being built against the API below.
- **Date:** 2026-10-10
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (push mode), ADR-0011 (topology), ADR-0015 (sensor
  stream), ADR-0017 (OTLP receiver), ADR-0018 (host mode)

## Context

The core has a TUI. The agent has no UI and no inbound API: it only pushes.
An operator on a cluster or host cannot see what the agent sees (which apps it
derived, what traffic and calls it observed, which sensors and OTLP senders
feed it, which events it raised) without reading the core.

## Decision

The agent can serve the same browser TUI, with two panels:

1. **app**: virtual apps with traffic, the dependency graph and a treemap.
2. **src**: sources on the left (Kubernetes watch or host units, eBPF,
   node-exporter and log-exporter sensors per node, OTLP resources) and the
   events they raise on the right, filtered by the selected source.

`--ui-listen` (chart `ui.listen`) turns it on; it is off by default and works
in cluster and host mode. The static files are embedded in the binary.

**Data.** `push.Observer` receives a read-only copy of every frame the Pusher
queues, once per resolved tenant, after exclusion; `SendSensorState` reports
to it too. `agentui.Store` keeps the latest RED/USE summaries (with a 60-point
history), gated log windows, topology edges (15 min), the last sensor snapshot
and the newest events of each tenant (`--ui-max-events`, default 100, 10 to 5000, chart `ui.maxEvents`). Apps come from `Pusher.Apps()`, connection state from
`Pusher.Link()`. The TUI therefore shows what the core is sent, no more.

**Replicas.** Only the leader's Pusher and collectors run, so only its Store
fills. With the replica gossip on, the leader sends its Store snapshot (services
and their history, edges, sensors, the newest 300 events), its apps and its
connection state to every peer every 5 s as a `ui` mesh frame; the other
replicas apply it (`agentui.Replica`) and serve it, so any replica shows the
same view, at most a few seconds behind. Event ids continue from the leader's,
so a replica that wins the lease goes on numbering. `/api/v1/status` reports
`role` (`leader`, `follower`, `single`).

**Tenants.** Every frame the Pusher queues is already split and stamped per
tenant, so the Store records the tenant of each service, edge, workload and
event, and `Pusher.AppTenants()` gives each app namespace's tenant. Every
endpoint takes `?tenant=<name>` (absent means all); `t` in the TUI cycles it.
An edge belongs to its caller's tenant; a callee in another tenant is returned
as `external: "other tenant"` without its name. The node-wide sensors (eBPF,
node-exporter, log-exporter) stay visible in every tenant's view and are
marked `shared`; OTLP sources are cut to the tenant, plus those with none.
The tenant picker is a view filter; the login (see below) is one admin key, not per-tenant access.

**Info.** `?` shows the controller's flags in effect, then the sensors': each sensor sends its own flags (secrets left out) in its stream Hello (`Hello.settings`, ADR-0015), every replica's hub holds them through the gossip, and `/api/v1/config` groups the sensors that run the same roles, version and settings, so an operator sees what the log-exporter and eBPF roles are really configured to do. The `?` view shows the agent's flags in effect (changed ones first; any flag
named like a token, password or credential is left out, and the core endpoint
is shown without credentials) and its statistics (process, replica role and
peers, the core link and frames sent per kind, what the view holds, sensors).

**API** (GET only): `/api/v1/status`, `tenants`, `apps`, `services`, `graph`,
`sources`, `events?family=&limit=`, `config`, `stats`, and `/api/v1/stream`
(server-sent events).

**Access.** The listen address must be loopback and requests must carry a
loopback `Host` (closes DNS rebinding); anything but GET/HEAD is refused (the
two exceptions are the login and logout POSTs); the CSP is
`default-src 'self'`. Operators reach it through `kubectl port-forward`. A
non-loopback address is a startup error, not a warning. Each replica serves
its own view; there is no leader gating.

**Login.** With an admin key configured the TUI asks for it and every
`/api/` call but the auth endpoints needs a session; the static files stay
public so the login page loads. The key is at least 16 bytes. In a cluster it
is read from a file the chart mounts from a Secret
(`ui.adminKey.existingSecret`, `--ui-admin-key-file`), never from an
argument or an environment variable, and the file is read on each use so a
rotated Secret applies without a restart. In host mode, where there is no
Secret, it may also come from `--ui-admin-key` or the environment variable
`TRIAGE_AGENT_UI_ADMIN_KEY` (the file wins; the packaged unit's
EnvironmentFile is the place for it; the flag is refused in a cluster and the
`?` view never lists it). The key is compared by constant-time hash
comparison. A session is a cookie (`HttpOnly`, `SameSite=Strict`, 12 h)
signed with a key derived from the admin key, so every replica accepts it and
rotating the key ends all sessions. Login and logout reject a foreign
`Origin`; after three failed logins the server refuses logins for 1 s,
doubling per failure up to a minute, even for the right key. Without a key
there is no login and the loopback bind is the only guard.

## Consequences

- A second view of the data that is always in step with what is sent.
- Events live in memory only: a restart empties them (a replica that was
  running keeps the leader's newest events, at most 300 per snapshot, and hands them on).
- A follower is up to one publish interval (5 s) behind the leader; if the
  gossip is down it serves its last snapshot.
- Event attribution to a source is by family (red, use, log, k8s, app,
  topology), not by sensor, because frames carry no sensor identity.

# ADR-0020: Monitoring API

- **Status:** Accepted. Implemented 2026-10-10; not yet enabled on a live
  cluster.
- **Date:** 2026-10-10
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0019 (local TUI), ADR-0015 (sensor stream, replica gossip),
  ADR-0018 (host mode)

## Context

The agent's own Prometheus endpoint (`--metrics-addr`) carries Go and
controller-runtime metrics and one agent gauge. What the agent knows about the
cluster (RED and USE signals per service, app readiness, who calls whom, which
sources report) reaches the core, and the local TUI, but no monitoring tool can
read it. The core holds the same data fleet-wide; an agent endpoint serves the
per-cluster and air-gapped cases and a Grafana that sits next to the cluster.

## Decision

An optional listener, `--api-listen` (chart `api.listen`), **off by default and
never unauthenticated**: it refuses to start without `--api-tokens-file`.

Endpoints, all GET:

- `/metrics`: Prometheus exposition of the derived signals. Per service:
  `triage_agent_service_{requests_per_second,error_ratio,latency_p99_seconds,
  cpu_pressure_ratio,memory_pressure_ratio,io_pressure_ratio,oom_kills_window,
  log_error_lines_window,log_warn_lines_window}{tenant,namespace,service}`;
  `triage_agent_app_ready{tenant,namespace,app}`;
  `triage_agent_edge_{calls,bytes}_per_second{tenant,src_*,dst_*,external,protocol}`;
  `triage_agent_source_up{kind,name,node,tenant}`;
  `triage_agent_events_total{tenant,family}`; and for an unrestricted token the
  link (`triage_agent_core_link_connected`, `triage_agent_frames_sent_total`,
  `..._dropped_total`) and `triage_agent_sensor_nodes`. Labels stay at tenant,
  namespace and service: no per-operation series.
- `/.well-known/triage-agent`: a JSON discovery document with the version,
  cluster, mode, replica role, the caller's scope and tenants, and the paths of
  the other endpoints.
- `/api/v1/{status,tenants,apps,services,graph,sources,events,stream}`: the
  TUI's JSON API, now a documented contract, for Grafana's Infinity datasource
  and other tools. The TUI's `config` and `stats` and the login stay on the
  TUI listener only.

**Tokens.** A file of `name tenant token` lines (`#` comments), mounted from a
Secret and reloaded when it changes (a broken reload keeps the tokens already
loaded). A token is at least 24 bytes and is compared by hash against every
entry in constant time. Tenant `*` is unrestricted. Any other tenant limits
the token to it, **enforced by the agent**: the `tenant` query is forced to
the token's tenant, a request naming another one is 403, the tenant list
shows only that tenant, and a restricted token sees neither the node-wide
sensors nor the core address and cluster-wide frame counts. This is real
per-tenant isolation, unlike the TUI's picker (which is behind one admin key).

**Leader only.** Only the leader's pusher and collectors run, so only the
leader exports signal series; a follower answers `/metrics` with
`triage_agent_info` (`role="follower"`) alone. Prometheus can therefore scrape
every replica, or the `<fullname>-api` Service, and sees each series once. A
leader change leaves a gap of a few seconds, and the `events_total` counters
restart from zero on the new leader (Prometheus treats that as a counter reset).
`/api/v1` is served by every replica from the gossiped view, as the TUI is.

**Transport.** The listener binds any address (Prometheus reaches it over the
network). `--api-tls-cert-file`/`--api-tls-key-file` serve TLS (chart
`api.tls.existingSecret`, a `kubernetes.io/tls` Secret); without them it serves
plain HTTP and logs that tokens cross the network in the clear, so use a
NetworkPolicy or TLS. The chart adds a Service `<fullname>-api` and, with
`api.serviceMonitor.enabled`, a ServiceMonitor that reads the scrape token from
a Secret holding that token alone. Host mode takes the same flags; the tokens
come from a file.

## Consequences

- Grafana (Prometheus datasource) and Alertmanager work against the agent with
  no plugin; per-tenant tokens give each team its own scope.
- Series are the last window's values (the window is the collector's), not raw
  samples, so rates move in steps of the window.
- Health endpoints are not served here: the listener has no unauthenticated path.
- Not done: a shipped Grafana dashboard, OTLP push out, token revocation
  lists (rotate by editing the Secret).

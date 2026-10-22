# Cilium/Hubble as an unprivileged RED + topology source

Status: ACCEPTED FOR IMPLEMENTATION 2026-08-21 (operator direction,
follow-up to the 2026-08-21 audit's finding 1). Implementation is sequenced
after the beyla-hardening branch lands (both touch values.yaml and the
Beyla templates).

## v1 scope decision

Target the OSS Hubble metric names and context labels
(`hubble_http_requests_total`, `hubble_http_request_duration_seconds`,
`hubble_flows_processed_total` with source/destination workload labels) —
that covers self-managed Cilium and AKS ACNS, which expose Hubble names.
GKE-managed Dataplane V2 renames its metrics and exposes them through
managed Prometheus; mapping those names is a follow-up, not v1. Validation:
fixture-exposition unit tests in the repo's existing fake-scrape style;
live validation needs a Cilium-CNI cluster.

## Idea

On clusters whose CNI is Cilium (GKE Dataplane V2, AKS Azure CNI powered by
Cilium / ACNS, self-managed Cilium), the privileged eBPF layer already runs
as the platform's own agent. Hubble exposes flow and (where enabled) HTTP
metrics from it. The agent can scrape those instead of running its own
Beyla DaemonSet, which removes every elevated privilege from OUR chart on
such clusters: no hostPID, no capabilities, no second DS. The wire contract
does not change: the collector resolves per-service counts and sends the
same `red_metrics` / `topology_snapshot` frames.

## What each platform actually provides (verified 2026-08-21)

- GKE Dataplane V2 (managed): L3/L4 flow-based metrics, network topology,
  policy verdicts. NO HTTP L7 request/latency metrics today. So on managed
  DPv2: full topology source, partial red source (connection-level errors,
  not HTTP status/latency). Beyla (scoped caps, see the beyla-hardening
  branch) remains the only full RED source there.
- Self-managed Cilium + Hubble with L7 visibility: `httpV2` Hubble metrics
  give request count, status, and duration per workload — a full
  unprivileged RED source for HTTP/gRPC.
- AKS ACNS (Azure CNI powered by Cilium): Hubble metrics available; verify
  L7 coverage per cluster tier before relying on it.
- Everywhere: SQL/Redis protocol parsing and per-route p99 are Beyla-only.

## Product framing

This is a deployment-mode capability of the product, not an edge case: the
chart offers two modes and the target cluster's CNI selects between them.

- **Beyla mode** — works on any cluster; scoped capabilities by default
  (beyla-hardening branch), privileged only as an explicit escape hatch.
- **Hubble mode** — on Cilium-CNI clusters (GKE Dataplane V2, AKS ACNS,
  self-managed Cilium): zero elevated privilege in the agent chart, the
  platform's own eBPF layer does the work.

For a security-sensitive customer the pitch is direct: the agent ships no
privileged workload unless you choose Beyla mode, and on a Cilium cluster
it ships none at all. Onboarding asks one question: what CNI does the
cluster run.

Note: clusters on a legacy dataplane cannot migrate in place (GKE enables
Dataplane V2 at cluster creation only), so hubble mode can only be
exercised on a cluster that already runs a Cilium CNI.

## Proposed shape (to be designed)

- values: `redMetrics.source: beyla | hubble` and
  `topology.source: beyla | hubble` (default `beyla`, today's behavior).
  `hubble` mode deploys NO Beyla DS; a collector in the controller scrapes
  the Hubble metrics endpoint (or the platform's managed metrics path) and
  maps workload labels to services the same way the other collectors do.
- Same gates: threshold send condition, first-seen baseline, frame caps.
- Capability detection stays manual (a values choice), never automatic —
  a collector must not probe for privileged alternatives on its own.

## Beyond Hubble: profiles and OTLP (boundary decision, 2026-08-21)

Hubble support adds no library dependency: the collector speaks
Prometheus/OpenMetrics text (already parsed everywhere in this repo) and the
vendor-specific part is only a naming table — metric names, label keys, and
an error predicate. Every comparable source repeats that pattern (Istio and
Linkerd meshes, Azure Retina, which is Hubble-metric-compatible, Pixie).

Two rules to keep this from becoming per-vendor adapter sprawl (operator
decision 2026-08-21, no exceptions):

1. ALL scrape-based RED/topology sources run through ONE profile-driven
   collector engine, where a profile is the naming table above — Beyla and
   Hubble both ship as built-in profiles from v1; there is no vendor
   package and no legacy bespoke collector kept alongside. A mesh source
   later is a profile, not a package. A profile can eventually be
   configuration, at which point the vendor knowledge leaves the code
   entirely.
2. Everything that is OTel-native goes through the planned OTLP receiver
   (triage ADR-022) instead of a scrape profile. OpenTelemetry semantic
   conventions are the standard for RED naming, and Beyla itself is an
   OpenTelemetry project.

## Open questions

- Hubble metrics endpoint access on each platform (direct scrape vs
  managed-Prometheus read) and its auth story.
- Label→service mapping parity with Beyla's discovery.
- Whether topology `hubble` mode can also carry the External edge notion
  ADR-0011 defines.

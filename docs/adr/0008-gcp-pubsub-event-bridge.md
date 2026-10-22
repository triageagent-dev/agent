# ADR-0008: GCP Event Bridge (cost anomalies + service health → triage)

- **Status:** Implemented
- **Date:** 2026-07-25
- **Deciders:** Denis (Principal SRE)
- **Related:** ADR-0005 (outbound push mode — the other direction of the same idea);
  triage-agent ADR-030 (multi-tenant service graph), ADR-031 (prebuilt graph artifact)

## Context

Two Google Cloud signals sit outside every ingestion path the estate has.

**Cost anomalies.** Cloud Billing anomaly detection already publishes to
`projects/example-project-0000/topics/cost-anomalies-alerts`. Nothing subscribes.
A BigQuery slot blowout or a runaway Dataflow job is visible in the console and
nowhere else.

**Service health.** Personalized Service Health reports Google-side incidents
against the specific products and locations this organization uses. Nothing
consumes it either.

Both are provider-level signals with real blast radius across the estate, and
both are invisible to triage today. They are also *correlated*: a Google-side
BigQuery incident and a BigQuery cost anomaly on the same afternoon are one story,
not two — but only if they resolve to the same thing.

The existing ingestion paths do not fit. Push mode (ADR-0005) is a long-lived
in-cluster agent streaming Application state over a WebSocket; these signals are
sporadic, with no cluster behind them. The triage events server
(`:8086`, `POST /events/<provider>/`, Bearer auth, `X-Tenant` header) already
accepts exactly this shape of thing.

## Decision

A **Cloud Run function** takes both signals — Pub/Sub for cost, a scheduled
Service Health API poll for status — resolves each to a GCP service, and POSTs it
to the triage events server as an event in a new `gcp` tenant.

```
cost-anomalies-alerts (Pub/Sub) ──► gcpevents ──► POST /events/gcp-cost/
Cloud Scheduler, every 15m ───────► gcpevents ──► POST /events/gcp-status/
                                    └─ lists ACTIVE events from        X-Tenant: gcp
                                       the Service Health API
```

Five choices worth recording:

### 1. The tenant's mental map is the GCP service catalog, not the project list

The `gcp` tenant's service graph has one node per billed GCP service (96 of them,
plus two catch-alls) with curated dependency edges — GKE depends on Compute Engine,
Networking, Artifact Registry; Composer depends on GKE, Cloud Storage, Cloud SQL.
That is what gives an anomaly a blast radius.

Projects are **not** nodes. They would swamp the index without adding a
single useful edge, and a project is not a thing that fails — it is where a failure
was billed. Projects ride on the event as message text and `raw.impactedProjects`.

### 2. The function resolves the service; nothing downstream re-guesses it

The event's source is `events/gcp-cost/<slug>`, where `<slug>` is the canonical
graph node name. Triage's classifier treats `events/` as a machine source and
resolves it deterministically — strip the provider segment, look up the graph — with
no embedding step and no second opinion.

So the id→slug mapping is the load-bearing part, and it is generated from the same
curated table as the service graph itself (`triage-agent hack/gen-gcp-catalog.go`
writes both `data/gcp-services.yaml` and this package's `catalog_gen.go`). The two
sides cannot drift into disagreeing about what `services/24E6-581D-38E5` means.

### 3. Decode by pattern, not by position

Neither payload is stable enough to bind to a struct. Google's own notification-format
documentation spells the cost anomaly root-cause tree in both camelCase and
snake_case, and gives spend amounts as a `google.type.Money` object in one row of
the table and a bare number in the next. The service health example renders
repeated fields as Python list literals (`"['us-east1', 'australia-southeast2']"`),
an artifact of the logging path they travel.

So the decoders work over `map[string]any` and pull out what they need by pattern:
a billing service id looks like `services/XXXX-XXXX-XXXX` wherever it appears in the
document, and an EventLog is the object carrying an `@type` ending in `EventLog`.
A field rename upstream costs nothing; only a change to those two shapes would.

Where a value genuinely matters and is genuinely ambiguous — `deviationPercentage`,
whose unit is not documented — we compute it ourselves from the amounts and keep
the reported value in `raw` for comparison against real traffic.

### 4. Service health is polled, not subscribed to

Cost anomalies arrive by Pub/Sub because Cloud Billing already publishes them
there. Service health does not: the obvious routes are a log sink on
`servicehealth.googleapis.com/activity`, or a Cloud Monitoring log-based alert
firing a webhook notification channel — and the org already has a Triage Gateway
webhook channel, so that looked like the cheap answer.

It was rejected because **an edge stream cannot describe the present.** At the
time this was built, three Google incidents were already open, having started on
June 30, July 6 and July 25. A webhook configured that day would have reported
none of them — possibly never, if they simply closed quietly. The same gap
reopens after any missed delivery: nothing in an edge stream lets you recover the
current state, because it only ever describes transitions.

The point of this feed is to answer "which GCP services are degraded right now",
which is a question about the present. The Service Health API answers it directly:
one call, `filter=state="ACTIVE"`, the complete deduplicated set, and a missed
poll costs nothing because the next one carries the whole truth again. It is the
same reason the triage side reconciles incident state against the service graph
instead of trusting recovery edges (ADR-032).

Cloud Scheduler drives it every 15 minutes — status does not move faster than
that, and at ~2,900 invocations/month it is 0.15% of the Cloud Run free tier. The
cost argument is a red herring in both directions; the correctness argument is
what decides it.

Two caveats worth recording, because the webhook is a reasonable thing to
reach for again later:

- It is more workable than "no structured payload" would suggest. Log-based alert
  policies support label extractors, so `impactedProducts` can be pulled into
  `incident.metric.labels`; it arrives as `"['Cloud Logging']"`, the same
  Python-rendered form `parseBracketedList` already handles.
- What it genuinely buys is latency — immediate instead of up to 15 minutes. If
  that ever matters, the answer is both: webhook for the edge, a slow poll as the
  reconciler that covers already-open incidents and missed deliveries. That is
  more machinery, not less, and only worth it if the 15-minute floor becomes a
  real constraint.

### 5. Nothing is dropped for being unrecognized

A cost anomaly with no service-level root cause resolves to `gcp-billing`; a health
event for a product outside the billed catalog resolves to `gcp-platform`. An
unattributed spike is still a spike.

## Consequences

- Two deployed functions from one module: `gcp-events-triage` (Pub/Sub trigger,
  cost) and `gcp-servicehealth-poll` (HTTP, driven by Cloud Scheduler). The
  Pub/Sub path dispatches on topic name with a payload-shape fallback, so a
  renamed topic degrades rather than goes silent.
- Ack semantics are asymmetric on purpose: anything permanent (unparseable payload,
  4xx from triage, an event that will not JSON-encode) is acked, because redelivery
  reproduces it exactly. Only transient delivery failure nacks — and only matters if
  the trigger has retry on. Classifying an encoding failure as transient is how the
  first `"Infinity"` payload turned one bad field into an unbounded redelivery loop
  of 500s; anything the decoder can produce but the encoder cannot carry has to land
  on the permanent side of that line.
- The generated catalog carries service data only; project ids and names are not
  vendored, consistent with §1 above. `projectLabel` renders the display name the
  notification carried, otherwise the bare id.
- The function is its own Go module so `--source ./gcpevents` builds only itself,
  not the controller module it lives beside.
- Triage-side prerequisites, all in the triage-agent repo: the `gcp` tenant in
  `--tenant-clusters`, its graph gob applied as a `mental_map` cell, `events/`
  narrowed out of `quarantineSources` so these events reach the classifier at all,
  and the events HTTPRoute enabled.
- Personalized Service Health needs the API enabled and a Logging sink per project,
  or one aggregated org-level sink. The org-level sink needs org admin; without it
  coverage is whatever projects have their own sink.

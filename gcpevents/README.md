# gcpevents

Bridges Google Cloud provider signals into the triage agent: Cloud Billing **cost
anomalies**, Personalized Service Health **service health events**, and Cloud
Monitoring **alert policy incidents**. Each is resolved to a service in the GCP
service catalog and POSTed to the triage events server as an event in the `gcp`
tenant.

```
cost-anomalies-alerts ──────► <function-name> ──────► POST $TRIAGE_EVENTS_URL/events/gcp-cost/
   (Pub/Sub, Cloud Billing)                              Authorization: Bearer <token from Secret Manager>
                                                         X-Tenant: gcp
monitoring-alerts ───────────► <function-name> ──────► POST .../events/gcp-alert/
   (Pub/Sub, Cloud Monitoring)                            (same function, same headers, second trigger)
Cloud Scheduler, */15 ──────► gcp-servicehealth-poll ──► POST .../events/gcp-status/
                                └─ lists ACTIVE events from the Service Health API
```

Two entry points, three signals, one module. Cost and monitoring alerts are
event-driven because both Cloud Billing and Cloud Monitoring already publish to
Pub/Sub. Status is **polled**, not subscribed to: an edge stream cannot tell you
what is already broken, and answering "which services are degraded right now" is
the entire point of that feed. See ADR-0008 §4.

## What it emits

| Source signal | Event type | Source path | Severity |
|---|---|---|---|
| Cost anomaly | `CostAnomaly` | `events/gcp-cost/<service>` | deviation-based (see below) |
| Service health, `state: ACTIVE` | `ProviderIncident` | `events/gcp-status/<service>` | `IMPACTED` → critical, `RELATED` → warning, else info |
| Service health, `state: CLOSED` | `ProviderIncidentResolved` | `events/gcp-status/<service>` | info |
| Cloud Monitoring alert incident | `MonitoringAlert` / `MonitoringAlertResolved` (closed) | `events/gcp-alert/<service>` | `policy_user_labels.severity` when it is info/warning/critical, else warning (open) or info (closed) |
| Cloud Monitoring **per-caller** alert (`consumed_api`) | `MonitoringAlert` / `MonitoringAlertResolved` (closed) | `events/gcp-alert/<vapp>` in the caller's own tenant, else `events/gcp-alert/<service>` | as above |

`<service>` is the canonical node name in the `gcp` tenant's service graph —
`bigquery`, `kubernetes-engine`, `cloud-storage`. The triage classifier resolves
it deterministically (`events/gcp-cost/bigquery` → drop the provider segment →
`bigquery` → graph lookup), so getting the slug right here is the whole job.
Nothing downstream re-guesses it.

A cost anomaly with no service-level root cause resolves to `gcp-billing`; a
health event or a monitoring alert for a product outside the catalog resolves to
`gcp-platform`. Neither is ever dropped.

A monitoring alert resolves its service from the API prefix of the alert's
metric type or monitored resource type (`aiplatform.googleapis.com/...` →
`aiplatform` → `vertex-ai`), translated through a small alias table where the
Monitoring API's own identifier differs from every spelling already in the
catalog.

### Caller attribution

An alert policy grouped by caller — metric
`serviceruntime.googleapis.com/api/request_count`, monitored resource type
`consumed_api`, grouped by the resource label `credential_id` — is not really
about the API it fired on. Every tenant calls Vertex AI; what the alert says is
*who* called it.

The label names that caller by id, not by anything a service graph would
recognize (`serviceaccount:100000000000000000001`, or
`oauth2:32555940559.apps.googleusercontent.com`). For a service account the id
is resolved through the IAM API —
`GET https://iam.googleapis.com/v1/projects/-/serviceAccounts/<uniqueId>`, with
the function's own credentials, 5s, memoized for the life of the instance — and
**the account's own description says where its events belong**:

```
gcloud iam service-accounts update <caller-sa>@$PROJECT.iam.gserviceaccount.com \
  --description="Per-tenant Vertex AI caller tenant=triage vapp=triageagent"
```

Any whitespace- or comma-separated `key=value` tokens in the description are
read (`displayName` is consulted for keys the description did not set); the rest
is prose and is ignored. Two keys matter:

| Token | Effect |
|---|---|
| `tenant=<name>` | `X-Tenant` for this event, overriding `TRIAGE_TENANT` |
| `vapp=<name>` (or `source=<name>`) | the event's source — the node in that tenant's graph |

`tenant=` without `vapp=` sources the event at the service account's own name
(the email local part, `<caller-sa>`).

Routing from the account rather than from a graph alias keeps the decision with
whoever provisioned the service account: a new tenant needs a description, not a
change here or an edit to a service graph.

**Everything else falls back to the ordinary alert shape** — source = the API
slug (`vertex-ai`), tenant = `TRIAGE_TENANT` — with the caller preserved in the
message and in `raw`. That covers an account whose description names no tenant,
an `oauth2:` client id (a person or a tool, which nothing routes), and an IAM
lookup that fails. None of them drops the alert.

The API slug for a per-caller alert is read from the monitored resource's
`service` label, since neither the metric's API (`serviceruntime`) nor the
resource type (`consumed_api`) is in the catalog.

Either way the caller lands in the message
(`… caller=<caller-sa> observed=412 …`) and in `raw.credentialId`,
`raw.callerKind`, `raw.callerName`, `raw.serviceAccountEmail`, plus
`raw.callerTenant` / `raw.callerVapp` when the description set them.
`raw.service` always names the API that was called.

The function's runtime service account needs `roles/iam.serviceAccountViewer` on
the project for the lookup — without it every caller falls back to the API.

A service health event affecting several products produces one event per product,
so each affected node of the graph sees it rather than a single opaque "GCP is
down".

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `TRIAGE_EVENTS_URL` | — (required) | Base URL of the triage events route, e.g. `https://<triage-events-host>` |
| `TRIAGE_TOKEN_SECRET` | — (one of the two required) | Full Secret Manager version name `projects/<project>/secrets/<name>/versions/<version>`, read at startup through the API with the function's own service account |
| `TRIAGE_TOKEN_FILE` | — (one of the two required) | Path of a volume-mounted file holding the token, used when `TRIAGE_TOKEN_SECRET` is unset |

The token is never read from a plain `TRIAGE_TOKEN` environment variable, and the function refuses to start without one of the two sources above. Trailing whitespace is trimmed.
| `TRIAGE_TENANT` | `gcp` | `X-Tenant` header value, unless a per-caller alert's service account names its own tenant (see [Caller attribution](#caller-attribution)) |
| `COST_PROVIDER` | `gcp-cost` | Path segment for cost events |
| `HEALTH_PROVIDER` | `gcp-status` | Path segment for health events |
| `MONITORING_PROVIDER` | `gcp-alert` | Path segment for Cloud Monitoring alert events |
| `COST_TOPIC` / `HEALTH_TOPIC` / `MONITORING_TOPIC` | — | Override topic-name routing when topics are named differently |
| `SERVICE_HEALTH_PROJECT` | — | Project whose service health is polled |
| `SERVICE_HEALTH_ORG` | — | Poll organization-wide instead (org must be enrolled) |
| `SERVICE_HEALTH_PARENT` | — | Fully-qualified parent, overriding both of the above |
| `COST_WARNING_PERCENT` | `50` | Deviation above forecast that makes a cost anomaly a warning |
| `COST_CRITICAL_PERCENT` | `200` | ...and critical |
| `COST_WARNING_ABSOLUTE` | `200` | Absolute deviation (billing currency) for warning |
| `COST_CRITICAL_ABSOLUTE` | `1000` | ...and critical |

Note: the external ingress of a triage deployment routes only the event paths it lists; an unlisted provider path falls through to the main API and is answered `401 invalid token`. Until `/events/gcp-alert/` is listed in the ingress values of the triage chart, deploy with `MONITORING_PROVIDER=gcp-cost` instead.

Percentage and absolute amount are independent triggers: a 900% overrun on a
trivial spend is as interesting as a modest percentage on a large one.

The percentage is computed from the actual and expected amounts rather than read
from `deviationPercentage`, whose unit (fraction or percent) the notification
format does not pin down. The reported value is preserved in
`raw.deviationPercentReported` so the two can be compared against real traffic.

An amount that is not a finite number is treated as absent. An anomaly on a
service that was costing nothing has an infinite deviation percentage, and Cloud
Billing renders that double the protojson way — as the string `"Infinity"` — which
`strconv.ParseFloat` happily accepts. Such a value is dropped from the event
(severity then rests on the absolute deviation, which is the real signal when the
forecast was zero) while the original spelling stays on the record in
`raw.anomaly`.

## Ack semantics

A message we deliberately skip — unparseable, nothing to report, rejected by
triage with a 4xx, or carrying an event that cannot be JSON-encoded — is
**acked**. Redelivering it produces the same bytes and the same outcome.

Only a transient failure to reach triage (5xx, timeout, connection error, after
three attempts) returns an error, which nacks the message. Enable **Retry on
failure** on the trigger for that to be worth anything.

## The service catalog

`catalog_gen.go` is generated. It maps every known spelling of a GCP service —
billing service id (`services/24E6-581D-38E5`), billing display name (`BigQuery`),
service health product name (`Google BigQuery`) — onto the graph slug.

Service data only — public Google product names and billing service ids. The
generator's billing export also carries the organisation's projects; those must not
be emitted here.

It is generated from the **same source** as the triage tenant's service graph, so
the two cannot disagree about what a service id means. Regenerate from a
triage-agent checkout beside this one:

```bash
cd ../triage-agent && go run hack/gen-gcp-catalog.go
```

That writes both `data/gcp-services.yaml` (the graph catalog) and this package's
`catalog_gen.go`. Adding a GCP service means adding it to the curated table in
that generator — it fails loudly on a service present in the export with no entry,
rather than leaving it silently unrepresented.

## Deploy

```bash
PROJECT=<project>
REGION=<region>
SA=<dedicated-sa>@$PROJECT.iam.gserviceaccount.com

# 1. A dedicated service account for the function, and the triage token in
#    Secret Manager, from the triageagent-api-tokens Secret in the cluster.
gcloud iam service-accounts create <dedicated-sa> --project="$PROJECT"
printf '%s' "$TRIAGE_TOKEN" | gcloud secrets create <secret-name> \
  --data-file=- --project="$PROJECT"

# 2. Let that account read this one secret and nothing else.
gcloud secrets add-iam-policy-binding <secret-name> --project="$PROJECT" \
  --member="serviceAccount:$SA" --role=roles/secretmanager.secretAccessor

# 3. Deploy with the cost anomaly trigger.
gcloud functions deploy <function-name> \
  --gen2 --runtime=go127 --region="$REGION" --project="$PROJECT" \
  --service-account="$SA" \
  --source=./gcpevents --entry-point=HandleGCPEvent \
  --trigger-topic=cost-anomalies-alerts \
  --retry \
  --max-instances=5 \
  --set-env-vars=TRIAGE_EVENTS_URL=https://<triage-events-host>,TRIAGE_TENANT=<tenant>,TRIAGE_TOKEN_SECRET=projects/$PROJECT/secrets/<secret-name>/versions/latest
```

The function reads the token from Secret Manager at startup with its own
service account; the token is not placed in an environment variable. To deploy
with `gcloud run deploy` instead, pass the same `--service-account` and
`--set-env-vars`. A volume-mounted secret works too: set `TRIAGE_TOKEN_FILE` to
the mount path in place of `TRIAGE_TOKEN_SECRET`.

### Cloud Monitoring alerts

Cloud Monitoring alert policies notify over a **notification channel** of type
Pub/Sub, one incident message per state transition. That needs its own topic, a
publisher grant for Monitoring's own service agent, a notification channel
pointed at the topic (wired into whichever alert policies should reach triage),
and a second Eventarc trigger on the already-deployed function.

```bash
# 1. The topic Cloud Monitoring publishes to.
gcloud pubsub topics create monitoring-alerts --project="$PROJECT"

# 2. Let Cloud Monitoring's service agent publish to it.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
gcloud pubsub topics add-iam-policy-binding monitoring-alerts --project="$PROJECT" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-monitoring-notification.iam.gserviceaccount.com" \
  --role=roles/pubsub.publisher

# 3. A Pub/Sub notification channel — attach it to alert policies from the
#    Console (Alerting → Edit notification channels) or gcloud alpha monitoring
#    channels create --type=pubsub --channel-labels=topic=projects/$PROJECT/topics/monitoring-alerts.

# 4. A second Eventarc trigger, same function, second topic.
gcloud eventarc triggers create <function-name>-monitoring \
  --location="$REGION" --project="$PROJECT" \
  --destination-run-service=<function-name> --destination-run-region="$REGION" \
  --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
  --transport-topic="projects/$PROJECT/topics/monitoring-alerts" \
  --service-account="$SA"

# 5. Per-caller policies only: let the function read the calling service
#    account, whose description routes the event (see Caller attribution).
#    Without this every per-caller alert lands on the API instead.
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA" --role=roles/iam.serviceAccountViewer --condition=None
```

`<function-name>` was deployed above with `--trigger-topic=cost-anomalies-alerts`,
which Cloud Functions turns into its own Eventarc trigger; this adds a second
trigger delivering to that same deployed service, so one function instance
serves both topics. `HandleGCPEvent` tells the two apart by topic name (falling
back to payload shape — an `incident` object — if the topic is renamed).

### Service health → the poller

Personalized Service Health is read from its API on a schedule. There is no
topic, no sink, and no subscription.

```bash
gcloud services enable servicehealth.googleapis.com --project="$PROJECT"

# The poller reads Service Health for whatever scope it is pointed at.
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA" --role=roles/servicehealth.viewer --condition=None

gcloud functions deploy gcp-servicehealth-poll \
  --gen2 --runtime=go127 --region="$REGION" --project="$PROJECT" \
  --source=./gcpevents --entry-point=PollServiceHealth --trigger-http \
  --no-allow-unauthenticated --ingress-settings=internal-only \
  --service-account="$SA" --max-instances=2 --timeout=120s \
  --set-env-vars=TRIAGE_EVENTS_URL=https://<triage-events-host>,TRIAGE_TENANT=<tenant>,TRIAGE_TOKEN_SECRET=projects/$PROJECT/secrets/<secret-name>/versions/latest,SERVICE_HEALTH_PROJECT="$PROJECT"

URI=$(gcloud functions describe gcp-servicehealth-poll --gen2 --region="$REGION" \
  --project="$PROJECT" --format='value(serviceConfig.uri)')

gcloud scheduler jobs create http gcp-servicehealth-poll \
  --location="$REGION" --project="$PROJECT" --schedule="*/15 * * * *" \
  --uri="$URI" --http-method=GET --attempt-deadline=120s \
  --oidc-service-account-email="$SA" --oidc-token-audience="$URI"
```

Scope defaults to `SERVICE_HEALTH_PROJECT`. Set `SERVICE_HEALTH_ORG` for
organization-wide coverage (the org must be enrolled in Personalized Service
Health), or `SERVICE_HEALTH_PARENT` to pass a fully-qualified parent directly.

## Verify

```bash
go test ./...

# Replay a golden payload through the real topic.
gcloud pubsub topics publish cost-anomalies-alerts --project="$PROJECT" \
  --message="$(cat testdata/anomaly.json)"

gcloud pubsub topics publish monitoring-alerts --project="$PROJECT" \
  --message="$(cat testdata/monitoring_alert_open.json)"

gcloud pubsub topics publish monitoring-alerts --project="$PROJECT" \
  --message="$(cat testdata/monitoring_alert_calls_per_caller.json)"

# Force a status poll rather than waiting for the schedule.
gcloud scheduler jobs run gcp-servicehealth-poll --location="$REGION" --project="$PROJECT"
gcloud functions logs read gcp-servicehealth-poll --region="$REGION" --limit=10

gcloud functions logs read <function-name> --region="$REGION" --limit=20
```

A successful delivery logs `sent: provider=gcp-cost source=bigquery type=CostAnomaly …`.
Then check the triage side: the event should appear in recent events for tenant
`gcp`, resolved to that service node.

The first real message of each kind is worth reading in the logs before trusting
the decoders — a skipped message logs the payload verbatim (truncated to 2 KB).

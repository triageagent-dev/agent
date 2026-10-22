# promevents

Bridges Prometheus alerting into triage as events, the same pattern as
`gcpevents`: Alertmanager (or Grafana alerting) posts its webhook here, and
each alert is sent to the triage events server.

```
Prometheus rule fires → Alertmanager → POST /alertmanager (this service, bearer token)
  → POST $TRIAGE_EVENTS_URL/events/prometheus/   X-Tenant: <namespace's tenant>
```

Each alert becomes one event:

| event field | from |
|---|---|
| `type` | `alertname` |
| `source` | the workload: first label present of `deployment`, `statefulset`, `daemonset`, `workload`, `service_name`, `job_name`, `pod` (its owner: `payment-7f9c8d6b5-x2kqp` gives `payment`), `container`, then the scrape labels `service` and `job` (an OTel `job` like `ns/name` keeps `name`) |
| `severity` | `critical` for severity critical/page/error, else `warning`; `info` when resolved |
| `message` | `<alertname> firing on <workload>: <summary>`, or `... resolved on ...` |
| `raw` | status, labels, annotations, fingerprint, startsAt, generatorURL |

Triage resolves `events/prometheus/<workload>` to a service the same way it
does for any provider event, attaches the alert to that service's open issue,
and counts a firing alert as an independent second sign.

`container` comes after the labels that name a workload because, on an alert
from a scraped series, it can be the exporter's own container while
`deployment` is the workload the alert is about. `service` and `job` say where
a series was scraped from: they are the last resort, and an alert with no
`namespace` label is never attributed by them. Such an alert is left
unattributed (skipped) rather than attributed to the scraper.

An alert with no workload label, or whose namespace no tenant owns, is
skipped and acknowledged. If triage cannot be reached, or answers 429 because
its queue is full, the webhook answers 502, so Alertmanager retries. The retry
carries the whole group again; an alert already delivered is not sent twice
within `DEDUP_WINDOW`, by (fingerprint, status, startsAt), so only what failed
is resent. Keep the window under Alertmanager's `repeat_interval`, or the
repeats of a still-firing alert are dropped as duplicates.

## Authentication

`/alertmanager` requires a bearer token read from the file named by
`WEBHOOK_TOKEN_FILE`, compared in constant time. Neither this token nor the
core token is read from an environment variable: both come from mounted
files, which rotate without a restart and do not appear in the pod spec. The
bridge posts into every
mapped tenant with its own credentials, so an open webhook would let any pod
that can reach the Service inject events into any of them. It refuses to
start without a token unless `WEBHOOK_INSECURE=true` is set. `/healthz` stays
open.

## Configuration

| env | |
|---|---|
| `TRIAGE_EVENTS_URL` | events server base URL, e.g. `http://triage-core-events.triage.svc:8086` |
| `TRIAGE_TOKEN_FILE` | projected ServiceAccount token (audience `triage-core`); re-read on every send; required |
| `NAMESPACE_TENANTS` | `ns=tenant,...`; only these namespaces are forwarded |
| `TRIAGE_TENANT` | tenant for every other namespace (optional) |
| `WEBHOOK_TOKEN_FILE` | file with the bearer token Alertmanager must send; re-read on every request |
| `WEBHOOK_INSECURE` | `true` serves `/alertmanager` without authentication (not recommended) |
| `DEDUP_WINDOW` | how long a delivered alert is not sent again, default `2m`; `0` disables |
| `WORKLOAD_LABELS` | overrides the label order above (`pod` means the pod's owner; `service` and `job` still need a `namespace` label) |
| `PROVIDER` | events path segment, default `prometheus` |
| `LISTEN` | default `:8080` |

## Deploy

`deploy/promevents.yaml` runs it in the core's namespace with a ServiceAccount
whose Role grants `ingest` on each tenant (`triageagent.dev`, `tenants`,
`resourceNames`). It reads the webhook token from the Secret
`triage-promevents-webhook` (key `token`), which you create and also give to
Alertmanager:

```
kubectl -n triage create secret generic triage-promevents-webhook --from-literal=token="$(openssl rand -hex 32)"
```

Build the image with ko from this directory:

```
cd promevents && KO_DOCKER_REPO=<registry>/promevents ko build --bare --tags <tag> .
```

Point Alertmanager at it with a webhook receiver:

```yaml
receivers:
  - name: triage
    webhook_configs:
      - url: http://triage-promevents.triage.svc:8080/alertmanager
        send_resolved: true
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/secrets/triage-promevents-webhook/token
route:
  receiver: triage
  repeat_interval: 5m   # a firing alert keeps arriving, so it stays a live sign
```

// Package main is promevents: a bridge from Prometheus alerting into the
// triage events server, the same pattern as gcpevents.
//
// Alertmanager (or Grafana alerting, whose webhook body has the same shape)
// posts its webhook to /alertmanager. Each alert in the body becomes one
// event, POSTed to the triage events server:
//
//	POST $TRIAGE_EVENTS_URL/events/prometheus/   {type,message,severity,source,raw}
//	Authorization: Bearer <projected ServiceAccount token, audience triage-core>
//	X-Tenant: <tenant of the alert's namespace>
//
// `source` is the workload the alert is about, read from its labels; the
// triage classifier turns events/prometheus/<workload> into a service-graph
// node (normalizeEvents drops the provider segment). Getting the workload
// right here is the whole job.
//
// Ack semantics: an alert we deliberately skip (no workload, a namespace no
// tenant owns) is acknowledged. Only a transient failure to reach triage, or
// triage being busy (429), returns 5xx, so Alertmanager retries the
// notification; alerts already delivered are not sent again on that retry.
package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// webhookBody is the part of Alertmanager's webhook (version 4) we read.
type webhookBody struct {
	Version string         `json:"version"`
	Status  string         `json:"status"`
	Alerts  []webhookAlert `json:"alerts"`
}

type webhookAlert struct {
	Status       string            `json:"status"` // firing | resolved
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// triageEvent is the events-server request body (internal/events.eventRequest).
type triageEvent struct {
	Type     string         `json:"type"`
	Message  string         `json:"message"`
	Severity string         `json:"severity,omitempty"`
	Source   string         `json:"source,omitempty"`
	Raw      map[string]any `json:"raw,omitempty"`

	// tenant selects X-Tenant for this event; not part of the body.
	tenant string `json:"-"`
}

// defaultWorkloadLabels are read, in order, for the workload an alert is
// about. The labels that name a workload outright come first. `pod` yields
// the pod's owner (podOwner). `container` comes after them: on an alert from
// a scraped series it can be the exporter's own container
// (kube-state-metrics), while `deployment` on the same alert is the workload
// the alert is about. `service` and `job` are scrape labels, naming the
// scraped Service and the scrape job; they are a last resort and are not used
// at all on an alert with no namespace label (scrapeLabels), which is left
// unattributed rather than attributed to the scraper.
var defaultWorkloadLabels = []string{"deployment", "statefulset", "daemonset", "workload", "service_name", "job_name", "pod", "container", "service", "job"}

// scrapeLabels describe where a series was scraped from, not what it is about.
var scrapeLabels = map[string]bool{"service": true, "job": true}

// Pod name suffixes. Kubernetes generates them from an alphabet with no
// vowels and no 0, 1 or 3, so an ordinary name ending in a five-letter word
// ("my-redis") is not taken for a generated one.
var (
	podOfReplicaSet = regexp.MustCompile(`^(.+)-[bcdfghjklmnpqrstvwxz2456789]{6,10}-[bcdfghjklmnpqrstvwxz2456789]{5}$`)
	podOfSet        = regexp.MustCompile(`^(.+)-[bcdfghjklmnpqrstvwxz2456789]{5}$`)
	podOfStateful   = regexp.MustCompile(`^(.+)-[0-9]+$`)
)

// podOwner derives the owning workload's name from a pod name:
// "payment-7f9c8d6b5-x2kqp" (Deployment), "agent-x2kqp" (DaemonSet, Job) and
// "db-0" (StatefulSet) give "payment", "agent" and "db". A name with no
// generated suffix is a bare pod and is returned as is.
func podOwner(pod string) string {
	for _, re := range []*regexp.Regexp{podOfReplicaSet, podOfSet, podOfStateful} {
		if m := re.FindStringSubmatch(pod); m != nil {
			return m[1]
		}
	}
	return pod
}

// mapper turns webhook alerts into triage events.
type mapper struct {
	// tenants maps a namespace to its triage tenant; an alert whose namespace
	// is not in it is skipped. fallbackTenant, when set, takes every other
	// namespace and alerts with no namespace label.
	tenants        map[string]string
	fallbackTenant string
	workloadLabels []string
}

// event maps one alert; ok is false when it names no workload or no tenant.
func (m mapper) event(a webhookAlert) (triageEvent, bool) {
	ns := a.Labels["namespace"]
	tenant, ok := m.tenants[ns]
	if !ok {
		tenant = m.fallbackTenant
	}
	if tenant == "" {
		return triageEvent{}, false
	}
	name := a.Labels["alertname"]
	workload := ""
	for _, l := range m.workloadLabels {
		if scrapeLabels[l] && ns == "" {
			continue // only scrape labels and no namespace: unattributed
		}
		if v := strings.TrimSpace(a.Labels[l]); v != "" {
			if l == "pod" {
				v = podOwner(v)
			}
			workload = v
			break
		}
	}
	if name == "" || workload == "" {
		return triageEvent{}, false
	}
	// A job label from the OTel Prometheus exporter is "<service.namespace>/<service.name>".
	if i := strings.LastIndex(workload, "/"); i >= 0 {
		workload = workload[i+1:]
	}

	summary := a.Annotations["summary"]
	if summary == "" {
		summary = a.Annotations["description"]
	}
	severity := "warning"
	switch strings.ToLower(a.Labels["severity"]) {
	case "critical", "page", "error":
		severity = "critical"
	}
	msg := fmt.Sprintf("%s firing on %s", name, workload)
	if a.Status == "resolved" {
		severity = "info"
		msg = fmt.Sprintf("%s resolved on %s", name, workload)
	}
	if summary != "" {
		msg += ": " + summary
	}
	return triageEvent{
		Type:     name,
		Message:  msg,
		Severity: severity,
		Source:   workload,
		Raw: map[string]any{
			"status":       a.Status,
			"operation":    name,
			"namespace":    ns,
			"labels":       a.Labels,
			"annotations":  a.Annotations,
			"fingerprint":  a.Fingerprint,
			"startsAt":     a.StartsAt,
			"generatorURL": a.GeneratorURL,
		},
		tenant: tenant,
	}, true
}

// parseTenants reads "ns=tenant,ns2=tenant2".
func parseTenants(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		ns, tenant, ok := strings.Cut(pair, "=")
		if !ok || strings.TrimSpace(ns) == "" || strings.TrimSpace(tenant) == "" {
			return nil, fmt.Errorf("bad namespace=tenant pair %q", pair)
		}
		out[strings.TrimSpace(ns)] = strings.TrimSpace(tenant)
	}
	return out, nil
}

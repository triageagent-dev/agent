package gcpevents

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Cloud Monitoring delivers alert policy state transitions to a Pub/Sub
// notification channel as a JSON "incident" envelope (notification format
// version 1.2). Every field may be absent — an incident can open with only a
// policy name, or only a condition summary — so, like the other two decoders in
// this package, this one pulls fields out of a decoded map[string]any by name
// rather than binding to a struct.
//
// See https://cloud.google.com/monitoring/support/notification-options#pubsub

// monitoringAPIAliases translates a Cloud Monitoring API service prefix (the
// text before ".googleapis.com" in a metric or monitored-resource type, e.g.
// "aiplatform") to a spelling the generated GCP service catalog
// (catalog_gen.go) recognizes, for the handful of prefixes whose API
// identifier does not already appear there. Not exhaustive — extend as new
// alerting policies show up on services not yet covered.
var monitoringAPIAliases = map[string]string{
	"aiplatform":     "vertex ai",
	"compute":        "compute engine",
	"container":      "kubernetes engine",
	"run":            "cloud run",
	"sqladmin":       "cloud sql",
	"cloudfunctions": "cloud functions",
}

// decodeMonitoringAlert turns one Cloud Monitoring incident notification into a
// single triage event.
func decodeMonitoringAlert(payload map[string]any) ([]triageEvent, error) {
	incident := obj(payload, "incident")
	if incident == nil {
		return nil, fmt.Errorf("not a monitoring alert: no incident object in payload")
	}

	policyName := str(incident, "policy_name", "policyName")
	summary := str(incident, "summary")
	conditionName := str(incident, "condition_name", "conditionName")
	if policyName == "" && summary == "" {
		return nil, fmt.Errorf("not a monitoring alert: no policy_name or summary in incident")
	}

	state := strings.ToLower(str(incident, "state"))
	resolved := state == "closed"
	eventType := "MonitoringAlert"
	if resolved {
		eventType = "MonitoringAlertResolved"
	}

	// A closed incident is always info: the policy's severity label describes
	// the condition firing, and carrying it onto the all-clear makes triage
	// mark the service degraded again on the very notification that says it
	// recovered (a warning "Resolved" event reopened an incident, 2026-09-13).
	labelSeverity := strings.ToLower(str(obj(incident, "policy_user_labels", "policyUserLabels"), "severity"))
	severity := labelSeverity
	switch {
	case resolved:
		severity = severityInfo
	case labelSeverity == severityInfo, labelSeverity == severityWarning, labelSeverity == severityCritical:
		// Honour the policy's own label.
	default:
		severity = severityWarning
	}

	slug, display := resolveMonitoringService(obj(incident, "metric"), obj(incident, "resource"))

	// A per-caller alert (monitored resource type consumed_api) is about the
	// identity that called the API, not the API. When that identity's service
	// account says where its events belong — tenant=, vapp= in its description,
	// see caller.go — the alert is routed there instead. When it says nothing,
	// the event keeps the shape every other monitoring alert has, and the
	// caller survives in the message and in raw.
	source, tenant := slug, ""
	caller, perCaller := resolveCaller(obj(incident, "resource"))
	if perCaller && caller.tenant != "" {
		tenant = caller.tenant
		source = firstNonEmpty(caller.vapp, caller.name)
	}

	detail := humanizeNumbers(firstNonEmpty(summary, conditionName))
	var message string
	switch {
	case policyName != "" && detail != "":
		message = policyName + ": " + detail
	case policyName != "":
		message = policyName
	default:
		message = detail
	}
	if perCaller {
		message += " caller=" + caller.name
	}
	observedValue := str(incident, "observed_value", "observedValue")
	thresholdValue := str(incident, "threshold_value", "thresholdValue")
	if observedValue != "" && thresholdValue != "" {
		message += fmt.Sprintf(" observed=%s threshold=%s", humanNumber(observedValue), humanNumber(thresholdValue))
	}
	url := str(incident, "url")
	if url != "" {
		message += " " + url
	}

	raw := map[string]any{
		"provider": "gcp",
		"signal":   "monitoring_alert",
		"cloud":    cloudGCP,
		"service":  slug,
	}
	if display != "" {
		raw["serviceDisplayName"] = display
	}
	if v, ok := incident["incident_id"]; ok {
		raw["incidentId"] = v
	}
	if state != "" {
		raw["state"] = state
	}
	if policyName != "" {
		raw["policyName"] = policyName
	}
	if conditionName != "" {
		raw["conditionName"] = conditionName
	}
	if v, ok := incident["started_at"]; ok {
		raw["startedAt"] = v
	}
	if v, ok := incident["ended_at"]; ok {
		raw["endedAt"] = v
	}
	if observedValue != "" {
		raw["observedValue"] = observedValue
	}
	if thresholdValue != "" {
		raw["thresholdValue"] = thresholdValue
	}
	if url != "" {
		raw["url"] = url
	}
	if resource := obj(incident, "resource"); resource != nil {
		if t := str(resource, "type"); t != "" {
			raw["resourceType"] = t
		}
		if labels := obj(resource, "labels"); labels != nil {
			raw["resourceLabels"] = labels
		}
	}
	if metric := obj(incident, "metric"); metric != nil {
		if t := str(metric, "type"); t != "" {
			raw["metricType"] = t
		}
		if labels := obj(metric, "labels"); labels != nil {
			raw["metricLabels"] = labels
		}
	}
	if v := str(incident, "scoping_project_id", "scopingProjectId"); v != "" {
		raw["scopingProjectId"] = v
	}
	if perCaller {
		// raw["service"] stays the API slug whichever way the event was routed:
		// the API that was called is a fact worth carrying either way.
		raw["credentialId"] = caller.raw
		raw["callerKind"] = caller.kind
		raw["callerName"] = caller.name
		if caller.email != "" {
			raw["serviceAccountEmail"] = caller.email
		}
		if caller.tenant != "" {
			raw["callerTenant"] = caller.tenant
		}
		if caller.vapp != "" {
			raw["callerVapp"] = caller.vapp
		}
	}

	return []triageEvent{{
		provider: env("MONITORING_PROVIDER", "gcp-alert"),
		tenant:   tenant,
		Type:     eventType,
		Severity: severity,
		Source:   source,
		Message:  message,
		Raw:      raw,
	}}, nil
}

// resolveMonitoringService finds the GCP service an alert's metric belongs to.
//
// Cloud Monitoring spells both the metric type and the monitored resource type
// as "<api>.googleapis.com/<Name>" (see
// https://cloud.google.com/monitoring/api/resources), so the API identifier
// ("aiplatform") is extracted from whichever is present and translated through
// monitoringAPIAliases ("vertex ai") into a spelling the catalog recognizes —
// the same "find a spelling the catalog knows" approach resolveAnomalyService
// in costanomaly.go uses for billing services. A policy with no service-level
// metric (or one on a product outside the catalog) resolves to the platform
// catch-all rather than being dropped.
//
// The monitored resource's own "service" label is tried first, because the
// resource types that carry one — consumed_api above all — name the API that
// was called there and nowhere else: a per-caller alert's metric is
// serviceruntime.googleapis.com/api/request_count (the metering system, which
// is in no catalog) and its resource type is the bare word "consumed_api", so
// only the label knows this was Vertex AI.
func resolveMonitoringService(metric, resource map[string]any) (slug, display string) {
	for _, prefix := range []string{
		apiPrefix(str(obj(resource, "labels"), "service")),
		apiPrefix(str(metric, "type")),
		apiPrefix(str(resource, "type")),
	} {
		if prefix == "" {
			continue
		}
		name := prefix
		if alias, ok := monitoringAPIAliases[prefix]; ok {
			name = alias
		}
		if s, ok := slugForKey(name); ok {
			return s, serviceDisplayBySlug[s]
		}
	}
	return catchAllHealthSlug, serviceDisplayBySlug[catchAllHealthSlug]
}

// apiPrefix extracts the API identifier from a Cloud Monitoring metric or
// monitored-resource type spelled "<api>.googleapis.com/<Name>" (e.g.
// "aiplatform.googleapis.com/PublisherModel" -> "aiplatform"). Returns "" when
// s is not shaped that way — many built-in monitored resource types, such as
// "gce_instance", are not.
func apiPrefix(s string) string {
	const marker = ".googleapis.com"
	if i := strings.Index(s, marker); i > 0 {
		return s[:i]
	}
	return ""
}

// monitoringTopicHints let a deployment with a differently-named topic override
// routing without a code change, mirroring costTopicHints/healthTopicHints.
func monitoringTopicHints() []string {
	if v := os.Getenv("MONITORING_TOPIC"); v != "" {
		return []string{strings.ToLower(v)}
	}
	return []string{"monitoring-alerts", "alerting", "monitoring"}
}

// humanNumber renders a numeric string in K/M/B units so a token-count alert
// reads "56M" rather than "56000000"; anything that is not a plain number is
// returned unchanged. Values below 10,000 keep their digits — "1200" is
// clearer than "1.2K" at that scale.
func humanNumber(v string) string {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || f < 10000 {
		return v
	}
	units := []struct {
		div    float64
		suffix string
	}{{1e9, "B"}, {1e6, "M"}, {1e3, "K"}}
	for _, u := range units {
		if f >= u.div {
			q := f / u.div
			if q >= 100 || q == float64(int64(q)) {
				return strconv.FormatInt(int64(q+0.5), 10) + u.suffix
			}
			return strconv.FormatFloat(math.Round(q*10)/10, 'f', 1, 64) + u.suffix
		}
	}
	return v
}

var bareNumberRE = regexp.MustCompile(`\b\d{5,}(?:\.\d+)?\b`)

// humanizeNumbers applies humanNumber to every bare number of five or more
// digits inside free text, such as Cloud Monitoring's generated summary
// ("... above the threshold of 5000000 with a value of 56000000").
func humanizeNumbers(text string) string {
	return bareNumberRE.ReplaceAllStringFunc(text, humanNumber)
}

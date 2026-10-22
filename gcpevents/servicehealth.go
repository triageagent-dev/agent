package gcpevents

import (
	"fmt"
	"sort"
	"strings"
)

// Personalized Service Health publishes events as Cloud Logging entries on
// servicehealth.googleapis.com/activity; a log sink routes those entries to
// Pub/Sub, so what arrives here is a LogEntry whose jsonPayload is a
// google.cloud.servicehealth.logging.v1.EventLog.
//
// See https://cloud.google.com/service-health/docs/configure-alerts-cloud-logging

// decodeServiceHealth turns one service health entry into one triage event per
// impacted product, so a multi-product Google incident lands on each affected
// node of the service graph instead of a single opaque "GCP is down".
func decodeServiceHealth(payload map[string]any) ([]triageEvent, error) {
	entry := findEventLog(payload)
	if entry == nil {
		return nil, fmt.Errorf("not a service health event: no EventLog object in the payload")
	}

	state := strings.ToUpper(str(entry, "state"))
	detailedState := str(entry, "detailedState", "detailed_state")
	category := strings.ToUpper(str(entry, "category"))
	relevance := strings.ToUpper(str(entry, "relevance"))
	title := firstNonEmpty(str(entry, "title"), str(entry, "description"))
	if title == "" {
		title = "Google Cloud service health event"
	}

	products := stringList(entry, "impactedProducts", "impacted_products", "impactedProductIds", "impacted_product_ids")
	locations := stringList(entry, "impactedLocations", "impacted_locations")

	resolved := state == "CLOSED"
	eventType := "ProviderIncident"
	if resolved {
		eventType = "ProviderIncidentResolved"
	}
	severity := healthSeverity(relevance, resolved)

	var b strings.Builder
	fmt.Fprintf(&b, "Google Cloud %s: %s", strings.ToLower(firstNonEmpty(category, "event")), title)
	if len(products) > 0 {
		fmt.Fprintf(&b, " — affects %s", strings.Join(products, ", "))
	}
	if len(locations) > 0 {
		fmt.Fprintf(&b, " in %s", strings.Join(locations, ", "))
	}
	if detailedState != "" {
		fmt.Fprintf(&b, " [%s]", detailedState)
	} else if state != "" {
		fmt.Fprintf(&b, " [%s]", state)
	}
	if relevance != "" {
		fmt.Fprintf(&b, " (relevance: %s)", relevance)
	}
	message := b.String()

	raw := map[string]any{
		"provider": "gcp",
		"signal":   "service_health",
		"eventLog": entry,
		"cloud":    cloudGCP,
	}
	// No projectIDs here, deliberately. A service health event is a Google-side
	// outage of a product; it is not spend billed to one of our projects, so
	// there is no owning team to attribute it to. Emitting the polled project
	// (SERVICE_HEALTH_PROJECT) would name whoever we happen to poll from rather
	// than anyone impacted.
	for key, val := range map[string]string{
		"state":         state,
		"detailedState": detailedState,
		"category":      category,
		"relevance":     relevance,
		"logName":       str(payload, "logName", "log_name"),
		"insertId":      str(payload, "insertId", "insert_id"),
		"startTime":     str(entry, "startTime", "start_time"),
		"updateTime":    str(entry, "updateTime", "update_time"),
		"endTime":       str(entry, "endTime", "end_time"),
	} {
		if val != "" {
			raw[key] = val
		}
	}
	if len(products) > 0 {
		raw["impactedProducts"] = products
	}
	if len(locations) > 0 {
		raw["impactedLocations"] = locations
	}

	provider := env("HEALTH_PROVIDER", "gcp-status")
	events := make([]triageEvent, 0, len(products))
	for _, slug := range resolveHealthServices(products) {
		evRaw := make(map[string]any, len(raw)+1)
		for k, v := range raw {
			evRaw[k] = v
		}
		evRaw["service"] = slug
		if display := serviceDisplayBySlug[slug]; display != "" {
			evRaw["serviceDisplayName"] = display
		}
		events = append(events, triageEvent{
			provider: provider,
			Type:     eventType,
			Severity: severity,
			Source:   slug,
			Message:  message,
			Raw:      evRaw,
		})
	}
	return events, nil
}

// resolveHealthServices maps impacted product names onto catalog slugs,
// de-duplicated and ordered. Products outside the catalog collapse onto the
// platform catch-all rather than being dropped: a Google incident on a product we
// do not bill for can still be the reason something looks wrong.
func resolveHealthServices(products []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, p := range products {
		slug, ok := slugForKey(p)
		if !ok {
			slug = catchAllHealthSlug
		}
		if seen[slug] {
			continue
		}
		seen[slug] = true
		out = append(out, slug)
	}
	if len(out) == 0 {
		return []string{catchAllHealthSlug}
	}
	sort.Strings(out)
	return out
}

// healthSeverity maps Personalized Service Health relevance onto triage severity.
//
// IMPACTED means Google confirmed impact on this organization's resources, so it
// is critical. RELATED means the incident touches products we use without
// confirmed impact — a warning, because treating every Google-side incident on a
// widely-used product as critical would page constantly. A closed event is
// informational whatever its relevance was.
func healthSeverity(relevance string, resolved bool) string {
	if resolved {
		return severityInfo
	}
	switch relevance {
	case "IMPACTED":
		return severityCritical
	case "RELATED":
		return severityWarning
	case "PARTIALLY_RELATED":
		return severityInfo
	default:
		return severityInfo
	}
}

package gcpevents

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Cloud Billing cost anomaly notifications carry the identity of the anomaly in
// the Pub/Sub message ATTRIBUTES (scope, billingAccountName, resourceName,
// resourceDisplayName) and the numbers plus a root-cause tree in the base64 data
// payload. The attributes are the documented, stable half, so they are the
// primary source here; the payload supplies the amounts and the service.
//
// See https://cloud.google.com/billing/docs/how-to/budgets-programmatic-notifications

// Default severity thresholds. A cost anomaly is not an outage — the point of the
// thresholds is that a routine 60% wobble stays a signal while a 4x blowout gets
// triaged. Overridable per deployment.
const (
	defaultWarnPercent      = 50.0
	defaultCriticalPercent  = 200.0
	defaultWarnAbsolute     = 200.0
	defaultCriticalAbsolute = 1000.0
)

// decodeCostAnomaly turns one anomaly notification into a single triage event.
func decodeCostAnomaly(payload map[string]any, attrs map[string]string) ([]triageEvent, error) {
	anomalyName := str(payload, "anomalyName", "anomaly_name", "name")
	resourceName := firstNonEmpty(attrs["resourceName"], str(payload, "resourceName", "resource_name"))
	resourceDisplay := firstNonEmpty(attrs["resourceDisplayName"], str(payload, "resourceDisplayName", "resource_display_name"))
	billingAccount := firstNonEmpty(attrs["billingAccountName"], str(payload, "billingAccountName", "billing_account_name"))

	if anomalyName == "" && resourceName == "" && billingAccount == "" {
		return nil, fmt.Errorf("not a cost anomaly: no anomalyName, resourceName or billingAccountName in attributes or payload")
	}

	actual, currency, hasActual := num(payload, "actualSpendAmount", "actual_spend_amount", "actualSpend", "actualCost")
	expected, expectedCur, hasExpected := num(payload, "expectedSpendAmount", "expected_spend_amount", "expectedSpend", "expectedCost")
	if currency == "" {
		currency = expectedCur
	}
	deviation, deviationCur, hasDeviation := num(payload, "deviationAmount", "deviation_amount", "costDifference")
	if currency == "" {
		currency = deviationCur
	}
	if !hasDeviation && hasActual && hasExpected {
		deviation, hasDeviation = actual-expected, true
	}

	// Prefer a percentage we compute from the amounts: the reported field's unit
	// (fraction vs percent) is not pinned down by the documentation, and a wrong
	// guess there moves events between severities. The reported value is kept in
	// raw for comparison.
	percent, hasPercent := 0.0, false
	if hasActual && hasExpected && expected > 0 {
		percent, hasPercent = (actual-expected)/expected*100, true
	} else if v, _, ok := num(payload, "deviationPercentage", "deviation_percentage"); ok {
		percent, hasPercent = v, true
	}
	// An expected spend small enough to overflow the ratio leaves percent
	// non-finite, which no severity comparison and no JSON body can carry. The
	// absolute deviation still says how bad it is.
	if math.IsInf(percent, 0) || math.IsNaN(percent) {
		percent, hasPercent = 0, false
	}

	slug, serviceDisplay := resolveAnomalyService(payload)
	projects, projectIDs := resolveAnomalyProjects(payload, resourceName, resourceDisplay)

	severity := costSeverity(percent, hasPercent, deviation, hasDeviation)

	var b strings.Builder
	fmt.Fprintf(&b, "GCP cost anomaly: %s", serviceDisplay)
	if len(projects) > 0 {
		fmt.Fprintf(&b, " in %s", strings.Join(projects, ", "))
	}
	if hasActual && hasExpected {
		fmt.Fprintf(&b, " — actual %s vs expected %s", formatAmount(actual, currency), formatAmount(expected, currency))
	} else if hasDeviation {
		fmt.Fprintf(&b, " — %s above forecast", formatAmount(deviation, currency))
	}
	if hasPercent {
		fmt.Fprintf(&b, " (%+.0f%%)", percent)
	}
	if date := str(payload, "detectionDate", "detection_date", "anomalyDate"); date != "" {
		fmt.Fprintf(&b, " detected %s", date)
	}

	raw := map[string]any{
		"provider":   "gcp",
		"signal":     "cost_anomaly",
		"service":    slug,
		"attributes": attrs,
		"anomaly":    payload,
		// cloud names the identifier key space of projectIDs below. Consumers
		// attributing spend to an owning team key their lookup per cloud (a GCP
		// project id, an Azure subscription GUID and an AWS account id are three
		// disjoint spaces), so this is declared rather than inferred from the
		// source path or guessed from the shape of the id.
		"cloud": cloudGCP,
	}
	if serviceDisplay != "" {
		raw["serviceDisplayName"] = serviceDisplay
	}
	if anomalyName != "" {
		raw["anomalyName"] = anomalyName
	}
	if billingAccount != "" {
		raw["billingAccountName"] = billingAccount
	}
	if len(projects) > 0 {
		// impactedProjects is the DISPLAY list ("Name (id)") that renders the
		// message above. projectIDs is the machine join key: bare, canonical,
		// lowercased. Keep them separate — a consumer that has to unwrap a
		// display label to find an id is coupled to this file's formatting.
		raw["impactedProjects"] = projects
		raw["projectIDs"] = projectIDs
	}
	if scope := firstNonEmpty(attrs["scope"], str(payload, "scope", "Scope")); scope != "" {
		raw["scope"] = scope
	}
	if hasPercent {
		raw["deviationPercentComputed"] = roundTo(percent, 2)
	}
	if v, _, ok := num(payload, "deviationPercentage", "deviation_percentage"); ok {
		raw["deviationPercentReported"] = v
	}
	if hasDeviation {
		raw["deviationAmount"] = roundTo(deviation, 2)
	}
	if currency != "" {
		raw["currency"] = currency
	}

	return []triageEvent{{
		provider: env("COST_PROVIDER", "gcp-cost"),
		Type:     "CostAnomaly",
		Severity: severity,
		Source:   slug,
		Message:  b.String(),
		Raw:      raw,
	}}, nil
}

// resolveAnomalyService finds the billed service the anomaly is about.
//
// The root-cause tree is where the service lives, but its field names are not
// dependable, so this scans the whole payload for a billing service id and takes
// the first one the catalog knows. An anomaly with no service-level cause (a
// project- or billing-account-scope anomaly) resolves to the catch-all node
// rather than being dropped.
func resolveAnomalyService(payload map[string]any) (slug, display string) {
	for _, id := range findMatching(payload, serviceIDRe) {
		if s, ok := slugForKey(id); ok {
			return s, serviceDisplayBySlug[s]
		}
	}
	// No id matched the catalog; fall back to a named service if one is spelled out.
	for _, key := range []string{"serviceDisplayName", "service_display_name", "serviceName", "service"} {
		if name := str(payload, key); name != "" {
			if s, ok := slugForKey(name); ok {
				return s, serviceDisplayBySlug[s]
			}
		}
	}
	return catchAllCostSlug, serviceDisplayBySlug[catchAllCostSlug]
}

// resolveAnomalyProjects lists the impacted projects, most specific first: the
// project the notification is scoped to, then any other project referenced in the
// root-cause tree. Names come from the generated catalog when the id is known.
//
// Two parallel lists come back from one walk so they can never drift apart:
// labels are display-shaped ("Name (id)") and feed the human-readable message,
// ids are the bare canonical project ids and are the JOIN KEY consumers use to
// attribute spend to an owning team. Never derive one from the other downstream —
// parsing an id back out of a label is exactly the coupling this avoids.
func resolveAnomalyProjects(payload map[string]any, resourceName, resourceDisplay string) (labels, ids []string) {
	seen := make(map[string]bool)

	add := func(ref, display string) {
		if ref == "" || !strings.HasPrefix(ref, "projects/") {
			return
		}
		if seen[ref] {
			return
		}
		seen[ref] = true
		label := projectLabel(ref, display)
		id := projectID(ref)
		if label == "" || id == "" {
			return
		}
		labels = append(labels, label)
		ids = append(ids, id)
	}

	add(resourceName, resourceDisplay)
	for _, ref := range findMatching(payload, projectRefRe) {
		add(ref, "")
	}
	return labels, ids
}

// costSeverity maps deviation onto the three severities the events server takes.
// Percentage and absolute amount are independent triggers: a 900% overrun on a
// trivial spend is as interesting as a modest percentage on a large one.
func costSeverity(percent float64, hasPercent bool, deviation float64, hasDeviation bool) string {
	warnPct := envFloat("COST_WARNING_PERCENT", defaultWarnPercent)
	critPct := envFloat("COST_CRITICAL_PERCENT", defaultCriticalPercent)
	warnAbs := envFloat("COST_WARNING_ABSOLUTE", defaultWarnAbsolute)
	critAbs := envFloat("COST_CRITICAL_ABSOLUTE", defaultCriticalAbsolute)

	if (hasPercent && percent >= critPct) || (hasDeviation && deviation >= critAbs) {
		return severityCritical
	}
	if (hasPercent && percent >= warnPct) || (hasDeviation && deviation >= warnAbs) {
		return severityWarning
	}
	return severityInfo
}

func envFloat(key string, def float64) float64 {
	v := strings.TrimSpace(env(key, ""))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

func roundTo(f float64, places int) float64 {
	shift := 1.0
	for range places {
		shift *= 10
	}
	return float64(int64(f*shift+0.5*sign(f))) / shift
}

func sign(f float64) float64 {
	if f < 0 {
		return -1
	}
	return 1
}

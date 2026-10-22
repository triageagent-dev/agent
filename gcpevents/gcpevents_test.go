package gcpevents

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func loadPayload(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return payload
}

// anomalyAttributes mirrors the documented Pub/Sub message attributes for a cost
// anomaly notification.
func anomalyAttributes() map[string]string {
	return map[string]string{
		"scope":               "scope_project",
		"billingAccountName":  "billingAccounts/0X0X0X-0X0X0X-0X0X0X",
		"resourceName":        "projects/example-data-prod",
		"resourceDisplayName": "Example Data Prod",
		"SchemaVersion":       "1.0",
	}
}

func TestDecodeCostAnomaly(t *testing.T) {
	events, err := decodeCostAnomaly(loadPayload(t, "anomaly.json"), anomalyAttributes())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]

	if ev.Type != "CostAnomaly" {
		t.Errorf("type = %q, want CostAnomaly", ev.Type)
	}
	// The root cause names services/24E6-581D-38E5; the source segment must be the
	// catalog slug, because that is what the triage classifier looks up.
	if ev.Source != "bigquery" {
		t.Errorf("source = %q, want bigquery", ev.Source)
	}
	if ev.provider != "gcp-cost" {
		t.Errorf("provider = %q, want gcp-cost", ev.provider)
	}
	// 1240.50 against an expected 310.00 is +300%, past the critical threshold.
	if ev.Severity != severityCritical {
		t.Errorf("severity = %q, want critical", ev.Severity)
	}
	for _, want := range []string{"BigQuery", "Example Data Prod (example-data-prod)", "1240.50 USD", "310.00 USD", "+300%"} {
		if !strings.Contains(ev.Message, want) {
			t.Errorf("message %q missing %q", ev.Message, want)
		}
	}
	if got := ev.Raw["service"]; got != "bigquery" {
		t.Errorf("raw.service = %v, want bigquery", got)
	}
	projects, _ := ev.Raw["impactedProjects"].([]string)
	if len(projects) != 1 || projects[0] != "Example Data Prod (example-data-prod)" {
		t.Errorf("raw.impactedProjects = %v", projects)
	}
	if got := ev.Raw["deviationPercentReported"]; got != 300.16 {
		t.Errorf("raw.deviationPercentReported = %v, want 300.16", got)
	}
}

// The notification format documents amounts as a Money object in one place and a
// bare number in another, and the root-cause tree appears in both camelCase and
// snake_case. Decoding must not depend on which we get.
func TestDecodeCostAnomalyToleratesShapeVariants(t *testing.T) {
	payload := map[string]any{
		"anomaly_name":          "billingAccounts/X/anomalies/y",
		"resource_name":         "projects/example-app-prd",
		"detection_date":        "2026-07-24",
		"actual_spend_amount":   float64(900),
		"expected_spend_amount": "600",
		"root_causes": []any{
			map[string]any{
				"resource":    "services/CCD8-9BF1-090E",
				"cause_type":  "CAUSE_TYPE_SERVICE",
				"sub_causes":  []any{map[string]any{"resource": "skus/AAAA-BBBB-CCCC"}},
				"displayName": "Kubernetes Engine",
			},
		},
	}
	events, err := decodeCostAnomaly(payload, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]
	if ev.Source != "kubernetes-engine" {
		t.Errorf("source = %q, want kubernetes-engine", ev.Source)
	}
	// +50% is the warning threshold, and the absolute deviation (300) is below the
	// critical floor.
	if ev.Severity != severityWarning {
		t.Errorf("severity = %q, want warning", ev.Severity)
	}
	if !strings.Contains(ev.Message, "example-app-prd") {
		t.Errorf("message %q missing the project", ev.Message)
	}
}

func TestDecodeCostAnomalyWithoutServiceRootCause(t *testing.T) {
	payload := map[string]any{
		"anomalyName":         "billingAccounts/X/anomalies/y",
		"resourceName":        "projects/example-project-0000",
		"actualSpendAmount":   float64(120),
		"expectedSpendAmount": float64(100),
	}
	events, err := decodeCostAnomaly(payload, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]
	if ev.Source != catchAllCostSlug {
		t.Errorf("source = %q, want %q", ev.Source, catchAllCostSlug)
	}
	// +20% and +20 absolute: below every threshold.
	if ev.Severity != severityInfo {
		t.Errorf("severity = %q, want info", ev.Severity)
	}
	// No resourceDisplayName in the payload, so the project appears as its id.
	if !strings.Contains(ev.Message, "example-project-0000") {
		t.Errorf("message %q did not name the project", ev.Message)
	}
}

// An anomaly on a service that was costing nothing has an infinite deviation
// percentage, and Cloud Billing renders that double the protojson way: as the
// string "Infinity". strconv.ParseFloat accepts it, so it used to land in the
// event body as +Inf, where json.Marshal rejected the whole event — the function
// answered 500 and Pub/Sub redelivered the same poison message indefinitely.
//
// The percentage is dropped and the event still ships, carrying the absolute
// deviation that is the real signal when expected spend was zero.
func TestDecodeCostAnomalyWithNonFinitePercentage(t *testing.T) {
	for _, reported := range []string{"Infinity", "-Infinity", "NaN"} {
		t.Run(reported, func(t *testing.T) {
			payload := map[string]any{
				"anomalyName":         "billingAccounts/0X0X0X-0X0X0X-0X0X0X/anomalies/9c2ea1f4",
				"resourceName":        "projects/example-project-0000",
				"detectionDate":       "2026-07-30",
				"expectedSpendAmount": map[string]any{"currencyCode": "USD", "units": "0", "nanos": 0},
				"actualSpendAmount":   map[string]any{"currencyCode": "USD", "units": "412", "nanos": 0},
				"deviationPercentage": reported,
				"rootCauses": []any{
					map[string]any{"resource": "services/FC7A-991E-1531", "causeType": "CAUSE_TYPE_SERVICE"},
				},
			}
			events, err := decodeCostAnomaly(payload, nil)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			ev := events[0]

			// The event must be encodable — this is the regression.
			if _, err := json.Marshal(ev); err != nil {
				t.Fatalf("marshal event: %v", err)
			}
			if _, ok := ev.Raw["deviationPercentReported"]; ok {
				t.Errorf("raw.deviationPercentReported = %v, want the field absent",
					ev.Raw["deviationPercentReported"])
			}
			if _, ok := ev.Raw["deviationPercentComputed"]; ok {
				t.Errorf("raw.deviationPercentComputed = %v, want the field absent",
					ev.Raw["deviationPercentComputed"])
			}
			if ev.Source != "places-aggregate-api" {
				t.Errorf("source = %q, want places-aggregate-api", ev.Source)
			}
			// 412 above a zero forecast: past the absolute warning floor, below critical.
			if ev.Severity != severityWarning {
				t.Errorf("severity = %q, want warning", ev.Severity)
			}
			if strings.Contains(ev.Message, "Inf") || strings.Contains(ev.Message, "NaN") {
				t.Errorf("message %q leaked a non-finite number", ev.Message)
			}
			// The unusable percentage is still on the record, in its original spelling.
			anomaly, _ := ev.Raw["anomaly"].(map[string]any)
			if anomaly["deviationPercentage"] != reported {
				t.Errorf("raw.anomaly lost the reported percentage: %v", anomaly["deviationPercentage"])
			}
		})
	}
}

// toNumber is the single funnel every amount passes through, so the non-finite
// spellings are rejected there once for every field and both decoders.
func TestToNumberRejectsNonFinite(t *testing.T) {
	for _, v := range []any{"Infinity", "-Infinity", "NaN", "inf", "+Inf", "nan",
		map[string]any{"units": "Infinity", "currencyCode": "USD"}} {
		if got, _, ok := toNumber(v); ok {
			t.Errorf("toNumber(%v) = %v, true; want not-ok", v, got)
		}
	}
	// Finite values still decode, including the Money shape.
	if got, cur, ok := toNumber(map[string]any{"units": "310", "nanos": float64(500000000), "currencyCode": "USD"}); !ok || got != 310.5 || cur != "USD" {
		t.Errorf("toNumber(Money) = %v, %q, %v", got, cur, ok)
	}
}

func TestDecodeCostAnomalyRejectsUnrelatedPayload(t *testing.T) {
	if _, err := decodeCostAnomaly(map[string]any{"hello": "world"}, nil); err == nil {
		t.Fatal("want an error for a payload with no anomaly identity")
	}
}

func TestDecodeServiceHealth(t *testing.T) {
	events, err := decodeServiceHealth(loadPayload(t, "servicehealth.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Two impacted products → one event each, so both graph nodes see it.
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	gotSources := []string{events[0].Source, events[1].Source}
	want := []string{"bigquery", "cloud-dataflow"}
	for i := range want {
		if gotSources[i] != want[i] {
			t.Errorf("source[%d] = %q, want %q", i, gotSources[i], want[i])
		}
	}
	ev := events[0]
	if ev.Type != "ProviderIncident" {
		t.Errorf("type = %q, want ProviderIncident", ev.Type)
	}
	if ev.provider != "gcp-status" {
		t.Errorf("provider = %q, want gcp-status", ev.provider)
	}
	// relevance IMPACTED is confirmed impact on our resources.
	if ev.Severity != severityCritical {
		t.Errorf("severity = %q, want critical", ev.Severity)
	}
	for _, want := range []string{"Elevated query failure rates", "us-east1", "CONFIRMED", "IMPACTED"} {
		if !strings.Contains(ev.Message, want) {
			t.Errorf("message %q missing %q", ev.Message, want)
		}
	}
	if got := ev.Raw["insertId"]; got != "1a2b3c4d5e" {
		t.Errorf("raw.insertId = %v", got)
	}
	// Each event carries its own resolved service, not the other's.
	if events[0].Raw["service"] == events[1].Raw["service"] {
		t.Error("both events carry the same raw.service")
	}
}

func TestDecodeServiceHealthClosed(t *testing.T) {
	payload := loadPayload(t, "servicehealth.json")
	entry := payload["jsonPayload"].(map[string]any)
	entry["state"] = "CLOSED"
	entry["detailedState"] = "RESOLVED"

	events, err := decodeServiceHealth(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, ev := range events {
		if ev.Type != "ProviderIncidentResolved" {
			t.Errorf("type = %q, want ProviderIncidentResolved", ev.Type)
		}
		if ev.Severity != severityInfo {
			t.Errorf("severity = %q, want info", ev.Severity)
		}
	}
}

func TestDecodeServiceHealthUnknownProduct(t *testing.T) {
	payload := map[string]any{
		"jsonPayload": map[string]any{
			"@type":            "type.googleapis.com/google.cloud.servicehealth.logging.v1.EventLog",
			"category":         "INCIDENT",
			"title":            "Disruption in a product we do not bill for",
			"state":            "ACTIVE",
			"relevance":        "RELATED",
			"impactedProducts": []any{"Cloud Deploy"},
		},
	}
	events, err := decodeServiceHealth(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 || events[0].Source != catchAllHealthSlug {
		t.Fatalf("got %+v, want a single %s event", events, catchAllHealthSlug)
	}
	if events[0].Severity != severityWarning {
		t.Errorf("severity = %q, want warning for RELATED", events[0].Severity)
	}
}

func TestDecodeServiceHealthRejectsUnrelatedPayload(t *testing.T) {
	if _, err := decodeServiceHealth(map[string]any{"jsonPayload": map[string]any{"message": "hi"}}); err == nil {
		t.Fatal("want an error when no EventLog is present")
	}
}

func TestSlugForKey(t *testing.T) {
	cases := map[string]string{
		"services/24E6-581D-38E5":            "bigquery", // billing service id
		"services/24e6-581d-38e5":            "bigquery", // ...case-insensitively
		"BigQuery":                           "bigquery", // billing display name
		"Google BigQuery":                    "bigquery", // service health product name
		"Google Kubernetes Engine":           "kubernetes-engine",
		"CloudSQL":                           "cloud-sql", // run-together health product name
		"Cloud Pub/Sub":                      "cloud-pubsub",
		"Google Cloud Dataflow":              "cloud-dataflow",
		"Compute Engine":                     "compute-engine",
		"services/CCD8-9BF1-090E":            "kubernetes-engine",
		"Cloud Key Management Service (KMS)": "cloud-kms",
	}
	for key, want := range cases {
		got, ok := slugForKey(key)
		if !ok {
			t.Errorf("slugForKey(%q): not resolved", key)
			continue
		}
		if got != want {
			t.Errorf("slugForKey(%q) = %q, want %q", key, got, want)
		}
	}
	if _, ok := slugForKey("Some Product That Does Not Exist"); ok {
		t.Error("unknown product resolved")
	}
}

// Every slug the function can emit must exist as a node in the tenant's service
// graph, or the event resolves to nothing downstream.
func TestCatchAllSlugsAreInTheCatalog(t *testing.T) {
	for _, slug := range []string{catchAllCostSlug, catchAllHealthSlug} {
		if _, ok := serviceDisplayBySlug[slug]; !ok {
			t.Errorf("catch-all %q is not in the generated catalog", slug)
		}
	}
	for key, slug := range serviceSlugByKey {
		if _, ok := serviceDisplayBySlug[slug]; !ok {
			t.Errorf("key %q maps to unknown slug %q", key, slug)
		}
	}
}

func TestTopicFromSource(t *testing.T) {
	cases := map[string]string{
		"//pubsub.googleapis.com/projects/example-project-0000/topics/cost-anomalies-alerts": "cost-anomalies-alerts",
		"//pubsub.googleapis.com/projects/p/topics/gcp-service-health-alerts":                "gcp-service-health-alerts",
		"cost-anomalies-alerts": "cost-anomalies-alerts",
	}
	for src, want := range cases {
		if got := topicFromSource(src); got != want {
			t.Errorf("topicFromSource(%q) = %q, want %q", src, got, want)
		}
	}
}

func TestDecodeRoutesByTopic(t *testing.T) {
	anomaly, err := os.ReadFile("testdata/anomaly.json")
	if err != nil {
		t.Fatal(err)
	}
	health, err := os.ReadFile("testdata/servicehealth.json")
	if err != nil {
		t.Fatal(err)
	}

	events, err := decode("cost-anomalies-alerts", anomaly, anomalyAttributes())
	if err != nil || len(events) != 1 || events[0].Type != "CostAnomaly" {
		t.Fatalf("cost topic: %+v, %v", events, err)
	}
	events, err = decode("gcp-service-health-alerts", health, nil)
	if err != nil || len(events) != 2 {
		t.Fatalf("health topic: %+v, %v", events, err)
	}

	// A topic renamed in the console must not silently stop producing events:
	// routing falls back to the payload shape.
	events, err = decode("something-else", health, nil)
	if err != nil || len(events) != 2 {
		t.Fatalf("sniffed health payload: %+v, %v", events, err)
	}
}

func TestDecodeRejectsNonJSON(t *testing.T) {
	if _, err := decode("cost-anomalies-alerts", []byte("not json"), nil); err == nil {
		t.Fatal("want an error for a non-JSON payload")
	}
}

func TestCostSeverityThresholds(t *testing.T) {
	cases := []struct {
		name       string
		percent    float64
		deviation  float64
		wantResult string
	}{
		{"below everything", 10, 20, severityInfo},
		{"percent warning", 60, 20, severityWarning},
		{"absolute warning", 5, 250, severityWarning},
		{"percent critical", 250, 20, severityCritical},
		{"absolute critical", 5, 5000, severityCritical},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := costSeverity(tc.percent, true, tc.deviation, true); got != tc.wantResult {
				t.Errorf("costSeverity(%v, %v) = %q, want %q", tc.percent, tc.deviation, got, tc.wantResult)
			}
		})
	}

	t.Setenv("COST_CRITICAL_PERCENT", "20")
	if got := costSeverity(25, true, 0, false); got != severityCritical {
		t.Errorf("with COST_CRITICAL_PERCENT=20, got %q, want critical", got)
	}
}

func TestSendRetriesServerErrors(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := atomic.AddInt32(&calls, 1); n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Tenant"); got != "gcp" {
			t.Errorf("X-Tenant = %q, want gcp", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q", got)
		}
		if !strings.HasSuffix(r.URL.Path, "/events/gcp-cost/") {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &triageClient{baseURL: srv.URL, token: "secret", tenant: "gcp", http: srv.Client()}
	if err := c.send(context.Background(), triageEvent{provider: "gcp-cost", Source: "bigquery"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("server calls = %d, want 2 (one retry)", got)
	}
}

// A 4xx repeats identically on redelivery, so it is dropped rather than nacked
// into a retry loop.
func TestSendDropsClientErrors(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := &triageClient{baseURL: srv.URL, token: "bad", tenant: "gcp", http: srv.Client()}
	if err := c.send(context.Background(), triageEvent{provider: "gcp-cost", Source: "bigquery"}); err != nil {
		t.Fatalf("send returned %v, want nil (dropped)", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("server calls = %d, want 1 (no retry)", got)
	}
}

// An event that cannot be encoded cannot be encoded on redelivery either, so it
// is dropped instead of nacked — a nack here is a 500 that Pub/Sub retries
// forever against a message no attempt can get past.
func TestSendDropsUnencodableEvent(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &triageClient{baseURL: srv.URL, token: "secret", tenant: "gcp", http: srv.Client()}
	ev := triageEvent{provider: "gcp-cost", Source: "places-aggregate-api",
		Raw: map[string]any{"deviationPercentReported": math.Inf(1)}}
	if err := c.send(context.Background(), ev); err != nil {
		t.Fatalf("send returned %v, want nil (dropped)", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("server calls = %d, want 0", got)
	}
}

// X-Tenant follows the event: a per-caller alert routed by its service account
// carries its own tenant, and everything else uses the configured one.
func TestSendUsesTheEventTenant(t *testing.T) {
	var (
		mu      sync.Mutex
		tenants []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tenants = append(tenants, r.Header.Get("X-Tenant"))
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := &triageClient{baseURL: srv.URL, token: "secret", tenant: "gcp", http: srv.Client()}
	for _, ev := range []triageEvent{
		{provider: "gcp-alert", Source: "triageagent", tenant: "triage"},
		{provider: "gcp-alert", Source: "vertex-ai"},
	} {
		if err := c.send(context.Background(), ev); err != nil {
			t.Fatalf("send %s: %v", ev.Source, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"triage", "gcp"}
	if len(tenants) != len(want) {
		t.Fatalf("tenants = %v, want %v", tenants, want)
	}
	for i, w := range want {
		if tenants[i] != w {
			t.Errorf("request %d X-Tenant = %q, want %q", i+1, tenants[i], w)
		}
	}
}

func TestProjectLabel(t *testing.T) {
	// No display name in the reference means the bare id.
	cases := map[string]string{
		"projects/example-data-prod":  "example-data-prod",
		"projects/not-a-real-project": "not-a-real-project",
	}
	for ref, want := range cases {
		if got := projectLabel(ref, ""); got != want {
			t.Errorf("projectLabel(%q) = %q, want %q", ref, got, want)
		}
	}
	if got := projectLabel("projects/123456", "My Project"); got != "My Project (123456)" {
		t.Errorf("projectLabel with explicit display name = %q", got)
	}
}

func TestProjectID(t *testing.T) {
	cases := map[string]string{
		"projects/example-project": "example-project",
		"projects/Example-Project": "example-project",
		"projects/ ":               "",
		"projects/":                "",
	}
	for ref, want := range cases {
		if got := projectID(ref); got != want {
			t.Errorf("projectID(%q) = %q, want %q", ref, got, want)
		}
	}
}

// The display list and the join key are two separate fields built from one walk.
// A consumer attributing spend to an owning team keys on raw.projectIDs, so it
// must carry bare canonical ids and must line up 1:1 with the display list —
// never require unwrapping "Name (id)" to recover an id.
func TestCostAnomalyEmitsCanonicalProjectIDs(t *testing.T) {
	events, err := decodeCostAnomaly(loadPayload(t, "anomaly.json"), anomalyAttributes())
	if err != nil {
		t.Fatalf("decodeCostAnomaly: %v", err)
	}
	ev := events[0]

	if got := ev.Raw["cloud"]; got != "gcp" {
		t.Errorf("raw.cloud = %v, want gcp", got)
	}
	ids, _ := ev.Raw["projectIDs"].([]string)
	labels, _ := ev.Raw["impactedProjects"].([]string)
	if len(ids) == 0 || len(labels) != len(ids) {
		t.Fatalf("impactedProjects (%d) and projectIDs (%d) must stay 1:1 and non-empty", len(labels), len(ids))
	}
	// Asserted structurally rather than against a literal id, so the fixture's
	// project name stays in testdata and out of the source.
	for i, id := range ids {
		// The display list keeps its formatting; the join key never inherits it.
		if strings.ContainsAny(id, "()") {
			t.Errorf("projectIDs[%d] = %q is display-shaped", i, id)
		}
		if id != strings.ToLower(id) {
			t.Errorf("projectIDs[%d] = %q is not canonical", i, id)
		}
		// The bare id is what the display label wraps.
		if !strings.Contains(strings.ToLower(labels[i]), id) {
			t.Errorf("projectIDs[%d] = %q does not match label %q", i, id, labels[i])
		}
	}
}

// A service health event is a Google-side product outage, not spend billed to
// one of our projects, so it declares its cloud but names no owner.
func TestServiceHealthEmitsCloudWithoutProjectIDs(t *testing.T) {
	events, err := decodeServiceHealth(loadPayload(t, "servicehealth.json"))
	if err != nil {
		t.Fatalf("decodeServiceHealth: %v", err)
	}
	for _, ev := range events {
		if got := ev.Raw["cloud"]; got != "gcp" {
			t.Errorf("raw.cloud = %v, want gcp", got)
		}
		if _, ok := ev.Raw["projectIDs"]; ok {
			t.Errorf("service health event must not carry projectIDs: %v", ev.Raw["projectIDs"])
		}
	}
}

func TestParseBracketedList(t *testing.T) {
	got := parseBracketedList("['us-east1', 'australia-southeast2']")
	if len(got) != 2 || got[0] != "us-east1" || got[1] != "australia-southeast2" {
		t.Errorf("parseBracketedList = %v", got)
	}
}

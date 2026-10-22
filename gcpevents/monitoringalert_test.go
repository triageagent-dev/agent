package gcpevents

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

// stubServiceAccountResolver replaces the IAM lookup for one test, so the
// decoder can be exercised without credentials or a network.
func stubServiceAccountResolver(t *testing.T, account serviceAccount, err error) {
	t.Helper()
	previous := resolveServiceAccount
	resolveServiceAccount = func(context.Context, string) (serviceAccount, error) { return account, err }
	t.Cleanup(func() { resolveServiceAccount = previous })
}

// vertexCaller is the service account the per-caller fixture fires for.
func vertexCaller(description string) serviceAccount {
	return serviceAccount{
		Email:       "triage-vertexai@example-project.iam.gserviceaccount.com",
		DisplayName: "Triage Vertex AI caller",
		Description: description,
		UniqueID:    "116448827429795893791",
	}
}

func TestDecodeMonitoringAlertOpen(t *testing.T) {
	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_open.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]

	if ev.Type != "MonitoringAlert" {
		t.Errorf("type = %q, want MonitoringAlert", ev.Type)
	}
	if ev.provider != "gcp-alert" {
		t.Errorf("provider = %q, want gcp-alert", ev.provider)
	}
	// The label says "warning" and the incident is open, so it is honoured either way.
	if ev.Severity != severityWarning {
		t.Errorf("severity = %q, want warning", ev.Severity)
	}
	// aiplatform.googleapis.com -> aiplatform -> vertex ai -> vertex-ai, the same
	// slug the catalog gives billing anomalies on Vertex AI.
	if ev.Source != "vertex-ai" {
		t.Errorf("source = %q, want vertex-ai", ev.Source)
	}
	// Not a per-caller alert, so the tenant stays the client's configured one.
	if ev.tenant != "" {
		t.Errorf("tenant = %q, want empty", ev.tenant)
	}
	if !strings.Contains(ev.Message, "observed=56M") || strings.Contains(ev.Message, "caller=") {
		t.Errorf("message %q, want no caller= segment", ev.Message)
	}
	for _, want := range []string{
		"vertex-ai-token-spike",
		"above the threshold of 5M",
		"observed=56M threshold=5M",
		"https://console.cloud.google.com/monitoring/alerting/incidents/0.abc",
	} {
		if !strings.Contains(ev.Message, want) {
			t.Errorf("message %q missing %q", ev.Message, want)
		}
	}
	if got := ev.Raw["service"]; got != "vertex-ai" {
		t.Errorf("raw.service = %v, want vertex-ai", got)
	}
	if got := ev.Raw["incidentId"]; got != "0.abc" {
		t.Errorf("raw.incidentId = %v, want 0.abc", got)
	}
	if got := ev.Raw["scopingProjectId"]; got != "example-project" {
		t.Errorf("raw.scopingProjectId = %v", got)
	}
	if got := ev.Raw["resourceType"]; got != "aiplatform.googleapis.com/PublisherModel" {
		t.Errorf("raw.resourceType = %v", got)
	}
	if got := ev.Raw["metricType"]; got != "aiplatform.googleapis.com/publisher/online_serving/token_count" {
		t.Errorf("raw.metricType = %v", got)
	}
}

func TestDecodeMonitoringAlertClosed(t *testing.T) {
	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_closed.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]

	if ev.Type != "MonitoringAlertResolved" {
		t.Errorf("type = %q, want MonitoringAlertResolved", ev.Type)
	}
	// No usable severity label on this fixture, so a closed incident defaults to info.
	if ev.Severity != severityInfo {
		t.Errorf("severity = %q, want info", ev.Severity)
	}
	if ev.Source != "vertex-ai" {
		t.Errorf("source = %q, want vertex-ai", ev.Source)
	}
}

// A closed incident is info even when the policy labels itself warning: the
// label describes the condition firing, and carrying it onto the all-clear made
// triage reopen the incident on the very notification that said it recovered.
func TestDecodeMonitoringAlertClosedIgnoresPolicySeverityLabel(t *testing.T) {
	payload := loadPayload(t, "monitoring_alert_closed.json")
	incident := payload["incident"].(map[string]any)
	incident["policy_user_labels"] = map[string]any{"severity": "warning"}

	events, err := decodeMonitoringAlert(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ev := events[0]; ev.Severity != severityInfo || ev.Type != "MonitoringAlertResolved" {
		t.Errorf("closed alert with severity=warning label: type=%q severity=%q, want MonitoringAlertResolved/info", ev.Type, ev.Severity)
	}
}

// A per-caller alert is routed by its service account's own description: the
// tenant it names is the event's tenant, and the vapp it names is the source.
func TestDecodeMonitoringAlertPerCallerRoutesByDescription(t *testing.T) {
	stubServiceAccountResolver(t, vertexCaller("Per-tenant Vertex AI caller tenant=triage vapp=triageagent"), nil)

	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_calls_per_caller.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]

	if ev.Type != "MonitoringAlert" {
		t.Errorf("type = %q, want MonitoringAlert", ev.Type)
	}
	if ev.Severity != severityWarning {
		t.Errorf("severity = %q, want warning", ev.Severity)
	}
	if ev.tenant != "triage" {
		t.Errorf("tenant = %q, want triage", ev.tenant)
	}
	if ev.Source != "triageagent" {
		t.Errorf("source = %q, want triageagent", ev.Source)
	}

	want := "vertex-ai-calls-per-caller: Vertex AI API calls per caller is above the threshold of 300 " +
		"with a value of 412. caller=triage-vertexai observed=412 threshold=300 " +
		"https://console.cloud.google.com/monitoring/alerting/incidents/0.def?project=example-project"
	if ev.Message != want {
		t.Errorf("message =\n%q\nwant\n%q", ev.Message, want)
	}

	for key, want := range map[string]any{
		"service":             "vertex-ai", // still the API that was called
		"credentialId":        "serviceaccount:116448827429795893791",
		"callerKind":          callerKindServiceAccount,
		"callerName":          "triage-vertexai",
		"serviceAccountEmail": "triage-vertexai@example-project.iam.gserviceaccount.com",
		"callerTenant":        "triage",
		"callerVapp":          "triageagent",
		"resourceType":        "consumed_api",
		"metricType":          "serviceruntime.googleapis.com/api/request_count",
	} {
		if got := ev.Raw[key]; got != want {
			t.Errorf("raw.%s = %v, want %v", key, got, want)
		}
	}
}

// tenant= without vapp= routes to the tenant under the service account's own
// name, which is what a caller with no virtual app of its own is.
func TestDecodeMonitoringAlertPerCallerTenantWithoutVapp(t *testing.T) {
	stubServiceAccountResolver(t, vertexCaller("tenant=triage"), nil)

	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_calls_per_caller.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]

	if ev.tenant != "triage" {
		t.Errorf("tenant = %q, want triage", ev.tenant)
	}
	if ev.Source != "triage-vertexai" {
		t.Errorf("source = %q, want the service account name", ev.Source)
	}
	if _, ok := ev.Raw["callerVapp"]; ok {
		t.Errorf("raw.callerVapp = %v, want absent", ev.Raw["callerVapp"])
	}
}

// An account that names no tenant is not a routing failure: the event keeps the
// shape every other monitoring alert has — the API as the source, the
// configured tenant — with the caller preserved in the message and in raw.
func TestDecodeMonitoringAlertPerCallerWithoutRoutingTokens(t *testing.T) {
	stubServiceAccountResolver(t, vertexCaller("Just a service account"), nil)

	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_calls_per_caller.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]

	if ev.tenant != "" {
		t.Errorf("tenant = %q, want empty (the client's configured tenant)", ev.tenant)
	}
	if ev.Source != "vertex-ai" {
		t.Errorf("source = %q, want the API slug vertex-ai", ev.Source)
	}
	if !strings.Contains(ev.Message, " caller=triage-vertexai observed=412") {
		t.Errorf("message %q missing the caller", ev.Message)
	}
	for key, want := range map[string]any{
		"callerName":          "triage-vertexai",
		"serviceAccountEmail": "triage-vertexai@example-project.iam.gserviceaccount.com",
	} {
		if got := ev.Raw[key]; got != want {
			t.Errorf("raw.%s = %v, want %v", key, got, want)
		}
	}
	for _, key := range []string{"callerTenant", "callerVapp"} {
		if _, ok := ev.Raw[key]; ok {
			t.Errorf("raw.%s = %v, want absent", key, ev.Raw[key])
		}
	}
}

// An OAuth client id names a person or a tool. Nothing on it says where its
// events belong, so the alert stays with the API.
func TestDecodeMonitoringAlertPerCallerOAuth2(t *testing.T) {
	stubServiceAccountResolver(t, serviceAccount{}, fmt.Errorf("must not be called for an oauth2 credential"))

	payload := loadPayload(t, "monitoring_alert_calls_per_caller.json")
	labels := payload["incident"].(map[string]any)["resource"].(map[string]any)["labels"].(map[string]any)
	labels["credential_id"] = "oauth2:32555940559.apps.googleusercontent.com"

	events, err := decodeMonitoringAlert(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]

	if ev.tenant != "" {
		t.Errorf("tenant = %q, want empty", ev.tenant)
	}
	if ev.Source != "vertex-ai" {
		t.Errorf("source = %q, want vertex-ai", ev.Source)
	}
	if got := ev.Raw["callerKind"]; got != callerKindOAuth2 {
		t.Errorf("raw.callerKind = %v, want %v", got, callerKindOAuth2)
	}
	if got := ev.Raw["callerName"]; got != oauth2CallerName {
		t.Errorf("raw.callerName = %v, want %v", got, oauth2CallerName)
	}
	if got := ev.Raw["credentialId"]; got != "oauth2:32555940559.apps.googleusercontent.com" {
		t.Errorf("raw.credentialId = %v", got)
	}
	if _, ok := ev.Raw["serviceAccountEmail"]; ok {
		t.Errorf("raw.serviceAccountEmail = %v, want absent for an oauth2 caller", ev.Raw["serviceAccountEmail"])
	}
	if !strings.Contains(ev.Message, " caller="+oauth2CallerName+" observed=412") {
		t.Errorf("message %q missing the caller", ev.Message)
	}
}

// An IAM lookup that fails must not cost us the alert: it lands on the API
// under the configured tenant, with the unique id as the caller's name.
func TestDecodeMonitoringAlertPerCallerLookupFailure(t *testing.T) {
	stubServiceAccountResolver(t, serviceAccount{}, fmt.Errorf("iam returned 403: permission denied"))

	events, err := decodeMonitoringAlert(loadPayload(t, "monitoring_alert_calls_per_caller.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := events[0]

	if ev.tenant != "" {
		t.Errorf("tenant = %q, want empty", ev.tenant)
	}
	if ev.Source != "vertex-ai" {
		t.Errorf("source = %q, want vertex-ai", ev.Source)
	}
	if got := ev.Raw["callerName"]; got != "116448827429795893791" {
		t.Errorf("raw.callerName = %v, want the raw unique id", got)
	}
	if _, ok := ev.Raw["serviceAccountEmail"]; ok {
		t.Errorf("raw.serviceAccountEmail = %v, want absent when the lookup failed", ev.Raw["serviceAccountEmail"])
	}
	if !strings.Contains(ev.Message, " caller=116448827429795893791 observed=412") {
		t.Errorf("message %q missing the caller", ev.Message)
	}
}

func TestResolveCaller(t *testing.T) {
	stubServiceAccountResolver(t, serviceAccount{}, fmt.Errorf("must not be called"))

	// Not a per-caller alert: the PublisherModel resource has no credential_id.
	if _, ok := resolveCaller(map[string]any{
		"type":   "aiplatform.googleapis.com/PublisherModel",
		"labels": map[string]any{"model_user_id": "gemini-2.5-flash"},
	}); ok {
		t.Error("a non-consumed_api resource must not resolve a caller")
	}
	// consumed_api without the grouping label is not one either.
	if _, ok := resolveCaller(map[string]any{
		"type":   consumedAPIResourceType,
		"labels": map[string]any{"service": "aiplatform.googleapis.com"},
	}); ok {
		t.Error("consumed_api without credential_id must not resolve a caller")
	}
	// A spelling we do not know is carried through rather than guessed at.
	caller, ok := resolveCaller(map[string]any{
		"type":   consumedAPIResourceType,
		"labels": map[string]any{"credential_id": "someone-new"},
	})
	if !ok || caller.name != "someone-new" || caller.kind != callerKindUnknown || caller.tenant != "" {
		t.Errorf("unknown credential shape = %+v, ok=%v", caller, ok)
	}
}

// The display name is read only for what the description did not already say.
func TestResolveCallerReadsTokensFromDisplayName(t *testing.T) {
	stubServiceAccountResolver(t, serviceAccount{
		Email:       "triage-vertexai@example.iam.gserviceaccount.com",
		DisplayName: "caller tenant=from-display vapp=from-display",
		Description: "tenant=from-description",
	}, nil)

	caller, ok := resolveCaller(map[string]any{
		"type":   consumedAPIResourceType,
		"labels": map[string]any{"credential_id": "serviceaccount:1", "service": "aiplatform.googleapis.com"},
	})
	if !ok {
		t.Fatal("want a caller")
	}
	if caller.tenant != "from-description" {
		t.Errorf("tenant = %q, want the description to win", caller.tenant)
	}
	if caller.vapp != "from-display" {
		t.Errorf("vapp = %q, want the display name to fill the gap", caller.vapp)
	}
}

func TestCallerTokens(t *testing.T) {
	tokens := callerTokens(`Per-tenant Vertex AI caller tenant=triage, vapp="triageagent" team=team-a`)
	for key, want := range map[string]string{"tenant": "triage", "vapp": "triageagent", "team": "team-a"} {
		if got := tokens[key]; got != want {
			t.Errorf("tokens[%q] = %q, want %q", key, got, want)
		}
	}
	if got := len(tokens); got != 3 {
		t.Errorf("parsed %d tokens from prose + pairs, want 3: %v", got, tokens)
	}
	// source= is the events path's spelling of the same thing.
	if got := callerTokens("tenant=triage source=triageagent")[sourceToken]; got != "triageagent" {
		t.Errorf("source token = %q, want triageagent", got)
	}
	if got := len(callerTokens("no pairs here at all")); got != 0 {
		t.Errorf("prose yielded %d tokens, want 0", got)
	}
}

func TestServiceAccountName(t *testing.T) {
	cases := map[string]string{
		"triage-vertexai@example-project.iam.gserviceaccount.com": "triage-vertexai",
		"not-an-email": "not-an-email",
		"":             "",
	}
	for in, want := range cases {
		if got := serviceAccountName(in); got != want {
			t.Errorf("serviceAccountName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecodeMonitoringAlertSeverityFromLabel(t *testing.T) {
	payload := loadPayload(t, "monitoring_alert_open.json")
	incident := payload["incident"].(map[string]any)
	incident["policy_user_labels"].(map[string]any)["severity"] = "critical"

	events, err := decodeMonitoringAlert(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The label wins even though the incident is open, which would otherwise
	// default to warning.
	if events[0].Severity != severityCritical {
		t.Errorf("severity = %q, want critical", events[0].Severity)
	}
}

func TestDecodeMonitoringAlertRejectsMissingIncident(t *testing.T) {
	if _, err := decodeMonitoringAlert(map[string]any{"hello": "world"}); err == nil {
		t.Fatal("want an error when there is no incident object")
	}
}

func TestDecodeMonitoringAlertRejectsEmptyIncident(t *testing.T) {
	if _, err := decodeMonitoringAlert(map[string]any{"incident": map[string]any{"state": "open"}}); err == nil {
		t.Fatal("want an error when the incident has neither policy_name nor summary")
	}
}

func TestResolveMonitoringServiceUnknownProduct(t *testing.T) {
	slug, _ := resolveMonitoringService(
		map[string]any{"type": "somethingnew.googleapis.com/Widget"},
		map[string]any{"type": "somethingnew.googleapis.com/Widget"},
	)
	if slug != catchAllHealthSlug {
		t.Errorf("slug = %q, want catch-all %q", slug, catchAllHealthSlug)
	}
}

func TestApiPrefix(t *testing.T) {
	cases := map[string]string{
		"aiplatform.googleapis.com/PublisherModel":                       "aiplatform",
		"aiplatform.googleapis.com/publisher/online_serving/token_count": "aiplatform",
		"gce_instance": "",
		"":             "",
	}
	for in, want := range cases {
		if got := apiPrefix(in); got != want {
			t.Errorf("apiPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecodeRoutesToMonitoringAlert(t *testing.T) {
	openPayload, err := os.ReadFile("testdata/monitoring_alert_open.json")
	if err != nil {
		t.Fatal(err)
	}

	events, err := decode("monitoring-alerts", openPayload, nil)
	if err != nil || len(events) != 1 || events[0].Type != "MonitoringAlert" {
		t.Fatalf("monitoring topic: %+v, %v", events, err)
	}

	// A monitoring incident delivered to a topic that merely looks like the cost
	// topic must still be recognised by shape, not swallowed by the cost decoder
	// or its error.
	events, err = decode("cost-anomalies-alerts", openPayload, nil)
	if err != nil || len(events) != 1 || events[0].Type != "MonitoringAlert" {
		t.Fatalf("cost topic with incident payload: %+v, %v", events, err)
	}
}

func TestHumanNumber(t *testing.T) {
	cases := map[string]string{
		"56000000": "56M", "5000000": "5M", "1250000": "1.3M", "850000": "850K",
		"12000": "12K", "1200": "1200", "abc": "abc", "": "", "2500000000": "2.5B",
	}
	for in, want := range cases {
		if got := humanNumber(in); got != want {
			t.Errorf("humanNumber(%q) = %q, want %q", in, got, want)
		}
	}
	got := humanizeNumbers("is above the threshold of 5000000 with a value of 56000000.")
	if want := "is above the threshold of 5M with a value of 56M."; got != want {
		t.Errorf("humanizeNumbers = %q, want %q", got, want)
	}
}

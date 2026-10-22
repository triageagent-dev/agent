package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testMapper() mapper {
	return mapper{tenants: map[string]string{"shop": "tenant-a"}, workloadLabels: defaultWorkloadLabels}
}

// TestMapper_Event: the workload comes from the first label present, a
// scraped series' `service` label loses to the container, an OTel job label
// keeps only the service name, and resolved is info saying so.
func TestMapper_Event(t *testing.T) {
	m := testMapper()
	ev, ok := m.event(webhookAlert{Status: "firing", Labels: map[string]string{
		"alertname": "KubePodNotReady", "namespace": "shop", "service": "kube-state-metrics", "container": "payment", "severity": "critical",
	}, Annotations: map[string]string{"summary": "payment not ready"}})
	if !ok || ev.Source != "payment" || ev.Type != "KubePodNotReady" || ev.Severity != "critical" || ev.tenant != "tenant-a" ||
		ev.Message != "KubePodNotReady firing on payment: payment not ready" || ev.Raw["status"] != "firing" {
		t.Fatalf("firing event = %+v", ev)
	}
	ev, ok = m.event(webhookAlert{Status: "resolved", Labels: map[string]string{
		"alertname": "HighErrorRate", "namespace": "shop", "job": "opentelemetry-demo/checkout", "severity": "warning",
	}})
	if !ok || ev.Source != "checkout" || ev.Severity != "info" || !strings.Contains(ev.Message, "resolved") {
		t.Fatalf("resolved event = %+v", ev)
	}
	for _, labels := range []map[string]string{
		{"alertname": "Watchdog", "namespace": "shop"},                   // no workload
		{"alertname": "X", "namespace": "other", "container": "payment"}, // no tenant
		{"namespace": "shop", "container": "payment"},                    // no name
	} {
		if ev, ok := m.event(webhookAlert{Status: "firing", Labels: labels}); ok {
			t.Errorf("labels %v mapped to %+v, want skipped", labels, ev)
		}
	}
	m.fallbackTenant = "tenant-b"
	if ev, ok := m.event(webhookAlert{Labels: map[string]string{"alertname": "X", "namespace": "other", "deployment": "cart"}}); !ok || ev.tenant != "tenant-b" {
		t.Errorf("fallback tenant: %+v %v", ev, ok)
	}
}

type fakeClient struct {
	sent []triageEvent
	err  error
	// failOnce fails the first send of the event with this type, once.
	failOnce string
}

func (f *fakeClient) send(_ context.Context, ev triageEvent) error {
	if f.err != nil {
		return f.err
	}
	if f.failOnce != "" && ev.Type == f.failOnce {
		f.failOnce = ""
		return errors.New("busy")
	}
	f.sent = append(f.sent, ev)
	return nil
}

func TestHandleWebhook(t *testing.T) {
	body := `{"version":"4","status":"firing","alerts":[
	  {"status":"firing","labels":{"alertname":"A","namespace":"shop","container":"payment"}},
	  {"status":"firing","labels":{"alertname":"Watchdog","namespace":"shop"}}]}`
	f := &fakeClient{}
	s := &server{m: testMapper(), client: f}
	rec := httptest.NewRecorder()
	s.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(body)))
	if rec.Code != http.StatusOK || len(f.sent) != 1 || f.sent[0].Source != "payment" {
		t.Fatalf("code=%d sent=%+v", rec.Code, f.sent)
	}

	s.client = &fakeClient{err: errors.New("down")}
	rec = httptest.NewRecorder()
	s.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(body)))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("triage down: code=%d, want 502 so Alertmanager retries", rec.Code)
	}
	rec = httptest.NewRecorder()
	s.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader("{")))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad body: code=%d, want 400", rec.Code)
	}
}

// TestTriageClient_Send: the event goes to /events/<provider>/ with the
// token file's current content and the event's tenant; a 4xx is dropped, a
// 5xx is retried and returned, and so are 429 (the core's queue is full) and
// 408, which pass.
func TestTriageClient_Send(t *testing.T) {
	defer func(d time.Duration) { retryBackoff = d }(retryBackoff)
	retryBackoff = time.Millisecond
	var got []*http.Request
	status := http.StatusAccepted
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r)
		w.WriteHeader(status)
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("sa-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &triageClient{baseURL: srv.URL, provider: "prometheus", tokenFile: tok, http: srv.Client()}
	ev := triageEvent{Type: "A", Message: "m", Severity: "warning", Source: "payment", tenant: "tenant-a"}
	if err := c.send(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	r := got[0]
	if r.URL.Path != "/events/prometheus/" || r.Header.Get("Authorization") != "Bearer sa-token" || r.Header.Get("X-Tenant") != "tenant-a" {
		t.Errorf("request = %s auth=%q tenant=%q", r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Tenant"))
	}
	status = http.StatusForbidden
	if err := c.send(context.Background(), ev); err != nil || len(got) != 2 {
		t.Errorf("4xx: err=%v attempts=%d, want dropped after one", err, len(got)-1)
	}
	status = http.StatusServiceUnavailable
	if err := c.send(context.Background(), ev); err == nil || len(got) != 2+maxAttempts {
		t.Errorf("5xx: err=%v attempts=%d, want %d and an error", err, len(got)-2, maxAttempts)
	}
	for _, busy := range []int{http.StatusTooManyRequests, http.StatusRequestTimeout} {
		status = busy
		before := len(got)
		if err := c.send(context.Background(), ev); err == nil || len(got)-before != maxAttempts {
			t.Errorf("%d: err=%v attempts=%d, want %d attempts and an error so the alert is retried, not dropped", busy, err, len(got)-before, maxAttempts)
		}
	}
}

// A busy core (429) reaches Alertmanager as a 5xx, so it retries the group.
func TestHandleWebhook_BusyCoreAsksForRetry(t *testing.T) {
	defer func(d time.Duration) { retryBackoff = d }(retryBackoff)
	retryBackoff = time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &server{m: testMapper(), client: &triageClient{baseURL: srv.URL, provider: "prometheus", tokenFile: tokenFile, http: srv.Client()}}
	rec := httptest.NewRecorder()
	s.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager",
		strings.NewReader(`{"alerts":[{"status":"firing","labels":{"alertname":"A","namespace":"shop","deployment":"payment"}}]}`)))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502 so Alertmanager retries", rec.Code)
	}
}

// The webhook needs the bearer token: the bridge posts into every mapped
// tenant with its own credentials, so an open webhook lets any pod that can
// reach it inject events.
func TestHandleWebhook_RequiresBearerToken(t *testing.T) {
	const body = `{"alerts":[{"status":"firing","labels":{"alertname":"A","namespace":"shop","deployment":"payment"}}]}`
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, auth := range map[string]*webhookAuth{"file": {tokenFile: tokenFile}} {
		f := &fakeClient{}
		s := &server{m: testMapper(), client: f, auth: auth}
		post := func(authorization string) int {
			req := httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(body))
			if authorization != "" {
				req.Header.Set("Authorization", authorization)
			}
			rec := httptest.NewRecorder()
			s.handleWebhook(rec, req)
			return rec.Code
		}
		for _, bad := range []string{"", "Bearer wrong", "Bearer s3cre", "Bearer s3cret-and-more", "Basic s3cret", "s3cret", "Bearer "} {
			if code := post(bad); code != http.StatusUnauthorized {
				t.Errorf("%s: Authorization %q: code = %d, want 401", name, bad, code)
			}
		}
		if len(f.sent) != 0 {
			t.Fatalf("%s: %d events sent without the token", name, len(f.sent))
		}
		if code := post("Bearer s3cret"); code != http.StatusOK || len(f.sent) != 1 {
			t.Errorf("%s: with the token: code = %d sent = %d, want 200 and 1", name, code, len(f.sent))
		}
	}
	// A token file that cannot be read fails closed.
	s := &server{m: testMapper(), client: &fakeClient{}, auth: &webhookAuth{tokenFile: filepath.Join(t.TempDir(), "missing")}}
	req := httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer ")
	rec := httptest.NewRecorder()
	s.handleWebhook(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unreadable token file: code = %d, want 401", rec.Code)
	}
}

// The bridge refuses to start with no webhook token unless the open webhook
// is asked for explicitly.
func TestWebhookAuthFromEnv(t *testing.T) {
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	if a, err := webhookAuthFromEnv(env(nil)); err == nil || a != nil {
		t.Errorf("no token: auth = %v err = %v, want an error", a, err)
	}
	if a, err := webhookAuthFromEnv(env(map[string]string{"WEBHOOK_INSECURE": "true"})); err != nil || a != nil {
		t.Errorf("insecure: auth = %v err = %v, want no auth and no error", a, err)
	}
	// A literal token in the environment is not a credential source.
	if a, err := webhookAuthFromEnv(env(map[string]string{"WEBHOOK_TOKEN": "x"})); err == nil || a != nil {
		t.Errorf("WEBHOOK_TOKEN only: auth = %v err = %v, want an error", a, err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if a, err := webhookAuthFromEnv(env(map[string]string{"WEBHOOK_TOKEN_FILE": tokenFile, "WEBHOOK_INSECURE": "true"})); err != nil || a == nil {
		t.Errorf("file and insecure: auth = %v err = %v, want the file to win", a, err)
	}
	if _, err := webhookAuthFromEnv(env(map[string]string{"WEBHOOK_TOKEN_FILE": filepath.Join(t.TempDir(), "missing")})); err == nil {
		t.Error("unreadable token file: want an error at start")
	}
}

// One failed alert answers 502 for the group, and Alertmanager sends the whole
// group again. Only the alerts that did not go through are sent on the retry;
// after the window, and for a new state of the same alert, it is sent again.
func TestHandleWebhook_RetryResendsOnlyWhatFailed(t *testing.T) {
	alert := func(name, fingerprint, status string) string {
		return fmt.Sprintf(`{"status":%q,"fingerprint":%q,"startsAt":"2026-01-02T03:04:05Z","labels":{"alertname":%q,"namespace":"shop","deployment":"payment"}}`, status, fingerprint, name)
	}
	group := `{"alerts":[` + alert("A", "fa", "firing") + "," + alert("B", "fb", "firing") + "," + alert("C", "fc", "firing") + `]}`
	now := time.Unix(1_700_000_000, 0)
	f := &fakeClient{failOnce: "B"}
	s := &server{m: testMapper(), client: f, sent: newSentAlerts(2 * time.Minute)}
	s.sent.now = func() time.Time { return now }
	post := func(body string) int {
		rec := httptest.NewRecorder()
		s.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(body)))
		return rec.Code
	}
	types := func() (out string) {
		for _, ev := range f.sent {
			out += ev.Type
		}
		return out
	}
	if code := post(group); code != http.StatusBadGateway || types() != "A" {
		t.Fatalf("first delivery: code = %d sent = %q, want 502 after A", code, types())
	}
	if code := post(group); code != http.StatusOK || types() != "ABC" {
		t.Fatalf("retry: code = %d sent = %q, want 200 and only B and C added", code, types())
	}
	if code := post(group); code != http.StatusOK || types() != "ABC" {
		t.Fatalf("duplicate group within the window: sent = %q, want nothing added", types())
	}
	// The same alert resolving is a new state.
	if post(`{"alerts":[` + alert("A", "fa", "resolved") + `]}`); types() != "ABCA" {
		t.Fatalf("resolved: sent = %q, want A sent again", types())
	}
	// Past the window a repeat of a still-firing alert goes through.
	now = now.Add(3 * time.Minute)
	if post(group); types() != "ABCAABC" {
		t.Fatalf("after the window: sent = %q, want the group sent again", types())
	}
	if len(s.sent.at) != 3 {
		t.Errorf("remembered alerts = %d, want the expired ones pruned (3)", len(s.sent.at))
	}
	// No window: every alert is sent every time.
	f2 := &fakeClient{}
	s2 := &server{m: testMapper(), client: f2, sent: newSentAlerts(0)}
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		s2.handleWebhook(rec, httptest.NewRequest(http.MethodPost, "/alertmanager", strings.NewReader(group)))
	}
	if len(f2.sent) != 6 {
		t.Errorf("dedup off: sent = %d, want 6", len(f2.sent))
	}
}

// Attribution: labels that name the workload outrank `container`, which on a
// scraped series can be the exporter's own; a pod name gives its owner; and
// an alert carrying only scrape labels and no namespace is left unattributed
// instead of being attributed to the scraper.
func TestMapper_WorkloadAttribution(t *testing.T) {
	m := testMapper()
	m.fallbackTenant = "tenant-b"
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   string // "" = unattributed
	}{
		{"deployment outranks the exporter's container", map[string]string{"namespace": "shop", "deployment": "payment", "container": "kube-state-metrics", "service": "kube-state-metrics", "job": "kube-state-metrics"}, "payment"},
		{"statefulset", map[string]string{"namespace": "shop", "statefulset": "db", "container": "exporter"}, "db"},
		{"daemonset", map[string]string{"namespace": "shop", "daemonset": "agent", "pod": "agent-x2kqp"}, "agent"},
		{"pod owner outranks container", map[string]string{"namespace": "shop", "pod": "payment-7f9c8d6b5-x2kqp", "container": "sidecar"}, "payment"},
		{"container when nothing better", map[string]string{"namespace": "shop", "container": "payment"}, "payment"},
		{"scrape labels with a namespace are the last resort", map[string]string{"namespace": "shop", "service": "cart", "job": "cart"}, "cart"},
		{"only scrape labels, no namespace", map[string]string{"service": "kube-state-metrics", "job": "kube-state-metrics"}, ""},
		{"only a job, no namespace", map[string]string{"job": "node-exporter"}, ""},
		{"a workload label needs no namespace", map[string]string{"deployment": "cart", "job": "kube-state-metrics"}, "cart"},
	} {
		tc.labels["alertname"] = "X"
		ev, ok := m.event(webhookAlert{Status: "firing", Labels: tc.labels})
		if got := ev.Source; ok != (tc.want != "") || got != tc.want {
			t.Errorf("%s: source = %q ok = %v, want %q", tc.name, got, ok, tc.want)
		}
	}
	for pod, want := range map[string]string{
		"payment-7f9c8d6b5-x2kqp": "payment",
		"agent-x2kqp":             "agent",
		"db-0":                    "db",
		"my-redis":                "my-redis", // an ordinary word, not a generated suffix
		"standalone":              "standalone",
	} {
		if got := podOwner(pod); got != want {
			t.Errorf("podOwner(%q) = %q, want %q", pod, got, want)
		}
	}
}

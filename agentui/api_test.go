package agentui

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/push"
)

const (
	tokAll = "all-all-all-all-all-all-all-1"
	tokT1  = "t1-t1-t1-t1-t1-t1-t1-t1-t1-1"
	tokT2  = "t2-t2-t2-t2-t2-t2-t2-t2-t2-2"
)

func tokensContent() string {
	return "# scrape tokens\nprom * " + tokAll + "\ngrafana-t1 t1 " + tokT1 + "\ngrafana-t2 t2 " + tokT2 + "\n"
}

func apiFixture(t *testing.T) (*APIServer, *Store, http.Handler) {
	t.Helper()
	srv, st := newTestServer(t, vapp("shop", "checkout", "checkout"), vapp("other", "billing", "billing"))
	srv.o.Log = logr.Discard()
	a, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: keyFile(t, tokensContent())})
	if err != nil {
		t.Fatal(err)
	}
	now := st.now().Unix()
	for _, tn := range []struct{ tenant, ns, svc string }{{"t1", "shop", "checkout"}, {"t2", "other", "billing"}} {
		st.Observe(tn.tenant, &push.Frame{Kind: push.KindRedSummary, RedSummary: &push.RedSummaryPayload{
			WindowStart: now - 60, WindowSec: 60, Services: []push.ServiceRedMetrics{{Namespace: tn.ns, Service: tn.svc, RequestCount: 120, ErrorCount: 12, P99Ms: 250}}}})
		st.Observe(tn.tenant, &push.Frame{Kind: push.KindRedMetrics, RedMetrics: &push.RedMetricsPayload{WindowSec: 60,
			Services: []push.ServiceRedMetrics{{Namespace: tn.ns, Service: tn.svc, Breach: push.BreachErrors}}}})
	}
	st.Observe("t1", &push.Frame{Kind: push.KindTopologySnapshot, TopologySnapshot: &push.TopologySnapshotPayload{
		CapturedAt: now, WindowSec: 60,
		Edges: []push.TopologyEdge{{SrcNamespace: "shop", SrcWorkload: "checkout", External: "api.example:443", Calls: 60, Protocol: "http"}}}})
	st.Observe("", &push.Frame{Kind: push.KindSensorState, SensorState: &push.SensorStatePayload{At: now,
		Sensors: []push.SensorStatus{{Node: "n1", Connected: true, LastSeen: now, Roles: []push.SensorRoleStatus{{Role: "ebpf"}}}},
		OTLP:    []push.OTLPSourceStatus{{Key: "a", Service: "a", Tenant: "t1", LastSeen: now}, {Key: "b", Service: "b", Tenant: "t2", LastSeen: now}}}})
	return a, st, a.Handler()
}

func call(h http.Handler, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "http://agent.example"+path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAPINeedsTokens(t *testing.T) {
	srv, _ := newTestServer(t)
	if _, err := NewAPI(srv, APIOptions{Addr: ":0"}); err == nil {
		t.Error("the API started without a tokens file")
	}
	if _, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: keyFile(t, "x tenant short")}); err == nil {
		t.Error("a short token was accepted")
	}
	if _, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: keyFile(t, "# nothing\n")}); err == nil {
		t.Error("an empty tokens file was accepted")
	}
	if _, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: keyFile(t, "a * "+tokAll+"\na * "+tokT1)}); err == nil {
		t.Error("a repeated token name was accepted")
	}
	if _, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: keyFile(t, tokensContent()), CertFile: "c.pem"}); err == nil {
		t.Error("a certificate without a key was accepted")
	}

	_, _, h := apiFixture(t)
	for _, p := range []string{"/metrics", "/.well-known/triage-agent", "/api/v1/status", "/api/v1/apps", "/api/v1/stream", "/"} {
		if w := call(h, p, ""); w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s without a token = %d", p, w.Code)
		}
		if w := call(h, p, "not-a-token-not-a-token-12345"); w.Code != http.StatusUnauthorized {
			t.Errorf("%s with a wrong token = %d", p, w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "http://agent.example/api/v1/status", nil)
	r.Header.Set("Authorization", "Bearer "+tokAll)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d, want 405", w.Code)
	}
	// The admin surfaces are not on this listener.
	for _, p := range []string{"/api/v1/config", "/api/v1/stats", "/api/v1/login"} {
		if w := call(h, p, tokAll); w.Code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404 on the API listener", p, w.Code)
		}
	}
}

func TestAPITenantScope(t *testing.T) {
	_, _, h := apiFixture(t)

	var apps struct{ Apps []appView }
	if err := json.Unmarshal(call(h, "/api/v1/apps", tokT1).Body.Bytes(), &apps); err != nil || len(apps.Apps) != 1 || apps.Apps[0].Tenant != "t1" {
		t.Errorf("t1 token apps = %+v, %v", apps.Apps, err)
	}
	if w := call(h, "/api/v1/apps?tenant=t2", tokT1); w.Code != http.StatusForbidden {
		t.Errorf("t1 token asking for t2 = %d, want 403", w.Code)
	}
	if err := json.Unmarshal(call(h, "/api/v1/apps?tenant=t1", tokT1).Body.Bytes(), &apps); err != nil || len(apps.Apps) != 1 {
		t.Errorf("t1 token naming its own tenant = %+v, %v", apps.Apps, err)
	}
	if err := json.Unmarshal(call(h, "/api/v1/apps", tokAll).Body.Bytes(), &apps); err != nil || len(apps.Apps) != 2 {
		t.Errorf("unrestricted token apps = %d, want 2", len(apps.Apps))
	}
	if err := json.Unmarshal(call(h, "/api/v1/apps?tenant=t2", tokAll).Body.Bytes(), &apps); err != nil || len(apps.Apps) != 1 || apps.Apps[0].Tenant != "t2" {
		t.Errorf("unrestricted token choosing t2 = %+v, %v", apps.Apps, err)
	}

	var tn struct{ Tenants []struct{ Name string } }
	_ = json.Unmarshal(call(h, "/api/v1/tenants", tokT1).Body.Bytes(), &tn)
	if len(tn.Tenants) != 1 || tn.Tenants[0].Name != "t1" {
		t.Errorf("t1 token sees tenants %+v", tn.Tenants)
	}
	var st struct {
		Tenants []string
		Link    map[string]any
	}
	_ = json.Unmarshal(call(h, "/api/v1/status", tokT1).Body.Bytes(), &st)
	if len(st.Tenants) != 1 || st.Link["host"] != nil || st.Link["sent"] != nil {
		t.Errorf("t1 token status = %+v: it must not see other tenants or the core address", st)
	}
	var ev struct{ Events []Event }
	_ = json.Unmarshal(call(h, "/api/v1/events", tokT1).Body.Bytes(), &ev)
	for _, e := range ev.Events {
		if e.Tenant != "t1" {
			t.Errorf("t1 token got an event of %q", e.Tenant)
		}
	}
	var src struct{ Sources []source }
	_ = json.Unmarshal(call(h, "/api/v1/sources", tokT1).Body.Bytes(), &src)
	for _, s := range src.Sources {
		if s.Kind == "ebpf" || s.Name == "b" {
			t.Errorf("t1 token sees source %+v", s)
		}
	}
	if len(src.Sources) != 2 { // k8s and OTLP "a"
		t.Errorf("t1 token sources = %d, want 2", len(src.Sources))
	}

	var wk struct {
		Scope   struct{ Restricted bool }
		Tenants []string
	}
	_ = json.Unmarshal(call(h, "/.well-known/triage-agent", tokT2).Body.Bytes(), &wk)
	if !wk.Scope.Restricted || len(wk.Tenants) != 1 || wk.Tenants[0] != "t2" {
		t.Errorf("well-known for the t2 token = %+v", wk)
	}
}

func TestMetricsScopeAndLeaderOnly(t *testing.T) {
	a, st, h := apiFixture(t)

	all := call(h, "/metrics", tokAll).Body.String()
	for _, want := range []string{
		`triage_agent_info{cluster="c1",mode="cluster",role="",version=""} 1`,
		`triage_agent_core_link_connected 1`,
		`triage_agent_service_requests_per_second{namespace="shop",service="checkout",tenant="t1"} 2`,
		`triage_agent_service_error_ratio{namespace="other",service="billing",tenant="t2"} 0.1`,
		`triage_agent_service_latency_p99_seconds{namespace="shop",service="checkout",tenant="t1"} 0.25`,
		`triage_agent_app_ready{app="checkout",namespace="shop",tenant="t1"} 1`,
		`triage_agent_edge_calls_per_second{dst_namespace="",dst_workload="",external="api.example:443",protocol="http",src_namespace="shop",src_workload="checkout",tenant="t1"} 1`,
		`triage_agent_events_total{family="red",tenant="t2"} 1`,
		`triage_agent_source_up{kind="ebpf",name="ebpf",node="n1",tenant=""} 1`,
		`triage_agent_sensor_nodes 1`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("unrestricted /metrics lacks %s", want)
		}
	}

	t1 := call(h, "/metrics", tokT1).Body.String()
	for _, bad := range []string{`tenant="t2"`, `triage_agent_core_link_connected`, `kind="ebpf"`, `triage_agent_sensor_nodes`, `frames_sent_total`} {
		if strings.Contains(t1, bad) {
			t.Errorf("t1 token /metrics contains %s", bad)
		}
	}
	if !strings.Contains(t1, `service="checkout",tenant="t1"`) {
		t.Errorf("t1 token /metrics has no t1 series:\n%s", t1)
	}
	if w := call(h, "/metrics?tenant=t2", tokT1); w.Code != http.StatusForbidden {
		t.Errorf("t1 token asking for t2 metrics = %d", w.Code)
	}

	// A follower exports the info series alone.
	rep := NewReplica(fakeLink{apps: a.s.o.Link.Apps()}, st, "b", func([]byte) {}, logr.Discard())
	a.s.o.Link = rep
	fol := call(a.Handler(), "/metrics", tokAll).Body.String()
	if !strings.Contains(fol, `role="follower"`) || strings.Contains(fol, "triage_agent_service_") || strings.Contains(fol, "triage_agent_app_ready") {
		t.Errorf("a follower exported signals:\n%s", fol)
	}
}

func TestTokensReloadOnChange(t *testing.T) {
	srv, _ := newTestServer(t)
	f := keyFile(t, "one * "+tokAll+"\n")
	a, err := NewAPI(srv, APIOptions{Addr: ":0", TokensFile: f})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Handler()
	if w := call(h, "/api/v1/status", tokT1); w.Code != 401 {
		t.Fatalf("unknown token = %d", w.Code)
	}
	if err := os.WriteFile(f, []byte("one * "+tokAll+"\ntwo t1 "+tokT1+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(f, later, later)
	if w := call(h, "/api/v1/status", tokT1); w.Code != 200 {
		t.Errorf("a token added to the file = %d, want 200 without a restart", w.Code)
	}
	// A broken reload keeps the tokens already loaded.
	if err := os.WriteFile(f, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	later = later.Add(2 * time.Second)
	_ = os.Chtimes(f, later, later)
	if w := call(h, "/api/v1/status", tokT1); w.Code != 200 {
		t.Errorf("after a broken file = %d, want the loaded tokens to stay", w.Code)
	}
	_ = io.Discard
}

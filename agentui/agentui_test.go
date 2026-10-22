package agentui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/push"
)

type fakeLink struct{ apps []*push.VirtualApp }

func (f fakeLink) Apps() []*push.VirtualApp { return f.apps }
func (f fakeLink) Link() push.Link          { return push.Link{Connected: true, Host: "core:443"} }
func (f fakeLink) Host() bool               { return false }
func (f fakeLink) AppTenants() map[string]string {
	return map[string]string{"shop": "t1", "other": "t2"}
}

func vapp(ns, name string, comps ...string) *push.VirtualApp {
	a := &push.VirtualApp{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name,
		Annotations: map[string]string{"triageagent.dev/tier": "1"}}}
	for _, c := range comps {
		a.Status.Objects = append(a.Status.Objects, push.ObjectStatus{Name: c, Kind: "Deployment", Status: "Ready"})
	}
	a.Status.Conditions = []push.Condition{{Type: "Ready", Status: corev1.ConditionTrue}}
	return a
}

func newTestServer(t *testing.T, apps ...*push.VirtualApp) (*Server, *Store) {
	t.Helper()
	st := NewStore()
	srv, err := New(Options{Addr: "127.0.0.1:0", Store: st, Link: fakeLink{apps}, Mode: "cluster", Cluster: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	return srv, st
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8088"+path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestNewRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{":8088", "0.0.0.0:8088", "10.0.0.1:8088", "example.com:80", "nope"} {
		if _, err := New(Options{Addr: addr, Store: NewStore()}); err == nil {
			t.Errorf("New(%q) accepted a non-loopback address", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:8088", "[::1]:8088", "localhost:8088"} {
		if _, err := New(Options{Addr: addr, Store: NewStore()}); err != nil {
			t.Errorf("New(%q): %v", addr, err)
		}
	}
}

func TestGuard(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d, want 405", w.Code)
	}

	r = httptest.NewRequest(http.MethodGet, "http://evil.example/api/v1/status", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Errorf("foreign Host = %d, want 403", w.Code)
	}

	if w := get(t, h, "/api/v1/status"); w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Errorf("status = %d, csp %q", w.Code, w.Header().Get("Content-Security-Policy"))
	}
}

func TestAppsJoinTrafficAndEdges(t *testing.T) {
	srv, st := newTestServer(t, vapp("shop", "checkout", "checkout", "cart"), vapp("shop", "payment", "payment"))
	h := srv.Handler()

	st.Observe("t1", &push.Frame{Kind: push.KindRedSummary, RedSummary: &push.RedSummaryPayload{
		WindowStart: st.now().Unix() - 60, WindowSec: 60, Services: []push.ServiceRedMetrics{
			{Namespace: "shop", Service: "checkout", RequestCount: 600, ErrorCount: 6, P99Ms: 120},
			{Namespace: "shop", Service: "cart", RequestCount: 300, ErrorCount: 0, P99Ms: 40},
		}}})
	st.Observe("t1", &push.Frame{Kind: push.KindTopologySnapshot, TopologySnapshot: &push.TopologySnapshotPayload{
		CapturedAt: st.now().Unix(), WindowSec: 60,
		Nodes: []push.TopologyNode{{Namespace: "shop", Workload: "checkout"}},
		Edges: []push.TopologyEdge{
			{SrcNamespace: "shop", SrcWorkload: "checkout", DstNamespace: "shop", DstWorkload: "payment", Calls: 120, Protocol: "grpc"},
			{SrcNamespace: "shop", SrcWorkload: "checkout", External: "api.stripe.com:443", Calls: 6, Protocol: "http"},
		}}})

	var apps struct {
		Apps []appView `json:"apps"`
	}
	if err := json.Unmarshal(get(t, h, "/api/v1/apps").Body.Bytes(), &apps); err != nil {
		t.Fatal(err)
	}
	if len(apps.Apps) != 2 {
		t.Fatalf("apps = %d, want 2", len(apps.Apps))
	}
	co := apps.Apps[0]
	if co.Name != "checkout" || co.Tier != "1" || co.Ready != "ready" {
		t.Errorf("checkout = %+v", co)
	}
	if co.Traffic == nil || co.Traffic.Req != 900 || co.Traffic.Err != 6 || co.Traffic.RPS != 15 || co.Traffic.P99Ms != 120 {
		t.Errorf("traffic = %+v", co.Traffic)
	}
	if len(co.CallsTo) != 1 || co.CallsTo[0] != "shop/payment" {
		t.Errorf("callsTo = %v", co.CallsTo)
	}
	if p := apps.Apps[1]; len(p.CalledBy) != 1 || p.CalledBy[0] != "shop/checkout" || p.Traffic != nil {
		t.Errorf("payment = %+v", p)
	}

	var g struct {
		Nodes []struct{ Workload, App string } `json:"nodes"`
		Edges []Edge                           `json:"edges"`
	}
	if err := json.Unmarshal(get(t, h, "/api/v1/graph").Body.Bytes(), &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Edges) != 2 || len(g.Nodes) != 2 {
		t.Errorf("graph nodes %d edges %d, want 2 and 2 (external is not a node)", len(g.Nodes), len(g.Edges))
	}
}

func TestEventsAndFamilies(t *testing.T) {
	srv, st := newTestServer(t)
	h := srv.Handler()
	ch, cancel := st.Subscribe()
	defer cancel()

	st.Observe("t1", &push.Frame{Kind: push.KindK8sEvent, K8sEvent: &push.K8sEventPayload{Event: &corev1.Event{
		Type: "Warning", Reason: "BackOff", Message: "restarting",
		InvolvedObject: corev1.ObjectReference{Namespace: "shop", Name: "cart-0"}}}})
	st.Observe("t1", &push.Frame{Kind: push.KindRedMetrics, RedMetrics: &push.RedMetricsPayload{WindowSec: 60,
		Services: []push.ServiceRedMetrics{{Namespace: "shop", Service: "checkout", RequestCount: 10, ErrorCount: 5, Breach: push.BreachErrors}}}})
	st.Observe("", &push.Frame{Kind: push.KindSensorState, SensorState: &push.SensorStatePayload{At: 1,
		Sensors: []push.SensorStatus{{Node: "n1", Connected: true, LastSeen: st.now().Unix(), Roles: []push.SensorRoleStatus{{Role: "ebpf"}, {Role: "log-exporter"}}}},
		OTLP:    []push.OTLPSourceStatus{{Key: "k", Service: "svc", LastSeen: st.now().Unix(), Families: []push.OTLPFamilyStatus{{Family: "red"}}}}}})

	if e := <-ch; e.Family != FamilyK8s || e.Severity != "warning" || e.Namespace != "shop" || e.Service != "cart-0" {
		t.Errorf("first event = %+v", e)
	}

	var ev struct{ Events []Event }
	if err := json.Unmarshal(get(t, h, "/api/v1/events?family=red").Body.Bytes(), &ev); err != nil {
		t.Fatal(err)
	}
	if len(ev.Events) != 1 || ev.Events[0].Severity != "critical" || ev.Events[0].Reason != "red errors" {
		t.Errorf("red events = %+v", ev.Events)
	}

	var src struct{ Sources []source }
	if err := json.Unmarshal(get(t, h, "/api/v1/sources").Body.Bytes(), &src); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]string{}
	for _, s := range src.Sources {
		kinds[s.Kind] = s.Status
	}
	for _, k := range []string{"k8s", "ebpf", "log-exporter", "otlp"} {
		if kinds[k] != "up" {
			t.Errorf("source %s status %q, want up (all: %v)", k, kinds[k], kinds)
		}
	}
}

func TestEventRingIsBounded(t *testing.T) {
	st := NewStoreSized(25)
	for i := 0; i < 80; i++ {
		st.add(Event{Family: FamilyK8s, Reason: "r"})
	}
	if n := len(st.Events("", nil, 0)); n != 25 {
		t.Errorf("kept %d events, want 25", n)
	}
	if ev := st.Events("", nil, 1); ev[0].ID != 80 {
		t.Errorf("newest id = %d, want 80: the newest events are the ones kept", ev[0].ID)
	}
	if n := len(st.Events("", nil, 1000)); n != 25 {
		t.Errorf("a limit over the cap returned %d, want 25", n)
	}
	for in, want := range map[int]int{1: MinMaxEvents, 100: 100, 1 << 20: MaxMaxEvents} {
		if got := NewStoreSized(in).MaxEvents(); got != want {
			t.Errorf("NewStoreSized(%d).MaxEvents() = %d, want %d", in, got, want)
		}
	}
	srv, _ := newTestServer(t)
	srv.o.Store = NewStoreSized(30)
	var status struct{ Limits struct{ Events int } }
	if err := json.Unmarshal(get(t, srv.Handler(), "/api/v1/status").Body.Bytes(), &status); err != nil || status.Limits.Events != 30 {
		t.Errorf("status limits = %+v, %v", status, err)
	}
}

func TestTenantFilter(t *testing.T) {
	srv, st := newTestServer(t, vapp("shop", "checkout", "checkout"), vapp("other", "billing", "billing"))
	h := srv.Handler()
	now := st.now().Unix()
	for _, tn := range []struct{ tenant, ns, svc string }{{"t1", "shop", "checkout"}, {"t2", "other", "billing"}} {
		st.Observe(tn.tenant, &push.Frame{Kind: push.KindRedSummary, RedSummary: &push.RedSummaryPayload{
			WindowStart: now - 60, WindowSec: 60, Services: []push.ServiceRedMetrics{{Namespace: tn.ns, Service: tn.svc, RequestCount: 60}}}})
		st.Observe(tn.tenant, &push.Frame{Kind: push.KindRedMetrics, RedMetrics: &push.RedMetricsPayload{WindowSec: 60,
			Services: []push.ServiceRedMetrics{{Namespace: tn.ns, Service: tn.svc, Breach: push.BreachErrors}}}})
	}
	// t1's checkout calls t2's billing: the callee must not be named to t1.
	st.Observe("t1", &push.Frame{Kind: push.KindTopologySnapshot, TopologySnapshot: &push.TopologySnapshotPayload{
		CapturedAt: now, WindowSec: 60,
		Nodes: []push.TopologyNode{{Namespace: "shop", Workload: "checkout"}},
		Edges: []push.TopologyEdge{{SrcNamespace: "shop", SrcWorkload: "checkout", DstNamespace: "other", DstWorkload: "billing", Calls: 6}}}})
	st.Observe("t2", &push.Frame{Kind: push.KindTopologySnapshot, TopologySnapshot: &push.TopologySnapshotPayload{
		CapturedAt: now, WindowSec: 60, Nodes: []push.TopologyNode{{Namespace: "other", Workload: "billing"}}}})
	st.Observe("", &push.Frame{Kind: push.KindSensorState, SensorState: &push.SensorStatePayload{At: now,
		OTLP: []push.OTLPSourceStatus{
			{Key: "a", Service: "a", Tenant: "t1", LastSeen: now}, {Key: "b", Service: "b", Tenant: "t2", LastSeen: now},
			{Key: "c", Service: "c", LastSeen: now}}}})

	var apps struct{ Apps []appView }
	var svcs struct{ Services []ServiceStat }
	var ev struct{ Events []Event }
	var g struct {
		Nodes []struct{ Workload string }
		Edges []Edge
	}
	var src struct{ Sources []source }
	var tn struct{ Tenants []struct{ Name string } }
	decode := func(path string, v any) {
		t.Helper()
		if err := json.Unmarshal(get(t, h, path).Body.Bytes(), v); err != nil {
			t.Fatal(err)
		}
	}

	decode("/api/v1/apps?tenant=t1", &apps)
	if len(apps.Apps) != 1 || apps.Apps[0].Name != "checkout" || apps.Apps[0].Tenant != "t1" {
		t.Errorf("t1 apps = %+v", apps.Apps)
	}
	if len(apps.Apps[0].CallsTo) != 0 {
		t.Errorf("t1 app lists a callee in another tenant: %v", apps.Apps[0].CallsTo)
	}
	decode("/api/v1/services?tenant=t2", &svcs)
	if len(svcs.Services) != 1 || svcs.Services[0].Service != "billing" || svcs.Services[0].Tenant != "t2" {
		t.Errorf("t2 services = %+v", svcs.Services)
	}
	decode("/api/v1/events?tenant=t1", &ev)
	if len(ev.Events) != 1 || ev.Events[0].Service != "checkout" {
		t.Errorf("t1 events = %+v", ev.Events)
	}
	decode("/api/v1/graph?tenant=t1", &g)
	if len(g.Nodes) != 1 || g.Nodes[0].Workload != "checkout" || len(g.Edges) != 1 ||
		g.Edges[0].DstWorkload != "" || g.Edges[0].External != "other tenant" {
		t.Errorf("t1 graph = %+v", g)
	}
	decode("/api/v1/sources?tenant=t1", &src)
	otlp := map[string]bool{}
	for _, s := range src.Sources {
		if s.Kind == "otlp" {
			otlp[s.Name] = true
		}
	}
	if !otlp["a"] || otlp["b"] || !otlp["c"] {
		t.Errorf("t1 otlp sources = %v, want a and the tenant-less c only", otlp)
	}
	decode("/api/v1/tenants", &tn)
	if len(tn.Tenants) != 2 || tn.Tenants[0].Name != "t1" || tn.Tenants[1].Name != "t2" {
		t.Errorf("tenants = %+v", tn.Tenants)
	}
	decode("/api/v1/apps", &apps)
	if len(apps.Apps) != 2 {
		t.Errorf("unfiltered apps = %d, want 2", len(apps.Apps))
	}
}

func TestConfigAndStats(t *testing.T) {
	srv, st := newTestServer(t, vapp("shop", "checkout", "checkout"))
	srv.o.Config = func() []ConfigEntry { return []ConfigEntry{{Name: "cluster-name", Value: "c1", Changed: true}} }
	srv.o.Peers = func() []string { return []string{"r2"} }
	h := srv.Handler()
	st.add(Event{Family: FamilyK8s})

	var cfg struct{ Flags []ConfigEntry }
	if err := json.Unmarshal(get(t, h, "/api/v1/config").Body.Bytes(), &cfg); err != nil || len(cfg.Flags) != 1 || cfg.Flags[0].Name != "cluster-name" {
		t.Errorf("config = %+v, %v", cfg, err)
	}
	var stats struct {
		Process map[string]any
		Replica struct{ Peers []string }
		Push    struct{ Connected bool }
		View    struct {
			Apps       int
			EventsKept int
		}
	}
	if err := json.Unmarshal(get(t, h, "/api/v1/stats").Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Process["goroutines"] == nil || len(stats.Replica.Peers) != 1 || !stats.Push.Connected || stats.View.Apps != 1 || stats.View.EventsKept != 1 {
		t.Errorf("stats = %+v", stats)
	}
}

func TestConfigGroupsSensors(t *testing.T) {
	srv, _ := newTestServer(t)
	same := []ConfigEntry{{Name: "log-exporter-include-namespaces", Value: "triage", Changed: true}}
	srv.o.Sensors = func() []SensorInfo {
		return []SensorInfo{
			{Node: "n2", Version: "v1", Roles: []string{"log-exporter", "ebpf"}, Connected: true, Settings: same},
			{Node: "n1", Version: "v1", Roles: []string{"ebpf", "log-exporter"}, Connected: true, Settings: same},
			{Node: "old", Version: "v0", Roles: []string{"ebpf"}, Connected: false}, // an older sensor sends no settings
		}
	}
	var cfg struct{ Sensors []sensorGroup }
	if err := json.Unmarshal(get(t, srv.Handler(), "/api/v1/config").Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sensors) != 2 {
		t.Fatalf("groups = %+v, want 2", cfg.Sensors)
	}
	g := cfg.Sensors[0]
	if len(g.Nodes) != 2 || g.Nodes[0] != "n1" || g.Connected != 2 || len(g.Roles) != 2 || g.Roles[0] != "ebpf" || len(g.Flags) != 1 {
		t.Errorf("the two sensors with the same settings are not one group: %+v", g)
	}
	if old := cfg.Sensors[1]; old.Nodes[0] != "old" || old.Connected != 0 || old.Flags == nil || len(old.Flags) != 0 {
		t.Errorf("older sensor group = %+v", old)
	}
}

type noKubeLink struct{ fakeLink }

func (noKubeLink) NoKubernetes() bool { return true }

// TestSources_NoKubernetesNamesTheAppSource keeps the app source from being
// labelled a Kubernetes watch when the agent watches nothing.
func TestSources_NoKubernetesNamesTheAppSource(t *testing.T) {
	s, err := New(Options{Addr: "127.0.0.1:0", Store: NewStore(), Link: noKubeLink{}, Mode: "cluster", Cluster: "c"})
	if err != nil {
		t.Fatal(err)
	}
	list, _ := s.sourceList("", false)
	if len(list) == 0 || list[0].Kind != "apps" || list[0].Name != "OTLP applications" {
		t.Fatalf("first source = %+v, want the OTLP applications source", list[0])
	}
	for _, f := range list[0].Families {
		if f == FamilyK8s {
			t.Errorf("families = %v, a no-watch agent raises no k8s events", list[0].Families)
		}
	}
	s2, err := New(Options{Addr: "127.0.0.1:0", Store: NewStore(), Link: fakeLink{}, Mode: "cluster", Cluster: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if l2, _ := s2.sourceList("", false); l2[0].Name != "Kubernetes watch" {
		t.Errorf("a watching agent's source = %q, want Kubernetes watch", l2[0].Name)
	}
}

func TestStatsListsTheEntriesHeldBackFromAFrame(t *testing.T) {
	srv, _ := newTestServer(t)
	ledger.Default.Reset()
	t.Cleanup(ledger.Default.Reset)
	ledger.Default.Drop("red_summary", "/cart", "no k8s.namespace.name on the source")

	var out struct {
		Drops []ledger.Drop
		Push  map[string]any
	}
	rec := get(t, srv.Handler(), "/api/v1/stats")
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%v: %s", err, rec.Body.String())
	}
	if len(out.Drops) != 1 || out.Drops[0].Subject != "/cart" || out.Drops[0].Frame != "red_summary" {
		t.Errorf("drops = %+v", out.Drops)
	}
	if _, ok := out.Push["malformedDropped"]; !ok {
		t.Errorf("push stats carry no malformedDropped: %v", out.Push)
	}
}

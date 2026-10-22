package agentui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/push"
)

//go:embed web
var webFS embed.FS

// Link is what the server needs from the push side. *push.Pusher implements it.
type Link interface {
	Apps() []*push.VirtualApp
	Link() push.Link
	Host() bool
	// AppTenants maps each app namespace to its tenant.
	AppTenants() map[string]string
}

// Options configure Server.
type Options struct {
	// Addr is host:port to listen on. The host must be a loopback address.
	Addr  string
	Store *Store
	Link  Link
	Log   logr.Logger
	// Mode is "cluster" or "host".
	Mode    string
	Version string
	Cluster string
	// Config lists the flags in effect (secrets already left out).
	Config func() []ConfigEntry
	// AdminKeyFile is the file holding the admin key. When set, every API
	// request needs a session from /api/v1/login; when empty there is no login.
	AdminKeyFile string
	// AdminKey is the key itself, for a host agent that has no Secret to
	// mount (flag or environment). AdminKeyFile wins when both are set.
	AdminKey string
	// Sensors lists the sensors connected to this controller with the settings
	// each sent in its hello; nil when there is no sensor stream.
	Sensors func() []SensorInfo
	// Peers names the connected replica peers; nil when there is no gossip.
	Peers func() []string
}

// SensorInfo is one sensor pod as the controller knows it: what it runs and
// the flags it was started with.
type SensorInfo struct {
	Node      string
	Pod       string
	Version   string
	Roles     []string
	Connected bool
	Settings  []ConfigEntry
}

// sensorGroup is the sensors that run the same roles, version and settings,
// shown once with their node names.
type sensorGroup struct {
	Nodes     []string      `json:"nodes"`
	Roles     []string      `json:"roles"`
	Version   string        `json:"version,omitempty"`
	Connected int           `json:"connected"`
	Flags     []ConfigEntry `json:"flags"`
}

// groupSensors folds sensors with identical roles, version and settings into
// one group each, biggest group first.
func groupSensors(in []SensorInfo) []sensorGroup {
	byKey := map[string]*sensorGroup{}
	var order []string
	for _, sn := range in {
		roles := append([]string(nil), sn.Roles...)
		sort.Strings(roles)
		key, _ := json.Marshal([]any{roles, sn.Version, sn.Settings})
		g := byKey[string(key)]
		if g == nil {
			flags := sn.Settings
			if flags == nil {
				flags = []ConfigEntry{}
			}
			g = &sensorGroup{Roles: roles, Version: sn.Version, Flags: flags}
			byKey[string(key)] = g
			order = append(order, string(key))
		}
		g.Nodes = append(g.Nodes, sn.Node)
		if sn.Connected {
			g.Connected++
		}
	}
	out := make([]sensorGroup, 0, len(order))
	for _, k := range order {
		g := byKey[k]
		sort.Strings(g.Nodes)
		out = append(out, *g)
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].Nodes) > len(out[j].Nodes) })
	return out
}

// ConfigEntry is one setting of the running agent.
type ConfigEntry struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Default string `json:"default"`
	Changed bool   `json:"changed"`
	Usage   string `json:"usage,omitempty"`
}

// Server is the local TUI's HTTP server: static files plus a read-only JSON
// API and one event stream.
type Server struct {
	o       Options
	now     func() time.Time
	started time.Time
	auth    *auth
}

// New validates o and returns a Server. It refuses a non-loopback address:
// the TUI has no login, so it is reached through a port-forward or not at all.
func New(o Options) (*Server, error) {
	if o.Store == nil {
		return nil, errors.New("agentui: no store")
	}
	if o.Addr != "" { // empty: no TUI, only the API (APIServer)
		host, _, err := net.SplitHostPort(o.Addr)
		if err != nil {
			return nil, fmt.Errorf("agentui: --ui-listen %q: %w", o.Addr, err)
		}
		if !isLoopbackHost(host) {
			return nil, fmt.Errorf("agentui: --ui-listen %q must be a loopback address (127.0.0.1, ::1 or localhost): the TUI is for a port-forward", o.Addr)
		}
	}
	s := &Server{o: o, now: time.Now, started: time.Now()}
	if o.AdminKeyFile != "" || o.AdminKey != "" {
		static := o.AdminKey
		if o.AdminKeyFile != "" {
			static = ""
		}
		a, err := newAuth(o.AdminKeyFile, static)
		if err != nil {
			return nil, err
		}
		s.auth = a
	}
	return s, nil
}

func isLoopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// Start serves until ctx ends. It implements manager.Runnable.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.o.Addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	s.o.Log.Info("agent TUI listening", "addr", ln.Addr().String())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// NeedLeaderElection lets every replica serve its own view.
func (s *Server) NeedLeaderElection() bool { return false }

// Handler returns the routes, behind the loopback Host check.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth", s.authState)
	mux.HandleFunc("/api/v1/login", s.login)
	mux.HandleFunc("/api/v1/logout", s.logout)
	mux.HandleFunc("/api/v1/status", s.status)
	mux.HandleFunc("/api/v1/tenants", s.tenants)
	mux.HandleFunc("/api/v1/config", s.config)
	mux.HandleFunc("/api/v1/stats", s.stats)
	mux.HandleFunc("/api/v1/apps", s.apps)
	mux.HandleFunc("/api/v1/services", s.services)
	mux.HandleFunc("/api/v1/graph", s.graph)
	mux.HandleFunc("/api/v1/sources", s.sources)
	mux.HandleFunc("/api/v1/events", s.events)
	mux.HandleFunc("/api/v1/stream", s.stream)
	sub, _ := fs.Sub(webFS, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))
	return guard(s.requireLogin(mux))
}

// requireLogin lets the static files and the auth endpoints through and
// answers 401 to the rest of the API unless the request has a session.
func (s *Server) requireLogin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		public := !strings.HasPrefix(p, "/api/") || p == "/api/v1/auth" || p == "/api/v1/login" || p == "/api/v1/logout"
		if s.auth != nil && !public && !s.auth.authenticated(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"login required"}` + "\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authState tells the page whether it must log in and whether it has.
func (s *Server) authState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"required": s.auth != nil, "authenticated": s.auth == nil || s.auth.authenticated(r)})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		http.Error(w, "no login is configured", http.StatusNotFound)
		return
	}
	s.auth.login(w, r)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		http.Error(w, "no login is configured", http.StatusNotFound)
		return
	}
	s.auth.logout(w, r)
}

// guard allows GET/HEAD to a loopback Host only. The Host check closes DNS
// rebinding: a page on another origin cannot read the API through a name it
// controls.
func guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		post := r.Method == http.MethodPost && (r.URL.Path == "/api/v1/login" || r.URL.Path == "/api/v1/logout")
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !post {
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		h := r.Host
		if hh, _, err := net.SplitHostPort(h); err == nil {
			h = hh
		}
		if !isLoopbackHost(strings.Trim(h, "[]")) {
			http.Error(w, "loopback host only", http.StatusForbidden)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	tenant := tenantParam(r)
	_, restricted := scopeOf(r)
	l := s.o.Link.Link()
	_, _, edges := s.graphFor(tenant)
	role := ""
	if r, ok := s.o.Link.(interface{ Role() string }); ok {
		role = r.Role()
	}
	writeJSON(w, map[string]any{
		"limits": map[string]any{"events": s.o.Store.MaxEvents()},
		"role":   role, "tenant": tenant, "tenants": s.visibleTenants(r), "cluster": s.o.Cluster, "mode": s.o.Mode, "version": s.o.Version, "now": s.now().Unix(),
		"link": linkView(l, restricted),
		"counts": map[string]any{
			"apps": len(s.appsFor(tenant)), "services": len(s.servicesFor(tenant)), "edges": len(edges),
			"events": s.o.Store.FamilyCounts(tenant),
		},
	})
}

// appView is one virtual app with its traffic folded in.
type appView struct {
	Tenant          string              `json:"tenant,omitempty"`
	Name            string              `json:"name"`
	Namespace       string              `json:"namespace"`
	Group           string              `json:"group,omitempty"`
	Tier            string              `json:"tier,omitempty"`
	Weight          string              `json:"weight,omitempty"`
	Domain          string              `json:"domain,omitempty"`
	Description     string              `json:"description,omitempty"`
	Version         string              `json:"version,omitempty"`
	Ready           string              `json:"ready"` // ready | not-ready | unknown
	ComponentsReady string              `json:"componentsReady,omitempty"`
	Components      []push.ObjectStatus `json:"components"`
	Services        []string            `json:"services"` // ns/service stat keys the app's components matched
	Traffic         *appTraffic         `json:"traffic,omitempty"`
	CallsTo         []string            `json:"callsTo"`  // other apps this one calls
	CalledBy        []string            `json:"calledBy"` // other apps that call this one
}

type appTraffic struct {
	RPS    float64 `json:"rps"`
	ErrPct float64 `json:"errPct"`
	P99Ms  float64 `json:"p99Ms,omitempty"`
	Req    int64   `json:"req"`
	Err    int64   `json:"err"`
}

func (s *Server) apps(w http.ResponseWriter, r *http.Request) {
	tenant := tenantParam(r)
	stats := map[string]ServiceStat{}
	for _, st := range s.servicesFor(tenant) {
		stats[svcKey(st.Namespace, st.Service)] = st
	}
	_, _, edges := s.graphFor(tenant)

	apps := s.appsFor(tenant)
	tn := s.o.Link.AppTenants()
	views := make([]appView, 0, len(apps))
	owner := map[string]string{} // ns/workload -> app key
	for _, a := range apps {
		v := appView{
			Tenant: tn[a.Namespace], Name: a.Name, Namespace: a.Namespace, Description: a.Spec.Descriptor.Description,
			Version: a.Spec.Descriptor.Version, Ready: appReady(a), ComponentsReady: a.Status.ComponentsReady,
			Components: a.Status.Objects, Services: []string{}, CallsTo: []string{}, CalledBy: []string{},
		}
		if v.Components == nil {
			v.Components = []push.ObjectStatus{}
		}
		ann := a.Annotations
		v.Group = ann[annotationkeys.VirtualAppGroup]
		v.Tier = ann[annotationkeys.Tier]
		v.Weight = ann[annotationkeys.Weight]
		v.Domain = ann[annotationkeys.Domain]
		var t appTraffic
		var p99 float64
		for _, c := range a.Status.Objects {
			k := svcKey(a.Namespace, c.Name)
			owner[k] = svcKey(a.Namespace, a.Name)
			if st, ok := stats[k]; ok {
				v.Services = append(v.Services, k)
				t.RPS += st.RPS
				t.Req += st.Requests
				t.Err += st.Errors
				if st.P99Ms > p99 {
					p99 = st.P99Ms
				}
			}
		}
		if len(v.Services) > 0 {
			if t.Req > 0 {
				t.ErrPct = 100 * float64(t.Err) / float64(t.Req)
			}
			t.P99Ms = p99
			v.Traffic = &t
		}
		views = append(views, v)
	}
	idx := map[string]int{}
	for i, v := range views {
		idx[svcKey(v.Namespace, v.Name)] = i
	}
	for _, e := range edges {
		if e.DstWorkload == "" {
			continue
		}
		a, ok1 := owner[svcKey(e.SrcNamespace, e.SrcWorkload)]
		b, ok2 := owner[svcKey(e.DstNamespace, e.DstWorkload)]
		if !ok1 || !ok2 || a == b {
			continue
		}
		views[idx[a]].CallsTo = addUnique(views[idx[a]].CallsTo, b)
		views[idx[b]].CalledBy = addUnique(views[idx[b]].CalledBy, a)
	}
	sort.Slice(views, func(i, j int) bool {
		return svcKey(views[i].Namespace, views[i].Name) < svcKey(views[j].Namespace, views[j].Name)
	})
	writeJSON(w, map[string]any{"apps": views})
}

func addUnique(in []string, v string) []string {
	for _, x := range in {
		if x == v {
			return in
		}
	}
	return append(in, v)
}

func (s *Server) services(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"services": s.servicesFor(tenantParam(r))})
}

func (s *Server) graph(w http.ResponseWriter, r *http.Request) {
	tenant := tenantParam(r)
	at, nodes, edges := s.graphFor(tenant)
	stats := map[string]ServiceStat{}
	for _, st := range s.servicesFor(tenant) {
		stats[svcKey(st.Namespace, st.Service)] = st
	}
	appOf := map[string]string{}
	for _, a := range s.appsFor(tenant) {
		for _, c := range a.Status.Objects {
			appOf[svcKey(a.Namespace, c.Name)] = a.Name
		}
	}
	type node struct {
		Namespace string  `json:"namespace"`
		Workload  string  `json:"workload"`
		App       string  `json:"app,omitempty"`
		RPS       float64 `json:"rps"`
		ErrPct    float64 `json:"errPct"`
	}
	out := make([]node, 0, len(nodes))
	for _, n := range nodes {
		k := svcKey(n.NS, n.Workload)
		st := stats[k]
		out = append(out, node{n.NS, n.Workload, appOf[k], st.RPS, st.ErrPct})
	}
	if edges == nil {
		edges = []Edge{}
	}
	writeJSON(w, map[string]any{"capturedAt": at, "nodes": out, "edges": edges})
}

// source is one input the agent reads: a sensor role on a node, an OTLP
// resource, or the Kubernetes watch. Families name the events it can raise.
type source struct {
	ID       string         `json:"id"`
	Kind     string         `json:"kind"` // ebpf | node-exporter | log-exporter | otlp | k8s | host | apps
	Name     string         `json:"name"`
	Node     string         `json:"node,omitempty"`
	Status   string         `json:"status"` // up | stale | down
	LastSeen int64          `json:"lastSeen,omitempty"`
	Version  string         `json:"version,omitempty"`
	Families []string       `json:"families"`
	Detail   map[string]any `json:"detail,omitempty"`
	// Ledger is what the collectors did with this source's data (see
	// internal/ledger); RoleLedger is the same role's totals over all its
	// sources, including what the gate held back and what was sent.
	Ledger     map[string]int64 `json:"ledger,omitempty"`
	RoleLedger map[string]int64 `json:"roleLedger,omitempty"`
}

var roleFamilies = map[string][]string{
	"ebpf":          {FamilyRed, FamilyTopology},
	"node-exporter": {FamilyUse},
	"log-exporter":  {FamilyLog},
}

var otlpFamilies = map[string][]string{"red": {FamilyRed, FamilyTopology}, "use": {FamilyUse}, "log": {FamilyLog}}

// ledgerEntries returns the signal-path counters, from the leader when this
// replica follows one.
func (s *Server) ledgerEntries() []ledger.Entry {
	if l, ok := s.o.Link.(interface{ Ledger() []ledger.Entry }); ok {
		return l.Ledger()
	}
	return ledger.Default.Snapshot()
}

// ledgerDrops returns the entries held back from a frame, by name, from the
// leader when this replica follows one.
func (s *Server) ledgerDrops() []ledger.Drop {
	if l, ok := s.o.Link.(interface{ Drops() []ledger.Drop }); ok {
		return l.Drops()
	}
	return ledger.Default.Drops()
}

func (s *Server) sources(w http.ResponseWriter, r *http.Request) {
	_, restricted := scopeOf(r)
	out, at := s.sourceList(tenantParam(r), restricted)
	writeJSON(w, map[string]any{"sources": out, "at": at})
}

// sourceList returns the sources for tenant (all when empty). A restricted
// caller gets no node-wide sensors and only the OTLP sources of its tenant.
func (s *Server) sourceList(tenant string, restricted bool) ([]source, int64) {
	now := s.now().Unix()
	stale := int64(sensorStale / time.Second)
	state := func(connected bool, last int64) string {
		switch {
		case !connected:
			return "down"
		case last != 0 && now-last > stale:
			return "stale"
		}
		return "up"
	}
	out := []source{}
	k8sKind, k8sName := "k8s", "Kubernetes watch"
	appFamilies := []string{FamilyK8s, FamilyApp}
	if s.o.Link.Host() {
		k8sKind, k8sName = "host", "Host units"
	}
	if nk, ok := s.o.Link.(interface{ NoKubernetes() bool }); ok && nk.NoKubernetes() {
		// No Kubernetes watch runs: the apps are the workloads and services
		// the OTLP senders report (docs/adr/0021).
		k8sKind, k8sName, appFamilies = "apps", "OTLP applications", []string{FamilyApp}
	}
	out = append(out, source{ID: "k8s", Kind: k8sKind, Name: k8sName, Status: "up",
		Families: appFamilies, Detail: map[string]any{"apps": len(s.appsFor(tenant))}})

	// byRole indexes the counters by role then source, and totals holds each
	// role's own collector totals (the ledger.All source).
	byRole := map[string]map[string]map[string]int64{}
	for _, e := range s.ledgerEntries() {
		if byRole[e.Role] == nil {
			byRole[e.Role] = map[string]map[string]int64{}
		}
		// A collector's target is "sensor/<node>" (OTLP: "sensor/otlp/<key>").
		byRole[e.Role][strings.TrimPrefix(e.Source, "sensor/")] = e.Counters
	}
	snap := s.o.Store.Sensors()
	for _, sn := range snap.Sensors {
		if restricted {
			break
		}
		for _, r := range sn.Roles {
			fams := roleFamilies[r.Role]
			if fams == nil {
				fams = []string{}
			}
			out = append(out, source{
				ID: "sensor:" + sn.Node + ":" + r.Role, Kind: r.Role, Name: r.Role, Node: sn.Node,
				Status: state(sn.Connected, sn.LastSeen), LastSeen: sn.LastSeen, Version: sn.Version, Families: fams,
				Ledger: byRole[r.Role][sn.Node], RoleLedger: byRole[r.Role][ledger.All],
				Detail: map[string]any{
					"pod": sn.Pod, "metrics": r.Metrics, "dataPoints": r.DataPoints, "gatherMs": float64(r.GatherMicros) / 1000,
					"gatherErrors": r.GatherErrors, "lastError": r.LastError, "heapBytes": sn.HeapBytes,
					"shared": true, "goroutines": sn.Goroutines, "reconnects": sn.Reconnects, "ackRttMs": float64(sn.AckRTTMicros) / 1000,
				},
			})
		}
	}
	for _, o := range snap.OTLP {
		if tenant != "" && o.Tenant != tenant && (restricted || o.Tenant != "") {
			continue
		}
		var fams []string
		var dp uint32
		for _, f := range o.Families {
			fams = append(fams, otlpFamilies[f.Family]...)
			dp += f.DataPoints
		}
		if fams == nil {
			fams = []string{}
		}
		name := o.Service
		if name == "" {
			name = o.Key
		}
		// An OTLP source feeds up to three collectors; show their counters summed.
		var otlpLedger map[string]int64
		for _, role := range []string{"ebpf", "node-exporter", "log-exporter"} {
			for o2, n := range byRole[role]["otlp/"+o.Key] {
				if otlpLedger == nil {
					otlpLedger = map[string]int64{}
				}
				otlpLedger[o2] += n
			}
		}
		out = append(out, source{ID: "otlp:" + o.Key, Kind: "otlp", Name: name, Node: o.Instance, Ledger: otlpLedger,
			Status: state(true, o.LastSeen), LastSeen: o.LastSeen, Families: fams,
			Detail: map[string]any{"namespace": o.Namespace, "tenant": o.Tenant, "dataPoints": dp}})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Node+out[i].Name < out[j].Node+out[j].Name
	})
	return out, snap.At
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	// no limit, or one over the cap, is the cap: Store.Events clamps it
	writeJSON(w, map[string]any{"events": s.o.Store.Events(tenantParam(r), familySet(r.URL.Query().Get("family")), limit)})
}

func familySet(q string) map[string]bool {
	if q == "" {
		return nil
	}
	m := map[string]bool{}
	for _, f := range strings.Split(q, ",") {
		m[f] = true
	}
	return m
}

// stream sends each new event as it arrives (server-sent events).
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	ch, cancel := s.o.Store.Subscribe()
	defer cancel()
	fams := familySet(r.URL.Query().Get("family"))
	tenant := tenantParam(r)
	fmt.Fprint(w, ": ok\n\n")
	fl.Flush()
	keep := time.NewTicker(20 * time.Second)
	defer keep.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keep.C:
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
		case e := <-ch:
			if (len(fams) > 0 && !fams[e.Family]) || (tenant != "" && e.Tenant != tenant) {
				continue
			}
			b, _ := json.Marshal(e)
			fmt.Fprintf(w, "event: event\ndata: %s\n\n", b)
			fl.Flush()
		}
	}
}

func tenantParam(r *http.Request) string { return r.URL.Query().Get("tenant") }

// appsFor returns the apps of tenant (all when empty).
func (s *Server) appsFor(tenant string) []*push.VirtualApp {
	apps := s.o.Link.Apps()
	if tenant == "" {
		return apps
	}
	tn := s.o.Link.AppTenants()
	out := make([]*push.VirtualApp, 0, len(apps))
	for _, a := range apps {
		if tn[a.Namespace] == tenant {
			out = append(out, a)
		}
	}
	return out
}

func (s *Server) servicesFor(tenant string) []ServiceStat {
	all := s.o.Store.Services()
	if tenant == "" {
		return all
	}
	out := all[:0:0]
	for _, st := range all {
		if st.Tenant == tenant {
			out = append(out, st)
		}
	}
	return out
}

// graphFor returns the graph of tenant (all when empty). An edge belongs to
// the tenant of its caller; a callee in another tenant is shown as external
// without its name.
func (s *Server) graphFor(tenant string) (int64, []NodeKey, []Edge) {
	at, nodes, edges := s.o.Store.Graph()
	if tenant == "" {
		return at, nodes, edges
	}
	owner := make(map[string]string, len(nodes))
	for _, n := range nodes {
		owner[svcKey(n.NS, n.Workload)] = n.Tenant
	}
	kn := nodes[:0:0]
	for _, n := range nodes {
		if n.Tenant == tenant {
			kn = append(kn, n)
		}
	}
	ke := edges[:0:0]
	for _, e := range edges {
		if e.Tenant != tenant {
			continue
		}
		if e.DstWorkload != "" {
			if t := owner[svcKey(e.DstNamespace, e.DstWorkload)]; t != "" && t != tenant {
				e.DstNamespace, e.DstWorkload, e.External = "", "", "other tenant"
			}
		}
		ke = append(ke, e)
	}
	return at, kn, ke
}

// visibleTenants is tenantNames cut to the caller's scope.
func (s *Server) visibleTenants(r *http.Request) []string {
	if t, restricted := scopeOf(r); restricted {
		return []string{t}
	}
	return s.tenantNames()
}

// tenantNames lists every tenant any of the views has data for.
func (s *Server) tenantNames() []string {
	set := map[string]bool{}
	for _, t := range s.o.Store.Tenants() {
		set[t] = true
	}
	for _, t := range s.o.Link.AppTenants() {
		set[t] = true
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (s *Server) tenants(w http.ResponseWriter, r *http.Request) {
	type row struct {
		Name     string `json:"name"`
		Apps     int    `json:"apps"`
		Services int    `json:"services"`
		Events   int    `json:"events"`
	}
	rows := []row{}
	for _, t := range s.visibleTenants(r) {
		rows = append(rows, row{t, len(s.appsFor(t)), len(s.servicesFor(t)), len(s.o.Store.Events(t, nil, 0))})
	}
	writeJSON(w, map[string]any{"tenants": rows})
}

func (s *Server) config(w http.ResponseWriter, _ *http.Request) {
	var cfg []ConfigEntry
	if s.o.Config != nil {
		cfg = s.o.Config()
	}
	if cfg == nil {
		cfg = []ConfigEntry{}
	}
	sensors := []sensorGroup{}
	if s.o.Sensors != nil {
		sensors = groupSensors(s.o.Sensors())
	}
	writeJSON(w, map[string]any{"mode": s.o.Mode, "cluster": s.o.Cluster, "version": s.o.Version, "flags": cfg, "sensors": sensors})
}

// stats reports what the agent has been doing: this replica's process, the
// leader's push connection, the gossip and what the TUI holds.
func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	l := s.o.Link.Link()
	snap := s.o.Store.Sensors()
	_, nodes, edges := s.o.Store.Graph()
	role := ""
	if r, ok := s.o.Link.(interface{ Role() string }); ok {
		role = r.Role()
	}
	var peers []string
	if s.o.Peers != nil {
		peers = s.o.Peers()
	}
	if peers == nil {
		peers = []string{}
	}
	nodeSet := map[string]bool{}
	for _, sn := range snap.Sensors {
		nodeSet[sn.Node] = true
	}
	now := s.now().Unix()
	writeJSON(w, map[string]any{
		"now": now,
		"process": map[string]any{
			"uptimeSec": int64(s.now().Sub(s.started).Seconds()), "goroutines": runtime.NumGoroutine(),
			"heapBytes": m.HeapAlloc, "sysBytes": m.Sys, "gcCycles": m.NumGC, "cpus": runtime.NumCPU(),
			"go": runtime.Version(), "version": s.o.Version, "mode": s.o.Mode,
		},
		"replica": map[string]any{"role": role, "peers": peers},
		"push": map[string]any{
			"connected": l.Connected, "everConnected": l.EverConnected, "standalone": l.Standalone, "host": l.Host, "tenant": l.Tenant,
			"sent": l.Sent, "dropped": l.Dropped, "noTenantDropped": l.NoTenantDropped, "outOfScopeDropped": l.OutOfScopeDropped,
			"malformedDropped": l.MalformedDropped,
		},
		"drops": s.ledgerDrops(),
		"view": map[string]any{
			"apps": len(s.o.Link.Apps()), "services": len(s.o.Store.Services()), "workloads": len(nodes), "edges": len(edges),
			"tenants": s.tenantNames(), "events": s.o.Store.FamilyCounts(""), "eventsKept": len(s.o.Store.Events("", nil, 0)),
		},
		"sensors": map[string]any{
			"nodes": len(nodeSet), "sensors": len(snap.Sensors), "otlpSources": len(snap.OTLP), "at": snap.At,
		},
	})
}

// linkView is the connection state; a restricted caller sees only whether it
// is up, not the core's address or the cluster-wide frame counts.
func linkView(l push.Link, restricted bool) map[string]any {
	if restricted {
		return map[string]any{"connected": l.Connected, "standalone": l.Standalone}
	}
	return map[string]any{
		"connected": l.Connected, "everConnected": l.EverConnected, "standalone": l.Standalone, "host": l.Host, "tenant": l.Tenant,
		"sent": l.Sent, "dropped": l.Dropped, "noTenantDropped": l.NoTenantDropped, "outOfScopeDropped": l.OutOfScopeDropped,
		"malformedDropped": l.MalformedDropped,
	}
}

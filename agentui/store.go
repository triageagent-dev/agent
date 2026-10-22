// Package agentui serves the agent's local TUI: a read-only view of what the
// agent sees (virtual apps, traffic, the service graph, its sources and the
// events they raise), fed by a push.Observer and the Pusher's own state.
//
// It listens on loopback only and takes GET requests only (docs/adr/0019).
package agentui

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/triageagent-dev/agent/push"
)

// Event families. A source feeds the families it carries; the TUI filters the
// event stream by them.
const (
	FamilyK8s      = "k8s"
	FamilyRed      = "red"
	FamilyUse      = "use"
	FamilyLog      = "log"
	FamilyApp      = "app"
	FamilyTopology = "topology"
)

const (
	// DefaultMaxEvents is how many events a Store keeps unless told otherwise.
	DefaultMaxEvents = 100
	// MinMaxEvents and MaxMaxEvents bound what --ui-max-events may ask for.
	MinMaxEvents = 10
	MaxMaxEvents = 5000
	seriesLen    = 60
	edgeTTL      = 15 * time.Minute
	serviceTTL   = 30 * time.Minute
	sensorStale  = 3 * time.Minute
)

// Event is one thing the agent saw or decided to report.
type Event struct {
	ID        uint64 `json:"id"`
	TS        int64  `json:"ts"`
	Family    string `json:"family"`
	Severity  string `json:"severity"` // info | warning | critical
	Tenant    string `json:"tenant,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Service   string `json:"service,omitempty"`
	Reason    string `json:"reason"`
	Message   string `json:"message,omitempty"`
}

// ServiceStat is one (namespace, service)'s latest golden signals and the
// short history behind the sparklines.
type ServiceStat struct {
	Tenant        string  `json:"tenant,omitempty"`
	Namespace     string  `json:"namespace"`
	Service       string  `json:"service"`
	Requests      int64   `json:"req"`
	Errors        int64   `json:"err"`
	RPS           float64 `json:"rps"`
	ErrPct        float64 `json:"errPct"`
	P99Ms         float64 `json:"p99Ms,omitempty"`
	BaselineP99Ms float64 `json:"baselineP99Ms,omitempty"`
	WindowSec     int     `json:"windowSec"`
	At            int64   `json:"at"`

	Use *UseStat `json:"use,omitempty"`
	Log *LogStat `json:"log,omitempty"`

	RPSSeries []float64 `json:"rpsSeries"`
	ErrSeries []float64 `json:"errSeries"`
}

// UseStat is the latest USE summary for a service.
type UseStat struct {
	CPUPct    float64 `json:"cpuPct,omitempty"`
	MemPct    float64 `json:"memPct,omitempty"`
	CPUPsiPct float64 `json:"cpuPsiPct"`
	MemPsiPct float64 `json:"memPsiPct"`
	IOPsiPct  float64 `json:"ioPsiPct"`
	OOMKills  int64   `json:"oomKills,omitempty"`
	At        int64   `json:"at"`
}

// LogStat is the latest gated log window for a service.
type LogStat struct {
	Errors int64 `json:"errors"`
	Warns  int64 `json:"warns"`
	Total  int64 `json:"total"`
	At     int64 `json:"at"`
}

// Edge is one observed caller→callee relationship over its last window.
type Edge struct {
	Tenant       string  `json:"tenant,omitempty"`
	SrcNamespace string  `json:"srcNamespace"`
	SrcWorkload  string  `json:"srcWorkload"`
	DstNamespace string  `json:"dstNamespace,omitempty"`
	DstWorkload  string  `json:"dstWorkload,omitempty"`
	External     string  `json:"external,omitempty"`
	Protocol     string  `json:"protocol,omitempty"`
	Calls        int64   `json:"calls"`
	Bytes        int64   `json:"bytes,omitempty"`
	CallsPerSec  float64 `json:"callsPerSec"`
	BytesPerSec  float64 `json:"bytesPerSec,omitempty"`
	At           int64   `json:"at"`
}

// SensorSnapshot is the last sensor_state the agent built.
type SensorSnapshot struct {
	At      int64
	Sensors []push.SensorStatus
	OTLP    []push.OTLPSourceStatus
}

// NodeKey names one workload in the graph.
type NodeKey struct{ Tenant, NS, Workload string }

// Store is the TUI's memory of what the Observer saw. All methods are safe
// for concurrent use.
type Store struct {
	now func() time.Time

	mu     sync.Mutex
	nextID uint64
	events []Event
	// maxEvents is how many events each tenant keeps: its newest ones.
	maxEvents int
	services  map[string]*ServiceStat
	edges     map[string]*Edge
	topoAt    int64
	nodes     map[string]NodeKey
	sensors   SensorSnapshot
	familyN   map[string]uint64
	// tenantN counts events per (tenant, family) since start, for the
	// Prometheus counters; it is not gossiped (only the leader exports).
	tenantN map[[2]string]uint64
	subs    map[chan Event]struct{}
}

// NewStore returns an empty Store.
func NewStore() *Store { return NewStoreSized(DefaultMaxEvents) }

// NewStoreSized returns an empty Store that keeps the newest maxEvents events
// (clamped to MinMaxEvents..MaxMaxEvents).
func NewStoreSized(maxEvents int) *Store {
	return &Store{
		maxEvents: min(max(maxEvents, MinMaxEvents), MaxMaxEvents),
		now:       time.Now,
		services:  map[string]*ServiceStat{},
		edges:     map[string]*Edge{},
		nodes:     map[string]NodeKey{},
		familyN:   map[string]uint64{},
		tenantN:   map[[2]string]uint64{},
		subs:      map[chan Event]struct{}{},
	}
}

// Observe implements push.Observer.
func (s *Store) Observe(tenant string, f *push.Frame) {
	switch f.Kind {
	case push.KindK8sEvent:
		if f.K8sEvent != nil && f.K8sEvent.Event != nil {
			e := f.K8sEvent.Event
			sev := "info"
			if e.Type == "Warning" {
				sev = "warning"
			}
			ns := e.InvolvedObject.Namespace
			if ns == "" {
				ns = e.Namespace
			}
			s.add(Event{Family: FamilyK8s, Severity: sev, Tenant: tenant, Namespace: ns,
				Service: e.InvolvedObject.Name, Reason: e.Reason, Message: e.Message})
		}
	case push.KindAppDelta:
		if f.AppDelta != nil && f.AppDelta.App != nil {
			a := f.AppDelta.App
			sev := "info"
			if appReady(a) == "not-ready" {
				sev = "warning"
			}
			s.add(Event{Family: FamilyApp, Severity: sev, Tenant: tenant, Namespace: a.Namespace,
				Service: a.Name, Reason: "app " + f.AppDelta.Op, Message: "ready " + a.Status.ComponentsReady})
		}
	case push.KindRedMetrics:
		if p := f.RedMetrics; p != nil {
			for _, m := range p.Services {
				s.add(Event{Family: FamilyRed, Severity: breachSeverity(m.Breach, m.StillBreaching), Tenant: tenant,
					Namespace: m.Namespace, Service: m.Service, Reason: breachReason("red", m.Breach, m.Operation),
					Message: firstNonEmpty(m.Sample, fmt.Sprintf("%d requests, %d errors", m.RequestCount, m.ErrorCount))})
			}
		}
	case push.KindUseMetrics:
		if p := f.UseMetrics; p != nil {
			for _, m := range p.Services {
				s.add(Event{Family: FamilyUse, Severity: breachSeverity(m.Breach, m.StillBreaching), Tenant: tenant,
					Namespace: m.Namespace, Service: m.Service, Reason: breachReason("use", m.Breach, m.Operation),
					Message: firstNonEmpty(m.Sample, fmt.Sprintf("cpu psi %.0f%%, mem psi %.0f%%", m.CPUPsiAvg60, m.MemPsiAvg60))})
			}
		}
	case push.KindLogMetrics:
		if p := f.LogMetrics; p != nil {
			for _, m := range p.Services {
				s.add(Event{Family: FamilyLog, Severity: breachSeverity(m.Breach, m.StillBreaching), Tenant: tenant,
					Namespace: m.Namespace, Service: m.Service, Reason: breachReason("log", m.Breach, m.Stream),
					Message: firstNonEmpty(m.Sample, fmt.Sprintf("%d error, %d warn lines", m.ErrorCount, m.WarnCount))})
				s.setLog(tenant, m, p.WindowStart+int64(p.WindowSec))
			}
		}
	case push.KindRedSummary:
		if p := f.RedSummary; p != nil {
			s.setRed(tenant, p)
		}
	case push.KindUseSummary:
		if p := f.UseSummary; p != nil {
			s.setUse(tenant, p)
		}
	case push.KindTopologySnapshot:
		if p := f.TopologySnapshot; p != nil {
			s.setTopology(tenant, p)
		}
	case push.KindSensorState:
		if p := f.SensorState; p != nil {
			s.mu.Lock()
			s.sensors = SensorSnapshot{At: p.At, Sensors: append([]push.SensorStatus(nil), p.Sensors...), OTLP: append([]push.OTLPSourceStatus(nil), p.OTLP...)}
			s.mu.Unlock()
		}
	}
}

func appReady(a *push.VirtualApp) string {
	for _, c := range a.Status.Conditions {
		if c.Type == "Ready" {
			switch string(c.Status) {
			case "True":
				return "ready"
			case "False":
				return "not-ready"
			}
		}
	}
	return "unknown"
}

func breachSeverity(breach string, still bool) string {
	switch {
	case still:
		return "info"
	case breach == push.BreachErrors || breach == push.BreachOpErrors || breach == push.BreachLatency ||
		breach == push.BreachOpLatency || breach == push.BreachThrottle:
		return "critical"
	default:
		return "warning"
	}
}

func breachReason(family, breach, detail string) string {
	r := family
	if breach != "" {
		r += " " + breach
	} else {
		r += " window"
	}
	if detail != "" {
		r += " · " + detail
	}
	return r
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (s *Store) add(e Event) {
	s.mu.Lock()
	s.nextID++
	e.ID = s.nextID
	e.TS = s.now().Unix()
	s.events = append(s.events, e)
	s.trimTenantLocked(e.Tenant)
	s.familyN[e.Family]++
	s.tenantN[[2]string{e.Tenant, e.Family}]++
	for ch := range s.subs {
		select {
		case ch <- e:
		default: // a slow viewer loses events, never blocks the collectors
		}
	}
	s.mu.Unlock()
}

func svcKey(ns, svc string) string { return ns + "/" + svc }

func (s *Store) svcLocked(tenant, ns, svc string) *ServiceStat {
	k := svcKey(ns, svc)
	st := s.services[k]
	if st == nil {
		st = &ServiceStat{Namespace: ns, Service: svc}
		s.services[k] = st
	}
	st.Tenant = tenant
	return st
}

func push1(series []float64, v float64) []float64 {
	series = append(series, v)
	if len(series) > seriesLen {
		series = series[len(series)-seriesLen:]
	}
	return series
}

func (s *Store) setRed(tenant string, p *push.RedSummaryPayload) {
	win := p.WindowSec
	if win <= 0 {
		win = 1
	}
	at := p.WindowStart + int64(p.WindowSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range p.Services {
		st := s.svcLocked(tenant, m.Namespace, m.Service)
		st.Requests, st.Errors, st.WindowSec, st.At = m.RequestCount, m.ErrorCount, p.WindowSec, at
		st.RPS = float64(m.RequestCount) / float64(win)
		st.ErrPct = 0
		if m.RequestCount > 0 {
			st.ErrPct = 100 * float64(m.ErrorCount) / float64(m.RequestCount)
		}
		st.P99Ms, st.BaselineP99Ms = m.P99Ms, m.BaselineP99Ms
		st.RPSSeries = push1(st.RPSSeries, st.RPS)
		st.ErrSeries = push1(st.ErrSeries, st.ErrPct)
	}
}

func (s *Store) setUse(tenant string, p *push.UseSummaryPayload) {
	at := p.WindowStart + int64(p.WindowSec)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range p.Services {
		s.svcLocked(tenant, m.Namespace, m.Service).Use = &UseStat{CPUPct: m.CPUPct, MemPct: m.MemPct,
			CPUPsiPct: m.CPUPsiPct, MemPsiPct: m.MemPsiPct, IOPsiPct: m.IOPsiPct, OOMKills: m.OOMKills, At: at}
	}
}

func (s *Store) setLog(tenant string, m push.ServiceLogMetrics, at int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.svcLocked(tenant, m.Namespace, m.Service).Log = &LogStat{Errors: m.ErrorCount, Warns: m.WarnCount, Total: m.TotalCount, At: at}
}

func (s *Store) setTopology(tenant string, p *push.TopologySnapshotPayload) {
	win := p.WindowSec
	if win <= 0 {
		win = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topoAt = p.CapturedAt
	for _, n := range p.Nodes {
		s.nodes[svcKey(n.Namespace, n.Workload)] = NodeKey{tenant, n.Namespace, n.Workload}
	}
	for _, e := range p.Edges {
		k := strings.Join([]string{e.SrcNamespace, e.SrcWorkload, e.DstNamespace, e.DstWorkload, e.External, e.Protocol}, "|")
		s.edges[k] = &Edge{Tenant: tenant, SrcNamespace: e.SrcNamespace, SrcWorkload: e.SrcWorkload, DstNamespace: e.DstNamespace,
			DstWorkload: e.DstWorkload, External: e.External, Protocol: e.Protocol, Calls: e.Calls, Bytes: e.Bytes,
			CallsPerSec: float64(e.Calls) / float64(win), BytesPerSec: float64(e.Bytes) / float64(win), At: p.CapturedAt}
	}
}

// Subscribe returns a channel of new events and a cancel func.
func (s *Store) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
	}
}

// Events returns the newest events first, at most limit of them, of the given
// tenant (all when empty) and in the given families (all when empty).
func (s *Store) Events(tenant string, families map[string]bool, limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > s.maxEvents {
		limit = s.maxEvents
	}
	out := make([]Event, 0, limit)
	for i := len(s.events) - 1; i >= 0 && len(out) < limit; i-- {
		e := s.events[i]
		if (tenant == "" || e.Tenant == tenant) && (len(families) == 0 || families[e.Family]) {
			out = append(out, e)
		}
	}
	return out
}

// Services returns the live service stats, sorted, dropping the stale.
func (s *Store) Services() []ServiceStat {
	cut := s.now().Add(-serviceTTL).Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ServiceStat, 0, len(s.services))
	for k, st := range s.services {
		if st.At != 0 && st.At < cut {
			delete(s.services, k)
			continue
		}
		c := *st
		c.RPSSeries = append([]float64(nil), st.RPSSeries...)
		c.ErrSeries = append([]float64(nil), st.ErrSeries...)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		return svcKey(out[i].Namespace, out[i].Service) < svcKey(out[j].Namespace, out[j].Service)
	})
	return out
}

// Graph returns the edges seen within the TTL and every workload they touch
// or the topology named.
func (s *Store) Graph() (capturedAt int64, nodes []NodeKey, edges []Edge) {
	cut := s.now().Add(-edgeTTL).Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	for k, e := range s.edges {
		if e.At < cut {
			delete(s.edges, k)
			continue
		}
		edges = append(edges, *e)
		for _, k := range []string{svcKey(e.SrcNamespace, e.SrcWorkload), svcKey(e.DstNamespace, e.DstWorkload)} {
			if !strings.HasSuffix(k, "/") {
				seen[k] = true
			}
		}
	}
	for k := range s.nodes {
		seen[k] = true
	}
	for k := range seen {
		ns, w, _ := strings.Cut(k, "/")
		nodes = append(nodes, s.nodeFor(ns, w))
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NS+"/"+nodes[i].Workload < nodes[j].NS+"/"+nodes[j].Workload })
	sort.Slice(edges, func(i, j int) bool { return edges[i].CallsPerSec > edges[j].CallsPerSec })
	return s.topoAt, nodes, edges
}

func (s *Store) nodeFor(ns, w string) NodeKey {
	k := svcKey(ns, w)
	if n, ok := s.nodes[k]; ok {
		return n
	}
	return NodeKey{Tenant: s.services[k].tenant(), NS: ns, Workload: w}
}

func (st *ServiceStat) tenant() string {
	if st == nil {
		return ""
	}
	return st.Tenant
}

// MaxEvents is how many events the Store keeps.
func (s *Store) MaxEvents() int { return s.maxEvents }

// Sensors returns the last sensor snapshot.
func (s *Store) Sensors() SensorSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sensors
}

// FamilyCounts returns how many events each family has raised since start;
// for one tenant, how many of the kept events it has.
func (s *Store) FamilyCounts(tenant string) map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tenant != "" {
		out := map[string]uint64{}
		for _, e := range s.events {
			if e.Tenant == tenant {
				out[e.Family]++
			}
		}
		return out
	}
	out := make(map[string]uint64, len(s.familyN))
	for k, v := range s.familyN {
		out[k] = v
	}
	return out
}

// maxSnapshotEvents bounds the events one snapshot carries.
const maxSnapshotEvents = 300

// maxSnapshotTotal bounds them over all tenants, the newest kept.
const maxSnapshotTotal = 1000

// Snapshot is the Store's whole state as one value, for the leader to send
// to the other replicas (Replica).
type Snapshot struct {
	Services []ServiceStat     `json:"services"`
	Edges    []Edge            `json:"edges"`
	Nodes    []NodeKey         `json:"nodes"`
	TopoAt   int64             `json:"topoAt"`
	Sensors  SensorSnapshot    `json:"sensors"`
	Events   []Event           `json:"events"`
	FamilyN  map[string]uint64 `json:"familyN"`
	NextID   uint64            `json:"nextId"`
}

// Export copies the Store's state.
func (s *Store) Export() Snapshot {
	svcs := s.Services() // copies, drops the stale
	s.mu.Lock()
	defer s.mu.Unlock()
	sn := Snapshot{Services: svcs, TopoAt: s.topoAt, Sensors: s.sensors, NextID: s.nextID,
		FamilyN: make(map[string]uint64, len(s.familyN))}
	for _, e := range s.edges {
		sn.Edges = append(sn.Edges, *e)
	}
	for _, n := range s.nodes {
		sn.Nodes = append(sn.Nodes, n)
	}
	// A copy, and bounded in total: per-tenant retention alone would grow the
	// snapshot with the number of tenants.
	sn.Events = append([]Event(nil), newestPerTenant(s.events, min(s.maxEvents, maxSnapshotEvents))...)
	if len(sn.Events) > maxSnapshotTotal {
		sn.Events = append([]Event(nil), sn.Events[len(sn.Events)-maxSnapshotTotal:]...)
	}
	for k, v := range s.familyN {
		sn.FamilyN[k] = v
	}
	return sn
}

// Import makes sn this Store's state. Events newer than the Store's own are
// appended and sent to subscribers; the ids continue from sn's, so a replica
// that becomes the leader goes on numbering where the last one stopped.
func (s *Store) Import(sn Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = make(map[string]*ServiceStat, len(sn.Services))
	for i := range sn.Services {
		st := sn.Services[i]
		s.services[svcKey(st.Namespace, st.Service)] = &st
	}
	s.edges = make(map[string]*Edge, len(sn.Edges))
	for i := range sn.Edges {
		e := sn.Edges[i]
		k := strings.Join([]string{e.SrcNamespace, e.SrcWorkload, e.DstNamespace, e.DstWorkload, e.External, e.Protocol}, "|")
		s.edges[k] = &e
	}
	s.nodes = make(map[string]NodeKey, len(sn.Nodes))
	for _, n := range sn.Nodes {
		s.nodes[svcKey(n.NS, n.Workload)] = n
	}
	s.topoAt, s.sensors = sn.TopoAt, sn.Sensors
	if sn.FamilyN != nil {
		s.familyN = sn.FamilyN
	}
	for _, e := range sn.Events {
		if e.ID <= s.nextID {
			continue
		}
		s.events = append(s.events, e)
		for ch := range s.subs {
			select {
			case ch <- e:
			default:
			}
		}
	}
	s.events = newestPerTenant(s.events, s.maxEvents)
	if sn.NextID > s.nextID {
		s.nextID = sn.NextID
	}
}

// Tenants returns every tenant the Store has data for, sorted.
func (s *Store) Tenants() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]bool{}
	for _, st := range s.services {
		set[st.Tenant] = true
	}
	for _, e := range s.edges {
		set[e.Tenant] = true
	}
	for _, e := range s.events {
		set[e.Tenant] = true
	}
	delete(set, "")
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// TenantFamilyCounts returns how many events each (tenant, family) has raised
// since this Store started, for tenant (all tenants when empty).
func (s *Store) TenantFamilyCounts(tenant string) map[[2]string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[[2]string]uint64{}
	for k, v := range s.tenantN {
		if tenant == "" || k[0] == tenant {
			out[k] = v
		}
	}
	return out
}

// newestPerTenant keeps each tenant's newest n events, in order. The limit is
// per tenant, so a tenant that raises many events (a noisy one) does not push
// a quiet tenant's out of the buffer. It returns events itself when no tenant
// is over the limit.
func newestPerTenant(events []Event, n int) []Event {
	if len(events) <= n {
		return events
	}
	count := map[string]int{}
	over := false
	for i := len(events) - 1; i >= 0; i-- {
		count[events[i].Tenant]++
		if count[events[i].Tenant] > n {
			over = true
		}
	}
	if !over {
		return events
	}
	seen := map[string]int{}
	keep := make([]bool, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		t := events[i].Tenant
		if seen[t] < n {
			keep[i] = true
			seen[t]++
		}
	}
	out := make([]Event, 0, len(events))
	for i, e := range events {
		if keep[i] {
			out = append(out, e)
		}
	}
	return out
}

// trimTenantLocked drops the oldest event of tenant when it holds more than
// maxEvents, so one event added costs one scan and at most one removal.
func (s *Store) trimTenantLocked(tenant string) {
	n, first := 0, -1
	for i, e := range s.events {
		if e.Tenant == tenant {
			if first < 0 {
				first = i
			}
			n++
		}
	}
	if n <= s.maxEvents {
		return
	}
	s.events = append(s.events[:first:first], s.events[first+1:]...)
}

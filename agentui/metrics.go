package agentui

import (
	"net/http"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dInfo      = prometheus.NewDesc("triage_agent_info", "The agent: version, cluster, mode and this replica's role.", []string{"version", "cluster", "mode", "role"}, nil)
	dLinkUp    = prometheus.NewDesc("triage_agent_core_link_connected", "1 while the agent is connected to the core.", nil, nil)
	dSent      = prometheus.NewDesc("triage_agent_frames_sent_total", "Frames sent to the core, per kind.", []string{"kind"}, nil)
	dDropped   = prometheus.NewDesc("triage_agent_frames_dropped_total", "Frames dropped before they reached the core.", nil, nil)
	dOutScope  = prometheus.NewDesc("triage_agent_frames_out_of_scope_dropped_total", "Payload items dropped because their namespace is outside the workspace scope the core set.", nil, nil)
	dNoTenant  = prometheus.NewDesc("triage_agent_frames_no_tenant_dropped_total", "Payload items dropped because their namespace maps to no tenant.", nil, nil)
	dMalformed = prometheus.NewDesc("triage_agent_frames_malformed_dropped_total", "Payload entries dropped because the core would have refused their whole frame, such as a service with no namespace.", nil, nil)
	dSensors   = prometheus.NewDesc("triage_agent_sensor_nodes", "Nodes whose sensors report to the agent.", nil, nil)
	dAppReady  = prometheus.NewDesc("triage_agent_app_ready", "1 when the virtual app is ready, 0 when it is not (unknown is left out).", []string{"tenant", "namespace", "app"}, nil)
	dRPS       = prometheus.NewDesc("triage_agent_service_requests_per_second", "Requests per second over the last window.", []string{"tenant", "namespace", "service"}, nil)
	dErrRatio  = prometheus.NewDesc("triage_agent_service_error_ratio", "Share of requests that failed over the last window, 0 to 1.", []string{"tenant", "namespace", "service"}, nil)
	dP99       = prometheus.NewDesc("triage_agent_service_latency_p99_seconds", "99th percentile request duration over the last window.", []string{"tenant", "namespace", "service"}, nil)
	dCPUPsi    = prometheus.NewDesc("triage_agent_service_cpu_pressure_ratio", "CPU pressure (PSI some avg60), 0 to 1.", []string{"tenant", "namespace", "service"}, nil)
	dMemPsi    = prometheus.NewDesc("triage_agent_service_memory_pressure_ratio", "Memory pressure (PSI some avg60), 0 to 1.", []string{"tenant", "namespace", "service"}, nil)
	dIOPsi     = prometheus.NewDesc("triage_agent_service_io_pressure_ratio", "IO pressure (PSI some avg60), 0 to 1.", []string{"tenant", "namespace", "service"}, nil)
	dOOM       = prometheus.NewDesc("triage_agent_service_oom_kills_window", "OOM kills in the last window.", []string{"tenant", "namespace", "service"}, nil)
	dLogErr    = prometheus.NewDesc("triage_agent_service_log_error_lines_window", "Error log lines in the last gated window.", []string{"tenant", "namespace", "service"}, nil)
	dLogWarn   = prometheus.NewDesc("triage_agent_service_log_warn_lines_window", "Warn log lines in the last gated window.", []string{"tenant", "namespace", "service"}, nil)
	dEdgeCalls = prometheus.NewDesc("triage_agent_edge_calls_per_second", "Calls per second from a caller to a callee over the last window.", []string{"tenant", "src_namespace", "src_workload", "dst_namespace", "dst_workload", "external", "protocol"}, nil)
	dEdgeBytes = prometheus.NewDesc("triage_agent_edge_bytes_per_second", "Bytes per second on a network-flow edge over the last window.", []string{"tenant", "src_namespace", "src_workload", "dst_namespace", "dst_workload", "external", "protocol"}, nil)
	dSourceUp  = prometheus.NewDesc("triage_agent_source_up", "1 while the source reports, 0 when it is stale or down.", []string{"kind", "name", "node", "tenant"}, nil)
	dLedger    = prometheus.NewDesc("triage_agent_signal_path_total", "What each stage did with a source's data: ticks, points, filtered, unattributed, services, gated, capped, sent, refused. source * is the role's total over its sources.", []string{"role", "source", "outcome"}, nil)
	dEvents    = prometheus.NewDesc("triage_agent_events_total", "Events the agent raised since it started, per tenant and family.", []string{"tenant", "family"}, nil)
)

// metrics serves the Prometheus exposition for the caller's scope. Only the
// leader holds signals (its collectors are the ones running), so a follower
// exports the info series alone and Prometheus sees each series once however
// many replicas it scrapes.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	tenant, restricted := scopeOf(r)
	if !restricted {
		tenant = tenantParam(r)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(&collector{s: s, tenant: tenant, restricted: restricted})
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

type collector struct {
	s          *Server
	tenant     string
	restricted bool
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(c, ch) }

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	g := func(d *prometheus.Desc, v float64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
	}
	ctr := func(d *prometheus.Desc, v float64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, lv...)
	}
	role := ""
	if rr, ok := s.o.Link.(interface{ Role() string }); ok {
		role = rr.Role()
	}
	g(dInfo, 1, s.o.Version, s.o.Cluster, s.o.Mode, role)
	if role == "follower" {
		return
	}

	if !c.restricted {
		l := s.o.Link.Link()
		g(dLinkUp, b2f(l.Connected))
		kinds := make([]string, 0, len(l.Sent))
		for k := range l.Sent {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			ctr(dSent, float64(l.Sent[k]), k)
		}
		for _, e := range s.ledgerEntries() {
			for o, n := range e.Counters {
				ctr(dLedger, float64(n), e.Role, e.Source, o)
			}
		}
		ctr(dDropped, float64(l.Dropped))
		ctr(dNoTenant, float64(l.NoTenantDropped))
		ctr(dOutScope, float64(l.OutOfScopeDropped))
		ctr(dMalformed, float64(l.MalformedDropped))
		nodes := map[string]bool{}
		for _, sn := range s.o.Store.Sensors().Sensors {
			nodes[sn.Node] = true
		}
		g(dSensors, float64(len(nodes)))
	}

	tn := s.o.Link.AppTenants()
	for _, a := range s.appsFor(c.tenant) {
		switch appReady(a) {
		case "ready":
			g(dAppReady, 1, tn[a.Namespace], a.Namespace, a.Name)
		case "not-ready":
			g(dAppReady, 0, tn[a.Namespace], a.Namespace, a.Name)
		}
	}
	for _, st := range s.servicesFor(c.tenant) {
		lv := []string{st.Tenant, st.Namespace, st.Service}
		if st.At != 0 {
			g(dRPS, st.RPS, lv...)
			g(dErrRatio, st.ErrPct/100, lv...)
			if st.P99Ms > 0 {
				g(dP99, st.P99Ms/1000, lv...)
			}
		}
		if u := st.Use; u != nil {
			g(dCPUPsi, u.CPUPsiPct/100, lv...)
			g(dMemPsi, u.MemPsiPct/100, lv...)
			g(dIOPsi, u.IOPsiPct/100, lv...)
			g(dOOM, float64(u.OOMKills), lv...)
		}
		if l := st.Log; l != nil {
			g(dLogErr, float64(l.Errors), lv...)
			g(dLogWarn, float64(l.Warns), lv...)
		}
	}
	_, _, edges := s.graphFor(c.tenant)
	for _, e := range edges {
		lv := []string{e.Tenant, e.SrcNamespace, e.SrcWorkload, e.DstNamespace, e.DstWorkload, e.External, e.Protocol}
		g(dEdgeCalls, e.CallsPerSec, lv...)
		if e.BytesPerSec > 0 {
			g(dEdgeBytes, e.BytesPerSec, lv...)
		}
	}
	srcs, _ := s.sourceList(c.tenant, c.restricted)
	for _, sr := range srcs {
		t, _ := sr.Detail["tenant"].(string)
		g(dSourceUp, b2f(sr.Status == "up"), sr.Kind, sr.Name, sr.Node, t)
	}
	counts := s.o.Store.TenantFamilyCounts(c.tenant)
	keys := make([][2]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+"|"+keys[i][1] < keys[j][0]+"|"+keys[j][1] })
	for _, k := range keys {
		ctr(dEvents, float64(counts[k]), k[0], k[1])
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

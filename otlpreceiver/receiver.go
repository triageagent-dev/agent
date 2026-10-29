// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package otlpreceiver serves the OTLP/gRPC metrics, traces and logs
// services and hands what arrives to the sensor stream
// (docs/adr/0017-otlp-receiver.md, docs/adr/0022-otlp-sensor-role.md).
//
// The receiver is a sensor: it translates each export as it arrives,
// forwards it and drops it. It holds no copy of what a sender sent. The
// only state it keeps is the handful of numbers the translations need
// across exports: the running total of a delta series, the counts of a
// span route, the record counts of a log source and the previous PSI
// reading of a pod — each expiring after Expire without an export. The
// controller's hub merges the exports of a source per series, so a source
// whose metrics arrive in several exports, or out of order, is whole there.
package otlpreceiver

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/internal/remotens"
	"github.com/triageagent-dev/agent/metricset"
)

// Default limits; each is an Options field and a flag.
const (
	defaultMaxResources          = 2000
	defaultMaxPointsPerResource  = 5000
	defaultMaxResourcesPerSender = 500
	defaultMaxRequestBytes       = 4 << 20
	defaultMaxSeries             = 2_000_000
)

// Sink receives one export's metrics of one role for the source under a
// stable key: sensorstream.Hub.Ingest, or the otlp role's send queue.
type Sink func(key, role string, rms []*metricspb.ResourceMetrics)

// Principal is the authenticated sender of an Export.
type Principal struct {
	// Namespace and Name identify the sender: the ServiceAccount of a token,
	// or the SPIFFE identity of a client certificate. Namespace is empty for
	// a forwarder credential that names none.
	Namespace, Name string
	// Forwarder is set for a sender that may publish for any namespace: a
	// Collector gateway listed in --otlp-forwarders, or a client certificate
	// without a SPIFFE namespace (a shared client CA), or a tenant
	// certificate.
	Forwarder bool
	// Tenants are the tenants a client certificate names (URI SANs
	// spiffe://<domain>/tenant/<name>). A sender with tenants publishes for
	// those tenants only: each resource is placed in one of them and its
	// namespaces are qualified with it (package remotens). Empty for a token
	// sender and for a certificate that names no tenant.
	Tenants []string
}

// String is the sender's name in logs and counters.
func (p Principal) String() string {
	switch {
	case p.Namespace != "":
		return p.Namespace + "/" + p.Name
	case p.Name != "":
		return p.Name
	case len(p.Tenants) > 0:
		return "tenant:" + strings.Join(p.Tenants, ",")
	}
	return "client-certificate"
}

// Authenticator checks an Export's bearer token and returns its sender.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
}

// maxSenders bounds the per-sender refusal counters.
const maxSenders = 1000

// principalFromCert derives the sender of a verified client certificate. The
// tenant URI SANs spiffe://<domain>/tenant/<name> make it a tenant sender
// (Principal.Tenants). Otherwise the namespace and name of a SPIFFE URI SAN
// spiffe://<domain>/ns/<ns>/sa/<name> bind it to one namespace, and a
// certificate with neither is a forwarder named by the common name, since a
// shared client CA says nothing about which namespace the holder belongs to.
// A certificate whose tenant URI SAN is malformed is refused, not treated as
// a forwarder: it was meant to be limited to a tenant.
func principalFromCert(c *x509.Certificate) (Principal, error) {
	var tenants []string
	for _, u := range c.URIs {
		if u.Scheme != "spiffe" {
			continue
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if parts[0] != "tenant" {
			continue
		}
		if u.Host == "" || len(parts) != 2 || !remotens.ValidTenant(parts[1]) {
			return Principal{}, fmt.Errorf("client certificate %q has a malformed tenant URI SAN", c.Subject.CommonName)
		}
		tenants = append(tenants, parts[1])
	}
	if len(tenants) > 0 {
		sort.Strings(tenants)
		tenants = slices.Compact(tenants)
		return Principal{Name: c.Subject.CommonName, Forwarder: true, Tenants: tenants}, nil
	}
	for _, u := range c.URIs {
		if u.Scheme != "spiffe" || u.Host == "" {
			continue
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) == 4 && parts[0] == "ns" && parts[1] != "" && parts[2] == "sa" && parts[3] != "" {
			return Principal{Namespace: parts[1], Name: parts[3]}, nil
		}
	}
	return Principal{Name: c.Subject.CommonName, Forwarder: true}, nil
}

// Options configures a Receiver.
type Options struct {
	// Addr the gRPC server listens on, for example ":4317".
	Addr string
	TLS  credentials.TransportCredentials
	// Insecure serves plain gRPC and authenticates nobody
	// (docs/adr/0022-otlp-sensor-role.md): for Collectors inside the cluster,
	// which have neither a client certificate nor a token for the agent. Every
	// sender is an anonymous forwarder named by its address, so the sender
	// quota (MaxResourcesPerSender) and the refusal counters apply per
	// address. TLS, Auth and tenant certificates are not used. The listener
	// must be fenced by the network (the chart's NetworkPolicy).
	Insecure bool
	// Auth checks a bearer token. A sender whose client certificate the TLS
	// layer verified (ClientCAs, mutual TLS) needs no token.
	Auth Authenticator
	Sink Sink
	// TenantAllowed, when non-nil, says whether this agent serves a tenant a
	// certificate names. A resource for any other tenant is refused with a
	// reason, so a certificate cannot place data on a tenant the agent has no
	// approval for at the core. Nil allows every tenant in the certificate.
	TenantAllowed func(tenant string) bool
	// NamespaceInScope, when non-nil, says whether a plain namespace (the
	// k8s.namespace.name a sender reported, before the "<tenant>~" prefix) is
	// inside the workspace scope the core set for a tenant. A resource of a
	// tenant-certificate sender outside it is refused and counted like any
	// refused point. Nil allows every namespace.
	NamespaceInScope func(tenant, ns string) bool
	// Wanted, when non-nil, says whether a metric name is read by any
	// collector. A metric it rejects is dropped at intake, before anything
	// is allocated for it. Nil keeps every metric. ReadByCollectors is the
	// production answer.
	Wanted func(name string) bool
	// Expire is how long the state kept across exports (a delta series'
	// running total, a span route's counts, a log source's counts, a pod's
	// PSI reading, a source's name) lives without an export refreshing it.
	Expire time.Duration
	// The limits are sanity bounds on the state kept across exports; past
	// one, points are refused and counted. MaxResources bounds the sources
	// known (default 2000) and MaxResourcesPerSender the sources one sender
	// may bring (500) so that one sender cannot crowd out the rest;
	// MaxPointsPerResource bounds the span routes of one resource (5000)
	// and MaxSeries the state entries in all (2000000: a cumulative series
	// costs its last timestamp, 40 bytes, a delta series its total, a span
	// route its attributes); MaxRequestBytes is the size of one Export
	// request (4 MiB, the gRPC receive limit).
	MaxResources          int
	MaxPointsPerResource  int
	MaxResourcesPerSender int
	MaxSeries             int
	MaxRequestBytes       int
	// Replica names this process in the resources it derives from spans and
	// logs (the shares of one service several receivers each see). Empty
	// takes the host name, which is the pod name in a pod.
	Replica string
	Log     logr.Logger
	now     func() time.Time
}

func (o *Options) setDefaults() {
	if o.Expire <= 0 {
		o.Expire = 10 * time.Minute
	}
	if o.MaxResources <= 0 {
		o.MaxResources = defaultMaxResources
	}
	if o.MaxPointsPerResource <= 0 {
		o.MaxPointsPerResource = defaultMaxPointsPerResource
	}
	if o.MaxResourcesPerSender <= 0 {
		o.MaxResourcesPerSender = defaultMaxResourcesPerSender
	}
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = defaultMaxRequestBytes
	}
	if o.MaxSeries <= 0 {
		o.MaxSeries = defaultMaxSeries
	}
	if o.now == nil {
		o.now = time.Now
	}
}

// source is what the receiver remembers of a source it forwarded: who
// brought it, for the sender quota, and when, for the announcement and the
// expiry. Nothing of what it sent.
type source struct {
	sender string
	seen   time.Time
}

// tick is what a cumulative series costs: the time of the last point
// forwarded, so an older point arriving after it (a retried export) is
// dropped instead of taking a newer value's place downstream, and when an
// export last carried the series, for the expiry.
type tick struct {
	ts   uint64
	seen int64 // unix seconds
}

// running is a delta series' running total, what makes its points
// cumulative for the collectors: the last interval's end, so a re-sent
// interval is not added twice, and the sum so far. A histogram keeps its
// bucket totals too.
type running struct {
	ts     uint64
	start  uint64
	isInt  bool
	i      int64
	f      float64
	bounds []float64
	counts []uint64
	count  uint64
	sum    float64
	hasSum bool
	seen   time.Time
}

// Receiver implements the OTLP metrics gRPC service.
type Receiver struct {
	collectorpb.UnimplementedMetricsServiceServer

	opts Options
	log  logr.Logger

	mu sync.Mutex
	// sources are the sources forwarded within Expire, by key.
	sources map[string]*source
	// bySender counts sources per sender, for MaxResourcesPerSender.
	bySender map[string]int
	// ticks holds the last time of every cumulative series forwarded, and
	// deltas the running totals of delta series, both by the hash of
	// source, metric and attributes.
	ticks  map[uint64]tick
	deltas map[uint64]*running
	// spans holds the routes derived from spans, by resource key (traces.go).
	spans map[string]*spanRoutes
	// logs holds the record counts derived from log records, by resource
	// key (logs.go).
	logs map[string]*logCounts
	// psi holds the previous cAdvisor PSI readings, by pod and resource
	// (cadvisor.go).
	psi map[string]psiPrev
	// nheld is the entries in ticks, deltas, spans (their routes), logs and
	// psi: what MaxSeries bounds.
	nheld     int
	lastSweep time.Time
	// Counted since the last summary line.
	exports, points, rejected, expired int64
	// unread counts points dropped at intake because no collector reads their
	// metric (Options.Wanted).
	unread int64
	// refusals counts refused points per sender, at most maxSenders keys.
	refusals map[string]int64
	// refusalLog holds what each (sender, reason) refused since it was last
	// logged, so a sender refused on every export is logged once a minute.
	refusalLog map[string]*refusalWindow
}

// summaryEvery is how often the receiver logs what it took in, and how often
// the state kept across exports is swept for entries past Expire.
const summaryEvery = time.Minute

// New returns a Receiver; Start serves it.
func New(opts Options) *Receiver {
	opts.setDefaults()
	return &Receiver{opts: opts, log: opts.Log.WithName("otlpreceiver"),
		sources: map[string]*source{}, bySender: map[string]int{}, ticks: map[uint64]tick{}, deltas: map[uint64]*running{},
		spans: map[string]*spanRoutes{}, logs: map[string]*logCounts{}, psi: map[string]psiPrev{},
		refusals: map[string]int64{}, refusalLog: map[string]*refusalWindow{}}
}

// NeedLeaderElection is false: every replica accepts OTLP and gossips it.
func (r *Receiver) NeedLeaderElection() bool { return false }

// Reads reports whether the receiver forwards a metric of this name, or
// drops it at intake (Options.Wanted). Without Wanted every name is
// forwarded.
func (r *Receiver) Reads(name string) bool {
	return r.opts.Wanted == nil || r.opts.Wanted(name)
}

// Held is the number of state entries kept across exports: the last time
// of each cumulative series, delta running totals, span routes, log sources
// and PSI readings. It grows with the series a cluster has, not with the
// exports that carry them, and each entry is a few numbers.
func (r *Receiver) Held() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nheld
}

// Start serves the listener until ctx ends.
func (r *Receiver) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", r.opts.Addr)
	if err != nil {
		return fmt.Errorf("otlp listen %s: %w", r.opts.Addr, err)
	}
	return r.Serve(ctx, lis)
}

// Serve serves on lis until ctx ends.
func (r *Receiver) Serve(ctx context.Context, lis net.Listener) error {
	var creds []grpc.ServerOption
	if !r.opts.Insecure {
		creds = append(creds, grpc.Creds(r.opts.TLS))
	}
	srv := grpc.NewServer(append(creds,
		grpc.MaxRecvMsgSize(r.opts.MaxRequestBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)...)
	collectorpb.RegisterMetricsServiceServer(srv, r)
	collectortrace.RegisterTraceServiceServer(srv, traceService{r: r})
	collectorlogs.RegisterLogsServiceServer(srv, logService{r: r})
	r.log.Info("otlp receiver listening", "addr", lis.Addr().String())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	summary := time.NewTicker(summaryEvery)
	defer summary.Stop()
	for {
		select {
		case <-ctx.Done():
			srv.GracefulStop()
			return nil
		case err := <-errCh:
			return err
		case <-summary.C:
			r.logSummary()
		}
	}
}

// Export implements collectorpb.MetricsServiceServer.
func (r *Receiver) Export(ctx context.Context, req *collectorpb.ExportMetricsServiceRequest) (*collectorpb.ExportMetricsServiceResponse, error) {
	p, err := r.authenticate(ctx)
	if err != nil {
		r.log.Info("otlp export refused", "reason", err.Error())
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	rejected, reasons := r.accept(p, req.GetResourceMetrics())
	resp := &collectorpb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectorpb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       strings.Join(reasons, "; "),
		}
	}
	return resp, nil
}

// resourceTenant picks the tenant of one resource sent with a tenant
// certificate: the resource's triage.tenant attribute, or the certificate's
// only tenant when it has one. It returns a reason when the resource must be
// refused.
func (r *Receiver) resourceTenant(p Principal, rm *metricspb.ResourceMetrics) (tenant, why string) {
	tenant = attr(rm.GetResource().GetAttributes(), remotens.TenantAttr)
	if tenant == "" {
		if len(p.Tenants) != 1 {
			return "", "the certificate names several tenants and the resource has no " + remotens.TenantAttr
		}
		tenant = p.Tenants[0]
	}
	if !slices.Contains(p.Tenants, tenant) {
		return "", "tenant " + tenant + " is not in the sender's certificate"
	}
	if r.opts.TenantAllowed != nil && !r.opts.TenantAllowed(tenant) {
		return "", "tenant " + tenant + " is not served by this agent"
	}
	return tenant, ""
}

func (r *Receiver) authenticate(ctx context.Context) (Principal, error) {
	if r.opts.Insecure {
		return anonymousPrincipal(ctx), nil
	}
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(ti.State.VerifiedChains) > 0 && len(ti.State.VerifiedChains[0]) > 0 {
			// mutual TLS: the client certificate chains to the client CA
			return principalFromCert(ti.State.VerifiedChains[0][0])
		}
	}
	md, _ := metadata.FromIncomingContext(ctx)
	var token string
	for _, v := range md.Get("authorization") {
		if t, ok := strings.CutPrefix(v, "Bearer "); ok {
			token = t
		}
	}
	if token == "" {
		return Principal{}, errors.New("no bearer token")
	}
	if r.opts.Auth == nil {
		return Principal{}, errors.New("no authenticator configured")
	}
	return r.opts.Auth.Authenticate(ctx, token)
}

// anonymousPrincipal is the sender of an Export to an insecure receiver: a
// forwarder named by its address, without the port a reconnect changes.
func anonymousPrincipal(ctx context.Context) Principal {
	name := "anonymous"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err != nil {
			host = p.Addr.String()
		}
		name = "anonymous@" + host
	}
	return Principal{Name: name, Forwarder: true}
}

// SenderRefusals returns how many points each sender had refused.
func (r *Receiver) SenderRefusals() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int64, len(r.refusals))
	for k, v := range r.refusals {
		out[k] = v
	}
	return out
}

// forward is one export's metrics of one role for one source, on its way
// to the sink once the lock is released.
type forward struct {
	key, role string
	rms       []*metricspb.ResourceMetrics
}

// noNamespaceReason is why a resource with no k8s.namespace.name is refused:
// the core cannot place it, so nothing of it would be used downstream. A
// resource whose points name a namespace themselves is placed by them, and
// a cAdvisor scrape's points name their pods' namespaces.
const noNamespaceReason = "resource has no k8s.namespace.name: the sender did not enrich it"

// placeable reports whether the core can place what rm carries: its
// resource names a namespace, or its points do.
func placeable(resource *resourcepb.Resource, rm *metricspb.ResourceMetrics) bool {
	if ns := attr(resource.GetAttributes(), nsAttr); ns != "" && !strings.HasSuffix(ns, remotens.Sep) {
		return true
	}
	if hasCAdvisor(rm) {
		return true
	}
	found := false
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			forEachPointAttrs(m, func(attrs []*commonpb.KeyValue) {
				if ns := attr(attrs, nsAttr); ns != "" && !strings.HasSuffix(ns, remotens.Sep) {
					found = true
				}
			})
		}
	}
	return found
}

// accept translates the metrics of rms, sent by p, forwards them and
// returns how many points it refused and why (once per reason).
func (r *Receiver) accept(p Principal, rms []*metricspb.ResourceMetrics) (int64, []string) {
	sender := p.String()
	f := r.newRefusal(sender, "points")
	reject := f.reject
	now := r.opts.now()
	var outs []forward

	r.mu.Lock()
	r.exports++
	var offered int64
	for _, rm := range rms {
		np := int64(countPoints(rm))
		r.points += np
		offered += np
		unread := int64(dropCAdvisorNoise(rm))
		if r.opts.Wanted != nil {
			unread += int64(dropUnwanted(rm, r.opts.Wanted))
		}
		r.unread += unread
		ledger.Default.Add("otlp", ledger.All, ledger.Unread, unread)
		if len(rm.GetScopeMetrics()) == 0 {
			continue
		}
		n := func() int { return countPoints(rm) }
		f.at(rm.GetResource())
		resource, ok := r.admit(p, rm, n, reject)
		if !ok {
			continue
		}
		if !placeable(resource, rm) {
			reject(n(), noNamespaceReason)
			continue
		}
		key := resourceKey(resource)
		if !r.touchSource(key, sender, now, n, reject) {
			continue
		}
		byRole := map[string][]*metricspb.Metric{}
		var cadvisor []*metricspb.Metric
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				ms := []*metricspb.Metric{m}
				if out, ok := translateSpanMetrics(m); ok {
					ms = out
				}
				for _, m := range ms {
					m, refused, why := r.prepare(key, m, now)
					if why != "" {
						reject(refused, why)
					}
					if m == nil {
						continue
					}
					if isCAdvisor(m.GetName()) {
						cadvisor = append(cadvisor, m)
						continue
					}
					if role := RoleOf(m.GetName()); role != "" {
						byRole[role] = append(byRole[role], m)
					}
				}
			}
		}
		for role, ms := range byRole {
			outs = append(outs, forward{key: key, role: role, rms: []*metricspb.ResourceMetrics{{
				Resource:     resource,
				ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp"}, Metrics: ms}},
			}}})
		}
		// cAdvisor series become per-pod use.* points under an empty resource:
		// the scrape's own resource names the kubelet, not the pods.
		if ms := r.translateCAdvisor(cadvisor, now); len(ms) > 0 {
			outs = append(outs, forward{key: key, role: metricset.RoleNodeExporter, rms: []*metricspb.ResourceMetrics{{
				Resource:     &resourcepb.Resource{},
				ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp/cadvisor"}, Metrics: ms}},
			}}})
		}
	}
	r.rejected += f.rejected
	ledger.Default.Add("otlp", ledger.All, ledger.Ticks, 1)
	ledger.Default.Add("otlp", ledger.All, ledger.Points, offered)
	ledger.Default.Add("otlp", ledger.All, ledger.Refused, f.rejected)
	r.sweep(now)
	r.mu.Unlock()

	r.send(outs)
	return f.rejected, f.sortedReasons()
}

// send hands the forwards to the sink, outside the lock: the sink may wait
// on the stream.
func (r *Receiver) send(outs []forward) {
	if r.opts.Sink == nil {
		return
	}
	for _, o := range outs {
		r.opts.Sink(o.key, o.role, o.rms)
	}
}

// touchSource records that key is forwarded by sender, within the source
// limits, or reports n() refused through reject and returns false. A source
// seen for the first time is announced. The caller holds r.mu.
func (r *Receiver) touchSource(key, sender string, now time.Time, n func() int, reject func(int, string)) bool {
	if s := r.sources[key]; s != nil {
		s.seen = now
		return true
	}
	if r.bySender[sender] >= r.opts.MaxResourcesPerSender {
		reject(n(), "sender resource quota reached")
		return false
	}
	if len(r.sources) >= r.opts.MaxResources {
		reject(n(), "resource limit reached")
		return false
	}
	r.sources[key] = &source{sender: sender, seen: now}
	r.bySender[sender]++
	return true
}

// prepare makes m ready to forward: the current name and unit, its points'
// attributes normalized and their exemplars dropped, and a delta series
// turned into the cumulative one the collectors read. It returns nil for a
// metric nothing is left of, with how many points it refused and why. The
// caller holds r.mu.
func (r *Receiver) prepare(key string, m *metricspb.Metric, now time.Time) (*metricspb.Metric, int, string) {
	name, unit, scale := canonicalMetric(m)
	m.Name, m.Unit = name, unit
	refused, why := 0, ""
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		delta := d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
		kept := d.Sum.DataPoints[:0]
		for _, dp := range d.Sum.GetDataPoints() {
			dp.Exemplars = nil
			normalizePoint(dp, scale)
			ok, reason := r.inOrder(key, name, dp, now)
			if delta {
				ok, reason = r.addDelta(key, name, dp, now)
			}
			if reason != "" {
				refused++
				why = reason
			}
			if !ok {
				continue
			}
			kept = append(kept, dp)
		}
		d.Sum.DataPoints = kept
		if delta {
			d.Sum.AggregationTemporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
		}
		if len(kept) == 0 {
			return nil, refused, why
		}
	case *metricspb.Metric_Gauge:
		kept := d.Gauge.DataPoints[:0]
		for _, dp := range d.Gauge.GetDataPoints() {
			dp.Exemplars = nil
			normalizePoint(dp, scale)
			ok, reason := r.inOrder(key, name, dp, now)
			if reason != "" {
				refused++
				why = reason
			}
			if !ok {
				continue
			}
			kept = append(kept, dp)
		}
		d.Gauge.DataPoints = kept
		if len(kept) == 0 {
			return nil, refused, why
		}
	case *metricspb.Metric_Histogram:
		delta := d.Histogram.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
		kept := d.Histogram.DataPoints[:0]
		for _, dp := range d.Histogram.GetDataPoints() {
			dp.Exemplars = nil
			normalizePoint(dp, scale)
			ok, reason := r.inOrder(key, name, dp, now)
			if delta {
				ok, reason = r.addDelta(key, name, dp, now)
			}
			if reason != "" {
				refused++
				why = reason
			}
			if !ok {
				continue
			}
			kept = append(kept, dp)
		}
		d.Histogram.DataPoints = kept
		if delta {
			d.Histogram.AggregationTemporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
		}
		if len(kept) == 0 {
			return nil, refused, why
		}
	case *metricspb.Metric_ExponentialHistogram:
		return nil, len(d.ExponentialHistogram.GetDataPoints()), "exponential histograms are not supported; use explicit buckets"
	default:
		return nil, 0, ""
	}
	return m, refused, why
}

// seriesID identifies one series across exports: the source, the metric and
// the point's attributes, hashed. Only a delta series is looked up by it.
func seriesID(key, name string, attrs []*commonpb.KeyValue) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(pointKey(attrs)))
	return h.Sum64()
}

// inOrder records the cumulative point p as its series' latest and returns
// true, or false for a point older than the last one forwarded, which must
// not take the newer value's place downstream, and for a new series past
// MaxSeries, with the reason. The caller holds r.mu.
func (r *Receiver) inOrder(key, name string, p proto.Message, now time.Time) (bool, string) {
	id := seriesID(key, name, pointAttrs(p))
	ts := pointTime(p)
	cur, ok := r.ticks[id]
	if ok && ts < cur.ts {
		return false, ""
	}
	if !ok && r.nheld >= r.opts.MaxSeries {
		return false, "series limit reached"
	}
	if !ok {
		r.nheld++
	}
	r.ticks[id] = tick{ts: ts, seen: now.Unix()}
	return true, ""
}

// addDelta adds the delta point p to its series' running total and rewrites
// p as the total, so what is forwarded is cumulative. It returns false for
// a point to drop: an interval that ends no later than the last one added is
// a retry of it (or older) and would count twice; a point the total cannot
// take comes with the reason. The caller holds r.mu.
func (r *Receiver) addDelta(key, name string, p proto.Message, now time.Time) (bool, string) {
	id := seriesID(key, name, pointAttrs(p))
	cur := r.deltas[id]
	if cur != nil && pointTime(p) <= cur.ts {
		return false, ""
	}
	if cur == nil {
		if r.nheld >= r.opts.MaxSeries {
			return false, "series limit reached"
		}
		cur = &running{}
		r.deltas[id] = cur
		r.nheld++
	}
	switch dp := p.(type) {
	case *metricspb.NumberDataPoint:
		_, isInt := dp.GetValue().(*metricspb.NumberDataPoint_AsInt)
		if cur.ts == 0 {
			cur.isInt, cur.start = isInt, dp.GetStartTimeUnixNano()
		}
		if cur.isInt && isInt {
			cur.i += dp.GetAsInt()
			dp.Value = &metricspb.NumberDataPoint_AsInt{AsInt: cur.i}
		} else {
			if cur.isInt {
				cur.f, cur.isInt = float64(cur.i), false
			}
			cur.f += numberValue(dp)
			dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: cur.f}
		}
		dp.StartTimeUnixNano = cur.start
	case *metricspb.HistogramDataPoint:
		if cur.ts == 0 {
			cur.bounds = append([]float64(nil), dp.GetExplicitBounds()...)
			cur.counts = make([]uint64, len(dp.GetBucketCounts()))
			cur.hasSum, cur.start = dp.Sum != nil, dp.GetStartTimeUnixNano()
		} else if !slices.Equal(cur.bounds, dp.GetExplicitBounds()) || len(cur.counts) != len(dp.GetBucketCounts()) {
			return false, "histogram bucket bounds changed"
		}
		for i, c := range dp.GetBucketCounts() {
			cur.counts[i] += c
		}
		cur.count += dp.GetCount()
		dp.Count, dp.BucketCounts = cur.count, append([]uint64(nil), cur.counts...)
		if cur.hasSum && dp.Sum != nil {
			cur.sum += dp.GetSum()
			sum := cur.sum
			dp.Sum = &sum
		} else {
			cur.hasSum, dp.Sum = false, nil
		}
		dp.Min, dp.Max = nil, nil // the extremes of one interval say nothing of the total
		dp.StartTimeUnixNano = cur.start
	default:
		return false, "a series changed its kind"
	}
	cur.ts, cur.seen = pointTime(p), now
	return true, ""
}

func numberValue(dp *metricspb.NumberDataPoint) float64 {
	if _, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt); ok {
		return float64(dp.GetAsInt())
	}
	return dp.GetAsDouble()
}

// refusal counts what one Export refused and why. The caller holds r.mu.
type refusal struct {
	r        *Receiver
	sender   string
	what     string // "points" or "spans", for the log line
	resource string // namespace/service of the resource being judged
	rejected int64
	reasons  map[string]bool
}

func (r *Receiver) newRefusal(sender, what string) *refusal {
	return &refusal{r: r, sender: sender, what: what, reasons: map[string]bool{}}
}

// at names the resource the refusals that follow are about.
func (f *refusal) at(res *resourcepb.Resource) {
	f.resource = attr(res.GetAttributes(), nsAttr) + "/" + attr(res.GetAttributes(), "service.name")
}

// refusalWindow is what one (sender, reason) refused since it was last
// logged, and the resource the first of it was about.
type refusalWindow struct {
	since    time.Time
	count    int64
	resource string
}

// refusalLogEvery is how often one (sender, reason) is logged: a Collector
// refused on every export sends about one a second, and the line is for
// the operator, not a count.
const refusalLogEvery = time.Minute

// maxRefusalKeys bounds refusalLog, which has one key per sender and reason.
const maxRefusalKeys = 4 * maxSenders

// reject counts n refused items against the sender. The log gets one line
// per sender and reason a minute, with the count refused since the last.
func (f *refusal) reject(n int, why string) {
	f.rejected += int64(n)
	f.reasons[why] = true
	r := f.r
	if _, ok := r.refusals[f.sender]; ok || len(r.refusals) < maxSenders {
		r.refusals[f.sender] += int64(n)
	}
	key := f.sender + "\x00" + why
	w := r.refusalLog[key]
	if w == nil {
		if len(r.refusalLog) >= maxRefusalKeys {
			return
		}
		w = &refusalWindow{}
		r.refusalLog[key] = w
	}
	w.count += int64(n)
	if w.resource == "" {
		w.resource = f.resource
	}
	now := r.opts.now()
	if !w.since.IsZero() && now.Sub(w.since) < refusalLogEvery {
		return
	}
	r.log.Info("otlp "+f.what+" refused", "sender", f.sender, "reason", why, "resource", w.resource, "refused", w.count)
	w.since, w.count, w.resource = now, 0, ""
}

func (f *refusal) sortedReasons() []string {
	out := make([]string, 0, len(f.reasons))
	for why := range f.reasons {
		out = append(out, why)
	}
	sort.Strings(out)
	return out
}

// admit applies to one resource the checks every signal faces: the namespace
// it names, the tenant its sender may publish for and the workspace scope.
// It returns the resource as it is forwarded (normalized, its namespace set
// from the sender's own when it names none, tenant-qualified for a tenant
// sender), or false after reporting n() refused through reject. The points
// of rm are judged too; a signal with no metric points passes an rm with
// its resource alone. The caller holds r.mu.
func (r *Receiver) admit(p Principal, rm *metricspb.ResourceMetrics, n func() int, reject func(int, string)) (*resourcepb.Resource, bool) {
	// One namespace per resource, judged by every check below and read by
	// every collector alike: the first and the last of a repeated key
	// would otherwise differ (metricset.FromOTLP keeps the last).
	resNS, multiple := resourceNamespace(rm.GetResource().GetAttributes())
	if multiple {
		reject(n(), multipleNamespacesReason)
		return nil, false
	}
	// Only this receiver puts the tenant into a namespace, from the
	// sender's certificate; a namespace of that form in what a sender
	// sends would route to a tenant the sender may not be authorised for.
	if claimsQualifiedNamespace(rm.GetResource().GetAttributes(), false) {
		reject(n(), qualifiedNamespaceReason)
		return nil, false
	}
	dropPointsWhere(rm, claimsQualifiedNamespace, func(n int) { reject(n, qualifiedNamespaceReason) })
	if len(p.Tenants) > 0 {
		tenant, why := r.resourceTenant(p, rm)
		if why != "" {
			reject(n(), why)
			return nil, false
		}
		if in := r.opts.NamespaceInScope; in != nil {
			if !in(tenant, resNS) {
				reject(n(), outOfScopeReason)
				return nil, false
			}
			dropPointsWhere(rm, func(attrs []*commonpb.KeyValue, cadvisor bool) bool {
				return pointOutOfScope(attrs, cadvisor, func(ns string) bool { return !in(tenant, ns) })
			}, func(n int) { reject(n, outOfScopeReason) })
		}
		qualifyResource(rm, tenant)
	}
	if ns := foreignNamespace(rm.GetResource().GetAttributes(), p.Namespace, false); !p.Forwarder && ns != "" {
		reject(n(), fmt.Sprintf("namespace %s is not the sender's namespace", ns))
		return nil, false
	}
	if !p.Forwarder {
		// metricset.FromOTLP lets a point's own k8s.namespace.name win
		// over the resource's, so the resource check alone does not
		// bind the points. Drop every point that names another
		// namespace; rewriting it would merge it into the sender's own
		// series under a false identity. The namespace is the tenant
		// boundary: a point's k8s.pod.name, k8s.owner.name and
		// service.name stay sender-controlled within it.
		dropForeignPoints(rm, p.Namespace, func(n int, ns string) {
			reject(n, fmt.Sprintf("namespace %s is not the sender's namespace", ns))
		})
	}
	resource := normalizeResource(rm.GetResource())
	if !p.Forwarder && p.Namespace != "" && attr(resource.GetAttributes(), nsAttr) == "" {
		resource.Attributes = append(resource.Attributes, kv(nsAttr, p.Namespace))
	}
	return resource, true
}

// foreignNamespace returns the first k8s.namespace.name value in kvs that is
// not ns, or "" when there is none. Every entry is checked, not only the
// first: metricset.FromOTLP keeps the last of a repeated key. A scalar of
// any type counts, as it does there. An empty value is treated as absent
// unless emptyIsForeign (the resource case stamps an absent namespace; a
// point with an empty one would still override the resource's).
func foreignNamespace(kvs []*commonpb.KeyValue, ns string, emptyIsForeign bool) string {
	for _, a := range kvs {
		if a.GetKey() != nsAttr {
			continue
		}
		switch a.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue, *commonpb.AnyValue_IntValue,
			*commonpb.AnyValue_DoubleValue, *commonpb.AnyValue_BoolValue:
		default:
			continue // not read as an attribute downstream
		}
		v := anyString(a.GetValue())
		if v != ns && (v != "" || emptyIsForeign) {
			if v == "" {
				return "(empty)"
			}
			return v
		}
	}
	return ""
}

// qualifiedNamespaceReason is why a resource or point naming a
// "<tenant>~<namespace>" namespace is refused.
const qualifiedNamespaceReason = "namespace contains the tenant separator"

// claimsQualifiedNamespace reports whether any namespace attribute in kvs
// already has the "<tenant>~<namespace>" form, which no Kubernetes namespace
// can have. The "namespace" label counts on a cAdvisor series only.
func claimsQualifiedNamespace(kvs []*commonpb.KeyValue, cadvisor bool) bool {
	for _, a := range kvs {
		if (remotens.NamespaceKey(a.GetKey()) || (cadvisor && a.GetKey() == cadvisorNsAttr)) && strings.Contains(anyString(a.GetValue()), remotens.Sep) {
			return true
		}
	}
	return false
}

// multipleNamespacesReason is why a resource naming several namespaces is
// refused.
const multipleNamespacesReason = "resource names more than one k8s.namespace.name"

// resourceNamespace returns the one k8s.namespace.name value of a resource's
// attributes ("" when it has none) and whether it carries more than one
// distinct value, which no check can judge consistently. Values are read as
// foreignNamespace reads them: a scalar of any type counts, an empty one too.
func resourceNamespace(kvs []*commonpb.KeyValue) (ns string, multiple bool) {
	seen := false
	for _, a := range kvs {
		if a.GetKey() != nsAttr {
			continue
		}
		switch a.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue, *commonpb.AnyValue_IntValue,
			*commonpb.AnyValue_DoubleValue, *commonpb.AnyValue_BoolValue:
		default:
			continue // not read as an attribute downstream
		}
		v := anyString(a.GetValue())
		if seen && v != ns {
			return "", true
		}
		ns, seen = v, true
	}
	return ns, false
}

// outOfScopeReason is why a resource or point outside the workspace scope is
// refused.
const outOfScopeReason = "namespace outside the workspace scope"

// dropForeignPoints removes from rm every data point of any kind that
// claims a namespace other than ns, reporting each drop. It runs before
// translateSpanMetrics and prepare, which read the points it leaves.
func dropForeignPoints(rm *metricspb.ResourceMetrics, ns string, report func(n int, claimed string)) {
	var claimed string
	dropPointsWhere(rm, func(attrs []*commonpb.KeyValue, _ bool) bool {
		claimed = foreignNamespace(attrs, ns, true)
		return claimed != ""
	}, func(n int) { report(n, claimed) })
}

// pointOutOfScope reports whether a point names a namespace for which
// outside returns true. A point with no namespace of its own belongs to its
// resource and is not judged here.
func pointOutOfScope(attrs []*commonpb.KeyValue, cadvisor bool, outside func(ns string) bool) bool {
	for _, a := range attrs {
		if a.GetKey() == nsAttr || (cadvisor && a.GetKey() == cadvisorNsAttr) {
			if ns := anyString(a.GetValue()); ns != "" && outside(ns) {
				return true
			}
		}
	}
	return false
}

// dropPointsWhere removes from rm every data point of any kind for which bad
// is true, calling report(1) for each. bad is told whether the point's metric
// is a cAdvisor series, the only kind whose "namespace" label is a namespace.
func dropPointsWhere(rm *metricspb.ResourceMetrics, bad func(attrs []*commonpb.KeyValue, cadvisor bool) bool, report func(n int)) {
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			cadvisor := isCAdvisor(m.GetName())
			switch d := m.GetData().(type) {
			case *metricspb.Metric_Sum:
				d.Sum.DataPoints = filterPoints(d.Sum.GetDataPoints(), cadvisor, bad, report,
					func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			case *metricspb.Metric_Gauge:
				d.Gauge.DataPoints = filterPoints(d.Gauge.GetDataPoints(), cadvisor, bad, report,
					func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			case *metricspb.Metric_Histogram:
				d.Histogram.DataPoints = filterPoints(d.Histogram.GetDataPoints(), cadvisor, bad, report,
					func(dp *metricspb.HistogramDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			case *metricspb.Metric_ExponentialHistogram:
				d.ExponentialHistogram.DataPoints = filterPoints(d.ExponentialHistogram.GetDataPoints(), cadvisor, bad, report,
					func(dp *metricspb.ExponentialHistogramDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			case *metricspb.Metric_Summary:
				d.Summary.DataPoints = filterPoints(d.Summary.GetDataPoints(), cadvisor, bad, report,
					func(dp *metricspb.SummaryDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			}
		}
	}
}

func filterPoints[T any](pts []T, cadvisor bool, bad func([]*commonpb.KeyValue, bool) bool, report func(int), attrs func(T) []*commonpb.KeyValue) []T {
	kept := pts[:0]
	for _, dp := range pts {
		if bad(attrs(dp), cadvisor) {
			report(1)
			continue
		}
		kept = append(kept, dp)
	}
	return kept
}

// logSummary logs what arrived since the last summary, and what is held.
func (r *Receiver) logSummary() {
	r.mu.Lock()
	r.sweep(r.opts.now())
	exports, points, rejected, unread, expired := r.exports, r.points, r.rejected, r.unread, r.expired
	r.exports, r.points, r.rejected, r.unread, r.expired = 0, 0, 0, 0, 0
	sources, held := len(r.sources), r.nheld
	r.mu.Unlock()
	r.log.Info("otlp received", "exports", exports, "points", points, "rejected", rejected, "unread", unread,
		"expired", expired, "sources", sources, "held", held, "window", summaryEvery.String())
}

// sweep forgets every state entry no export refreshed for Expire, at most
// once a minute. The caller holds r.mu.
func (r *Receiver) sweep(now time.Time) {
	if now.Sub(r.lastSweep) < summaryEvery {
		return
	}
	r.lastSweep = now
	stale := func(seen time.Time) bool { return now.Sub(seen) > r.opts.Expire }
	for key, s := range r.sources {
		if stale(s.seen) {
			delete(r.sources, key)
			if r.bySender[s.sender]--; r.bySender[s.sender] <= 0 {
				delete(r.bySender, s.sender)
			}
		}
	}
	for id, t := range r.ticks {
		if stale(time.Unix(t.seen, 0)) {
			delete(r.ticks, id)
			r.nheld--
			r.expired++
		}
	}
	for id, d := range r.deltas {
		if stale(d.seen) {
			delete(r.deltas, id)
			r.nheld--
			r.expired++
		}
	}
	for key, sr := range r.spans {
		for k, s := range sr.routes {
			if stale(s.seen) {
				delete(sr.routes, k)
				r.nheld--
				r.expired++
			}
		}
		if len(sr.routes) == 0 {
			delete(r.spans, key)
		}
	}
	for key, c := range r.logs {
		if stale(c.seen) {
			delete(r.logs, key)
			r.nheld--
			r.expired++
		}
	}
	for key, p := range r.psi {
		if stale(p.seen) {
			delete(r.psi, key)
			r.nheld--
			r.expired++
		}
	}
}

// RoleOf names the sensor role whose collector reads metric name: use.* is
// the USE collector's (node-exporter), the log counters the log collector's
// (log-exporter), and everything else (the RED and client histograms, L4
// flow bytes) the RED and topology collector's (ebpf). Raw cAdvisor series
// have none: they reach the USE collector translated (translateCAdvisor).
func RoleOf(name string) string {
	switch {
	case isCAdvisor(name):
		return ""
	case strings.HasPrefix(name, "use."):
		return metricset.RoleNodeExporter
	case name == "log.errors", name == "log.warns", name == "log.lines":
		return metricset.RoleLogExporter
	}
	return metricset.RoleEBPF
}

func countPoints(rm *metricspb.ResourceMetrics) int {
	n := 0
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			n += countMetricPoints(m)
		}
	}
	return n
}

func countMetricPoints(m *metricspb.Metric) int {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		return len(d.Sum.GetDataPoints())
	case *metricspb.Metric_Gauge:
		return len(d.Gauge.GetDataPoints())
	case *metricspb.Metric_Histogram:
		return len(d.Histogram.GetDataPoints())
	case *metricspb.Metric_ExponentialHistogram:
		return len(d.ExponentialHistogram.GetDataPoints())
	}
	return 0
}

func pointAttrs(p proto.Message) []*commonpb.KeyValue {
	switch dp := p.(type) {
	case *metricspb.NumberDataPoint:
		return dp.GetAttributes()
	case *metricspb.HistogramDataPoint:
		return dp.GetAttributes()
	}
	return nil
}

func pointTime(p proto.Message) uint64 {
	switch dp := p.(type) {
	case *metricspb.NumberDataPoint:
		return dp.GetTimeUnixNano()
	case *metricspb.HistogramDataPoint:
		return dp.GetTimeUnixNano()
	}
	return 0
}

// pointKey is an attribute set's identity: sorted "k=v" pairs.
func pointKey(kvs []*commonpb.KeyValue) string {
	pairs := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		pairs = append(pairs, kv.GetKey()+"="+anyString(kv.GetValue()))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\x00")
}

// resourceKey is a stable short name for a resource: a hash of its sorted
// attributes. A restarted process carries a new service.instance.id and so
// becomes a new source.
func resourceKey(r *resourcepb.Resource) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(pointKey(r.GetAttributes())))
	return fmt.Sprintf("%016x", h.Sum64())
}

func anyString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	}
	return ""
}

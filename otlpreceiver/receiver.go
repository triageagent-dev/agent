// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package otlpreceiver accepts OTLP metrics pushed over gRPC by OpenTelemetry
// SDKs and Collectors, for clusters that run no eBPF DaemonSet
// (docs/adr/0017-otlp-receiver.md). It keeps each resource's latest
// cumulative points and hands them to the sensor stream hub under the role
// whose collector reads them (RoleOf): RED, topology and L4 flows as the
// ebpf role, use.* as node-exporter, log counters as log-exporter. The
// collectors then read them exactly as they read the DaemonSet's.
package otlpreceiver

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/internal/remotens"
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

	"github.com/triageagent-dev/agent/metricset"
)

// Default limits; each is an Options field and a flag.
const (
	defaultMaxResources          = 2000
	defaultMaxPointsPerResource  = 5000
	defaultMaxResourcesPerSender = 500
	defaultMaxRequestBytes       = 4 << 20
	defaultMaxSeries             = 250_000
)

// Sink receives one resource's accumulated metrics of one role under a
// stable key: sensorstream.Hub.Ingest.
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
	// tenant-certificate sender outside it is refused before it is stored and
	// counted like any refused point. Nil allows every namespace.
	NamespaceInScope func(tenant, ns string) bool
	// Wanted, when non-nil, says whether a metric name is read by any
	// collector. A metric it rejects is dropped before its resource is stored
	// or counted against a quota, so a sender that sends everything it has
	// cannot crowd out what is read. Nil keeps every metric. ReadByCollectors
	// is the production answer.
	Wanted func(name string) bool
	// Flush is how often changed resources are handed to Sink.
	Flush time.Duration
	// Expire drops a resource that sent nothing for this long, and a series
	// of a live resource that no export updated for this long: a node's
	// cAdvisor scrape is one resource that never goes silent while the pods
	// under it come and go.
	Expire time.Duration
	// The limits bound memory; past one, points are refused and counted.
	// MaxResources bounds the resources held (default 2000),
	// MaxPointsPerResource the series of one resource (5000),
	// MaxResourcesPerSender the resources one sender may hold (500) so that
	// one allowed sender cannot fill every slot, MaxSeries the series held
	// in all (250000: what the memory is sized by, since the resource and
	// per-resource limits together allow 10 million; a span histogram counts
	// beside the point derived from it), and MaxRequestBytes the size of one
	// Export request (4 MiB, the gRPC receive limit).
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
	if o.Flush <= 0 {
		o.Flush = 5 * time.Second
	}
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

// series is one metric's shape and its latest point per attribute set.
type series struct {
	shell  *metricspb.Metric // name, unit and data kind; no points
	points map[string]held
}

// held is one stored point and when an export last set it, by the
// receiver's clock: a sender's timestamps do not decide what is stale.
type held struct {
	p    proto.Message
	seen time.Time
}

type resourceState struct {
	resource *resourcepb.Resource
	metrics  map[string]*series
	npoints  int
	// sender is the principal that created the resource, for its quota.
	sender string
	seen   time.Time
	dirty  bool
	// announced is set once the source's first flush was logged.
	announced bool
	// psi holds the previous cAdvisor PSI readings (translateCAdvisor).
	psi map[string]psiPrev
	// spans holds the duration histograms derived from spans (traces.go).
	spans map[string]*spanSeries
	// logs holds the record counts derived from log records (logs.go).
	logs *logCounts
}

// Receiver implements the OTLP metrics gRPC service.
type Receiver struct {
	collectorpb.UnimplementedMetricsServiceServer

	opts Options
	log  logr.Logger

	mu  sync.Mutex
	res map[string]*resourceState
	// nseries is the series held over every resource (MaxSeries).
	nseries int
	// lastSweep is when series silent for Expire were last dropped.
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

// summaryEvery is how often the receiver logs what it took in.
const summaryEvery = time.Minute

// New returns a Receiver; Start serves it.
func New(opts Options) *Receiver {
	opts.setDefaults()
	return &Receiver{opts: opts, log: opts.Log.WithName("otlpreceiver"), res: map[string]*resourceState{},
		refusals: map[string]int64{}, refusalLog: map[string]*refusalWindow{}}
}

// NeedLeaderElection is false: every replica accepts OTLP and gossips it.
func (r *Receiver) NeedLeaderElection() bool { return false }

// Reads reports whether the receiver stores a metric of this name, or drops
// it at intake (Options.Wanted). Without Wanted every name is stored.
func (r *Receiver) Reads(name string) bool {
	return r.opts.Wanted == nil || r.opts.Wanted(name)
}

// Start serves the listener and flushes to the sink until ctx ends.
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
	ticker := time.NewTicker(r.opts.Flush)
	defer ticker.Stop()
	summary := time.NewTicker(summaryEvery)
	defer summary.Stop()
	for {
		select {
		case <-ctx.Done():
			srv.GracefulStop()
			r.flush()
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			r.flush()
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

// SenderRefusals returns how many points were refused per sender since
// start.
func (r *Receiver) SenderRefusals() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int64, len(r.refusals))
	for k, v := range r.refusals {
		out[k] = v
	}
	return out
}

// accept merges rms, sent by p, into the stored resources and returns how
// many points it refused and why (once per reason).
func (r *Receiver) accept(p Principal, rms []*metricspb.ResourceMetrics) (int64, []string) {
	sender := p.String()
	f := r.newRefusal(sender, "points")
	reject := f.reject
	now := r.opts.now()

	r.mu.Lock()
	defer r.mu.Unlock()
	r.exports++
	var offered int64
	defer func() {
		r.rejected += f.rejected
		// Totals only: a per-sender key would grow with every sender that ever
		// authenticated (SenderRefusals keeps those, bounded).
		ledger.Default.Add("otlp", ledger.All, ledger.Ticks, 1)
		ledger.Default.Add("otlp", ledger.All, ledger.Points, offered)
		ledger.Default.Add("otlp", ledger.All, ledger.Refused, f.rejected)
	}()
	for _, rm := range rms {
		np := int64(countPoints(rm))
		r.points += np
		offered += np
		if r.opts.Wanted != nil {
			unread := int64(dropUnwanted(rm, r.opts.Wanted))
			r.unread += unread
			ledger.Default.Add("otlp", ledger.All, ledger.Unread, unread)
			if len(rm.GetScopeMetrics()) == 0 {
				continue
			}
		}
		n := func() int { return countPoints(rm) }
		f.at(rm.GetResource())
		resource, ok := r.admit(p, rm, n, reject)
		if !ok {
			continue
		}
		key := resourceKey(resource)
		st := r.stateFor(key, resource, sender, n, reject)
		if st == nil {
			continue
		}
		st.seen = now
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				ms := []*metricspb.Metric{m}
				if out, ok := translateSpanMetrics(m); ok {
					ms = out
				}
				for _, m := range ms {
					n, why := r.mergeMetric(st, m, now)
					if why != "" {
						reject(n, why)
					}
				}
			}
		}
	}
	return f.rejected, f.sortedReasons()
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
	f.resource = attr(res.GetAttributes(), "k8s.namespace.name") + "/" + attr(res.GetAttributes(), "service.name")
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
// It returns the resource as it is stored (normalized, its namespace set from
// the sender's own when it names none, tenant-qualified for a tenant sender),
// or false after reporting n() refused through reject. The points of rm are
// judged too; a signal with no metric points passes an rm with its resource
// alone. The caller holds r.mu.
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
	if !p.Forwarder && p.Namespace != "" && attr(resource.GetAttributes(), "k8s.namespace.name") == "" {
		resource.Attributes = append(resource.Attributes, kv("k8s.namespace.name", p.Namespace))
	}
	return resource, true
}

// stateFor returns the state of the resource stored under key, creating it
// within the quotas, or nil after reporting n() refused through reject. The
// caller holds r.mu.
func (r *Receiver) stateFor(key string, resource *resourcepb.Resource, sender string, n func() int, reject func(int, string)) *resourceState {
	st := r.res[key]
	if st != nil {
		return st
	}
	if r.heldBy(sender) >= r.opts.MaxResourcesPerSender {
		reject(n(), "sender resource quota reached")
		return nil
	}
	if len(r.res) >= r.opts.MaxResources {
		reject(n(), "resource limit reached")
		return nil
	}
	st = &resourceState{resource: resource, metrics: map[string]*series{}, sender: sender}
	r.res[key] = st
	return st
}

// foreignNamespace returns the first k8s.namespace.name value in kvs that is
// not ns, or "" when there is none. Every entry is checked, not only the
// first: metricset.FromOTLP keeps the last of a repeated key. A scalar of
// any type counts, as it does there. An empty value is treated as absent
// unless emptyIsForeign (the resource case stamps an absent namespace; a
// point with an empty one would still override the resource's).
func foreignNamespace(kvs []*commonpb.KeyValue, ns string, emptyIsForeign bool) string {
	for _, a := range kvs {
		if a.GetKey() != "k8s.namespace.name" {
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
// translateSpanMetrics and mergeMetric, which read the points it leaves.
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

// heldBy counts the resources sender created that are still held. The
// caller holds r.mu.
func (r *Receiver) heldBy(sender string) int {
	n := 0
	for _, st := range r.res {
		if st.sender == sender {
			n++
		}
	}
	return n
}

// mergeMetric stores m's points in st, the newest per attribute set. A delta
// point is added to the cumulative point held for its series, so the
// collectors only ever see cumulative data (docs/adr/0022-otlp-sensor-role.md).
// It returns the points refused and why.
func (r *Receiver) mergeMetric(st *resourceState, m *metricspb.Metric, now time.Time) (int, string) {
	name, unit, scale := canonicalMetric(m)
	var shell *metricspb.Metric
	var pts []proto.Message
	delta := false
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		delta = d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
		shell = &metricspb.Metric{Name: name, Unit: unit, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: cumulativeOf(d.Sum.GetAggregationTemporality()), IsMonotonic: d.Sum.GetIsMonotonic(),
		}}}
		for _, dp := range d.Sum.GetDataPoints() {
			pts = append(pts, dp)
		}
	case *metricspb.Metric_Gauge:
		shell = &metricspb.Metric{Name: name, Unit: unit, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}}
		for _, dp := range d.Gauge.GetDataPoints() {
			pts = append(pts, dp)
		}
	case *metricspb.Metric_Histogram:
		delta = d.Histogram.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
		shell = &metricspb.Metric{Name: name, Unit: unit, Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			AggregationTemporality: cumulativeOf(d.Histogram.GetAggregationTemporality()),
		}}}
		for _, dp := range d.Histogram.GetDataPoints() {
			pts = append(pts, dp)
		}
	case *metricspb.Metric_ExponentialHistogram:
		return len(d.ExponentialHistogram.GetDataPoints()), "exponential histograms are not supported; use explicit buckets"
	default:
		return 0, ""
	}

	s := st.metrics[name]
	if s == nil || kindOf(s.shell) != kindOf(shell) {
		if s != nil {
			st.npoints -= len(s.points)
			r.nseries -= len(s.points)
		}
		s = &series{shell: shell, points: map[string]held{}}
		st.metrics[name] = s
	}
	refused, why := 0, ""
	for _, p := range pts {
		dropExemplars(p)
		p = proto.Clone(p)
		normalizePoint(p, scale)
		k := pointKey(pointAttrs(p))
		cur, ok := s.points[k]
		if delta {
			// An interval that ends no later than the last one added is a
			// retry of it (or older): adding it again would count it twice.
			if ok && pointTime(p) <= pointTime(cur.p) {
				continue
			}
			merged, reason := addDelta(cur.p, p)
			if reason != "" {
				refused++
				why = reason
				continue
			}
			p = merged
		}
		if !ok {
			if st.npoints >= r.opts.MaxPointsPerResource {
				refused++
				why = "series limit reached for a resource"
				continue
			}
			if r.nseries >= r.opts.MaxSeries {
				refused++
				why = "series limit reached"
				continue
			}
			st.npoints++
			r.nseries++
		} else if !delta && pointTime(p) < pointTime(cur.p) {
			continue // an older point than the one held: out of order
		}
		s.points[k] = held{p: p, seen: now}
		st.dirty = true
	}
	return refused, why
}

// dropExemplars removes a point's exemplars before it is stored: no
// collector reads them, and a histogram scraped from an instrumented server
// carries one per bucket, each with a trace id and attributes of its own.
func dropExemplars(p proto.Message) {
	switch dp := p.(type) {
	case *metricspb.NumberDataPoint:
		dp.Exemplars = nil
	case *metricspb.HistogramDataPoint:
		dp.Exemplars = nil
	}
}

// cumulativeOf is the temporality a stored series has: a delta one is held
// as the running total, so it is cumulative.
func cumulativeOf(t metricspb.AggregationTemporality) metricspb.AggregationTemporality {
	if t == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
		return metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	}
	return t
}

// addDelta returns the running total after adding the delta point p to cur,
// the total held so far (nil for the first point of a series). It returns a
// reason instead when the two cannot be added.
func addDelta(cur, p proto.Message) (proto.Message, string) {
	if cur == nil {
		return p, ""
	}
	switch c := cur.(type) {
	case *metricspb.NumberDataPoint:
		d, ok := p.(*metricspb.NumberDataPoint)
		if !ok {
			return nil, "a series changed its kind"
		}
		out := proto.Clone(c).(*metricspb.NumberDataPoint)
		out.TimeUnixNano = d.GetTimeUnixNano()
		_, cInt := c.GetValue().(*metricspb.NumberDataPoint_AsInt)
		_, dInt := d.GetValue().(*metricspb.NumberDataPoint_AsInt)
		if cInt && dInt {
			out.Value = &metricspb.NumberDataPoint_AsInt{AsInt: c.GetAsInt() + d.GetAsInt()}
		} else {
			out.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: numberValue(c) + numberValue(d)}
		}
		return out, ""
	case *metricspb.HistogramDataPoint:
		d, ok := p.(*metricspb.HistogramDataPoint)
		if !ok {
			return nil, "a series changed its kind"
		}
		if !slices.Equal(c.GetExplicitBounds(), d.GetExplicitBounds()) || len(c.GetBucketCounts()) != len(d.GetBucketCounts()) {
			return nil, "histogram bucket bounds changed"
		}
		out := proto.Clone(c).(*metricspb.HistogramDataPoint)
		out.TimeUnixNano = d.GetTimeUnixNano()
		out.Min, out.Max = nil, nil // the extremes of the first interval say nothing of the total
		out.Count = c.GetCount() + d.GetCount()
		for i := range out.BucketCounts {
			out.BucketCounts[i] += d.BucketCounts[i]
		}
		if c.Sum != nil && d.Sum != nil {
			sum := c.GetSum() + d.GetSum()
			out.Sum = &sum
		} else {
			out.Sum = nil
		}
		return out, ""
	}
	return nil, "a series changed its kind"
}

func numberValue(dp *metricspb.NumberDataPoint) float64 {
	if _, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt); ok {
		return float64(dp.GetAsInt())
	}
	return dp.GetAsDouble()
}

// logSummary logs what arrived since the last summary, and the sources held.
func (r *Receiver) logSummary() {
	r.mu.Lock()
	exports, points, rejected, unread, expired := r.exports, r.points, r.rejected, r.unread, r.expired
	r.exports, r.points, r.rejected, r.unread, r.expired = 0, 0, 0, 0, 0
	resources, series := len(r.res), r.nseries
	r.mu.Unlock()
	r.log.Info("otlp received", "exports", exports, "points", points, "rejected", rejected, "unread", unread,
		"expired", expired, "resources", resources, "series", series, "window", summaryEvery.String())
}

// sweepEvery is how often the series silent for Expire are looked for.
const sweepEvery = time.Minute

// sweep drops every series that no export updated for Expire: a metric's
// point and a span histogram alike. A resource expires as a whole only when
// it goes silent, and a node-level scrape never does while the pods under
// it come and go; without this it kept every series of what was gone until
// it hit MaxPointsPerResource and refused what was live. A resource left
// with nothing is kept until it expires itself. The caller holds r.mu.
func (r *Receiver) sweep(now time.Time) {
	if now.Sub(r.lastSweep) < sweepEvery {
		return
	}
	r.lastSweep = now
	for _, st := range r.res {
		for name, s := range st.metrics {
			for k, h := range s.points {
				if now.Sub(h.seen) <= r.opts.Expire {
					continue
				}
				delete(s.points, k)
				st.npoints--
				r.nseries--
				r.expired++
				st.dirty = true
			}
			if len(s.points) == 0 {
				delete(st.metrics, name)
			}
		}
		for k, s := range st.spans {
			if now.Sub(s.seen) > r.opts.Expire {
				delete(st.spans, k)
				r.nseries--
				r.expired++
			}
		}
	}
}

// flush hands every changed resource to the sink and drops expired ones.
func (r *Receiver) flush() {
	now := r.opts.now()
	type out struct {
		key, role string
		rms       []*metricspb.ResourceMetrics
	}
	var outs []out
	r.mu.Lock()
	r.sweep(now)
	for key, st := range r.res {
		if now.Sub(st.seen) > r.opts.Expire {
			delete(r.res, key)
			r.nseries -= st.npoints + len(st.spans)
			continue
		}
		if !st.dirty {
			continue
		}
		st.dirty = false
		if !st.announced {
			st.announced = true
			names := make([]string, 0, len(st.metrics))
			for n := range st.metrics {
				names = append(names, n)
			}
			sort.Strings(names)
			r.log.V(1).Info("otlp source", "key", key,
				"service", attr(st.resource.GetAttributes(), "service.name"),
				"namespace", attr(st.resource.GetAttributes(), "k8s.namespace.name"),
				"owner", attr(st.resource.GetAttributes(), "k8s.owner.name"),
				"derived", attr(st.resource.GetAttributes(), replicaAttr) != "",
				"metrics", names)
			if attr(st.resource.GetAttributes(), "k8s.namespace.name") == "" {
				r.log.Info("otlp source has no k8s.namespace.name: the sender did not enrich it, so the core cannot place it and it is held back",
					"service", attr(st.resource.GetAttributes(), "service.name"),
					"attributeKeys", attributeKeys(st.resource.GetAttributes()),
					"podIP", attr(st.resource.GetAttributes(), "k8s.pod.ip"),
					"containerID", attr(st.resource.GetAttributes(), "container.id"),
					"podUID", attr(st.resource.GetAttributes(), "k8s.pod.uid"))
			}
		}
		for role, rms := range st.build() {
			outs = append(outs, out{key: key, role: role, rms: rms})
		}
	}
	r.mu.Unlock()
	if r.opts.Sink == nil {
		return
	}
	for _, o := range outs {
		r.opts.Sink(o.key, o.role, o.rms)
	}
}

// build returns the resource's points as one ResourceMetrics per role.
func (st *resourceState) build() map[string][]*metricspb.ResourceMetrics {
	names := make([]string, 0, len(st.metrics))
	for n := range st.metrics {
		names = append(names, n)
	}
	sort.Strings(names)
	byRole := map[string][]*metricspb.Metric{}
	for _, n := range names {
		s := st.metrics[n]
		m := proto.Clone(s.shell).(*metricspb.Metric)
		keys := make([]string, 0, len(s.points))
		for k := range s.points {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			switch d := m.GetData().(type) {
			case *metricspb.Metric_Sum:
				d.Sum.DataPoints = append(d.Sum.DataPoints, s.points[k].p.(*metricspb.NumberDataPoint))
			case *metricspb.Metric_Gauge:
				d.Gauge.DataPoints = append(d.Gauge.DataPoints, s.points[k].p.(*metricspb.NumberDataPoint))
			case *metricspb.Metric_Histogram:
				d.Histogram.DataPoints = append(d.Histogram.DataPoints, s.points[k].p.(*metricspb.HistogramDataPoint))
			}
		}
		if role := RoleOf(n); role != "" {
			byRole[role] = append(byRole[role], m)
		}
	}
	out := make(map[string][]*metricspb.ResourceMetrics, len(byRole)+1)
	for role, ms := range byRole {
		out[role] = []*metricspb.ResourceMetrics{{
			Resource:     st.resource,
			ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp"}, Metrics: ms}},
		}}
	}
	// cAdvisor series become per-pod use.* points under an empty resource:
	// the scrape's own resource names the kubelet, not the pods.
	if ms := translateCAdvisor(st); len(ms) > 0 {
		out[metricset.RoleNodeExporter] = append(out[metricset.RoleNodeExporter], &metricspb.ResourceMetrics{
			Resource:     &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp/cadvisor"}, Metrics: ms}},
		})
	}
	return out
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

func kindOf(m *metricspb.Metric) string {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		return "sum/" + strconv.FormatBool(d.Sum.GetIsMonotonic())
	case *metricspb.Metric_Gauge:
		return "gauge"
	case *metricspb.Metric_Histogram:
		return "histogram"
	}
	return ""
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

// attributeKeys returns the keys of kvs, sorted: what a sender gave a source,
// without the values.
func attributeKeys(kvs []*commonpb.KeyValue) []string {
	keys := make([]string, 0, len(kvs))
	for _, a := range kvs {
		keys = append(keys, a.GetKey())
	}
	sort.Strings(keys)
	return keys
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/triageagent-dev/agent/internal/remotens"
	"github.com/triageagent-dev/agent/metricset"
)

// tenantAuth hands out tenant-certificate principals by token, standing in
// for the TLS layer that derives them from a verified client certificate.
type tenantAuth map[string][]string

func (a tenantAuth) Authenticate(_ context.Context, token string) (Principal, error) {
	return Principal{Name: "cert-" + token, Forwarder: true, Tenants: a[token]}, nil
}

func asToken(tok string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+tok))
}

func newTenantTest(t *testing.T, allowed ...string) (*Receiver, *[]sunk) {
	t.Helper()
	var got []sunk
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{
		Auth: tenantAuth{"acme": {"acme"}, "both": {"acme", "globex"}, "globex": {"globex"}},
		Sink: func(key, role string, rms []*metricspb.ResourceMetrics) {
			got = append(got, sunk{key: key, role: role, set: metricset.FromOTLP(rms)})
		},
		TenantAllowed: func(tenant string) bool {
			for _, a := range allowed {
				if a == tenant {
					return true
				}
			}
			return false
		},
		Log: logr.Discard(),
		now: func() time.Time { return now },
	})
	return r, &got
}

func tenantURI(domain, tenant string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: domain, Path: "/tenant/" + tenant}
}

func TestPrincipalFromCert_Tenants(t *testing.T) {
	c := &x509.Certificate{
		Subject: pkix.Name{CommonName: "remote-gateway"},
		URIs:    []*url.URL{tenantURI("example.org", "globex"), tenantURI("example.org", "acme"), tenantURI("example.org", "acme")},
	}
	p, err := principalFromCert(c)
	if err != nil || !p.Forwarder || !reflect.DeepEqual(p.Tenants, []string{"acme", "globex"}) {
		t.Fatalf("principal = %+v, want a forwarder with tenants acme,globex", p)
	}
	if p.String() != "remote-gateway" {
		t.Fatalf("String = %q", p.String())
	}

	// A tenant name outside the allowed shape is refused, not read as a plain
	// forwarder, and so is a malformed one beside a valid one.
	for _, bad := range []string{"Acme", "a~b", "-x", "", "a/b"} {
		c := &x509.Certificate{Subject: pkix.Name{CommonName: "x"}, URIs: []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/tenant/" + bad}}}
		if p, err := principalFromCert(c); err == nil {
			t.Fatalf("tenant %q accepted: %+v", bad, p)
		}
		c.URIs = append(c.URIs, tenantURI("example.org", "acme"))
		if p, err := principalFromCert(c); err == nil {
			t.Fatalf("tenant %q accepted beside a valid one: %+v", bad, p)
		}
	}
	if _, err := principalFromCert(&x509.Certificate{URIs: []*url.URL{{Scheme: "spiffe", Path: "/tenant/acme"}}}); err == nil {
		t.Fatal("a tenant URI without a trust domain was accepted")
	}
	// The tenant URI wins over a namespace one.
	both := &x509.Certificate{URIs: []*url.URL{tenantURI("example.org", "acme"), {Scheme: "spiffe", Host: "example.org", Path: "/ns/demo/sa/c"}}}
	if p, err := principalFromCert(both); err != nil || len(p.Tenants) != 1 || p.Namespace != "" {
		t.Fatalf("mixed certificate principal = %+v", p)
	}
}

func TestTenant_NamespacesAreQualified(t *testing.T) {
	r, got := newTenantTest(t, "acme")
	resp := export(t, r, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", "default"),
		counter("log.errors", 1, 1))
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("refused: %+v", resp.GetPartialSuccess())
	}
	r.flush()
	ns := sunkNamespaces(*got)
	if !ns["acme~default"] || ns["default"] {
		t.Fatalf("namespaces = %v, want only acme~default", ns)
	}
}

func TestTenant_SameNamespaceOfTwoTenantsStaysApart(t *testing.T) {
	r, got := newTenantTest(t, "acme", "globex")
	for _, tok := range []string{"acme", "globex"} {
		export(t, r, asToken(tok), resource("service.name", "cart", "k8s.namespace.name", "default"), counter("log.errors", 1, 1))
	}
	r.flush()
	if len(r.res) != 2 {
		t.Fatalf("resources = %d, want one per tenant", len(r.res))
	}
	ns := sunkNamespaces(*got)
	if !ns["acme~default"] || !ns["globex~default"] {
		t.Fatalf("namespaces = %v", ns)
	}
}

func TestTenant_PointNamespacesAndMissingNamespace(t *testing.T) {
	r, got := newTenantTest(t, "acme")
	// A point names its own namespace; a resource names none.
	export(t, r, asToken("acme"), resource("service.name", "gw"), counter("log.errors", 1, 1, strAttr("k8s.namespace.name", "shop")))
	export(t, r, asToken("acme"), resource("service.name", "node"), counter("log.warns", 1, 1))
	r.flush()
	ns := sunkNamespaces(*got)
	if !ns["acme~shop"] || !ns["acme~"] {
		t.Fatalf("namespaces = %v, want acme~shop and acme~", ns)
	}
	for k := range ns {
		if !strings.HasPrefix(k, "acme~") {
			t.Fatalf("unqualified namespace %q leaked", k)
		}
	}
}

func TestTenant_CadvisorNamespaceLabelQualified(t *testing.T) {
	r, _ := newTenantTest(t, "acme")
	export(t, r, asToken("acme"), resource("service.name", "kubelet"),
		counter("container_cpu_usage_seconds_total", 1, 1, strAttr("namespace", "shop"), strAttr("pod", "p1")),
		counter("app_orders_total", 1, 1, strAttr("namespace", "emea")))
	checked := 0
	for _, st := range r.res {
		for name, s := range st.metrics {
			for _, pt := range s.points {
				dp := pt.(*metricspb.NumberDataPoint)
				got := attr(dp.GetAttributes(), "namespace")
				checked++
				switch name {
				case "container_cpu_usage_seconds_total":
					if got != "acme~shop" {
						t.Fatalf("cAdvisor namespace label = %q, want acme~shop", got)
					}
				case "app_orders_total":
					if got != "emea" {
						t.Fatalf("application dimension namespace = %q, was rewritten", got)
					}
				}
			}
		}
	}
	if checked != 2 {
		t.Fatalf("checked %d points, want 2", checked)
	}
}

func TestTenant_Refusals(t *testing.T) {
	r, got := newTenantTest(t, "acme") // globex is in the certificate but not served here
	res := resource("service.name", "cart", "k8s.namespace.name", "default")

	// A tenant the agent does not serve.
	ps := export(t, r, asToken("globex"), res, counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "not served by this agent") {
		t.Fatalf("unserved tenant: %+v", ps)
	}
	// Two tenants in the certificate and no choice on the resource.
	ps = export(t, r, asToken("both"), res, counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "several tenants") {
		t.Fatalf("ambiguous tenant: %+v", ps)
	}
	// A resource that picks a tenant the certificate does not name.
	ps = export(t, r, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", "default", remotens.TenantAttr, "globex"),
		counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "not in the sender's certificate") {
		t.Fatalf("foreign tenant: %+v", ps)
	}
	r.flush()
	if len(*got) != 0 {
		t.Fatalf("refused points were flushed: %+v", *got)
	}
}

func TestTenant_ResourceChoosesAmongCertificateTenants(t *testing.T) {
	r, got := newTenantTest(t, "acme", "globex")
	for _, tenant := range []string{"acme", "globex"} {
		resp := export(t, r, asToken("both"),
			resource("service.name", "cart", "k8s.namespace.name", "default", remotens.TenantAttr, tenant), counter("log.errors", 1, 1))
		if resp.GetPartialSuccess() != nil {
			t.Fatalf("%s refused: %+v", tenant, resp.GetPartialSuccess())
		}
	}
	r.flush()
	ns := sunkNamespaces(*got)
	if !ns["acme~default"] || !ns["globex~default"] {
		t.Fatalf("namespaces = %v", ns)
	}
	for _, st := range r.res {
		if attr(st.resource.GetAttributes(), remotens.TenantAttr) != "" {
			t.Fatal("the tenant attribute was kept on the stored resource")
		}
	}
}

func TestTenant_NamespaceOutsideWorkspaceScopeRefused(t *testing.T) {
	r, got := newTenantTest(t, "acme", "globex")
	r.opts.NamespaceInScope = func(tenant, ns string) bool {
		return tenant != "acme" || ns == "shop" // acme is limited to shop
	}
	// A resource outside the scope is refused whole and counted.
	ps := export(t, r, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", "default"),
		counter("log.errors", 1, 1), counter("log.warns", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 2 || !strings.Contains(ps.GetErrorMessage(), "namespace outside the workspace scope") {
		t.Fatalf("out-of-scope resource: %+v", ps)
	}
	if r.SenderRefusals()["cert-acme"] != 2 {
		t.Fatalf("refusals = %v", r.SenderRefusals())
	}
	// The scope is per tenant: the same namespace of another tenant passes.
	if ps := export(t, r, asToken("globex"), resource("service.name", "cart", "k8s.namespace.name", "default"),
		counter("log.errors", 1, 1)).GetPartialSuccess(); ps != nil {
		t.Fatalf("globex refused: %+v", ps)
	}
	// An in-scope resource is stored; a point of its own outside the scope is
	// dropped on its own.
	ps = export(t, r, asToken("acme"), resource("service.name", "gw", "k8s.namespace.name", "shop"),
		counter("log.errors", 1, 1), counter("log.warns", 1, 1, strAttr("k8s.namespace.name", "default"))).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 {
		t.Fatalf("out-of-scope point: %+v", ps)
	}
	r.flush()
	ns := sunkNamespaces(*got)
	if !ns["acme~shop"] || !ns["globex~default"] || ns["acme~default"] {
		t.Fatalf("namespaces = %v", ns)
	}
}

func TestTenant_RepeatedResourceNamespaceRefused(t *testing.T) {
	r, got := newTenantTest(t, "acme")
	r.opts.NamespaceInScope = func(_, ns string) bool { return ns == "shop" }
	// In scope first, out of scope second: the scope check used to read the
	// first value while the collectors read the last.
	for _, order := range [][2]string{{"shop", "vault"}, {"vault", "shop"}} {
		ps := export(t, r, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", order[0], "k8s.namespace.name", order[1]),
			counter("log.errors", 1, 1)).GetPartialSuccess()
		if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "more than one k8s.namespace.name") {
			t.Fatalf("%v: %+v", order, ps)
		}
	}
	// The same value twice names one namespace and passes.
	if ps := export(t, r, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", "shop", "k8s.namespace.name", "shop"),
		counter("log.errors", 1, 1)).GetPartialSuccess(); ps != nil {
		t.Fatalf("one namespace twice refused: %+v", ps)
	}
	r.flush()
	if ns := sunkNamespaces(*got); len(ns) != 1 || !ns["acme~shop"] {
		t.Fatalf("namespaces = %v", ns)
	}
}

func TestIdentity_ForwarderRepeatedResourceNamespaceRefused(t *testing.T) {
	r, got, _ := newTest(t)
	ps := export(t, r, authed(), resource("k8s.namespace.name", "a", "k8s.namespace.name", "b"), counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %+v", ps)
	}
	r.flush()
	if len(*got) != 0 {
		t.Fatalf("refused points were flushed: %+v", *got)
	}
}

func TestAuthenticate_MalformedTenantCertificateRefused(t *testing.T) {
	r, _ := newTenantTest(t, "acme")
	ctx := func(c *x509.Certificate) context.Context {
		return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{c}}},
		}})
	}
	bad := &x509.Certificate{Subject: pkix.Name{CommonName: "gw"}, URIs: []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/tenant/Acme"}}}
	if _, err := r.Export(ctx(bad), &collectorpb.ExportMetricsServiceRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("malformed tenant certificate: %v", err)
	}
	good := &x509.Certificate{Subject: pkix.Name{CommonName: "gw"}, URIs: []*url.URL{tenantURI("example.org", "acme")}}
	if _, err := r.Export(ctx(good), &collectorpb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("tenant certificate: %v", err)
	}
}

// Only the receiver puts a tenant into a namespace. A forwarder or a token
// sender that names "<tenant>~<ns>" itself must not reach that tenant.
func TestTenant_SenderWithoutCertificateTenantCannotNameOne(t *testing.T) {
	r, got, _ := newTest(t)
	ps := export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "globex~prod"),
		counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "tenant separator") {
		t.Fatalf("qualified resource namespace: %+v", ps)
	}
	// A point of its own, and a flow-style source namespace, are dropped on
	// their own; the plain point is kept.
	ps = export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "prod"),
		counter("log.errors", 1, 1),
		counter("log.warns", 1, 1, strAttr("k8s.namespace.name", "globex~prod")),
		counter("flows", 1, 1, strAttr("k8s.src.namespace", "globex~prod"))).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 2 {
		t.Fatalf("qualified points: %+v", ps)
	}
	r.flush()
	for ns := range sunkNamespaces(*got) {
		if strings.Contains(ns, remotens.Sep) {
			t.Fatalf("qualified namespace reached the sink: %v", sunkNamespaces(*got))
		}
	}
	// A tenant certificate sender cannot pre-qualify a namespace either.
	rt, _ := newTenantTest(t, "acme", "globex")
	ps = export(t, rt, asToken("acme"), resource("service.name", "cart", "k8s.namespace.name", "globex~prod"),
		counter("log.errors", 1, 1)).GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 {
		t.Fatalf("tenant sender, qualified namespace: %+v", ps)
	}
}

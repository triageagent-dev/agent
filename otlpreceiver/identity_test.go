// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestIdentity_OwnNamespacePasses(t *testing.T) {
	r, got, _ := newTest(t)
	resp := export(t, r, asDemo(), resource("service.name", "cart", "k8s.namespace.name", "demo"), counter("log.errors", 1, 1))
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("own namespace refused: %+v", resp.GetPartialSuccess())
	}
	r.flush()
	if len(*got) == 0 {
		t.Fatal("nothing reached the sink")
	}
}

func TestIdentity_OtherNamespaceRefused(t *testing.T) {
	r, got, _ := newTest(t)
	resp := export(t, r, asDemo(), resource("service.name", "cart", "k8s.namespace.name", "example"),
		counter("log.errors", 1, 1), counter("log.warns", 1, 1))
	ps := resp.GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 2 || ps.GetErrorMessage() != "namespace example is not the sender's namespace" {
		t.Fatalf("partial success = %+v", ps)
	}
	r.flush()
	if len(*got) != 0 {
		t.Fatalf("refused points were flushed: %+v", *got)
	}
	if n := r.SenderRefusals()["demo/app"]; n != 2 {
		t.Fatalf("per-sender refusals = %d, want 2", n)
	}
}

func TestIdentity_ForwarderMayClaimAnyNamespace(t *testing.T) {
	r, _, _ := newTest(t)
	resp := export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "example"), counter("log.errors", 1, 1))
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("forwarder refused: %+v", resp.GetPartialSuccess())
	}
}

func TestIdentity_MissingNamespaceIsStamped(t *testing.T) {
	r, got, _ := newTest(t)
	export(t, r, asDemo(), resource("service.name", "cart"), counter("log.errors", 1, 1))
	if ns := sunkNamespaces(*got); !ns["demo"] || len(ns) != 1 {
		t.Fatalf("namespaces = %v, want the sender's own, demo", ns)
	}
}

// A forwarder's resource is not stamped with a namespace, and one that
// names none is refused: the core could not place it.
func TestIdentity_ForwarderMissingNamespaceIsRefused(t *testing.T) {
	r, got, _ := newTest(t)
	resp := export(t, r, authed(), resource("service.name", "cart"), counter("log.errors", 1, 1))
	ps := resp.GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 || !strings.Contains(ps.GetErrorMessage(), "no k8s.namespace.name") {
		t.Fatalf("partial success = %+v, want the point refused for the missing namespace", ps)
	}
	if len(*got) != 0 {
		t.Fatalf("an unplaceable resource reached the sink: %+v", *got)
	}
}

func TestPrincipalFromCert(t *testing.T) {
	spiffe := &x509.Certificate{URIs: []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/demo/sa/collector"}}}
	if p, err := principalFromCert(spiffe); err != nil || p.Namespace != "demo" || p.Name != "collector" || p.Forwarder || len(p.Tenants) != 0 {
		t.Fatalf("spiffe principal = %+v", p)
	}
	for _, c := range []*x509.Certificate{
		{Subject: pkix.Name{CommonName: "collector"}},
		{Subject: pkix.Name{CommonName: "collector"}, URIs: []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/workload/one"}}},
		{Subject: pkix.Name{CommonName: "collector"}, URIs: []*url.URL{{Scheme: "https", Host: "example.org", Path: "/ns/demo/sa/collector"}}},
	} {
		if p, err := principalFromCert(c); err != nil || !p.Forwarder || p.Namespace != "" {
			t.Fatalf("non-SPIFFE principal = %+v, want a forwarder", p)
		}
	}
}

func TestTokenReviewPrincipal_Forwarder(t *testing.T) {
	a := TokenReviewAuth{Allowed: []string{"obs/*"}, Forwarders: []string{"obs/gateway", "other/gateway"}}
	ns, sa, _ := splitServiceAccount("system:serviceaccount:obs:gateway")
	if !matches(a.Allowed, ns, sa) || !matches(a.Forwarders, ns, sa) {
		t.Fatal("obs/gateway should be allowed and a forwarder")
	}
	ns, sa, _ = splitServiceAccount("system:serviceaccount:obs:agent")
	if matches(a.Forwarders, ns, sa) {
		t.Fatal("obs/agent must not be a forwarder")
	}
}

// One sender may hold only MaxResourcesPerSender resources; another sender
// still gets its own, and an update of a held resource is not refused.
func TestSenderQuota(t *testing.T) {
	r, _, _ := newTest(t)
	r.opts.MaxResourcesPerSender = 2
	for _, svc := range []string{"a", "b"} {
		if resp := export(t, r, asDemo(), resource("service.name", svc), counter("log.errors", 1, 1)); resp.GetPartialSuccess() != nil {
			t.Fatalf("%s refused: %+v", svc, resp.GetPartialSuccess())
		}
	}
	resp := export(t, r, asDemo(), resource("service.name", "c"), counter("log.errors", 1, 1))
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() != 1 || ps.GetErrorMessage() != "sender resource quota reached" {
		t.Fatalf("quota not applied: %+v", ps)
	}
	if resp := export(t, r, asDemo(), resource("service.name", "a"), counter("log.errors", 2, 2)); resp.GetPartialSuccess() != nil {
		t.Fatalf("update of a held resource refused: %+v", resp.GetPartialSuccess())
	}
	if resp := export(t, r, authed(), resource("service.name", "c", "k8s.namespace.name", "obs"), counter("log.errors", 1, 1)); resp.GetPartialSuccess() != nil {
		t.Fatalf("another sender refused: %+v", resp.GetPartialSuccess())
	}
	if n := r.SenderRefusals()["demo/app"]; n != 1 {
		t.Fatalf("per-sender refusals = %d, want 1", n)
	}
}

func sumPoint(name string, ts uint64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return counter(name, ts, 1, attrs...)
}

func expHistogram(name string, ts uint64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
		AggregationTemporality: cum,
		DataPoints:             []*metricspb.ExponentialHistogramDataPoint{{Attributes: attrs, TimeUnixNano: ts, Count: 1}},
	}}}
}

// sunkNamespaces returns every k8s.namespace.name any sunk point carries.
func sunkNamespaces(got []sunk) map[string]bool {
	out := map[string]bool{}
	for _, s := range got {
		for _, m := range s.set {
			for _, p := range m.Points {
				out[p.Attrs["k8s.namespace.name"]] = true
			}
		}
	}
	return out
}

func TestIdentity_PointClaimingOtherNamespaceRefused(t *testing.T) {
	other := strAttr("k8s.namespace.name", "other")
	cases := map[string]*metricspb.Metric{
		"sum":           sumPoint("log.errors", 1, other),
		"histogram":     histogram("http.server.request.duration", "s", cum, 1, 1, []float64{1}, other),
		"exponential":   expHistogram("http.server.request.duration", 1, other),
		"duplicate key": sumPoint("log.errors", 1, strAttr("k8s.namespace.name", "demo"), other),
		"int value":     sumPoint("log.errors", 1, intAttr("k8s.namespace.name", 7)),
		"empty value":   sumPoint("log.errors", 1, strAttr("k8s.namespace.name", "")),
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			r, got, _ := newTest(t)
			resp := export(t, r, asDemo(), resource("service.name", "cart", "k8s.namespace.name", "demo"), m)
			ps := resp.GetPartialSuccess()
			if ps.GetRejectedDataPoints() != 1 || !strings.HasSuffix(ps.GetErrorMessage(), " is not the sender's namespace") {
				t.Fatalf("partial success = %+v", ps)
			}
			r.flush()
			if ns := sunkNamespaces(*got); len(ns) != 0 {
				t.Fatalf("points reached the sink under %v", ns)
			}
			if n := r.SenderRefusals()["demo/app"]; n != 1 {
				t.Fatalf("per-sender refusals = %d, want 1", n)
			}
		})
	}
	t.Run("message names the namespace", func(t *testing.T) {
		r, _, _ := newTest(t)
		resp := export(t, r, asDemo(), resource("service.name", "cart"), sumPoint("log.errors", 1, other))
		if msg := resp.GetPartialSuccess().GetErrorMessage(); msg != "namespace other is not the sender's namespace" {
			t.Fatalf("message = %q", msg)
		}
	})
}

func TestIdentity_ResourceDuplicateNamespaceRefused(t *testing.T) {
	r, _, _ := newTest(t)
	resp := export(t, r, asDemo(), resource("k8s.namespace.name", "demo", "k8s.namespace.name", "other"), counter("log.errors", 1, 1))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %+v", resp.GetPartialSuccess())
	}
}

func TestIdentity_MixedPointsKeepOwnAndUnlabelled(t *testing.T) {
	r, got, _ := newTest(t)
	resp := export(t, r, asDemo(), resource("service.name", "cart"),
		sumPoint("log.errors", 1, strAttr("k8s.namespace.name", "demo")),
		sumPoint("log.warns", 1),
		sumPoint("log.errors", 2, strAttr("k8s.namespace.name", "other"), strAttr("k8s.pod.name", "p")))
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %+v", ps)
	}
	r.flush()
	ns := sunkNamespaces(*got)
	if len(ns) != 1 || !ns["demo"] {
		t.Fatalf("namespaces sunk = %v, want only demo", ns)
	}
	npoints := 0
	for _, s := range *got {
		for _, m := range s.set {
			npoints += len(m.Points)
		}
	}
	if npoints != 2 {
		t.Fatalf("points sunk = %d, want 2", npoints)
	}
}

func TestIdentity_ForwarderPointsPassUnchanged(t *testing.T) {
	r, got, _ := newTest(t)
	resp := export(t, r, authed(), resource("service.name", "cart"),
		sumPoint("log.errors", 1, strAttr("k8s.namespace.name", "other")),
		expHistogram("http.server.request.duration", 1, strAttr("k8s.namespace.name", "other")))
	// the exponential histogram is refused for its type, not its namespace
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() != 1 || strings.Contains(ps.GetErrorMessage(), "namespace") {
		t.Fatalf("partial success = %+v", ps)
	}
	r.flush()
	if ns := sunkNamespaces(*got); !ns["other"] {
		t.Fatalf("forwarder's point lost: %v", ns)
	}
}

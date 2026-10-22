// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"testing"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func rmIn(ns string, pointNS ...string) *metricspb.ResourceMetrics {
	var ms []*metricspb.Metric
	for _, p := range pointNS {
		ms = append(ms, counter("requests", 1, 1, strAttr("k8s.namespace.name", p)))
	}
	if len(ms) == 0 {
		ms = append(ms, counter("requests", 1, 1))
	}
	return &metricspb.ResourceMetrics{
		Resource:     resource("k8s.namespace.name", ns, "service.name", "svc"),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: ms}},
	}
}

func TestScopeFilter(t *testing.T) {
	allowed := func(tenant string) bool { return tenant == "acme" }
	inScope := func(tenant, ns string) bool { return ns != "secret" }
	f := ScopeFilter(allowed, inScope)

	cases := []struct {
		name string
		in   *metricspb.ResourceMetrics
		keep bool
	}{
		{"a plain namespace passes", rmIn("shop"), true},
		{"an allowed tenant in scope passes", rmIn("acme~shop"), true},
		{"a tenant the agent did not join is dropped", rmIn("other~shop"), false},
		{"a namespace outside the workspace scope is dropped", rmIn("acme~secret"), false},
		{"a tenant with no namespace of its own passes", rmIn("acme~"), true},
	}
	for _, c := range cases {
		got := f([]*metricspb.ResourceMetrics{c.in})
		if (len(got) == 1) != c.keep {
			t.Errorf("%s: kept %d resources, want keep=%v", c.name, len(got), c.keep)
		}
	}
}

func TestScopeFilter_DropsOnlyTheOutOfScopePoints(t *testing.T) {
	f := ScopeFilter(func(string) bool { return true }, func(_, ns string) bool { return ns != "secret" })
	got := f([]*metricspb.ResourceMetrics{rmIn("acme~", "acme~shop", "acme~secret")})
	if len(got) != 1 {
		t.Fatalf("resources = %d, want 1", len(got))
	}
	if n := countPoints(got[0]); n != 1 {
		t.Fatalf("points = %d, want only the in-scope one", n)
	}
}

func TestScopeFilter_NilChecksAllowEverythingButMultipleNamespaces(t *testing.T) {
	f := ScopeFilter(nil, nil)
	if got := f([]*metricspb.ResourceMetrics{rmIn("anyone~anything")}); len(got) != 1 {
		t.Fatalf("with no checks a qualified resource was dropped: %d", len(got))
	}
	rm := rmIn("a")
	rm.Resource.Attributes = append(rm.Resource.Attributes, strAttr("k8s.namespace.name", "b"))
	if got := f([]*metricspb.ResourceMetrics{rm}); len(got) != 0 {
		t.Fatal("a resource naming two namespaces passed")
	}
}

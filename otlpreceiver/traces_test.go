// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"testing"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/triageagent-dev/agent/metricset"
)

func span(name string, kind tracepb.Span_SpanKind, code tracepb.Status_StatusCode, dur uint64, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{
		Name: name, Kind: kind, Status: &tracepb.Status{Code: code},
		StartTimeUnixNano: 1_000_000_000, EndTimeUnixNano: 1_000_000_000 + dur, Attributes: attrs,
	}
}

func exportSpans(t *testing.T, r *Receiver, res *resourcepb.Resource, spans ...*tracepb.Span) *collectortrace.ExportTraceServiceResponse {
	t.Helper()
	resp, err := traceService{r: r}.Export(authed(), &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: res, ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

const (
	srv  = tracepb.Span_SPAN_KIND_SERVER
	cli  = tracepb.Span_SPAN_KIND_CLIENT
	ok   = tracepb.Status_STATUS_CODE_UNSET
	fail = tracepb.Status_STATUS_CODE_ERROR
	ms   = uint64(1_000_000)
)

func ebpfRole(t *testing.T, r *Receiver, got *[]sunk) metricset.Set {
	t.Helper()
	r.flush()
	for _, s := range *got {
		if s.role == metricset.RoleEBPF {
			return s.set
		}
	}
	t.Fatalf("no ebpf-role snapshot flushed: %+v", *got)
	return nil
}

func TestTraces_RefusesWithoutToken(t *testing.T) {
	r, _, _ := newTest(t)
	if _, err := (traceService{r: r}).Export(context.Background(), &collectortrace.ExportTraceServiceRequest{}); err == nil {
		t.Fatal("an export of spans without a token was accepted")
	}
}

func TestTraces_ServerAndClientSpansBecomeREDHistograms(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	exportSpans(t, r, res,
		span("GET /cart", srv, ok, 30*ms, strAttr("http.request.method", "GET"), intAttr("http.response.status_code", 200), strAttr("http.route", "/cart")),
		span("GET /cart", srv, ok, 70*ms, strAttr("http.request.method", "GET"), intAttr("http.response.status_code", 200), strAttr("http.route", "/cart")),
		span("GET /cart", srv, fail, 5*ms, strAttr("http.request.method", "GET"), strAttr("http.route", "/cart")),
		span("GET payment", cli, ok, 20*ms, strAttr("http.request.method", "GET"), strAttr("server.address", "payment")),
		span("internal work", tracepb.Span_SPAN_KIND_INTERNAL, ok, 9*ms),
	)
	set := ebpfRole(t, r, got)

	server := set.Get("http.server.request.duration")
	if server == nil || len(server.Points) != 2 {
		t.Fatalf("server histogram = %+v, want two series (200 and the failed one)", server)
	}
	byCode := map[string]float64{}
	for _, p := range server.Points {
		byCode[p.Attr("http.response.status_code")] = p.Value
	}
	if byCode["200"] != 2 || byCode["500"] != 1 {
		t.Fatalf("server counts by status = %v, want 200:2 and 500:1 (a failed span with no status code is a 500)", byCode)
	}
	client := set.Get("http.client.request.duration")
	if client == nil || len(client.Points) != 1 || client.Points[0].Value != 1 || client.Points[0].Attr("server.address") != "payment" {
		t.Fatalf("client histogram = %+v", client)
	}
	if set.Get("traces.span.metrics.duration") != nil || set.Get("http.server.request.duration") == nil {
		t.Fatal("the connector-shaped metric leaked past the translation")
	}
}

func TestTraces_GRPCSpansBecomeRPCHistograms(t *testing.T) {
	r, got, _ := newTest(t)
	exportSpans(t, r, resource("service.name", "cart", "k8s.namespace.name", "shop"),
		span("hipstershop.CartService/GetCart", srv, ok, 10*ms, strAttr("rpc.system", "grpc"), strAttr("rpc.method", "GetCart"), intAttr("rpc.grpc.status_code", 0)),
		span("hipstershop.ProductCatalog/List", cli, ok, 10*ms, strAttr("rpc.system", "grpc"), strAttr("rpc.method", "List")),
	)
	set := ebpfRole(t, r, got)
	if m := set.Get("rpc.server.call.duration"); m == nil || m.Points[0].Value != 1 {
		t.Fatalf("rpc server histogram = %+v", m)
	}
	if m := set.Get("rpc.client.call.duration"); m == nil || m.Points[0].Value != 1 {
		t.Fatalf("rpc client histogram = %+v", m)
	}
}

// An SDK on the current semantic conventions names the system rpc.system.name:
// its server spans are gRPC, not HTTP, and its status is rpc.response.status_code.
func TestTraces_GRPCSpansOfTheCurrentConventionsBecomeRPCHistograms(t *testing.T) {
	r, got, _ := newTest(t)
	exportSpans(t, r, resource("service.name", "product-catalog", "k8s.namespace.name", "shop"),
		span("oteldemo.ProductCatalogService/GetProduct", srv, ok, 10*ms, strAttr("rpc.system.name", "grpc"), strAttr("rpc.method", "GetProduct"), strAttr("rpc.response.status_code", "OK")),
	)
	set := ebpfRole(t, r, got)
	if m := set.Get("rpc.server.call.duration"); m == nil || m.Points[0].Value != 1 {
		t.Fatalf("rpc server histogram = %+v", m)
	}
	if m := set.Get("http.server.request.duration"); m != nil {
		t.Fatalf("a gRPC span was counted as HTTP too: %+v", m)
	}
}

// What comes out of spans is what the connector's metric comes out as: the
// same series, with the same counts, as translateSpanMetrics gives for the
// connector's output of the same traffic.
func TestTraces_AgreeWithTheConnectorPath(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	exportSpans(t, r, res,
		span("GET /cart", srv, ok, 30*ms, strAttr("http.request.method", "GET"), intAttr("http.response.status_code", 200), strAttr("http.route", "/cart")),
		span("GET /cart", srv, ok, 30*ms, strAttr("http.request.method", "GET"), intAttr("http.response.status_code", 200), strAttr("http.route", "/cart")),
	)
	derived := ebpfRole(t, r, got).Get("http.server.request.duration")

	// The connector exports milliseconds by default.
	h := &metricspb.Histogram{AggregationTemporality: cum, DataPoints: []*metricspb.HistogramDataPoint{{
		Attributes: []*commonpb.KeyValue{
			strAttr("span.name", "GET /cart"), strAttr("span.kind", "SPAN_KIND_SERVER"), strAttr("status.code", "STATUS_CODE_UNSET"),
			strAttr("http.request.method", "GET"), intAttr("http.response.status_code", 200), strAttr("http.route", "/cart"),
		},
		TimeUnixNano: 5, Count: 2, ExplicitBounds: []float64{2, 100}, BucketCounts: []uint64{0, 2, 0},
	}}}
	conn, _ := translateSpanMetrics(&metricspb.Metric{Name: spanMetricsDuration, Unit: "ms", Data: &metricspb.Metric_Histogram{Histogram: h}})
	if len(conn) != 1 {
		t.Fatalf("connector path gave %d metrics", len(conn))
	}
	want := conn[0].GetHistogram().GetDataPoints()[0]
	if len(derived.Points) != 1 {
		t.Fatalf("derived series = %d, want 1", len(derived.Points))
	}
	got0 := derived.Points[0]
	if got0.Value != float64(want.GetCount()) {
		t.Fatalf("count = %v, want %v", got0.Value, want.GetCount())
	}
	for _, k := range []string{"span.name", "span.kind", "status.code", "http.request.method", "http.response.status_code", "http.route"} {
		var w string
		for _, a := range want.GetAttributes() {
			if a.GetKey() == k {
				w = anyString(a.GetValue())
			}
		}
		if got0.Attr(k) != w {
			t.Errorf("attribute %s = %q, want %q", k, got0.Attr(k), w)
		}
	}
}

func TestTraces_CountsAreCumulativeAcrossExports(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	one := span("GET /", srv, ok, 10*ms, intAttr("http.response.status_code", 200))
	exportSpans(t, r, res, one)
	if v := ebpfRole(t, r, got).Get("http.server.request.duration").Points[0].Value; v != 1 {
		t.Fatalf("after the first export count = %v, want 1", v)
	}
	*got = nil
	exportSpans(t, r, res, one, one)
	if v := ebpfRole(t, r, got).Get("http.server.request.duration").Points[0].Value; v != 3 {
		t.Fatalf("after the second export count = %v, want 3 (cumulative)", v)
	}
}

func TestTraces_DerivedResourceCarriesTheReplica(t *testing.T) {
	r, got, _ := newTest(t)
	r.opts.Replica = "pod-a"
	exportSpans(t, r, resource("service.name", "checkout", "k8s.namespace.name", "shop"), span("GET /", srv, ok, ms, intAttr("http.response.status_code", 200)))
	p := ebpfRole(t, r, got).Get("http.server.request.duration").Points[0]
	if p.Attr(replicaAttr) != "pod-a" {
		t.Fatalf("replica attribute = %q, want pod-a (attrs %v)", p.Attr(replicaAttr), p.Attrs)
	}

	// A second receiver sees another share of the same service: a separate source.
	r2, got2, _ := newTest(t)
	r2.opts.Replica = "pod-b"
	exportSpans(t, r2, resource("service.name", "checkout", "k8s.namespace.name", "shop"), span("GET /", srv, ok, ms, intAttr("http.response.status_code", 200)))
	ebpfRole(t, r2, got2)
	if (*got)[0].key == (*got2)[0].key {
		t.Fatal("two replicas derived the same source key: the newest would overwrite the other's share")
	}
}

func TestTraces_SeriesLimitRefusesNewSeries(t *testing.T) {
	r, _, _ := newTest(t)
	r.opts.MaxPointsPerResource = 2
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	resp := exportSpans(t, r, res,
		span("a", srv, ok, ms), span("b", srv, ok, ms), span("c", srv, ok, ms),
	)
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("rejected = %d, want 1 (the third series)", resp.GetPartialSuccess().GetRejectedSpans())
	}
}

func TestTraces_SenderMayNotPublishForAnotherNamespace(t *testing.T) {
	r, _, _ := newTest(t)
	resp, err := traceService{r: r}.Export(asDemo(), &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   resource("service.name", "x", "k8s.namespace.name", "other"),
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span("a", srv, ok, ms)}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("a span claiming another namespace was accepted: %+v", resp.GetPartialSuccess())
	}
}

func TestTraces_TenantSenderNamespacesAreQualified(t *testing.T) {
	r, got := newTenantTest(t, "acme")
	_, err := traceService{r: r}.Export(asToken("acme"), &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   resource("service.name", "x", "k8s.namespace.name", "shop"),
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span("a", srv, ok, ms, intAttr("http.response.status_code", 200))}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	p := ebpfRole(t, r, got).Get("http.server.request.duration").Points[0]
	if p.Attr("k8s.namespace.name") != "acme~shop" {
		t.Fatalf("namespace = %q, want acme~shop", p.Attr("k8s.namespace.name"))
	}
}

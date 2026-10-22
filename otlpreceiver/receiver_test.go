// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/metadata"

	"github.com/triageagent-dev/agent/metricset"
)

type okAuth struct{}

// okAuth accepts the token "good" as a forwarder, so tests may claim any
// namespace; "demo" is a plain ServiceAccount of namespace demo.
func (okAuth) Authenticate(_ context.Context, token string) (Principal, error) {
	switch token {
	case "good":
		return Principal{Namespace: "obs", Name: "otel-collector", Forwarder: true}, nil
	case "demo":
		return Principal{Namespace: "demo", Name: "app"}, nil
	}
	return Principal{}, errors.New("bad token")
}

func asDemo() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer demo"))
}

type sunk struct {
	key, role string
	set       metricset.Set
}

func newTest(t *testing.T) (*Receiver, *[]sunk, *time.Time) {
	t.Helper()
	var got []sunk
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{
		Auth: okAuth{},
		Sink: func(key, role string, rms []*metricspb.ResourceMetrics) {
			got = append(got, sunk{key: key, role: role, set: metricset.FromOTLP(rms)})
		},
		Log: logr.Discard(),
		now: func() time.Time { return now },
	})
	return r, &got, &now
}

func authed() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer good"))
}

func resource(kv ...string) *resourcepb.Resource {
	r := &resourcepb.Resource{}
	for i := 0; i+1 < len(kv); i += 2 {
		r.Attributes = append(r.Attributes, strAttr(kv[i], kv[i+1]))
	}
	return r
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func intAttr(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}

func histogram(name, unit string, temp metricspb.AggregationTemporality, ts uint64, count uint64, bounds []float64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	counts := make([]uint64, len(bounds)+1)
	counts[0] = count
	return &metricspb.Metric{Name: name, Unit: unit, Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		AggregationTemporality: temp,
		DataPoints: []*metricspb.HistogramDataPoint{{
			Attributes: attrs, TimeUnixNano: ts, Count: count, ExplicitBounds: bounds, BucketCounts: counts,
		}},
	}}}
}

func counter(name string, ts uint64, v float64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		IsMonotonic: true, AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		DataPoints: []*metricspb.NumberDataPoint{{Attributes: attrs, TimeUnixNano: ts, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}}},
	}}}
}

func export(t *testing.T, r *Receiver, ctx context.Context, res *resourcepb.Resource, ms ...*metricspb.Metric) *collectorpb.ExportMetricsServiceResponse {
	t.Helper()
	resp, err := r.Export(ctx, &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: res, ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: ms}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

const cum = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE

func TestExport_RefusesWithoutToken(t *testing.T) {
	r, _, _ := newTest(t)
	if _, err := r.Export(context.Background(), &collectorpb.ExportMetricsServiceRequest{}); err == nil {
		t.Fatal("an export without a token was accepted")
	}
	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer nope"))
	if _, err := r.Export(bad, &collectorpb.ExportMetricsServiceRequest{}); err == nil {
		t.Fatal("an export with a bad token was accepted")
	}
}

// Batches of one resource that carry different metrics add up to one
// snapshot, and the RED and USE families go to their own roles.
func TestExport_AccumulatesAndRoutesByRole(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop", "k8s.deployment.name", "checkout")
	export(t, r, authed(), res, histogram("http.server.request.duration", "s", cum, 10, 5, []float64{0.1}, strAttr("http.response.status_code", "200")))
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 42, strAttr("k8s.pod.name", "checkout-1")))
	r.flush()

	roles := map[string]metricset.Set{}
	for _, s := range *got {
		roles[s.role] = s.set
	}
	ebpf := roles[metricset.RoleEBPF]
	if m := ebpf.Get("http.server.request.duration"); m == nil || m.Points[0].Value != 5 {
		t.Fatalf("ebpf role: %+v", ebpf)
	}
	p := ebpf.Get("http.server.request.duration").Points[0]
	if p.Attr("k8s.owner.name") != "checkout" || p.Attr("service.name") != "checkout" {
		t.Fatalf("resource attributes not on the point: %v", p.Attrs)
	}
	if ebpf.Get("use.cpu.usage.usec") != nil {
		t.Fatal("a use.* metric landed in the ebpf role")
	}
	if m := roles[metricset.RoleNodeExporter].Get("use.cpu.usage.usec"); m == nil || m.Points[0].Value != 42 {
		t.Fatalf("node-exporter role: %+v", roles[metricset.RoleNodeExporter])
	}

	// Nothing changed: nothing flushed again.
	*got = nil
	r.flush()
	if len(*got) != 0 {
		t.Fatalf("an unchanged resource was flushed again: %d", len(*got))
	}
}

func TestExport_KeepsNewestPointPerSeries(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart")
	export(t, r, authed(), res, counter("log.errors", 20, 7))
	export(t, r, authed(), res, counter("log.errors", 10, 3)) // out of order
	r.flush()
	if len(*got) != 1 || (*got)[0].role != metricset.RoleLogExporter {
		t.Fatalf("flushed %+v", *got)
	}
	if v := (*got)[0].set.Get("log.errors").Points[0].Value; v != 7 {
		t.Fatalf("log.errors = %v, want the newer 7", v)
	}
}

func TestExport_RejectsExponentialHistograms(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart")
	exp := &metricspb.Metric{Name: "x", Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
		DataPoints: []*metricspb.ExponentialHistogramDataPoint{{}},
	}}}
	resp := export(t, r, authed(), res, exp)
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() != 1 || ps.GetErrorMessage() == "" {
		t.Fatalf("partial success = %+v", ps)
	}
	r.flush()
	if len(*got) != 0 {
		t.Fatalf("rejected points were flushed: %+v", *got)
	}
}

// Older semantic conventions are rewritten to the names the RED profile
// reads: metric name, milliseconds to seconds, status attributes, and the
// numeric gRPC status as its code name.
func TestExport_NormalizesOlderConventions(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "payment")
	export(t, r, authed(), res,
		histogram("http.server.duration", "ms", cum, 10, 3, []float64{100, 250}, intAttr("http.status_code", 503), strAttr("http.method", "GET")),
		histogram("rpc.server.duration", "ms", cum, 10, 2, []float64{50}, intAttr("rpc.grpc.status_code", 14)))
	r.flush()
	set := (*got)[0].set

	h := set.Get("http.server.request.duration")
	if h == nil {
		t.Fatalf("http.server.duration not renamed: %v", set)
	}
	p := h.Points[0]
	if p.Attr("http.response.status_code") != "503" || p.Attr("http.request.method") != "GET" {
		t.Fatalf("attributes not renamed: %v", p.Attrs)
	}
	if b := p.Buckets[0].UpperBound; math.Abs(b-0.1) > 1e-9 {
		t.Fatalf("first bound = %v, want 0.1 s", b)
	}
	g := set.Get("rpc.server.call.duration")
	if g == nil || g.Points[0].Attr("rpc.response.status_code") != "UNAVAILABLE" {
		t.Fatalf("gRPC status not mapped: %+v", g)
	}
}

func TestExport_Limits(t *testing.T) {
	r, _, _ := newTest(t)
	r.opts.MaxResources, r.opts.MaxPointsPerResource = 1, 1
	export(t, r, authed(), resource("service.name", "a"), counter("log.errors", 1, 1, strAttr("k", "1")))
	resp := export(t, r, authed(), resource("service.name", "a"), counter("log.errors", 1, 1, strAttr("k", "2")))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("series limit not applied: %+v", resp.GetPartialSuccess())
	}
	resp = export(t, r, authed(), resource("service.name", "b"), counter("log.errors", 1, 1))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("resource limit not applied: %+v", resp.GetPartialSuccess())
	}
}

func TestFlush_ExpiresSilentResources(t *testing.T) {
	r, _, now := newTest(t)
	export(t, r, authed(), resource("service.name", "a"), counter("log.errors", 1, 1))
	*now = now.Add(r.opts.Expire + time.Second)
	r.flush()
	if len(r.res) != 0 {
		t.Fatalf("a silent resource was kept: %d", len(r.res))
	}
}

func TestParseAllowedAndAllows(t *testing.T) {
	if _, err := ParseAllowed("obs"); err == nil {
		t.Fatal("an entry without a slash was accepted")
	}
	allowed, err := ParseAllowed(" obs/otel-collector , apps/* ")
	if err != nil {
		t.Fatal(err)
	}
	a := TokenReviewAuth{Allowed: allowed}
	for user, want := range map[string]bool{
		"system:serviceaccount:obs:otel-collector": true,
		"system:serviceaccount:obs:other":          false,
		"system:serviceaccount:apps:anything":      true,
		"admin":                                    false,
	} {
		if a.allows(user) != want {
			t.Errorf("allows(%q) = %v, want %v", user, !want, want)
		}
	}
}

func TestRoleOf(t *testing.T) {
	for name, want := range map[string]string{
		"http.server.request.duration": metricset.RoleEBPF,
		"obi.network.flow.bytes":       metricset.RoleEBPF,
		"use.mem.bytes":                metricset.RoleNodeExporter,
		"log.warns":                    metricset.RoleLogExporter,
		"log.exporter.lines.processed": metricset.RoleEBPF,
	} {
		if got := RoleOf(name); got != want {
			t.Errorf("RoleOf(%q) = %q, want %q", name, got, want)
		}
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/usemetrics"
)

func gaugeM(name string, ts uint64, v float64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
		DataPoints: []*metricspb.NumberDataPoint{{Attributes: attrs, TimeUnixNano: ts, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}}},
	}}}
}

// podAttrs are a pod's own cgroup labels in cAdvisor: empty container and
// image.
func podAttrs(ns, pod string) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{strAttr("namespace", ns), strAttr("pod", pod), strAttr("container", ""), strAttr("image", "")}
}

func cadvisorScrape(ts uint64, cpuSec, psiCPU float64) []*metricspb.Metric {
	pod := podAttrs("shop", "cart-1")
	ctr := []*commonpb.KeyValue{strAttr("namespace", "shop"), strAttr("pod", "cart-1"), strAttr("container", "cart"), strAttr("image", "cart:1")}
	return []*metricspb.Metric{
		counter("container_cpu_usage_seconds_total", ts, cpuSec, pod...),
		counter("container_cpu_usage_seconds_total", ts, 999, ctr...), // a container's: ignored
		counter("container_pressure_cpu_waiting_seconds_total", ts, psiCPU, pod...),
		gaugeM("container_memory_working_set_bytes", ts, 256<<20, pod...),
		gaugeM("container_spec_memory_limit_bytes", ts, 512<<20, pod...),
		gaugeM("container_spec_cpu_quota", ts, 50000, pod...),
		gaugeM("container_spec_cpu_period", ts, 100000, pod...),
		gaugeM("machine_cpu_cores", ts, 16),
	}
}

func TestCAdvisor_TranslatesPodCgroupToUSE(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "kubelet-cadvisor")
	const s = uint64(1e9)
	export(t, r, authed(), res, cadvisorScrape(10*s, 2, 1)...)
	r.flush()
	export(t, r, authed(), res, cadvisorScrape(20*s, 3, 2)...) // +1 s waiting over 10 s: 10%
	r.flush()

	var use metricset.Set
	for _, s := range *got {
		if s.role == metricset.RoleNodeExporter {
			use = s.set
		}
		if s.role == metricset.RoleEBPF && s.set.Get("container_cpu_usage_seconds_total") != nil {
			t.Fatal("raw cAdvisor series were routed to a collector")
		}
	}
	if use == nil {
		t.Fatalf("no node-exporter set: %+v", *got)
	}
	one := func(name string) metricset.Point {
		t.Helper()
		m := use.Get(name)
		if m == nil || len(m.Points) != 1 {
			t.Fatalf("%s = %+v", name, m)
		}
		p := m.Points[0]
		if p.Attr(usemetrics.LabelPod) != "cart-1" || p.Attr(usemetrics.LabelNamespace) != "shop" {
			t.Fatalf("%s attrs = %v", name, p.Attrs)
		}
		if p.Attr("service.name") != "" {
			t.Fatalf("%s carries the scrape's service.name: %v", name, p.Attrs)
		}
		return p
	}
	if v := one(usemetrics.MetricCPUUsageUsecTotal).Value; v != 3e6 {
		t.Errorf("cpu usage usec = %v, want 3e6 (the pod's, not the container's)", v)
	}
	if v := one(usemetrics.MetricMemWorkingSetPct).Value; v != 50 {
		t.Errorf("ws pct = %v, want 50", v)
	}
	if v := one(usemetrics.MetricCPULimitCores).Value; v != 0.5 {
		t.Errorf("cpu limit cores = %v, want 0.5", v)
	}
	if v := one(usemetrics.MetricCPUNodeCores).Value; v != 16 {
		t.Errorf("node cores = %v, want 16", v)
	}
	if v := one(usemetrics.MetricCPUPsiAvg60).Value; math.Abs(v-10) > 1e-9 {
		t.Errorf("cpu psi = %v, want 10", v)
	}
}

func spanPoint(kind, status string, count uint64, attrs ...*commonpb.KeyValue) *metricspb.HistogramDataPoint {
	attrs = append(attrs, strAttr("span.kind", kind), strAttr("status.code", status))
	return &metricspb.HistogramDataPoint{Attributes: attrs, TimeUnixNano: 10, Count: count,
		ExplicitBounds: []float64{100, 1000}, BucketCounts: []uint64{count, 0, 0}}
}

func TestSpanMetrics_TranslateToREDAndEdges(t *testing.T) {
	r, got, _ := newTest(t)
	m := &metricspb.Metric{Name: spanMetricsDuration, Unit: "ms", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		AggregationTemporality: cum,
		DataPoints: []*metricspb.HistogramDataPoint{
			spanPoint("SPAN_KIND_SERVER", "STATUS_CODE_ERROR", 4, strAttr("span.name", "GET /cart")),
			spanPoint("SPAN_KIND_SERVER", "STATUS_CODE_UNSET", 6, strAttr("rpc.system", "grpc"), intAttr("rpc.grpc.status_code", 14), strAttr("rpc.method", "GetCart")),
			spanPoint("SPAN_KIND_CLIENT", "STATUS_CODE_UNSET", 3, strAttr("server.address", "redis"), strAttr("span.name", "HGET")),
			spanPoint("SPAN_KIND_INTERNAL", "STATUS_CODE_UNSET", 9),
		},
	}}}
	calls := counter(spanMetricsCalls, 10, 13)
	export(t, r, authed(), resource("service.name", "cart", "k8s.namespace.name", "shop"), m, calls)
	r.flush()
	set := (*got)[0].set

	h := set.Get("http.server.request.duration")
	if h == nil || h.Points[0].Attr("http.response.status_code") != "500" || h.Points[0].Attr("http.route") != "GET /cart" {
		t.Fatalf("http server = %+v", h)
	}
	if b := h.Points[0].Buckets[0].UpperBound; math.Abs(b-0.1) > 1e-9 {
		t.Fatalf("bound = %v, want 0.1 s", b)
	}
	g := set.Get("rpc.server.call.duration")
	if g == nil || g.Points[0].Attr("rpc.response.status_code") != "UNAVAILABLE" {
		t.Fatalf("grpc server = %+v", g)
	}
	c := set.Get("http.client.request.duration")
	if c == nil || c.Points[0].Attr("server.address") != "redis" || c.Points[0].Value != 3 {
		t.Fatalf("client = %+v", c)
	}
	if set.Get(spanMetricsDuration) != nil || set.Get(spanMetricsCalls) != nil {
		t.Fatal("raw span metrics were kept")
	}
	if total := len(h.Points) + len(g.Points) + len(c.Points); total != 3 {
		t.Fatalf("points = %d, want 3 (internal span dropped)", total)
	}
}

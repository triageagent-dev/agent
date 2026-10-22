// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package metricset

import (
	"math"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func rm(metrics ...*metricspb.Metric) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{{
		Resource:     &resourcepb.Resource{Attributes: KV("service.name", "api", "k8s.namespace.name", "ns-a")},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: metrics}},
	}}
}

func TestFromOTLP_HistogramBecomesCumulativeBuckets(t *testing.T) {
	set := FromOTLP(rm(&metricspb.Metric{
		Name: "http.server.request.duration",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricspb.HistogramDataPoint{{
				Attributes:     KV("http.response.status_code", "500", "service.name", "api-override"),
				Count:          10,
				ExplicitBounds: []float64{0.1, 0.5},
				BucketCounts:   []uint64{3, 5, 2},
			}},
		}},
	}))
	m := set.Get("http.server.request.duration")
	if m == nil || m.Kind != Histogram || len(m.Points) != 1 {
		t.Fatalf("metric = %+v", m)
	}
	p := m.Points[0]
	if p.Value != 10 {
		t.Errorf("count = %v", p.Value)
	}
	want := []Bucket{{0.1, 3}, {0.5, 8}, {math.Inf(1), 10}}
	if len(p.Buckets) != len(want) {
		t.Fatalf("buckets = %v", p.Buckets)
	}
	for i := range want {
		if p.Buckets[i] != want[i] {
			t.Errorf("bucket %d = %v, want %v", i, p.Buckets[i], want[i])
		}
	}
	if p.Attr("k8s.namespace.name") != "ns-a" {
		t.Error("resource attribute not merged")
	}
	if p.Attr("service.name") != "api-override" {
		t.Error("a point attribute must win over the resource attribute")
	}
	if p.Key() == "" {
		t.Error("key is empty")
	}
}

func TestFromOTLP_SkipsDeltaTemporality(t *testing.T) {
	set := FromOTLP(rm(&metricspb.Metric{
		Name: "requests",
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			IsMonotonic:            true,
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			DataPoints:             []*metricspb.NumberDataPoint{{Value: &metricspb.NumberDataPoint_AsInt{AsInt: 4}}},
		}},
	}))
	if set.Get("requests") != nil {
		t.Error("delta points must not be read as cumulative values")
	}
}

func TestFromOTLP_NonStringAttributes(t *testing.T) {
	set := FromOTLP(rm(&metricspb.Metric{
		Name: "g",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
			Attributes: []*commonpb.KeyValue{
				{Key: "code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 503}}},
				{Key: "ok", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}},
			},
			Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.5},
		}}}},
	}))
	p := set.Get("g").Points[0]
	if p.Attr("code") != "503" || p.Attr("ok") != "false" || p.Value != 1.5 {
		t.Errorf("point = %+v", p)
	}
}

func TestBuilder_RoundTrip(t *testing.T) {
	b := NewBuilder("k8s.node.name", "node-a")
	b.Counter("c", KV("k", "v"), 3)
	b.Gauge("g", nil, 7)
	set := FromOTLP(b.ResourceMetrics())
	if c := set.Get("c"); c.Kind != Counter || c.Points[0].Value != 3 || c.Points[0].Attr("k8s.node.name") != "node-a" {
		t.Errorf("counter = %+v", c)
	}
	if g := set.Get("g"); g.Kind != Gauge || g.Points[0].Value != 7 {
		t.Errorf("gauge = %+v", g)
	}
	if NewBuilder().ResourceMetrics() != nil {
		t.Error("an empty builder must return nothing")
	}
}

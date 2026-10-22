// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package metricset

import (
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Builder assembles one resource's metrics as cumulative OTLP, for the
// sensors' own exporters.
type Builder struct {
	resource []*commonpb.KeyValue
	metrics  map[string]*metricspb.Metric
	order    []string
	now      uint64
}

// NewBuilder starts a snapshot for a resource with the given attributes
// (key, value pairs).
func NewBuilder(resourceKV ...string) *Builder {
	return &Builder{resource: KV(resourceKV...), metrics: map[string]*metricspb.Metric{}, now: uint64(time.Now().UnixNano())}
}

// KV turns key, value pairs into OTLP string attributes.
func KV(kv ...string) []*commonpb.KeyValue {
	out := make([]*commonpb.KeyValue, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, &commonpb.KeyValue{Key: kv[i], Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: kv[i+1]}}})
	}
	return out
}

func (b *Builder) metric(name string, mk func() *metricspb.Metric) *metricspb.Metric {
	m := b.metrics[name]
	if m == nil {
		m = mk()
		b.metrics[name] = m
		b.order = append(b.order, name)
	}
	return m
}

// Gauge records a gauge point.
func (b *Builder) Gauge(name string, attrs []*commonpb.KeyValue, v float64) {
	m := b.metric(name, func() *metricspb.Metric {
		return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}}
	})
	g := m.GetGauge()
	g.DataPoints = append(g.DataPoints, &metricspb.NumberDataPoint{Attributes: attrs, TimeUnixNano: b.now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}})
}

// Counter records a cumulative monotonic sum point.
func (b *Builder) Counter(name string, attrs []*commonpb.KeyValue, v float64) {
	m := b.metric(name, func() *metricspb.Metric {
		return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			IsMonotonic:            true,
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		}}}
	})
	s := m.GetSum()
	s.DataPoints = append(s.DataPoints, &metricspb.NumberDataPoint{Attributes: attrs, TimeUnixNano: b.now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}})
}

// ResourceMetrics returns the snapshot, metrics in the order first recorded.
// Empty when nothing was recorded.
func (b *Builder) ResourceMetrics() []*metricspb.ResourceMetrics {
	if len(b.order) == 0 {
		return nil
	}
	ms := make([]*metricspb.Metric, 0, len(b.order))
	for _, n := range b.order {
		ms = append(ms, b.metrics[n])
	}
	return []*metricspb.ResourceMetrics{{
		Resource:     &resourcepb.Resource{Attributes: b.resource},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "triage-agent"}, Metrics: ms}},
	}}
}

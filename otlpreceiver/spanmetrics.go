// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// A Collector's spanmetrics connector turns traces into one duration
// histogram per (service, span name, kind, status) plus the dimensions it is
// configured with. With http.response.status_code (or http.status_code),
// http.request.method, http.route, rpc.system, rpc.method,
// rpc.grpc.status_code and server.address among them, its server spans carry
// what the RED profile reads and its client spans what topology reads.
// translateSpanMetrics rewrites them to those histograms, so a cluster whose
// services are traced but export no metrics still reports RED, SIG and L7
// topology.

const (
	spanMetricsDuration = "traces.span.metrics.duration"
	spanMetricsCalls    = "traces.span.metrics.calls"
)

// translateSpanMetrics returns the RED and client histograms m's points
// stand for; nil for any other metric. The calls counter is dropped: the
// histogram's count carries the same number.
func translateSpanMetrics(m *metricspb.Metric) (out []*metricspb.Metric, handled bool) {
	switch m.GetName() {
	case spanMetricsCalls:
		return nil, true
	case spanMetricsDuration:
	default:
		return nil, false
	}
	h := m.GetHistogram()
	if h == nil {
		return nil, true
	}
	scale := 1.0
	if m.GetUnit() == "ms" || m.GetUnit() == "" {
		scale = 0.001 // spanmetrics' default unit
	}
	byName := map[string]*metricspb.Metric{}
	for _, dp := range h.GetDataPoints() {
		attrs := dp.GetAttributes()
		kind := attr(attrs, "span.kind")
		server := kind == "SPAN_KIND_SERVER" || kind == "SPAN_KIND_CONSUMER"
		client := kind == "SPAN_KIND_CLIENT" || kind == "SPAN_KIND_PRODUCER"
		if !server && !client {
			continue // internal spans are not requests
		}
		failed := attr(attrs, "status.code") == "STATUS_CODE_ERROR"
		// The current semantic conventions name the system rpc.system.name and
		// the status rpc.response.status_code; older SDKs use rpc.system and
		// rpc.grpc.status_code. A span of either is gRPC, not HTTP.
		grpc := attr(attrs, "rpc.system") == "grpc" || attr(attrs, "rpc.system.name") == "grpc" ||
			attr(attrs, "rpc.grpc.status_code") != ""

		p := proto.Clone(dp).(*metricspb.HistogramDataPoint)
		var name string
		switch {
		case grpc && server:
			name = "rpc.server.call.duration"
			if attr(attrs, "rpc.grpc.status_code") == "" && attr(attrs, "rpc.response.status_code") == "" {
				code := "OK"
				if failed {
					code = "UNKNOWN"
				}
				p.Attributes = append(p.Attributes, kv("rpc.response.status_code", code))
			}
		case grpc:
			name = "rpc.client.call.duration"
		case server:
			name = "http.server.request.duration"
			if attr(attrs, "http.response.status_code") == "" && attr(attrs, "http.status_code") == "" {
				code := "200"
				if failed {
					code = "500"
				}
				p.Attributes = append(p.Attributes, kv("http.response.status_code", code))
			}
		default:
			name = "http.client.request.duration"
		}
		if attr(attrs, "http.route") == "" && server && !grpc {
			p.Attributes = append(p.Attributes, kv("http.route", attr(attrs, "span.name")))
		}
		if grpc && attr(attrs, "rpc.method") == "" {
			p.Attributes = append(p.Attributes, kv("rpc.method", attr(attrs, "span.name")))
		}
		normalizePoint(p, scale)
		out := byName[name]
		if out == nil {
			out = &metricspb.Metric{Name: name, Unit: "s", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				AggregationTemporality: h.GetAggregationTemporality(),
			}}}
			byName[name] = out
		}
		out.GetHistogram().DataPoints = append(out.GetHistogram().DataPoints, p)
	}
	for _, n := range []string{"http.server.request.duration", "rpc.server.call.duration", "http.client.request.duration", "rpc.client.call.duration"} {
		if m := byName[n]; m != nil {
			out = append(out, m)
		}
	}
	return out, true
}

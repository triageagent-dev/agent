// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// ownerKeys are the resource attributes the Collector's k8sattributes
// processor sets for a pod's workload, in the order tried for
// k8s.owner.name, the attribute the RED collector resolves a caller's
// virtual app from (OBI sets it itself).
var ownerKeys = []string{
	"k8s.deployment.name",
	"k8s.statefulset.name",
	"k8s.daemonset.name",
	"k8s.cronjob.name",
	"k8s.job.name",
}

// normalizeResource returns r with k8s.owner.name set from the first
// workload attribute present, when it is not set already.
func normalizeResource(r *resourcepb.Resource) *resourcepb.Resource {
	out := proto.Clone(r).(*resourcepb.Resource)
	if out == nil {
		out = &resourcepb.Resource{}
	}
	if attr(out.GetAttributes(), "k8s.owner.name") != "" {
		return out
	}
	for _, k := range ownerKeys {
		if v := attr(out.GetAttributes(), k); v != "" {
			out.Attributes = append(out.Attributes, kv("k8s.owner.name", v))
			break
		}
	}
	return out
}

// renamed maps the older semantic-convention duration histograms, still
// emitted by many SDKs, to the current names the RED profile reads.
var renamed = map[string]string{
	"http.server.duration": "http.server.request.duration",
	"http.client.duration": "http.client.request.duration",
	"rpc.server.duration":  "rpc.server.call.duration",
	"rpc.client.duration":  "rpc.client.call.duration",
}

// durationMetrics are the histograms the RED profile reads, in seconds.
var durationMetrics = map[string]bool{
	"http.server.request.duration": true,
	"http.client.request.duration": true,
	"rpc.server.call.duration":     true,
	"rpc.client.call.duration":     true,
	"db.client.operation.duration": true,
}

// canonicalMetric returns the name m is stored under, its unit, and the
// factor its bucket bounds are scaled by: 0.001 for a duration histogram
// in milliseconds, 1 otherwise.
func canonicalMetric(m *metricspb.Metric) (name, unit string, scale float64) {
	name, unit, scale = m.GetName(), m.GetUnit(), 1
	if n, ok := renamed[name]; ok {
		name = n
	}
	if durationMetrics[name] && unit == "ms" {
		unit, scale = "s", 0.001
	}
	return name, unit, scale
}

// renamedAttrs maps older attribute names to the current ones; the current
// name wins when both are present.
var renamedAttrs = map[string]string{
	"http.status_code": "http.response.status_code",
	"http.method":      "http.request.method",
	"net.peer.name":    "server.address",
}

// grpcCodes are the gRPC status code names by number, as OBI reports them
// on rpc.response.status_code and redsource.IsGRPCServerError reads them.
var grpcCodes = []string{
	"OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED",
	"NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED",
	"FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED",
	"INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED",
}

// normalizePoint rewrites a point in place: current attribute names, the
// gRPC status as a code name, and bucket bounds scaled by scale.
func normalizePoint(p proto.Message, scale float64) {
	var attrs *[]*commonpb.KeyValue
	switch dp := p.(type) {
	case *metricspb.NumberDataPoint:
		attrs = &dp.Attributes
	case *metricspb.HistogramDataPoint:
		attrs = &dp.Attributes
		if scale != 1 {
			for i, b := range dp.ExplicitBounds {
				dp.ExplicitBounds[i] = b * scale
			}
			if dp.Sum != nil {
				s := *dp.Sum * scale
				dp.Sum = &s
			}
			if dp.Min != nil {
				v := *dp.Min * scale
				dp.Min = &v
			}
			if dp.Max != nil {
				v := *dp.Max * scale
				dp.Max = &v
			}
		}
	default:
		return
	}
	for old, cur := range renamedAttrs {
		if v := attr(*attrs, old); v != "" && attr(*attrs, cur) == "" {
			*attrs = append(*attrs, kv(cur, v))
		}
	}
	if attr(*attrs, "rpc.response.status_code") == "" {
		if v := attr(*attrs, "rpc.grpc.status_code"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 && n < len(grpcCodes) {
				*attrs = append(*attrs, kv("rpc.response.status_code", grpcCodes[n]))
			}
		}
	}
}

func attr(kvs []*commonpb.KeyValue, key string) string {
	for _, a := range kvs {
		if a.GetKey() == key {
			return anyString(a.GetValue())
		}
	}
	return ""
}

func kv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

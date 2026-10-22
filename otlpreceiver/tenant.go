// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/triageagent-dev/agent/internal/remotens"
)

// Namespace attributes of a point. The first is the OpenTelemetry one; the
// second is the Prometheus label of the kubelet's cAdvisor series, which
// translateCAdvisor reads.
const (
	nsAttr         = "k8s.namespace.name"
	cadvisorNsAttr = "namespace"
)

// qualifyResource places rm in tenant: every namespace it names, on the
// resource and on each point, becomes "<tenant>~<namespace>" (package
// remotens), and the tenant attribute is dropped. The result keeps data of
// different tenants apart in every later stage, and no point is left with an
// unqualified namespace, which would be routed as a local one.
//
// A resource with no namespace gets "<tenant>~", the tenant's namespace-less
// source. Points that name their own namespace keep it, qualified.
func qualifyResource(rm *metricspb.ResourceMetrics, tenant string) {
	res := rm.GetResource()
	if res == nil {
		res = &resourcepb.Resource{}
		rm.Resource = res
	}
	kept := res.Attributes[:0]
	have := false
	for _, a := range res.Attributes {
		switch a.GetKey() {
		case remotens.TenantAttr:
			continue
		case nsAttr:
			have = true
			setString(a, remotens.Qualify(tenant, anyString(a.GetValue())))
		}
		kept = append(kept, a)
	}
	res.Attributes = kept
	if !have {
		res.Attributes = append(res.Attributes, kv(nsAttr, remotens.Qualify(tenant, "")))
	}
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			forEachPointAttrs(m, func(attrs []*commonpb.KeyValue) {
				for _, a := range attrs {
					switch a.GetKey() {
					case nsAttr:
						setString(a, remotens.Qualify(tenant, anyString(a.GetValue())))
					case cadvisorNsAttr:
						// Only cAdvisor series use this label as a namespace;
						// on any other metric it is an application dimension.
						if isCAdvisor(m.GetName()) {
							setString(a, remotens.Qualify(tenant, anyString(a.GetValue())))
						}
					}
				}
			})
		}
	}
}

func setString(a *commonpb.KeyValue, v string) {
	a.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

// forEachPointAttrs calls fn with the attributes of every data point of m.
func forEachPointAttrs(m *metricspb.Metric, fn func([]*commonpb.KeyValue)) {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			fn(dp.GetAttributes())
		}
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			fn(dp.GetAttributes())
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			fn(dp.GetAttributes())
		}
	case *metricspb.Metric_ExponentialHistogram:
		for _, dp := range d.ExponentialHistogram.GetDataPoints() {
			fn(dp.GetAttributes())
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			fn(dp.GetAttributes())
		}
	}
}

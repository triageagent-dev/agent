// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/internal/remotens"
)

// ScopeFilter returns the controller's own check of what an otlp-role sensor
// streams (docs/adr/0022-otlp-sensor-role.md). The sensor pod's receiver has
// already qualified a tenant sender's namespaces as "<tenant>~<namespace>",
// but it holds no copy of the tenants the agent joined or of the workspace
// scope the core sets, so the controller applies both again to what arrives:
// a resource or point in a qualified namespace of a tenant that is not
// allowed, or outside inScope, is dropped. Plain namespaces pass. A nil
// inScope allows every namespace of an allowed tenant.
func ScopeFilter(tenantAllowed func(tenant string) bool, inScope func(tenant, ns string) bool) func([]*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics {
	outside := func(qualified string) bool {
		tenant, ns, ok := remotens.Split(qualified)
		if !ok {
			return false
		}
		if tenantAllowed != nil && !tenantAllowed(tenant) {
			return true
		}
		return inScope != nil && !inScope(tenant, ns)
	}
	return func(rms []*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics {
		out := make([]*metricspb.ResourceMetrics, 0, len(rms))
		for _, rm := range rms {
			ns, multiple := resourceNamespace(rm.GetResource().GetAttributes())
			if multiple || outside(ns) {
				continue
			}
			dropPointsWhere(rm, func(attrs []*commonpb.KeyValue, cadvisor bool) bool {
				return pointOutOfScope(attrs, cadvisor, outside)
			}, func(int) {})
			out = append(out, rm)
		}
		return out
	}
}

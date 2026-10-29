// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"strings"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// A Collector that sends everything it has (docs/adr/0022-otlp-sensor-role.md)
// sends hundreds of metric names and a resource for every pod of the cluster,
// and the collectors read a few dozen names. Without a cut at intake those
// resources fill the per-sender and total quotas and the ones that matter are
// refused. Options.Wanted drops a metric nobody reads before its resource
// takes a slot.

// readByCollectors are the metric names the collectors read, as the sender
// names them: the RED profile's histograms, the spanmetrics connector's
// output, the log counters the count connector produces, and the workload
// counts the apps source reads from the k8s_cluster receiver. The cAdvisor
// series and the use.* points are matched by prefix below.
var readByCollectors = map[string]bool{
	spanMetricsDuration: true,
	spanMetricsCalls:    true,
	"log.errors":        true,
	"log.warns":         true,
	"log.lines":         true,

	"k8s.deployment.desired":                true,
	"k8s.deployment.available":              true,
	"k8s.statefulset.desired_pods":          true,
	"k8s.statefulset.ready_pods":            true,
	"k8s.daemonset.desired_scheduled_nodes": true,
	"k8s.daemonset.ready_nodes":             true,
}

// ReadByCollectors reports whether any collector reads a metric of this name.
func ReadByCollectors(name string) bool {
	if readByCollectors[name] || durationMetrics[name] {
		return true
	}
	if _, ok := renamed[name]; ok {
		return true
	}
	return readsCAdvisor(name) || strings.HasPrefix(name, "use.")
}

// dropUnwanted removes from rm every metric wanted rejects and returns how
// many points that was, telling count the points of each metric it dropped.
// A scope left with no metrics is removed too, so a resource whose metrics are
// all unread has nothing left to store.
func dropUnwanted(rm *metricspb.ResourceMetrics, wanted func(name string) bool, count func(name string, n int)) int {
	dropped := 0
	scopes := rm.ScopeMetrics[:0]
	for _, sm := range rm.GetScopeMetrics() {
		kept := sm.Metrics[:0]
		for _, m := range sm.GetMetrics() {
			if wanted(m.GetName()) {
				kept = append(kept, m)
				continue
			}
			n := countMetricPoints(m)
			dropped += n
			count(m.GetName(), n)
		}
		sm.Metrics = kept
		if len(kept) > 0 {
			scopes = append(scopes, sm)
		}
	}
	rm.ScopeMetrics = scopes
	return dropped
}

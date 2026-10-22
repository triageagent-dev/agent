// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"sort"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/triageagent-dev/agent/usemetrics"
)

// The kubelet's cAdvisor endpoint (/metrics/cadvisor), scraped by a
// Collector's prometheus receiver, carries every USE input the node-exporter
// role reads from cgroup files: usage, throttling, working set, limits, OOM
// events and, on Kubernetes 1.34 and later, PSI. A pod's own cgroup is the
// series with a pod label and empty container and image labels.
// translateCAdvisor turns those series into the use.* points the USE
// collector reads, so a cluster without the DaemonSet still reports USE.

// cadvisorPrefixes mark the raw series, which no collector reads directly.
var cadvisorPrefixes = []string{"container_", "machine_"}

func isCAdvisor(name string) bool {
	for _, p := range cadvisorPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// cadvisorName strips the "_total" a counter may or may not keep, depending
// on the prometheus receiver's normalization.
func cadvisorName(name string) string { return strings.TrimSuffix(name, "_total") }

type podRef struct{ namespace, pod string }

type cadvisorPod struct {
	values map[string]float64
	ts     uint64
}

// psiPrev is one PSI counter's previous reading, for the rate.
type psiPrev struct {
	v  float64
	ts uint64
}

// translateCAdvisor builds the use.* metrics for every pod in st's cAdvisor
// series. PSI is the rate of the "waiting" (some) counter between this and
// the previous reading, as a percentage, the same meaning as cgroup
// pressure's "some avg" the DaemonSet reads; the first reading of a pod
// yields none.
func translateCAdvisor(st *resourceState) []*metricspb.Metric {
	pods := map[podRef]*cadvisorPod{}
	var nodeCores, nodeMem float64
	for name, s := range st.metrics {
		if !isCAdvisor(name) {
			continue
		}
		base := cadvisorName(name)
		for _, p := range s.points {
			dp, ok := p.(*metricspb.NumberDataPoint)
			if !ok {
				continue
			}
			v := dp.GetAsDouble()
			if iv, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt); ok {
				v = float64(iv.AsInt)
			}
			attrs := dp.GetAttributes()
			switch base {
			case "machine_cpu_cores":
				nodeCores = v
				continue
			case "machine_memory_bytes":
				nodeMem = v
				continue
			}
			pod, ns := attr(attrs, "pod"), attr(attrs, "namespace")
			if pod == "" || ns == "" || attr(attrs, "container") != "" || attr(attrs, "image") != "" {
				continue // not a pod's own cgroup
			}
			ref := podRef{namespace: ns, pod: pod}
			cp := pods[ref]
			if cp == nil {
				cp = &cadvisorPod{values: map[string]float64{}}
				pods[ref] = cp
			}
			cp.values[base] = v
			if dp.GetTimeUnixNano() > cp.ts {
				cp.ts = dp.GetTimeUnixNano()
			}
		}
	}
	if len(pods) == 0 {
		return nil
	}
	if st.psi == nil {
		st.psi = map[string]psiPrev{}
	}

	out := map[string]*metricspb.Metric{}
	gauge := func(name string, attrs []*commonpb.KeyValue, ts uint64, v float64) {
		m := out[name]
		if m == nil {
			m = &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}}
			out[name] = m
		}
		g := m.GetGauge()
		g.DataPoints = append(g.DataPoints, &metricspb.NumberDataPoint{Attributes: attrs, TimeUnixNano: ts, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}})
	}
	counter := func(name string, attrs []*commonpb.KeyValue, ts uint64, v float64) {
		m := out[name]
		if m == nil {
			m = &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				IsMonotonic: true, AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			}}}
			out[name] = m
		}
		s := m.GetSum()
		s.DataPoints = append(s.DataPoints, &metricspb.NumberDataPoint{Attributes: attrs, TimeUnixNano: ts, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: v}})
	}

	seen := map[string]bool{}
	for ref, cp := range pods {
		attrs := []*commonpb.KeyValue{kv(usemetrics.LabelNamespace, ref.namespace), kv(usemetrics.LabelPod, ref.pod)}
		val := func(k string) (float64, bool) { v, ok := cp.values[k]; return v, ok }
		ts := cp.ts

		if v, ok := val("container_cpu_usage_seconds"); ok {
			counter(usemetrics.MetricCPUUsageUsecTotal, attrs, ts, v*1e6)
		}
		if v, ok := val("container_cpu_cfs_periods"); ok {
			counter(usemetrics.MetricCPUNrPeriodsTotal, attrs, ts, v)
		}
		if v, ok := val("container_cpu_cfs_throttled_periods"); ok {
			counter(usemetrics.MetricCPUNrThrottledTotal, attrs, ts, v)
		}
		if v, ok := val("container_oom_events"); ok {
			counter(usemetrics.MetricOOMKillTotal, attrs, ts, v)
		}
		quota, okQ := val("container_spec_cpu_quota")
		period, okP := val("container_spec_cpu_period")
		if okQ && okP && quota > 0 && period > 0 {
			gauge(usemetrics.MetricCPULimitCores, attrs, ts, quota/period)
		}
		ws, okWS := val("container_memory_working_set_bytes")
		usage, okU := val("container_memory_usage_bytes")
		limit, okL := val("container_spec_memory_limit_bytes")
		if okWS {
			gauge(usemetrics.MetricMemBytes, attrs, ts, ws)
		}
		if okL && limit > 0 {
			gauge(usemetrics.MetricMemLimitBytes, attrs, ts, limit)
			if okWS {
				gauge(usemetrics.MetricMemWorkingSetPct, attrs, ts, ws/limit*100)
			}
			if okU {
				gauge(usemetrics.MetricMemPct, attrs, ts, usage/limit*100)
			}
		}
		if nodeCores > 0 {
			gauge(usemetrics.MetricCPUNodeCores, attrs, ts, nodeCores)
		}
		if nodeMem > 0 {
			gauge(usemetrics.MetricMemNodeTotalBytes, attrs, ts, nodeMem)
		}
		for res, metric := range map[string]string{
			"cpu":    usemetrics.MetricCPUPsiAvg60,
			"memory": usemetrics.MetricMemPsiAvg60,
			"io":     usemetrics.MetricIOPsiAvg60,
		} {
			v, ok := val("container_pressure_" + res + "_waiting_seconds")
			if !ok {
				continue
			}
			key := ref.namespace + "/" + ref.pod + "/" + res
			seen[key] = true
			prev, had := st.psi[key]
			st.psi[key] = psiPrev{v: v, ts: ts}
			if !had || ts <= prev.ts || v < prev.v {
				continue
			}
			pct := (v - prev.v) / (float64(ts-prev.ts) / 1e9) * 100
			if pct > 100 {
				pct = 100
			}
			gauge(metric, attrs, ts, pct)
		}
	}
	for key := range st.psi {
		if !seen[key] {
			delete(st.psi, key)
		}
	}

	names := make([]string, 0, len(out))
	for n := range out {
		names = append(names, n)
	}
	sort.Strings(names)
	ms := make([]*metricspb.Metric, 0, len(names))
	for _, n := range names {
		ms = append(ms, proto.Clone(out[n]).(*metricspb.Metric))
	}
	return ms
}

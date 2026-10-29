// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"sort"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

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

// cadvisorRead are the cAdvisor series translateCAdvisor reads, by base
// name: the only ones admitted. A scrape carries fifty more per cgroup.
var cadvisorRead = map[string]bool{
	"machine_cpu_cores":                         true,
	"machine_memory_bytes":                      true,
	"container_cpu_usage_seconds":               true,
	"container_cpu_cfs_periods":                 true,
	"container_cpu_cfs_throttled_periods":       true,
	"container_oom_events":                      true,
	"container_spec_cpu_quota":                  true,
	"container_spec_cpu_period":                 true,
	"container_memory_working_set_bytes":        true,
	"container_memory_usage_bytes":              true,
	"container_spec_memory_limit_bytes":         true,
	"container_pressure_cpu_waiting_seconds":    true,
	"container_pressure_memory_waiting_seconds": true,
	"container_pressure_io_waiting_seconds":     true,
}

// readsCAdvisor reports whether name is one of the cAdvisor series read.
func readsCAdvisor(name string) bool { return cadvisorRead[cadvisorName(name)] }

// cadvisorName strips the "_total" a counter may or may not keep, depending
// on the prometheus receiver's normalization.
func cadvisorName(name string) string { return strings.TrimSuffix(name, "_total") }

// hasCAdvisor reports whether rm carries a cAdvisor series.
func hasCAdvisor(rm *metricspb.ResourceMetrics) bool {
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			if isCAdvisor(m.GetName()) {
				return true
			}
		}
	}
	return false
}

// podCgroup reports whether a container_* point is a pod's own cgroup: the
// only kind translateCAdvisor reads.
func podCgroup(attrs []*commonpb.KeyValue) bool {
	return attr(attrs, "pod") != "" && attr(attrs, cadvisorNsAttr) != "" && attr(attrs, "container") == "" && attr(attrs, "image") == ""
}

// dropCAdvisorNoise removes from rm every container_* point that is not a
// pod's own cgroup (a container's, the node's, a system slice's) and
// returns how many that was: nothing reads them, and a node has several
// such points for every pod one. count is told, per metric, how many of its
// points were dropped.
func dropCAdvisorNoise(rm *metricspb.ResourceMetrics, count func(name string, n int)) int {
	dropped := 0
	for _, sm := range rm.GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			if !strings.HasPrefix(m.GetName(), "container_") {
				continue
			}
			bad := func(attrs []*commonpb.KeyValue, _ bool) bool { return !podCgroup(attrs) }
			before := dropped
			report := func(n int) { dropped += n }
			switch d := m.GetData().(type) {
			case *metricspb.Metric_Sum:
				d.Sum.DataPoints = filterPoints(d.Sum.GetDataPoints(), true, bad, report,
					func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			case *metricspb.Metric_Gauge:
				d.Gauge.DataPoints = filterPoints(d.Gauge.GetDataPoints(), true, bad, report,
					func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue { return dp.GetAttributes() })
			}
			if dropped > before {
				count(m.GetName(), dropped-before)
			}
		}
	}
	return dropped
}

type podRef struct{ namespace, pod string }

type cadvisorPod struct {
	values map[string]float64
	ts     uint64
}

// psiPrev is one PSI counter's previous reading, for the rate.
type psiPrev struct {
	v    float64
	ts   uint64
	seen time.Time
	src  *source
}

// translateCAdvisor builds the use.* metrics for every pod in the cAdvisor
// series of one export. PSI is the rate of the "waiting" (some) counter
// between this and the previous reading, as a percentage, the same meaning
// as cgroup pressure's "some avg" the DaemonSet reads; the first reading of
// a pod yields none. The previous readings are the only state kept. The
// caller holds r.mu.
func (r *Receiver) translateCAdvisor(src *source, ms []*metricspb.Metric, now time.Time) []*metricspb.Metric {
	pods := map[podRef]*cadvisorPod{}
	var nodeCores, nodeMem float64
	for _, m := range ms {
		base := cadvisorName(m.GetName())
		var pts []*metricspb.NumberDataPoint
		switch d := m.GetData().(type) {
		case *metricspb.Metric_Sum:
			pts = d.Sum.GetDataPoints()
		case *metricspb.Metric_Gauge:
			pts = d.Gauge.GetDataPoints()
		}
		for _, dp := range pts {
			v := numberValue(dp)
			attrs := dp.GetAttributes()
			switch base {
			case "machine_cpu_cores":
				nodeCores = v
				continue
			case "machine_memory_bytes":
				nodeMem = v
				continue
			}
			if !podCgroup(attrs) {
				continue
			}
			ref := podRef{namespace: attr(attrs, cadvisorNsAttr), pod: attr(attrs, "pod")}
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
			prev, had := r.psi[key]
			owner := prev.src
			if !had {
				if r.nheld >= r.opts.MaxSeries {
					continue
				}
				r.nheld++
				src.held++
				owner = src
			}
			r.psi[key] = psiPrev{v: v, ts: ts, seen: now, src: owner}
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

	names := make([]string, 0, len(out))
	for n := range out {
		names = append(names, n)
	}
	sort.Strings(names)
	ms = make([]*metricspb.Metric, 0, len(names))
	for _, n := range names {
		ms = append(ms, out[n])
	}
	return ms
}

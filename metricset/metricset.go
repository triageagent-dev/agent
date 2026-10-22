// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package metricset is the metric model the controller's collectors read
// (docs/adr/0015-sensor-stream-grpc.md). The sensors deliver OTLP, read
// with FromOTLP. Values are cumulative: a collector diffs a point against
// the same point in the previous set.
package metricset

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// Sensor roles, each one source of metrics on a node.
const (
	RoleEBPF         = "ebpf"
	RoleNodeExporter = "node-exporter"
	RoleLogExporter  = "log-exporter"
)

// OTLPSourcePrefix starts the name of a source that is one OTLP resource
// pushed to the controller's OTLP receiver, after the target kind prefix
// ("sensor/otlp/<key>"), rather than a DaemonSet sensor.
const OTLPSourcePrefix = "otlp/"

// IsOTLPTarget reports whether target is an OTLP source.
func IsOTLPTarget(target string) bool {
	_, rest, ok := strings.Cut(target, "/")
	return ok && strings.HasPrefix(rest, OTLPSourcePrefix)
}

// SensorsFirst returns targets with the DaemonSet sensors before the OTLP
// sources, each group in its original order. A collector that prefers the
// sensors' reading of a service reads them first.
func SensorsFirst(targets []string) []string {
	out := make([]string, 0, len(targets))
	var otlp []string
	for _, t := range targets {
		if IsOTLPTarget(t) {
			otlp = append(otlp, t)
		} else {
			out = append(out, t)
		}
	}
	return append(out, otlp...)
}

// Source is where a collector reads each node's latest metrics.
type Source interface {
	// Targets lists the sources that currently have role's metrics, sorted.
	// A target's name is stable while its sensor keeps running.
	Targets(role string) []string
	// Set returns target's latest metrics for role. The set is shared:
	// callers must not modify it.
	Set(role, target string) (Set, error)
}

// TimedSource is a Source that also knows when it received each set. A
// collector that turns a counter delta into a rate needs the real time between
// the two sets it diffs: a collector tick is not a sensor tick, and a sensor
// can go without delivering for several ticks.
type TimedSource interface {
	Source
	// SetAt is Set plus the time target's latest set for role was received.
	// Two calls returning the same time returned the same set.
	SetAt(role, target string) (Set, time.Time, error)
}

// Kind is a metric's instrument kind.
type Kind uint8

const (
	Counter Kind = iota + 1
	Gauge
	Histogram
)

// Bucket is one histogram bucket with its cumulative count.
type Bucket struct {
	UpperBound      float64
	CumulativeCount uint64
}

// Point is one series of a metric.
type Point struct {
	Attrs map[string]string
	// Value is the counter or gauge value, or a histogram's total count.
	Value float64
	// Buckets is set for a histogram: ascending, cumulative, the last one
	// +Inf.
	Buckets []Bucket
}

// Attr returns the attribute value, or "" when absent or name is "".
func (p Point) Attr(name string) string {
	if name == "" {
		return ""
	}
	return p.Attrs[name]
}

// Key is the point's full attribute set as a sorted "k=v,k=v" string: its
// identity within the metric.
func (p Point) Key() string {
	if len(p.Attrs) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(p.Attrs))
	for k, v := range p.Attrs {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Metric is a named set of points of one kind.
type Metric struct {
	Name   string
	Kind   Kind
	Points []Point
}

// Set is every metric of one source at one moment, by name.
type Set map[string]*Metric

// Get returns the named metric, or nil.
func (s Set) Get(name string) *Metric {
	if s == nil {
		return nil
	}
	return s[name]
}

func (s Set) add(name string, kind Kind, p Point) {
	m := s[name]
	if m == nil {
		m = &Metric{Name: name, Kind: kind}
		s[name] = m
	}
	m.Points = append(m.Points, p)
}

// DerivedAttr is the resource attribute of a source whose points the OTLP
// receiver derived from spans or log records, rather than the sender reporting
// them as metrics. Its value names the receiver process.
const DerivedAttr = "triage.otlp.replica"

// Derived reports whether the set came from a source the OTLP receiver derived
// from spans or log records: a resource is one or the other, so the first point
// of any metric says so.
func (s Set) Derived() bool {
	for _, m := range s {
		if len(m.Points) > 0 {
			_, ok := m.Points[0].Attrs[DerivedAttr]
			return ok
		}
	}
	return false
}

// FromOTLP reads cumulative OTLP metrics. Resource attributes are merged
// into every point's attributes; a point's own attribute wins. Delta
// temporality points and exponential histograms are skipped.
func FromOTLP(rms []*metricspb.ResourceMetrics) Set {
	out := Set{}
	for _, rm := range rms {
		res := attrs(rm.GetResource().GetAttributes(), nil)
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Sum:
					if d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						continue
					}
					kind := Gauge
					if d.Sum.GetIsMonotonic() {
						kind = Counter
					}
					for _, dp := range d.Sum.GetDataPoints() {
						out.add(m.GetName(), kind, Point{Attrs: attrs(dp.GetAttributes(), res), Value: numberValue(dp)})
					}
				case *metricspb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						out.add(m.GetName(), Gauge, Point{Attrs: attrs(dp.GetAttributes(), res), Value: numberValue(dp)})
					}
				case *metricspb.Metric_Histogram:
					if d.Histogram.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						continue
					}
					for _, dp := range d.Histogram.GetDataPoints() {
						out.add(m.GetName(), Histogram, Point{
							Attrs:   attrs(dp.GetAttributes(), res),
							Value:   float64(dp.GetCount()),
							Buckets: otlpBuckets(dp.GetExplicitBounds(), dp.GetBucketCounts(), dp.GetCount()),
						})
					}
				}
			}
		}
	}
	return out
}

func numberValue(dp *metricspb.NumberDataPoint) float64 {
	if v, ok := dp.GetValue().(*metricspb.NumberDataPoint_AsInt); ok {
		return float64(v.AsInt)
	}
	return dp.GetAsDouble()
}

// otlpBuckets turns OTLP per-bucket counts into cumulative buckets ending in
// +Inf.
func otlpBuckets(bounds []float64, counts []uint64, total uint64) []Bucket {
	out := make([]Bucket, 0, len(bounds)+1)
	var cum uint64
	for i, b := range bounds {
		if i < len(counts) {
			cum += counts[i]
		}
		out = append(out, Bucket{UpperBound: b, CumulativeCount: cum})
	}
	return append(out, Bucket{UpperBound: math.Inf(1), CumulativeCount: total})
}

func attrs(kvs []*commonpb.KeyValue, base map[string]string) map[string]string {
	out := make(map[string]string, len(kvs)+len(base))
	for k, v := range base {
		out[k] = v
	}
	for _, kv := range kvs {
		if s, ok := anyString(kv.GetValue()); ok {
			out[kv.GetKey()] = s
		}
	}
	return out
}

func anyString(v *commonpb.AnyValue) (string, bool) {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue, true
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10), true
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64), true
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue), true
	}
	return "", false
}

// PointCount is the number of data points in the set.
func (s Set) PointCount() int {
	n := 0
	for _, m := range s {
		if m != nil {
			n += len(m.Points)
		}
	}
	return n
}

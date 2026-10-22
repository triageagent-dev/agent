// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
)

const dlt = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA

func deltaCounter(name string, ts uint64, v int64, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		IsMonotonic: true, AggregationTemporality: dlt,
		DataPoints: []*metricspb.NumberDataPoint{{Attributes: attrs, StartTimeUnixNano: ts - 10, TimeUnixNano: ts, Value: &metricspb.NumberDataPoint_AsInt{AsInt: v}}},
	}}}
}

func redPoint(t *testing.T, r *Receiver, got *[]sunk, name string) metricset.Point {
	t.Helper()
	r.flush()
	for i := len(*got) - 1; i >= 0; i-- {
		if m := (*got)[i].set.Get(name); m != nil && len(m.Points) > 0 {
			return m.Points[0]
		}
	}
	t.Fatalf("%s was not flushed: %+v", name, *got)
	return metricset.Point{}
}

func TestDelta_SumsAddUpToACumulativeTotal(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	export(t, r, authed(), res, deltaCounter("log.errors", 100, 3))
	export(t, r, authed(), res, deltaCounter("log.errors", 200, 4))
	if v := redPoint(t, r, got, "log.errors").Value; v != 7 {
		t.Fatalf("total = %v, want 7", v)
	}
}

func TestDelta_AnIntervalAlreadyAddedIsNotAddedTwice(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	export(t, r, authed(), res, deltaCounter("log.errors", 100, 3))
	export(t, r, authed(), res, deltaCounter("log.errors", 100, 3)) // the sender retried
	export(t, r, authed(), res, deltaCounter("log.errors", 50, 9))  // older than the last
	if v := redPoint(t, r, got, "log.errors").Value; v != 3 {
		t.Fatalf("total = %v, want 3: a retry or an older interval must not count again", v)
	}
}

func TestDelta_HistogramsAddBucketByBucket(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	h := func(ts, count uint64) *metricspb.Metric {
		return histogram("http.server.request.duration", "s", dlt, ts, count, []float64{0.1}, strAttr("http.response.status_code", "200"))
	}
	export(t, r, authed(), res, h(100, 2))
	export(t, r, authed(), res, h(200, 5))
	p := redPoint(t, r, got, "http.server.request.duration")
	if p.Value != 7 {
		t.Fatalf("count = %v, want 7", p.Value)
	}
	m := (*got)[len(*got)-1].set.Get("http.server.request.duration")
	if m.Kind != metricset.Histogram || m.Points[0].Buckets[0].CumulativeCount != 7 {
		t.Fatalf("buckets = %+v, want 7 in the first", m.Points[0].Buckets)
	}
}

func TestDelta_ChangedBucketBoundsAreRefused(t *testing.T) {
	r, _, _ := newTest(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	export(t, r, authed(), res, histogram("http.server.request.duration", "s", dlt, 100, 2, []float64{0.1}))
	resp := export(t, r, authed(), res, histogram("http.server.request.duration", "s", dlt, 200, 2, []float64{0.1, 0.5}))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %+v, want the changed-bounds point refused", resp.GetPartialSuccess())
	}
}

func TestDelta_SeriesLimitApplies(t *testing.T) {
	r, _, _ := newTest(t)
	r.opts.MaxPointsPerResource = 1
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	export(t, r, authed(), res, deltaCounter("log.errors", 100, 1, strAttr("k", "a")))
	resp := export(t, r, authed(), res, deltaCounter("log.errors", 100, 1, strAttr("k", "b")))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %+v, want the second series refused", resp.GetPartialSuccess())
	}
}

func TestDelta_SeriesAreKeptApart(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	export(t, r, authed(), res, deltaCounter("log.errors", 100, 1, strAttr("k", "a")), deltaCounter("log.errors", 100, 5, strAttr("k", "b")))
	export(t, r, authed(), res, deltaCounter("log.errors", 200, 2, strAttr("k", "a")))
	r.flush()
	m := (*got)[len(*got)-1].set.Get("log.errors")
	totals := map[string]float64{}
	for _, p := range m.Points {
		totals[p.Attr("k")] = p.Value
	}
	if totals["a"] != 3 || totals["b"] != 5 {
		t.Fatalf("totals = %v, want a:3 b:5", totals)
	}
}

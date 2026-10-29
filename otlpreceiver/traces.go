// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"os"
	"sort"
	"strings"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/triageagent-dev/agent/internal/ledger"
	"github.com/triageagent-dev/agent/metricset"
)

// A Collector needs the spanmetrics connector to turn traces into the
// duration histograms the RED profile reads. This receiver also serves the
// OTLP traces service and builds those histograms itself
// (docs/adr/0022-otlp-sensor-role.md): a sender exports its spans as they
// are, and what comes out is what translateSpanMetrics makes of the
// connector's output, so the two paths agree on every series.

// replicaAttr marks a resource whose points this process derived from what it
// received. Several replicas (or otlp-role pods) can receive spans of one
// service; each derives only the share it saw, and a replica's share is not
// the whole. The attribute makes the derived resource's key differ per
// process, so the shares are separate sources that the collectors sum and a
// newest-wins merge never overwrites one with another.
const replicaAttr = metricset.DerivedAttr

// spanDurationBounds are the explicit bucket bounds, in seconds, of the
// derived duration histograms: the spanmetrics connector's defaults.
var spanDurationBounds = []float64{0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1, 1.4, 2, 5, 10, 15}

// spanDimensions are the span attributes the derived series are split by:
// what the RED profile and topology read. They are the dimensions the
// documented spanmetrics configuration lists.
var spanDimensions = []string{
	"http.response.status_code", "http.status_code", "http.request.method", "http.method", "http.route",
	"rpc.system", "rpc.system.name", "rpc.method", "rpc.service", "rpc.grpc.status_code", "rpc.response.status_code",
	"server.address", "net.peer.name",
}

// spanSeries is one derived series: the cumulative duration histogram of the
// spans that share its attributes. The attributes are the one thing kept
// beyond numbers, since the histogram is emitted under them; the routes of
// a resource are bounded by MaxPointsPerResource and all of them by
// MaxSeries.
type spanSeries struct {
	attrs  []*commonpb.KeyValue
	start  uint64
	counts []uint64
	count  uint64
	sum    float64
	seen   time.Time
}

// spanRoutes are the series derived from one resource's spans.
type spanRoutes struct {
	routes map[string]*spanSeries
}

// traceService serves the OTLP traces service on the receiver. It is a type
// of its own because the metrics, traces and logs services each have an
// Export method.
type traceService struct {
	collectortrace.UnimplementedTraceServiceServer
	r *Receiver
}

// Export implements collectortrace.TraceServiceServer.
func (s traceService) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	p, err := s.r.authenticate(ctx)
	if err != nil {
		s.r.log.Info("otlp export refused", "reason", err.Error())
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	rejected, reasons := s.r.acceptTraces(p, req.GetResourceSpans())
	resp := &collectortrace.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectortrace.ExportTracePartialSuccess{
			RejectedSpans: rejected,
			ErrorMessage:  strings.Join(reasons, "; "),
		}
	}
	return resp, nil
}

func countSpans(rs *tracepb.ResourceSpans) int {
	n := 0
	for _, ss := range rs.GetScopeSpans() {
		n += len(ss.GetSpans())
	}
	return n
}

// replicaName names this process in derived resources.
func (r *Receiver) replicaName() string {
	if r.opts.Replica != "" {
		return r.opts.Replica
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}

// acceptTraces folds the spans of rss, sent by p, into the duration
// histograms of the resources that sent them, forwards the histograms the
// spans touched and returns how many spans it refused and why.
func (r *Receiver) acceptTraces(p Principal, rss []*tracepb.ResourceSpans) (int64, []string) {
	sender := p.String()
	now := r.opts.now()
	replica := r.replicaName()
	f := r.newRefusal(sender, "spans")
	reject := f.reject
	var outs []forward

	r.mu.Lock()
	r.exports++
	var offered int64
	for _, rs := range rss {
		ns := int64(countSpans(rs))
		r.points += ns
		offered += ns
		n := func() int { return countSpans(rs) }
		f.at(rs.GetResource())
		resource, ok := r.admit(p, &metricspb.ResourceMetrics{Resource: rs.GetResource()}, n, reject)
		if !ok {
			continue
		}
		if attr(resource.GetAttributes(), nsAttr) == "" {
			reject(n(), noNamespaceReason)
			continue
		}
		resource.Attributes = append(resource.Attributes, kv(replicaAttr, replica))
		key := resourceKey(resource)
		if !r.touchSource(key, sender, now, n, reject) {
			continue
		}
		sr := r.spans[key]
		if sr == nil {
			sr = &spanRoutes{routes: map[string]*spanSeries{}}
			r.spans[key] = sr
		}
		touched := map[string]bool{}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				if k, refused := r.foldSpan(sr, sp, now); refused {
					reject(1, "series limit reached for a resource")
				} else if k != "" {
					touched[k] = true
				}
			}
		}
		if len(touched) == 0 {
			continue
		}
		// What the connector would have exported: every touched series, cumulative.
		out, _ := translateSpanMetrics(spanMetricsOf(sr, touched, now))
		var ms []*metricspb.Metric
		for _, mo := range out {
			if m, cnt, why := r.prepare(key, mo, now); why != "" {
				reject(cnt, why)
			} else if m != nil {
				ms = append(ms, m)
			}
		}
		if len(ms) > 0 {
			outs = append(outs, forward{key: key, role: metricset.RoleEBPF, rms: []*metricspb.ResourceMetrics{{
				Resource:     resource,
				ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: &commonpb.InstrumentationScope{Name: "otlp"}, Metrics: ms}},
			}}})
		}
	}
	r.rejected += f.rejected
	ledger.Default.Add("otlp", ledger.All, ledger.Ticks, 1)
	ledger.Default.Add("otlp", ledger.All, ledger.Points, offered)
	ledger.Default.Add("otlp", ledger.All, ledger.Refused, f.rejected)
	r.sweep(now)
	r.mu.Unlock()

	r.send(outs)
	return f.rejected, f.sortedReasons()
}

// foldSpan adds sp to its series in sr and returns the series' key. The key
// is empty for a span no histogram stands for (an internal span); refused is
// set when a new series would pass the resource's or the total limit. The
// caller holds r.mu.
func (r *Receiver) foldSpan(sr *spanRoutes, sp *tracepb.Span, now time.Time) (key string, refused bool) {
	switch sp.GetKind() {
	case tracepb.Span_SPAN_KIND_SERVER, tracepb.Span_SPAN_KIND_CONSUMER,
		tracepb.Span_SPAN_KIND_CLIENT, tracepb.Span_SPAN_KIND_PRODUCER:
	default:
		return "", false // internal spans are not requests
	}
	attrs := make([]*commonpb.KeyValue, 0, 3+len(spanDimensions))
	attrs = append(attrs,
		kv("span.name", sp.GetName()),
		kv("span.kind", sp.GetKind().String()),
		kv("status.code", sp.GetStatus().GetCode().String()),
	)
	for _, want := range spanDimensions {
		for _, a := range sp.GetAttributes() {
			if a.GetKey() == want {
				attrs = append(attrs, kv(a.GetKey(), anyString(a.GetValue())))
				break
			}
		}
	}
	k := pointKey(attrs)
	s := sr.routes[k]
	if s == nil {
		if len(sr.routes) >= r.opts.MaxPointsPerResource || r.nheld >= r.opts.MaxSeries {
			return "", true
		}
		s = &spanSeries{attrs: attrs, start: sp.GetStartTimeUnixNano(), counts: make([]uint64, len(spanDurationBounds)+1)}
		sr.routes[k] = s
		r.nheld++
	}
	s.seen = now
	d := float64(0)
	if end, start := sp.GetEndTimeUnixNano(), sp.GetStartTimeUnixNano(); end > start {
		d = float64(end-start) / float64(time.Second)
	}
	i := sort.SearchFloat64s(spanDurationBounds, d)
	s.counts[i]++
	s.count++
	s.sum += d
	return k, false
}

// spanMetricsOf is the spanmetrics connector's traces.span.metrics.duration
// for the touched series of sr: one cumulative histogram point per series, in
// seconds.
func spanMetricsOf(sr *spanRoutes, touched map[string]bool, now time.Time) *metricspb.Metric {
	keys := make([]string, 0, len(touched))
	for k := range touched {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := &metricspb.Histogram{AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE}
	for _, k := range keys {
		s := sr.routes[k]
		sum := s.sum
		h.DataPoints = append(h.DataPoints, &metricspb.HistogramDataPoint{
			Attributes:        s.attrs,
			StartTimeUnixNano: s.start,
			TimeUnixNano:      uint64(now.UnixNano()),
			Count:             s.count,
			Sum:               &sum,
			ExplicitBounds:    spanDurationBounds,
			BucketCounts:      append([]uint64(nil), s.counts...),
		})
	}
	return &metricspb.Metric{Name: spanMetricsDuration, Unit: "s", Data: &metricspb.Metric_Histogram{Histogram: h}}
}

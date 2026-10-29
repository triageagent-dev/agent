// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/triageagent-dev/agent/metricset"
)

// The receiver is a sensor: what it holds does not grow with the exports it
// forwards. N exports of the same series leave it with one entry per
// series (the last timestamp), whatever N is, and every export reached the
// sink.
func TestHeld_DoesNotGrowWithExports(t *testing.T) {
	r, got, _ := newTest(t)
	res := resource("service.name", "kubelet", "k8s.namespace.name", "kube-system")
	series := 200
	export := func(ts uint64) {
		ms := make([]*metricspb.Metric, 0, series)
		for i := range series {
			ms = append(ms, counter("use.cpu.usage.usec", ts, float64(ts), strAttr("k8s.pod.name", fmt.Sprintf("pod-%d", i))))
		}
		export(t, r, authed(), res, ms...)
	}
	export(10)
	after1 := r.Held()
	for ts := uint64(11); ts <= 59; ts++ {
		export(ts)
	}
	if r.Held() != after1 || after1 != series {
		t.Fatalf("held after 1 export: %d, after 50: %d; want %d both times", after1, r.Held(), series)
	}
	if len(*got) != 50 {
		t.Fatalf("the sink got %d exports, want every one of the 50", len(*got))
	}
	if len(r.sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(r.sources))
	}
}

// A series silent for Expire frees its entry; the sweep runs at most once a
// minute, on the receiver's own clock.
func TestSweep_DropsSeriesSilentForExpire(t *testing.T) {
	r, _, now := newTest(t)
	res := resource("service.name", "kubelet", "k8s.namespace.name", "kube-system")
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 1, strAttr("k8s.pod.name", "a")))
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 2, strAttr("k8s.pod.name", "b")))
	if r.Held() != 2 {
		t.Fatalf("held: %d, want 2", r.Held())
	}
	*now = now.Add(time.Minute)
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 20, 3, strAttr("k8s.pod.name", "a")))
	*now = now.Add(r.opts.Expire - 30*time.Second) // b silent past Expire, a not yet
	r.flush()
	if r.Held() != 1 || r.expired != 1 {
		t.Fatalf("after the sweep: %d held, %d expired; want 1 and 1", r.Held(), r.expired)
	}
}

// A span route no span fell into for Expire is dropped; the live one stays.
func TestSweep_DropsSpanRoutesSilentForExpire(t *testing.T) {
	r, _, now := newTest(t)
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	exportSpans(t, r, res, span("GET /old", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	*now = now.Add(time.Minute)
	exportSpans(t, r, res, span("GET /live", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	*now = now.Add(r.opts.Expire - 30*time.Second)
	r.flush()
	var sr *spanRoutes
	for _, s := range r.spans {
		sr = s
	}
	if sr == nil || len(sr.routes) != 1 {
		t.Fatalf("span routes after the sweep: %+v, want 1", sr)
	}
	for _, s := range sr.routes {
		if attr(s.attrs, "span.name") != "GET /live" {
			t.Fatalf("the live route was dropped; kept %q", attr(s.attrs, "span.name"))
		}
	}
}

// A span route counts against the total limit like any entry, and a
// refused route leaves nothing behind.
func TestMaxSeries_CountsSpanRoutes(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), MaxSeries: 2, now: func() time.Time { return now }})
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	// One route: the route and the timestamp of the histogram derived from it.
	exportSpans(t, r, res, span("GET /a", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	if r.Held() != 2 {
		t.Fatalf("held after one route: %d, want 2", r.Held())
	}
	resp := exportSpans(t, r, res, span("GET /b", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("a second route past MaxSeries=2 was not refused: %+v", resp.GetPartialSuccess())
	}
	var sr *spanRoutes
	for _, s := range r.spans {
		sr = s
	}
	if sr == nil || len(sr.routes) != 1 || r.Held() != 2 {
		t.Fatalf("the refused route left something behind: %+v, %d held", sr, r.Held())
	}
}

// Exemplars are not forwarded: no collector reads them and a scraped
// histogram carries one per bucket.
func TestExemplarsDropped(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var sunk []*metricspb.ResourceMetrics
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), now: func() time.Time { return now },
		Sink: func(_, _ string, rms []*metricspb.ResourceMetrics) { sunk = append(sunk, rms...) }})
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	m := histogram("http.server.request.duration", "s", cum, 10, 5, []float64{0.1}, strAttr("http.response.status_code", "200"))
	m.GetHistogram().DataPoints[0].Exemplars = []*metricspb.Exemplar{{TimeUnixNano: 9, TraceId: []byte("0123456789abcdef")}}
	export(t, r, authed(), res, m)
	if len(sunk) != 1 {
		t.Fatalf("forwarded %d resources, want 1", len(sunk))
	}
	for _, sm := range sunk[0].GetScopeMetrics() {
		for _, m := range sm.GetMetrics() {
			for _, dp := range m.GetHistogram().GetDataPoints() {
				if len(dp.GetExemplars()) != 0 {
					t.Fatalf("exemplars were forwarded: %v", dp.GetExemplars())
				}
			}
		}
	}
}

// A sender refused on every export is logged once a minute per reason, with
// the resource and the count refused since the last line.
func TestRefusalLog_OncePerMinute(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var lines []string
	log := funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
	r := New(Options{Auth: okAuth{}, Log: log, now: func() time.Time { return now }})
	res := resource("service.name", "checkout") // no namespace: refused every time
	over := func() {
		export(t, r, authed(), res, counter("app_requests_total", 10, 1, strAttr("code", "200")))
	}
	over()
	now = now.Add(10 * time.Second)
	over()
	now = now.Add(20 * time.Second)
	over()
	if len(lines) != 1 {
		t.Fatalf("refusals within a minute logged %d lines, want 1:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	now = now.Add(31 * time.Second)
	over()
	if len(lines) != 2 {
		t.Fatalf("a refusal a minute after the first line logged %d lines, want 2:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if l := lines[1]; !strings.Contains(l, `"refused"=3`) || !strings.Contains(l, `"resource"="/checkout"`) || !strings.Contains(l, "no k8s.namespace.name") {
		t.Fatalf("second line: %s", l)
	}
}

// The cAdvisor scrape of a node is admitted by the twelve series read and
// the pod cgroups alone; everything else is dropped at intake, unread.
func TestCAdvisor_IntakeKeepsOnlyWhatIsRead(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var outs []sunk
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), Wanted: ReadByCollectors, now: func() time.Time { return now },
		Sink: func(key, role string, rms []*metricspb.ResourceMetrics) {
			outs = append(outs, sunk{key: key, role: role, set: metricset.FromOTLP(rms)})
		}})
	res := resource("service.name", "kubelet") // a cAdvisor scrape names no namespace itself
	pod := []*commonpb.KeyValue{strAttr("namespace", "shop"), strAttr("pod", "p1")}
	container := []*commonpb.KeyValue{strAttr("namespace", "shop"), strAttr("pod", "p1"), strAttr("container", "app"), strAttr("image", "app:1")}
	export(t, r, authed(), res,
		counter("container_cpu_usage_seconds_total", 10, 3, pod...),
		counter("container_cpu_usage_seconds_total", 10, 2, container...),
		counter("container_network_receive_bytes_total", 10, 9, pod...),
		counter("container_fs_reads_total", 10, 9, container...))
	r.mu.Lock()
	unread := r.unread
	r.mu.Unlock()
	if unread != 3 {
		t.Fatalf("unread = %d, want 3: the container's point and the two series nobody reads", unread)
	}
	if len(outs) != 1 || outs[0].role != metricset.RoleNodeExporter {
		t.Fatalf("forwarded %+v, want one use set", outs)
	}
	m := outs[0].set.Get("use.cpu.usage.usec")
	if m == nil || len(m.Points) != 1 || m.Points[0].Value != 3e6 {
		t.Fatalf("use.cpu.usage.usec = %+v, want the pod cgroup's 3 s", m)
	}
}

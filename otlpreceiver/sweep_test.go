// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/triageagent-dev/agent/metricset"
)

// A resource that never goes silent keeps only the series an export
// updated within Expire: the pod that stopped reporting is dropped, the one
// still reporting stays, and the sink gets the resource without it.
func TestSweep_DropsSeriesSilentForExpire(t *testing.T) {
	r, got, now := newTest(t)
	res := resource("service.name", "kubelet", "k8s.namespace.name", "kube-system")
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 1, strAttr("k8s.pod.name", "a")))
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 2, strAttr("k8s.pod.name", "b")))
	r.flush()
	if r.nseries != 2 {
		t.Fatalf("series held: %d, want 2", r.nseries)
	}

	*now = now.Add(5 * time.Minute)
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 20, 3, strAttr("k8s.pod.name", "a")))
	*now = now.Add(6 * time.Minute) // b silent for 11 minutes, a for 6
	*got = nil
	r.flush()

	if r.nseries != 1 || r.expired != 1 {
		t.Fatalf("after the sweep: %d series held, %d expired; want 1 and 1", r.nseries, r.expired)
	}
	var set metricset.Set
	for _, s := range *got {
		if s.role == metricset.RoleNodeExporter {
			set = s.set
		}
	}
	m := set.Get("use.cpu.usage.usec")
	if m == nil || len(m.Points) != 1 || m.Points[0].Attr("k8s.pod.name") != "a" {
		t.Fatalf("the sink did not get the resource without the silent series: %+v", m)
	}
	if len(r.res) != 1 {
		t.Fatalf("the resource itself expired: %d held", len(r.res))
	}
}

// A series whose every point expired frees its metric, and a resource with
// no series left is kept until it expires itself.
func TestSweep_LeavesAnEmptyResourceToItsOwnExpiry(t *testing.T) {
	r, _, now := newTest(t)
	res := resource("service.name", "kubelet", "k8s.namespace.name", "kube-system")
	export(t, r, authed(), res, counter("use.cpu.usage.usec", 10, 1, strAttr("k8s.pod.name", "a")))
	*now = now.Add(11 * time.Minute)
	r.sweep(*now)
	st := r.res[resourceKey(normalizeResource(res))]
	if st == nil || len(st.metrics) != 0 || st.npoints != 0 || r.nseries != 0 {
		t.Fatalf("resource after its only series expired: %+v, series held %d", st, r.nseries)
	}
	r.flush() // the resource is 11 minutes silent too
	if len(r.res) != 0 {
		t.Fatalf("the silent resource was kept: %d held", len(r.res))
	}
}

// A span histogram no span fell into for Expire is dropped with the points
// derived from it, so a route no request has taken frees its slot.
func TestSweep_DropsSpanSeriesSilentForExpire(t *testing.T) {
	r, _, now := newTest(t)
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	exportSpans(t, r, res, span("GET /old", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	*now = now.Add(5 * time.Minute)
	exportSpans(t, r, res, span("GET /live", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	*now = now.Add(6 * time.Minute)
	r.sweep(*now)
	var st *resourceState
	for _, s := range r.res {
		st = s
	}
	if st == nil || len(st.spans) != 1 {
		t.Fatalf("span series after the sweep: %d, want 1", len(st.spans))
	}
	for _, s := range st.spans {
		if attr(s.attrs, "span.name") != "GET /live" {
			t.Fatalf("the live series was dropped; kept %q", attr(s.attrs, "span.name"))
		}
	}
	// The points derived from the old route's histogram went with it, and
	// the count holds the live route's histogram beside its points.
	if r.nseries == 0 || r.nseries != st.npoints+len(st.spans) {
		t.Fatalf("series held: %d in all, %d points and %d span histograms in the resource", r.nseries, st.npoints, len(st.spans))
	}
	for _, s := range st.metrics {
		for _, h := range s.points {
			if name := attr(pointAttrs(h.p), "span.name"); name != "GET /live" {
				t.Fatalf("a point of the dropped route was kept: %q", name)
			}
		}
	}
}

// The total series limit refuses a new series of any resource once it is
// reached, and reports it to the sender.
func TestMaxSeries_RefusesPastTotal(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), MaxSeries: 2, now: func() time.Time { return now }})
	a := resource("service.name", "a", "k8s.namespace.name", "shop")
	b := resource("service.name", "b", "k8s.namespace.name", "shop")
	export(t, r, authed(), a, counter("app_requests_total", 10, 1, strAttr("code", "200")))
	export(t, r, authed(), a, counter("app_requests_total", 10, 1, strAttr("code", "500")))
	resp := export(t, r, authed(), b, counter("app_requests_total", 10, 1, strAttr("code", "200")))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 || !strings.Contains(resp.GetPartialSuccess().GetErrorMessage(), "series limit reached") {
		t.Fatalf("third series past MaxSeries=2 was not refused: %+v", resp.GetPartialSuccess())
	}
	// An update of a held series is not a new one.
	resp = export(t, r, authed(), a, counter("app_requests_total", 20, 2, strAttr("code", "200")))
	if resp.GetPartialSuccess() != nil {
		t.Fatalf("an update of a held series was refused: %+v", resp.GetPartialSuccess())
	}
	if r.nseries != 2 {
		t.Fatalf("series held: %d, want 2", r.nseries)
	}
}

// A span histogram counts against the total series limit like a point, so
// traces cannot grow past it either.
func TestMaxSeries_CountsSpanHistograms(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{Auth: okAuth{}, Log: logr.Discard(), MaxSeries: 2, now: func() time.Time { return now }})
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	// One route: its histogram and the one point derived from it fill the limit.
	exportSpans(t, r, res, span("GET /a", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	if r.nseries != 2 {
		t.Fatalf("series held after one route: %d, want 2", r.nseries)
	}
	resp := exportSpans(t, r, res, span("GET /b", tracepb.Span_SPAN_KIND_SERVER, tracepb.Status_STATUS_CODE_OK, 5_000_000))
	if resp.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("a second route past MaxSeries=2 was not refused: %+v", resp.GetPartialSuccess())
	}
	var st *resourceState
	for _, s := range r.res { // the derived resource carries the replica attribute
		st = s
	}
	if st == nil || len(st.spans) != 1 || r.nseries != 2 {
		t.Fatalf("the refused route left a histogram behind: %+v, %d series", st, r.nseries)
	}
}

// Exemplars are not stored: no collector reads them and a scraped
// histogram carries one per bucket.
func TestExemplarsDropped(t *testing.T) {
	r, _, _ := newTest(t)
	res := resource("service.name", "api", "k8s.namespace.name", "shop")
	m := histogram("http.server.request.duration", "s", cum, 10, 5, []float64{0.1}, strAttr("http.response.status_code", "200"))
	m.GetHistogram().DataPoints[0].Exemplars = []*metricspb.Exemplar{{TimeUnixNano: 9, TraceId: []byte("0123456789abcdef")}}
	export(t, r, authed(), res, m)
	for _, st := range r.res {
		for _, s := range st.metrics {
			for _, h := range s.points {
				if dp := h.p.(*metricspb.HistogramDataPoint); len(dp.GetExemplars()) != 0 {
					t.Fatalf("exemplars were stored: %v", dp.GetExemplars())
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
	r := New(Options{Auth: okAuth{}, Log: log, MaxPointsPerResource: 1, now: func() time.Time { return now }})
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	over := func() {
		export(t, r, authed(), res,
			counter("app_requests_total", 10, 1, strAttr("code", "200")),
			counter("app_requests_total", 10, 1, strAttr("code", "500")))
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
	if l := lines[1]; !strings.Contains(l, `"refused"=3`) || !strings.Contains(l, `"resource"="shop/checkout"`) || !strings.Contains(l, "series limit reached for a resource") {
		t.Fatalf("second line: %s", l)
	}
}

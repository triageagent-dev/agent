package redsource

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/triageagent-dev/agent/metricset"
)

// ===========================================================================
// ProfileOBI gRPC server RED (rpc.server.call.duration) fixtures and tests.
// ===========================================================================

// grpcPoint builds one rpc.server.call.duration series with the OBI
// profile's attribute keys; status is the gRPC code NAME OBI exports.
func grpcPoint(namespace, service, method, status string, count int) metricset.Point {
	return metricset.Point{
		Attrs: map[string]string{
			"k8s.namespace.name":       namespace,
			"service.name":             service,
			"rpc.method":               method,
			"rpc.system":               "grpc",
			"rpc.response.status_code": status,
		},
		Value: float64(count),
	}
}

// grpcMetric builds the rpc.server.call.duration histogram from points.
func grpcMetric(points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: "rpc.server.call.duration", Kind: metricset.Histogram, Points: points}
}

func TestOBI_Collector_GRPCOnlyServiceCrossesGate(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(grpcMetric(
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "UNAVAILABLE", 0),
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "OK", 0),
	)))
	c, sender := obiRED(t, src, 10, 5, Options{})
	ctx := context.Background()
	c.runOnce(ctx)
	if sender.redCallCount() != 0 {
		t.Fatalf("first read: red callCount = %d, want 0 (first-seen baseline)", sender.redCallCount())
	}

	src.set(testTarget, setOf(grpcMetric( // 20 calls this window, 15 UNAVAILABLE
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "UNAVAILABLE", 15),
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "OK", 5),
	)))
	c.runOnce(ctx)

	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("red services = %+v, want 1", svcs)
	}
	got := svcs[0]
	if got.Namespace != "ns-a" || got.Service != "payment" || got.RequestCount != 20 || got.ErrorCount != 15 {
		t.Errorf("service = %+v, want {ns-a payment requests=20 errors=15}", got)
	}
	if want := "grpc /pay.Charge/Do -> UNAVAILABLE x15"; got.Sample != want {
		t.Errorf("sample = %q, want %q", got.Sample, want)
	}
	sum := sender.lastRedSummary()
	if len(sum) != 1 || sum[0].Service != "payment" || sum[0].RequestCount != 20 || sum[0].ErrorCount != 15 {
		t.Errorf("red_summary = %+v, want payment requests=20 errors=15", sum)
	}
	if sum[0].Sample != "" {
		t.Errorf("red_summary sample = %q, want empty", sum[0].Sample)
	}
}

func TestOBI_Collector_GRPCClientFaultStatusesAreNotErrors(t *testing.T) {
	body := func(nf, ia int) metricset.Set {
		return setOf(grpcMetric(
			grpcPoint("ns-a", "cart", "/cart.Cart/Get", "NOT_FOUND", nf),
			grpcPoint("ns-a", "cart", "/cart.Cart/Get", "INVALID_ARGUMENT", ia),
		))
	}
	src := newFakeSource()
	src.set(testTarget, body(0, 0))
	c, sender := obiRED(t, src, 1, 0, Options{})
	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, body(30, 10))
	c.runOnce(ctx)

	if sender.redCallCount() != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (NOT_FOUND/INVALID_ARGUMENT are not server errors)", sender.redCallCount())
	}
	sum := sender.lastRedSummary()
	if len(sum) != 1 || sum[0].RequestCount != 40 || sum[0].ErrorCount != 0 {
		t.Errorf("red_summary = %+v, want cart requests=40 errors=0", sum)
	}
}

func TestOBI_Collector_HTTPAndGRPCTotalsAreSummed(t *testing.T) {
	body := func(h500, h200, gInternal, gOK int) metricset.Set {
		return setOf(
			obiMetric(
				obiPoint("ns-a", "checkout", "GET", "/cart", "500", h500),
				obiPoint("ns-a", "checkout", "GET", "/cart", "200", h200),
			),
			grpcMetric(
				grpcPoint("ns-a", "checkout", "/co.Checkout/Place", "INTERNAL", gInternal),
				grpcPoint("ns-a", "checkout", "/co.Checkout/Place", "OK", gOK),
			),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(0, 0, 0, 0))
	c, sender := obiRED(t, src, 10, 5, Options{})
	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, body(4, 36, 9, 51)) // http 40 (4 err) + grpc 60 (9 err)
	c.runOnce(ctx)

	svcs := sender.redServices()
	if len(svcs) != 1 {
		t.Fatalf("red services = %+v, want 1", svcs)
	}
	if svcs[0].RequestCount != 100 || svcs[0].ErrorCount != 13 {
		t.Errorf("service = %+v, want requests=100 errors=13 (HTTP+gRPC summed)", svcs[0])
	}
	// The largest-delta error series across both metrics wins the sample.
	if want := "grpc /co.Checkout/Place -> INTERNAL x9"; svcs[0].Sample != want {
		t.Errorf("sample = %q, want %q", svcs[0].Sample, want)
	}
}

// The HTTP baseline must survive a gRPC read for the same target (and vice
// versa): both metrics share prevReq[addr]/prevBuckets[addr].
func TestOBI_Collector_HTTPAndGRPCBaselinesDoNotCollide(t *testing.T) {
	body := func(h, g int) metricset.Set {
		return setOf(
			obiMetric(obiPoint("ns-a", "svc", "GET", "/x", "200", h)),
			grpcMetric(grpcPoint("ns-a", "svc", "/s.S/M", "OK", g)),
		)
	}
	src := newFakeSource()
	src.set(testTarget, body(1000, 5000))
	c, sender := obiRED(t, src, 10, 5, Options{})
	ctx := context.Background()
	c.runOnce(ctx)
	src.set(testTarget, body(1010, 5020))
	c.runOnce(ctx)

	sum := sender.lastRedSummary()
	if len(sum) != 1 || sum[0].RequestCount != 30 {
		t.Errorf("red_summary = %+v, want requests=30 (10 http + 20 grpc deltas)", sum)
	}
	if n := len(c.prevReq[testTarget]); n != 2 {
		t.Errorf("len(prevReq[target]) = %d, want 2 (one series per metric)", n)
	}
}

func TestOBI_Collector_GRPCFirstWindowReportsNoLifetimeTotals(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(grpcMetric(
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "UNAVAILABLE", 4000),
		grpcPoint("ns-a", "payment", "/pay.Charge/Do", "OK", 400000),
	)))
	c, sender := obiRED(t, src, 10, 5, Options{})
	c.runOnce(context.Background())

	if sender.redCallCount() != 0 {
		t.Errorf("red_metrics frames = %d, want 0 on the first read", sender.redCallCount())
	}
	for _, s := range sender.lastRedSummary() {
		if s.RequestCount != 0 || s.ErrorCount != 0 {
			t.Errorf("first-window summary = %+v, want zero counts (lifetime totals must not leak)", s)
		}
	}
}

func TestOBI_Collector_GRPCOnlyServerIsATopologyNode(t *testing.T) {
	src := newFakeSource()
	src.set(testTarget, setOf(grpcMetric(grpcPoint("ns-a", "currency", "/c.C/Get", "OK", 0))))
	c, sender := obiTopology(t, src, Options{})
	c.runOnce(context.Background())

	nodes := sender.lastTopology().nodes
	if len(nodes) != 1 || nodes[0].Namespace != "ns-a" || nodes[0].Workload != "currency" {
		t.Errorf("nodes = %+v, want [ns-a/currency]", nodes)
	}
}

func TestIsGRPCServerError(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"OK", false},
		{"CANCELLED", false},
		{"UNKNOWN", true},
		{"INVALID_ARGUMENT", false},
		{"DEADLINE_EXCEEDED", true},
		{"NOT_FOUND", false},
		{"ALREADY_EXISTS", false},
		{"PERMISSION_DENIED", false},
		{"RESOURCE_EXHAUSTED", false},
		{"FAILED_PRECONDITION", false},
		{"ABORTED", false},
		{"OUT_OF_RANGE", false},
		{"UNIMPLEMENTED", true},
		{"INTERNAL", true},
		{"UNAVAILABLE", true},
		{"DATA_LOSS", true},
		{"UNAUTHENTICATED", false},
		{"", false},         // OBI omits the attribute for a status outside the gRPC enum
		{"internal", false}, // OBI emits upper-case names only
		{"14", false},       // numeric codes are never emitted
	}
	for _, tc := range tests {
		if got := IsGRPCServerError(tc.status); got != tc.want {
			t.Errorf("IsGRPCServerError(%q) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestFormatSampleGRPC(t *testing.T) {
	got := formatSampleGRPC(SampleContext{Status: "INTERNAL", Delta: 7, Extra: map[string]string{"rpc.method": "/a.B/C"}})
	if got != "grpc /a.B/C -> INTERNAL x7" {
		t.Errorf("sample = %q", got)
	}
	got = formatSampleGRPC(SampleContext{Status: "INTERNAL", Delta: 7})
	if !strings.HasPrefix(got, "grpc ? -> INTERNAL") {
		t.Errorf("missing-method sample = %q, want the ? fallback", got)
	}
}

func TestNew_DisabledWithoutUsableRequestMetric(t *testing.T) {
	if c := New(Options{Profile: Profile{Name: "x"}, Source: newFakeSource(), EnableRED: true}, &fakeSender{}, logr.Discard()); c != nil {
		t.Errorf("New with a profile that has no RequestMetrics = %v, want nil", c)
	}
}

// ===========================================================================
// Per-operation gate arm (Options.OperationErrorRatePct).
// ===========================================================================

// opHTTPBody builds an http window: route POST /api/checkout with the given
// 500/200 counts, and GET /page with pageOK 200s.
func opHTTPBody(checkout500, checkout200, pageOK int) metricset.Set {
	return setOf(obiMetric(
		obiPoint("ns-a", "web", "POST", "/api/checkout", "500", checkout500),
		obiPoint("ns-a", "web", "POST", "/api/checkout", "200", checkout200),
		obiPoint("ns-a", "web", "GET", "/page", "200", pageOK),
	))
}

// runOpWindow primes a zero baseline then delivers body as one window. The
// baseline arms are off: these tests are about the fixed operation arm, and a
// route failing outright on its first window is the baseline arms' cold and
// low-volume case (baseline_arms_test.go).
func runOpWindow(t *testing.T, opPct float64, zero, body metricset.Set) *fakeSender {
	t.Helper()
	src := newFakeSource()
	src.set(testTarget, zero)
	off := false
	c, sender := obiRED(t, src, 10, 5, Options{OperationErrorRatePct: opPct, Baseline: BaselineOptions{Enabled: &off}})
	c.runOnce(context.Background())
	src.set(testTarget, body)
	c.runOnce(context.Background())
	return sender
}

// A fully broken route diluted under the service-wide 5% gate still sends.
func TestOBI_Collector_OperationArmFrameForDilutedRoute(t *testing.T) {
	// 470 requests: checkout 19 (all 500), page 451 -> service rate ~4%.
	s := runOpWindow(t, 50, opHTTPBody(0, 0, 0), opHTTPBody(19, 0, 451))
	svcs := s.redServices()
	if len(svcs) != 1 {
		t.Fatalf("red services = %+v, want 1", svcs)
	}
	if svcs[0].ErrorCount != 19 || svcs[0].RequestCount != 470 {
		t.Errorf("service = %+v, want requests=470 errors=19 (service-wide counts)", svcs[0])
	}
	if want := "POST /api/checkout -> 500 x19"; svcs[0].Sample != want {
		t.Errorf("sample = %q, want %q", svcs[0].Sample, want)
	}
}

// Under the error-count threshold the operation arm does not fire.
func TestOBI_Collector_OperationArmNeedsErrorCount(t *testing.T) {
	s := runOpWindow(t, 50, opHTTPBody(0, 0, 0), opHTTPBody(9, 0, 461))
	if n := s.redCallCount(); n != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (9 errors < threshold 10)", n)
	}
}

// 19 errors but only 19% of that route's own traffic: not a broken route.
func TestOBI_Collector_OperationArmNeedsOperationRate(t *testing.T) {
	s := runOpWindow(t, 50, opHTTPBody(0, 0, 0), opHTTPBody(19, 81, 370))
	if n := s.redCallCount(); n != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (route rate 19%% < 50%%, service rate 4%%)", n)
	}
}

// Threshold 0 disables the arm; the service-wide gate still works.
func TestOBI_Collector_OperationArmDisabledAtZero(t *testing.T) {
	s := runOpWindow(t, 0, opHTTPBody(0, 0, 0), opHTTPBody(19, 0, 451))
	if n := s.redCallCount(); n != 0 {
		t.Errorf("red_metrics frames = %d, want 0 with the arm disabled", n)
	}
	// Service-wide 20 of 100 = 20% >= 5% and >= 10: sent as before.
	s = runOpWindow(t, 0, opHTTPBody(0, 0, 0), opHTTPBody(10, 10, 80))
	if n := s.redCallCount(); n != 1 {
		t.Errorf("red_metrics frames = %d, want 1 via the service-wide gate", n)
	}
}

func TestOBI_Collector_OperationArmGRPC(t *testing.T) {
	body := func(unavail, ok, other int) metricset.Set {
		return setOf(grpcMetric(
			grpcPoint("ns-a", "payment", "/pay.Charge/Do", "UNAVAILABLE", unavail),
			grpcPoint("ns-a", "payment", "/pay.Charge/Do", "OK", ok),
			grpcPoint("ns-a", "payment", "/pay.Health/Check", "OK", other),
		))
	}
	s := runOpWindow(t, 50, body(0, 0, 0), body(12, 0, 988)) // service rate 1.2%
	svcs := s.redServices()
	if len(svcs) != 1 {
		t.Fatalf("red services = %+v, want 1", svcs)
	}
	if want := "grpc /pay.Charge/Do -> UNAVAILABLE x12"; svcs[0].Sample != want {
		t.Errorf("sample = %q, want %q", svcs[0].Sample, want)
	}
}

// Two operations at 100% but 6 errors each: neither reaches the count
// threshold alone, and the service total (12 of 1000) fails the rate gate.
func TestOBI_Collector_OperationArmCountIsPerOperation(t *testing.T) {
	body := func(a, b, ok int) metricset.Set {
		return setOf(obiMetric(
			obiPoint("ns-a", "web", "POST", "/a", "500", a),
			obiPoint("ns-a", "web", "POST", "/b", "500", b),
			obiPoint("ns-a", "web", "GET", "/page", "200", ok),
		))
	}
	s := runOpWindow(t, 50, body(0, 0, 0), body(6, 6, 988))
	if n := s.redCallCount(); n != 0 {
		t.Errorf("red_metrics frames = %d, want 0 (6+6 errors never pooled by the operation arm)", n)
	}
	// Service-wide gate unchanged: 12 of 40 = 30% still sends.
	s = runOpWindow(t, 50, body(0, 0, 0), body(6, 6, 28))
	if n := s.redCallCount(); n != 1 {
		t.Errorf("red_metrics frames = %d, want 1 via the service-wide gate", n)
	}
}

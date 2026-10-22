// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	"github.com/triageagent-dev/agent/metricset"
)

func fromAddr(ip string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 40000}})
}

func newInsecure(t *testing.T) (*Receiver, *[]sunk) {
	t.Helper()
	var got []sunk
	r := New(Options{
		Insecure: true,
		Sink: func(key, role string, rms []*metricspb.ResourceMetrics) {
			got = append(got, sunk{key: key, role: role, set: metricset.FromOTLP(rms)})
		},
		Log: logr.Discard(),
		now: func() time.Time { return time.Unix(1_800_000_000, 0) },
	})
	return r, &got
}

func TestInsecure_NeedsNoToken(t *testing.T) {
	r, got := newInsecure(t)
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	if _, err := r.Export(fromAddr("10.0.0.5"), &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: res, ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{counter("log.errors", 10, 4)}}},
	}}}); err != nil {
		t.Fatalf("an insecure receiver refused a sender without credentials: %v", err)
	}
	r.flush()
	if len(*got) != 1 || (*got)[0].set.Get("log.errors").Points[0].Value != 4 {
		t.Fatalf("flushed = %+v", *got)
	}
}

func TestInsecure_SendersAreNamedByAddress(t *testing.T) {
	if p := anonymousPrincipal(fromAddr("10.0.0.5")); p.String() != "anonymous@10.0.0.5" || !p.Forwarder {
		t.Fatalf("principal = %+v (%s)", p, p.String())
	}
	if p := anonymousPrincipal(context.Background()); p.String() != "anonymous" {
		t.Fatalf("a call with no peer is %q", p.String())
	}
}

func TestInsecure_QuotaIsPerAddress(t *testing.T) {
	r, _ := newInsecure(t)
	r.opts.MaxResourcesPerSender = 1
	send := func(ip, svc string) int64 {
		resp, err := r.Export(fromAddr(ip), &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: resource("service.name", svc, "k8s.namespace.name", "shop"), ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{counter("log.errors", 10, 1)}}},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetPartialSuccess().GetRejectedDataPoints()
	}
	if n := send("10.0.0.5", "a"); n != 0 {
		t.Fatalf("first resource of an address refused: %d", n)
	}
	if n := send("10.0.0.5", "b"); n != 1 {
		t.Fatalf("second resource of the same address accepted (refused %d)", n)
	}
	if n := send("10.0.0.6", "c"); n != 0 {
		t.Fatalf("another address was held to the first one's quota: %d", n)
	}
	if got := r.SenderRefusals(); got["anonymous@10.0.0.5"] != 1 {
		t.Fatalf("refusals = %v", got)
	}
}

func TestInsecure_StillRefusesAQualifiedNamespaceClaim(t *testing.T) {
	r, _ := newInsecure(t)
	resp, err := r.Export(fromAddr("10.0.0.5"), &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: resource("service.name", "x", "k8s.namespace.name", "acme~shop"), ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{counter("log.errors", 10, 1)}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Fatalf("an anonymous sender placed data in a tenant's namespace: %+v", resp.GetPartialSuccess())
	}
}

// Over a real plaintext connection all three services answer and their data
// reaches the sink.
func TestInsecure_ServesAllThreeSignalsOverPlainGRPC(t *testing.T) {
	var mu sync.Mutex
	roles := map[string]metricset.Set{}
	r := New(Options{
		Insecure: true,
		Sink: func(_, role string, rms []*metricspb.ResourceMetrics) {
			mu.Lock()
			defer mu.Unlock()
			for n, m := range metricset.FromOTLP(rms) {
				if roles[role] == nil {
					roles[role] = metricset.Set{}
				}
				roles[role][n] = m
			}
		},
		Flush: 20 * time.Millisecond,
		Log:   logr.Discard(),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Serve(ctx, lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	res := resource("service.name", "checkout", "k8s.namespace.name", "shop")
	cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
	defer ccancel()

	if _, err := collectorpb.NewMetricsServiceClient(conn).Export(cctx, &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: res, ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{histogram("http.server.request.duration", "s", cum, 10, 3, []float64{1}, strAttr("http.response.status_code", "200"))}}},
	}}}); err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if _, err := collectortrace.NewTraceServiceClient(conn).Export(cctx, &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: res, ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span("GET /x", cli, ok, ms, strAttr("server.address", "payment"))}}},
	}}}); err != nil {
		t.Fatalf("traces: %v", err)
	}
	if _, err := collectorlogs.NewLogsServiceClient(conn).Export(cctx, &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: res, ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{rec(logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR")}}},
	}}}); err != nil {
		t.Fatalf("logs: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		e := roles[metricset.RoleEBPF]
		l := roles[metricset.RoleLogExporter]
		done := e.Get("http.server.request.duration") != nil && e.Get("http.client.request.duration") != nil && l.Get("log.errors") != nil
		mu.Unlock()
		if done {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("not everything reached the sink: %+v", roles)
}

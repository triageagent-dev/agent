// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// serveInsecure serves r on a loopback listener for the test and returns a
// client for it.
func serveInsecure(t *testing.T, r *Receiver) collectorpb.MetricsServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Serve(ctx, lis) }()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); cancel(); <-done })
	return collectorpb.NewMetricsServiceClient(conn)
}

func exportReq() *collectorpb.ExportMetricsServiceRequest {
	return &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource:     resource("service.name", "cart", "k8s.namespace.name", "shop"),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{counter("log.errors", 1, 1)}}},
	}}}
}

// retryDelay returns the retry delay a refusal carries, or zero.
func retryDelay(err error) (codes.Code, time.Duration) {
	st := status.Convert(err)
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			return st.Code(), ri.GetRetryDelay().AsDuration()
		}
	}
	return st.Code(), 0
}

// With one export allowed in flight, a second one is refused before it is
// read, with RESOURCE_EXHAUSTED and a retry delay, and taken once the
// first is done. The refusal is counted as a refused export against the sender.
func TestBackpressure_RefusesExportsPastTheInFlightBound(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	r := New(Options{Insecure: true, Log: logr.Discard(), MaxInFlight: 1,
		Sink: func(string, string, []*metricspb.ResourceMetrics) {
			entered <- struct{}{}
			<-release
		}})
	client := serveInsecure(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first := make(chan error, 1)
	go func() { _, err := client.Export(ctx, exportReq()); first <- err }()
	<-entered // the first export is in the sink, in flight
	_, err := client.Export(ctx, exportReq())
	if code, delay := retryDelay(err); code != codes.ResourceExhausted || delay != retryInFlight {
		t.Fatalf("second export: %v (code %v, retry %v), want RESOURCE_EXHAUSTED with a %v retry", err, code, delay, retryInFlight)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first export: %v", err)
	}
	if _, err := client.Export(ctx, exportReq()); err != nil {
		t.Fatalf("export after the first finished: %v", err)
	}
	// The refusal was an export, not points: no per-sender point count, and
	// the intake ledger says so apart from the two exports that were taken.
	if n, ok := r.SenderRefusals()["anonymous@127.0.0.1"]; ok {
		t.Fatalf("refused points against the sender = %d, want none: a refused export is not points", n)
	}
	r.logSummary()
	in := r.Intake()
	if len(in) != 1 || in[0].Sender != "anonymous@127.0.0.1" {
		t.Fatalf("intake = %+v, want one entry for the sender", in)
	}
	if in[0].RefusedExports != 1 || in[0].Refused != 0 || in[0].Exports != 2 {
		t.Fatalf("intake = %+v, want 1 refused export, 0 refused points, 2 exports taken", in[0])
	}
	if in, mem := r.Refused(); in != 1 || mem != 0 {
		t.Fatalf("refused = %d in flight, %d memory; want 1 and 0", in, mem)
	}
	if r.InFlight() != 0 {
		t.Fatalf("in flight after the exports = %d, want 0", r.InFlight())
	}
}

// With the live heap past 75% of the memory limit exports are refused with
// a longer retry delay, and keep being refused until it is back under
// 60%: a heap that hovers at the mark does not flap the senders.
func TestBackpressure_ShedsWhileTheHeapIsNearTheLimit(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var heap uint64
	r := New(Options{Insecure: true, Log: logr.Discard(), now: func() time.Time { return now },
		Sink: func(string, string, []*metricspb.ResourceMetrics) {}})
	r.adm.memoryLimit = func() int64 { return 1000 }
	r.adm.liveHeap = func() uint64 { return heap }
	client := serveInsecure(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	try := func(live uint64) error {
		heap = live
		now = now.Add(memorySampleEvery) // a fresh sample
		_, err := client.Export(ctx, exportReq())
		return err
	}
	if err := try(700); err != nil {
		t.Fatalf("at 70%%: %v, want taken", err)
	}
	if code, delay := retryDelay(try(800)); code != codes.ResourceExhausted || delay != retryMemory {
		t.Fatalf("at 80%%: code %v, retry %v; want RESOURCE_EXHAUSTED with a %v retry", code, delay, retryMemory)
	}
	if code, _ := retryDelay(try(650)); code != codes.ResourceExhausted {
		t.Fatalf("at 65%% after shedding: code %v, want still refused (under 60%% ends it)", code)
	}
	if err := try(550); err != nil {
		t.Fatalf("at 55%%: %v, want taken again", err)
	}
	if in, mem := r.Refused(); in != 0 || mem != 2 {
		t.Fatalf("refused = %d in flight, %d memory; want 0 and 2", in, mem)
	}
}

// Without a Go memory limit there is no mark to shed at.
func TestBackpressure_NoMemoryLimitNoShedding(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	a := newAdmission(0)
	a.memoryLimit = func() int64 { return 1<<63 - 1 }
	a.liveHeap = func() uint64 { return 1 << 40 }
	if a.overMemory(now) {
		t.Fatal("shedding without a memory limit")
	}
	if a.max < 4 {
		t.Fatalf("default in-flight bound = %d, want at least 4", a.max)
	}
}

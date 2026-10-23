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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// A Collector's OTLP gRPC exporter compresses with gzip unless told not to,
// and with zstd or snappy when its config says so. A gRPC server answers
// "Decompressor is not installed" to a codec nobody registered, so the
// receiver registers all three itself instead of relying on some other import
// having done it. (This file imports none of the codec packages on purpose: a
// test that did would register them for the whole binary.)
func TestReceiverAcceptsTheCodecsACollectorUses(t *testing.T) {
	r := New(Options{Insecure: true, Log: logr.Discard(), Sink: func(string, string, []*metricspb.ResourceMetrics) {}})
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

	for _, name := range []string{"gzip", "zstd", "snappy"} {
		t.Run(name, func(t *testing.T) {
			if encoding.GetCompressor(name) == nil {
				t.Fatalf("%s is not registered: a Collector exporting with it is refused", name)
			}
			cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
			defer ccancel()
			_, err := collectorpb.NewMetricsServiceClient(conn).Export(cctx, &collectorpb.ExportMetricsServiceRequest{
				ResourceMetrics: []*metricspb.ResourceMetrics{{Resource: resource("service.name", "checkout", "k8s.namespace.name", "shop")}},
			}, grpc.UseCompressor(name))
			if err != nil {
				t.Fatalf("export compressed with %s: %v", name, err)
			}
		})
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/otlpreceiver"
	"github.com/triageagent-dev/agent/sensorstream"
)

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// The whole path of ADR-0022 in one process: a Collector's raw OTLP (spans
// and log records, over plain gRPC) reaches the otlp role's receiver, the role
// streams the derived sources to the controller's hub with the pinned serving
// certificate and no token, and the hub holds RED histograms and log counters
// as sources the collectors read.
func TestOTLPRole_RawOTLPReachesTheHubAsREDAndLogSignals(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeCert(t, dir)
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}

	// The controller's hub: serving certificate pinned as the role, and the
	// controller's own tenant and scope check on what the role streams.
	hub := sensorstream.NewHub(sensorstream.HubOptions{
		TLS:          credentials.NewTLS(sensorstream.ServerTLS(pair)),
		PinnedPeer:   pair.Certificate[0],
		SourceFilter: otlpreceiver.ScopeFilter(func(string) bool { return true }, nil),
		Log:          logr.Discard(),
	})
	hubLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = hub.Serve(ctx, hubLis) }()

	// The role: an insecure receiver feeding the stream client.
	buf := sensorstream.NewSourceBuffer(otlpQueuePoints)
	recv, err := newOTLPSensorReceiver(otlpSensorFlags{listen: "127.0.0.1:0", insecure: true}, buf.Put)
	if err != nil {
		t.Fatal(err)
	}
	otlpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = recv.Serve(ctx, otlpLis) }()
	go func() {
		_ = sensorstream.RunClient(ctx, sensorstream.ClientOptions{
			Addr: net.JoinHostPort("127.0.0.1", portOf(hubLis)), CAFile: certFile,
			ClientCertFile: certFile, ClientKeyFile: keyFile,
			Node: "otlp-pod-a", Pod: "otlp-pod-a", Version: "test",
			Feed: buf, Tick: 30 * time.Millisecond, Log: logr.Discard(),
		})
	}()

	// A stock Collector's raw export.
	conn, err := grpc.NewClient(otlpLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	res := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "checkout"), strKV("k8s.namespace.name", "shop")}}
	cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
	defer ccancel()
	if _, err := collectortrace.NewTraceServiceClient(conn).Export(cctx, &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: res,
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{Name: "GET /cart", Kind: tracepb.Span_SPAN_KIND_SERVER, StartTimeUnixNano: 1e9, EndTimeUnixNano: 1e9 + 30e6, Attributes: []*commonpb.KeyValue{strKV("http.route", "/cart")}},
			{Name: "GET /cart", Kind: tracepb.Span_SPAN_KIND_SERVER, StartTimeUnixNano: 2e9, EndTimeUnixNano: 2e9 + 50e6, Attributes: []*commonpb.KeyValue{strKV("http.route", "/cart")}},
		}}},
	}}}); err != nil {
		t.Fatalf("traces: %v", err)
	}
	if _, err := collectorlogs.NewLogsServiceClient(conn).Export(cctx, &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: res,
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{
			{SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR}, {SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO},
		}}},
	}}}); err != nil {
		t.Fatalf("logs: %v", err)
	}

	var red, logs float64
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		red, logs = 0, 0
		for _, target := range hub.Targets(metricset.RoleEBPF) {
			if set, err := hub.Set(metricset.RoleEBPF, target); err == nil {
				if m := set.Get("http.server.request.duration"); m != nil {
					for _, p := range m.Points {
						red += p.Value
					}
				}
			}
		}
		for _, target := range hub.Targets(metricset.RoleLogExporter) {
			if set, err := hub.Set(metricset.RoleLogExporter, target); err == nil {
				if m := set.Get("log.errors"); m != nil {
					for _, p := range m.Points {
						logs += p.Value
					}
				}
			}
		}
		if red == 2 && logs == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the hub holds %v server spans and %v error logs, want 2 and 1", red, logs)
}

func portOf(l net.Listener) string {
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}

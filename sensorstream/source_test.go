// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// pinnedHub is a hub that pins its serving certificate as the otlp role's
// client certificate, with the files an otlp-role client presents it from.
type pinnedHub struct {
	hub                 *Hub
	addr, caFile        string
	certFile, keyFile   string
	otherCert, otherKey string
}

func startPinned(t *testing.T) pinnedHub {
	t.Helper()
	certPEM, keyPEM, err := newSelfSigned(testSANs)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	hub := NewHub(HubOptions{
		TLS:        credentials.NewTLS(ServerTLS(pair)),
		Auth:       fakeAuth{node: "node-a"},
		PinnedPeer: pair.Certificate[0],
		Log:        logr.Discard(),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = hub.Serve(ctx, lis) }()
	_, port, _ := net.SplitHostPort(lis.Addr().String())

	otherCert, otherKey, err := newSelfSigned(testSANs)
	if err != nil {
		t.Fatal(err)
	}
	return pinnedHub{
		hub: hub, addr: net.JoinHostPort("127.0.0.1", port), caFile: write("ca.crt", certPEM),
		certFile: write("tls.crt", certPEM), keyFile: write("tls.key", keyPEM),
		otherCert: write("other.crt", otherCert), otherKey: write("other.key", otherKey),
	}
}

// runOTLPSensor starts an otlp-role client that presents certFile and keyFile
// instead of a token.
func runOTLPSensor(t *testing.T, h pinnedHub, certFile, keyFile, pod string, feed SourceFeed, roles map[string]RoleSource) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = RunClient(ctx, ClientOptions{
			Addr: h.addr, CAFile: h.caFile, ClientCertFile: certFile, ClientKeyFile: keyFile,
			Node: pod, Pod: pod, Version: "test",
			Roles: roles, Feed: feed,
			Tick: 50 * time.Millisecond, Log: logr.Discard(),
		})
	}()
}

func sourceKeys(h *Hub) map[string]bool {
	out := map[string]bool{}
	for _, s := range h.OTLPSources() {
		out[s.Key] = true
	}
	return out
}

func TestOTLPRole_SourcesReachTheHubAsSources(t *testing.T) {
	h := startPinned(t)
	hub := h.hub
	buf := NewSourceBuffer(time.Minute)
	runOTLPSensor(t, h, h.certFile, h.keyFile, "otlp-pod-a", buf, nil)

	buf.Put("src-1", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("http.server.request.duration", 7, map[string]string{"k8s.namespace.name": "ns-a", "service.name": "svc"})})
	waitFor(t, "the source in the hub", func() bool { return sourceKeys(hub)["src-1"] })

	if got := hub.Targets(metricset.RoleEBPF); len(got) != 1 || got[0] != TargetPrefix+OTLPNodePrefix+"src-1" {
		t.Fatalf("targets = %v, want the one OTLP source", got)
	}
	// The sensor's own node holds no role metrics: only its liveness.
	var seen bool
	for _, n := range hub.Nodes() {
		if n.Node == "otlp-pod-a" {
			seen = len(n.Roles) == 1 && n.Roles[0] == RoleOTLP
		}
	}
	if !seen {
		t.Fatalf("nodes = %+v, want otlp-pod-a with the otlp role", hub.Nodes())
	}
}

func TestOTLPRole_ReplaysEverySourceAfterAReconnect(t *testing.T) {
	buf := NewSourceBuffer(time.Minute)
	buf.Put("a", metricset.RoleEBPF, nil)
	buf.Put("b", metricset.RoleNodeExporter, nil)
	if got := buf.Pending(false); len(got) != 2 {
		t.Fatalf("first pending = %d, want 2", len(got))
	}
	if got := buf.Pending(false); len(got) != 0 {
		t.Fatalf("second pending = %d, want 0: nothing changed", len(got))
	}
	if got := buf.Pending(true); len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("full pending = %+v, want both in key order", got)
	}
	buf.Put("a", metricset.RoleEBPF, nil)
	if got := buf.Pending(false); len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("pending after a change = %+v, want only a", got)
	}
}

func TestOTLPRole_BufferForgetsSilentSources(t *testing.T) {
	buf := NewSourceBuffer(time.Minute)
	now := time.Now()
	buf.now = func() time.Time { return now }
	buf.Put("a", metricset.RoleEBPF, nil)
	now = now.Add(2 * time.Minute)
	buf.Put("b", metricset.RoleEBPF, nil)
	if got := buf.Pending(true); len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("pending = %+v, want only b", got)
	}
	if buf.Len() != 1 {
		t.Fatalf("len = %d, want 1", buf.Len())
	}
}

func TestOTLPRole_ATokenSensorCannotSendSources(t *testing.T) {
	h := startPinned(t)
	buf := NewSourceBuffer(time.Minute)
	buf.Put("forged", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	// An eBPF sensor (token, no certificate) announcing the otlp role is refused at the hello.
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("good-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = RunClient(ctx, ClientOptions{Addr: h.addr, CAFile: h.caFile, TokenFile: tokenFile, Node: "node-a", Pod: "p", Feed: buf, Tick: 50 * time.Millisecond, Log: logr.Discard()})
	}()
	time.Sleep(500 * time.Millisecond)
	if len(h.hub.OTLPSources()) != 0 {
		t.Fatalf("sources = %v, want none: a token sensor may not send OTLP sources", sourceKeys(h.hub))
	}
}

func TestOTLPRole_ATokenSensorStillWorksOnThePinnedListener(t *testing.T) {
	h := startPinned(t)
	runSensor(t, h.addr, h.caFile, "good-token", "node-a")
	waitFor(t, "an ebpf target", func() bool { return len(h.hub.Targets(metricset.RoleEBPF)) == 1 })
}

func TestOTLPRole_AnotherCertificateIsRefusedAtTheHandshake(t *testing.T) {
	h := startPinned(t)
	buf := NewSourceBuffer(time.Minute)
	buf.Put("forged", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	runOTLPSensor(t, h, h.otherCert, h.otherKey, "intruder", buf, nil)
	time.Sleep(500 * time.Millisecond)
	if len(h.hub.OTLPSources()) != 0 {
		t.Fatalf("sources = %v, want none: only the pinned certificate is the otlp role", sourceKeys(h.hub))
	}
}

func TestOTLPRole_TheRoleRunsNoOtherRole(t *testing.T) {
	h := startPinned(t)
	src := RoleSourceFunc(func(context.Context) ([]*metricspb.ResourceMetrics, error) {
		return []*metricspb.ResourceMetrics{counter("requests", 42, map[string]string{"k8s.namespace.name": "ns-a"})}, nil
	})
	runOTLPSensor(t, h, h.certFile, h.keyFile, "otlp-pod-a", nil, map[string]RoleSource{metricset.RoleEBPF: src})
	time.Sleep(500 * time.Millisecond)
	if len(h.hub.Targets(metricset.RoleEBPF)) != 0 {
		t.Fatal("the otlp role was accepted as a node's ebpf sensor")
	}
}

func TestOTLPRole_FilterDropsRefusedResources(t *testing.T) {
	h := startPinned(t)
	hub := h.hub
	hub.opts.SourceFilter = func(rms []*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics {
		var out []*metricspb.ResourceMetrics
		for _, rm := range rms {
			if qualifiedNamespace(metricset.FromOTLP([]*metricspb.ResourceMetrics{rm})) == "" {
				out = append(out, rm)
			}
		}
		return out
	}
	buf := NewSourceBuffer(time.Minute)
	runOTLPSensor(t, h, h.certFile, h.keyFile, "otlp-pod-a", buf, nil)
	buf.Put("refused", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, map[string]string{"k8s.namespace.name": "other~ns"})})
	buf.Put("kept", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, map[string]string{"k8s.namespace.name": "ns-a"})})
	waitFor(t, "the kept source", func() bool { return sourceKeys(hub)["kept"] })
	if sourceKeys(hub)["refused"] {
		t.Fatal("a resource the filter refused reached the hub")
	}
}

func TestOTLPRole_NotADataRoleIsDropped(t *testing.T) {
	h := startPinned(t)
	hub := h.hub
	buf := NewSourceBuffer(time.Minute)
	runOTLPSensor(t, h, h.certFile, h.keyFile, "otlp-pod-a", buf, nil)
	buf.Put("odd", "made-up-role", []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	buf.Put("ok", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	waitFor(t, "the ok source", func() bool { return sourceKeys(hub)["ok"] })
	if sourceKeys(hub)["odd"] {
		t.Fatal("a snapshot for an unknown role reached the hub")
	}
}

// A token sensor that announces only a data role cannot file OTLP sources by
// putting a source on its snapshots: the stream is for the otlp role's
// certificate alone, whatever the hello says.
func TestOTLPRole_ATokenSensorCannotForgeASourceOnADataRoleStream(t *testing.T) {
	h := startPinned(t)
	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(h.caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool.AppendCertsFromPEM(caPEM)
	conn, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS13})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer good-token"), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := sensorpb.NewSensorServiceClient(conn).Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sensorpb.StreamRequest{Seq: 1, Body: &sensorpb.StreamRequest_Hello{Hello: &sensorpb.Hello{Node: "node-a", Pod: "p", Roles: []string{metricset.RoleEBPF}}}}); err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&sensorpb.StreamRequest{Seq: 2, Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
		Role: metricset.RoleEBPF, Source: "forged-source",
		ResourceMetrics: []*metricspb.ResourceMetrics{counter("requests", 1, nil)},
	}}})
	// The hub ends the stream; wait for it rather than sleeping.
	_, _ = stream.Recv()
	if len(h.hub.OTLPSources()) != 0 {
		t.Fatalf("sources = %v, want none: a data role stream may not file OTLP sources", sourceKeys(h.hub))
	}
}

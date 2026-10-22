// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/credentials"

	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

type fakeAuth struct{ node string }

func (f fakeAuth) Authenticate(_ context.Context, token string) (Identity, error) {
	if token != "good-token" {
		return Identity{}, errors.New("bad token")
	}
	return Identity{User: "system:serviceaccount:triage:triage-agent-ebpf", Node: f.node}, nil
}

func counter(name string, value int64, attrs map[string]string) *metricspb.ResourceMetrics {
	var kvs []*commonpb.KeyValue
	for k, v := range attrs {
		kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}})
	}
	return &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{Key: "k8s.node.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "node-a"}}}}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
			Name: name,
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				IsMonotonic:            true,
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				DataPoints:             []*metricspb.NumberDataPoint{{Attributes: kvs, Value: &metricspb.NumberDataPoint_AsInt{AsInt: value}}},
			}},
		}}}},
	}
}

// testSANs names the test certificates: the sensor dials the loopback IP, not
// "localhost", whose lookup can stall gRPC's resolver for seconds.
var testSANs = []string{"localhost", "127.0.0.1"}

// startHub serves a hub on localhost with a fresh self-signed certificate
// and returns its address and the certificate file a sensor pins.
func startHub(t *testing.T, auth Authenticator) (*Hub, string, string) {
	t.Helper()
	certPEM, keyPEM, err := newSelfSigned(testSANs)
	if err != nil {
		t.Fatal(err)
	}
	return startHubWith(t, auth, certPEM, keyPEM, nil)
}

// startHubWith is startHub with a given certificate and gossip hook, so two
// hubs can share one certificate like two replicas.
func startHubWith(t *testing.T, auth Authenticator, certPEM, keyPEM []byte, gossip func(mesh.NodeSnapshot)) (*Hub, string, string) {
	t.Helper()
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	hub := NewHub(HubOptions{
		TLS:    credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13}),
		Auth:   auth,
		Gossip: gossip,
		Log:    logr.Discard(),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = hub.Serve(ctx, lis) }()
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return hub, net.JoinHostPort("127.0.0.1", port), caFile
}

func runSensor(t *testing.T, addr, caFile, token, node string) {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	src := RoleSourceFunc(func(context.Context) ([]*metricspb.ResourceMetrics, error) {
		return []*metricspb.ResourceMetrics{counter("requests", 42, map[string]string{"k8s.namespace.name": "ns-a"})}, nil
	})
	go func() {
		_ = RunClient(ctx, ClientOptions{
			Addr: addr, CAFile: caFile, TokenFile: tokenFile,
			Node: node, Pod: "sensor-a", Version: "test",
			Roles:    map[string]RoleSource{metricset.RoleEBPF: src},
			Settings: []*sensorpb.Setting{{Name: "log-exporter-include-namespaces", Value: "triage", Default: "", Changed: true}},
			Tick:     50 * time.Millisecond,
			Log:      logr.Discard(),
		})
	}()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestStream_SnapshotAndStateReachTheHub(t *testing.T) {
	hub, addr, caFile := startHub(t, fakeAuth{node: "node-a"})
	runSensor(t, addr, caFile, "good-token", "node-a")

	waitFor(t, "a target", func() bool { return len(hub.Targets(metricset.RoleEBPF)) == 1 })
	nodes := hub.Nodes()
	if len(nodes) != 1 || len(nodes[0].Settings) != 1 || nodes[0].Settings[0].GetName() != "log-exporter-include-namespaces" ||
		nodes[0].Settings[0].GetValue() != "triage" || !nodes[0].Settings[0].GetChanged() {
		t.Errorf("the hub did not get the settings from the hello: %+v", nodes)
	}
	target := hub.Targets(metricset.RoleEBPF)[0]
	set, err := hub.Set(metricset.RoleEBPF, target)
	if err != nil {
		t.Fatal(err)
	}
	m := set.Get("requests")
	if m == nil || m.Kind != metricset.Counter || len(m.Points) != 1 || m.Points[0].Value != 42 {
		t.Fatalf("requests = %+v", m)
	}
	if got := m.Points[0].Attr("k8s.node.name"); got != "node-a" {
		t.Errorf("resource attribute not merged: %q", got)
	}
	if got := m.Points[0].Attr("k8s.namespace.name"); got != "ns-a" {
		t.Errorf("point attribute = %q", got)
	}

	// SetAt (metricset.TimedSource) reports when the set was received, and a
	// later snapshot moves it forward: the collectors divide a counter delta
	// by the time between two sets.
	_, at, err := hub.SetAt(metricset.RoleEBPF, target)
	if err != nil || at.IsZero() || time.Since(at) > time.Minute {
		t.Fatalf("SetAt time = %v, err = %v, want the receive time", at, err)
	}
	waitFor(t, "a later snapshot", func() bool {
		_, later, err := hub.SetAt(metricset.RoleEBPF, target)
		return err == nil && later.After(at)
	})

	waitFor(t, "an acked state", func() bool {
		n := hub.Nodes()
		return len(n) == 1 && n[0].State != nil && n[0].State.GetAckRttMicros() > 0
	})
	n := hub.Nodes()[0]
	if !n.Connected || n.Node != "node-a" || len(n.State.GetRoles()) != 1 || n.State.GetRoles()[0].GetDataPoints() != 1 {
		t.Errorf("node status = %+v", n)
	}
}

func TestStream_RefusesBadTokenAndForeignNode(t *testing.T) {
	hub, addr, caFile := startHub(t, fakeAuth{node: "node-a"})
	runSensor(t, addr, caFile, "bad-token", "node-a")
	runSensor(t, addr, caFile, "good-token", "node-b")
	time.Sleep(300 * time.Millisecond)
	if n := hub.Nodes(); len(n) != 0 {
		t.Fatalf("no sensor should be accepted, got %+v", n)
	}
}

func TestStream_UnpinnedCertificateRefused(t *testing.T) {
	hub, addr, _ := startHub(t, fakeAuth{})
	other, _, err := newSelfSigned([]string{"localhost"})
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "other.crt")
	if err := os.WriteFile(caFile, other, 0o600); err != nil {
		t.Fatal(err)
	}
	runSensor(t, addr, caFile, "good-token", "node-a")
	time.Sleep(300 * time.Millisecond)
	if n := hub.Nodes(); len(n) != 0 {
		t.Fatalf("a sensor pinning another certificate must not connect, got %+v", n)
	}
}

// RoleSourceFunc adapts a function to RoleSource.
type RoleSourceFunc func(ctx context.Context) ([]*metricspb.ResourceMetrics, error)

// Collect implements RoleSource.
func (f RoleSourceFunc) Collect(ctx context.Context) ([]*metricspb.ResourceMetrics, error) {
	return f(ctx)
}

// A sensor reports namespaces of its own cluster; one with the tenant
// separator would route to a tenant the sensor token was never bound to.
func TestStream_QualifiedNamespaceSnapshotRefused(t *testing.T) {
	h := NewHub(HubOptions{Log: logr.Discard()})
	sid, _ := h.open(&sensorpb.Hello{Node: "node-a", Roles: []string{metricset.RoleEBPF}}, 1, nil)
	snap := func(seq uint64, ns string) *sensorpb.StreamRequest {
		return &sensorpb.StreamRequest{Seq: seq, Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
			Role: metricset.RoleEBPF, ResourceMetrics: []*metricspb.ResourceMetrics{counter("requests", 1, map[string]string{"k8s.namespace.name": ns})},
		}}}
	}
	if _, ok := h.receive("node-a", sid, snap(2, "other~prod"), 1, nil); ok {
		t.Fatal("a snapshot naming a qualified namespace was accepted")
	}
	if got := h.Targets(metricset.RoleEBPF); len(got) != 0 {
		t.Fatalf("the refused snapshot was stored: %v", got)
	}
	if _, ok := h.receive("node-a", sid, snap(3, "prod"), 2, nil); !ok {
		t.Fatal("a plain namespace was refused")
	}
	if got := h.Targets(metricset.RoleEBPF); len(got) != 1 {
		t.Fatalf("targets = %v", got)
	}
}

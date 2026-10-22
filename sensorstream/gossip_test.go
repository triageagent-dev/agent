// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"crypto/tls"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// record builds a peer record for node: one ebpf snapshot whose "requests"
// counter is value.
func record(t *testing.T, node string, seq uint64, at time.Time, value int64) mesh.NodeSnapshot {
	t.Helper()
	msg := &sensorpb.StreamRequest{Seq: seq, Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
		Role:            metricset.RoleEBPF,
		ResourceMetrics: []*metricspb.ResourceMetrics{counter("requests", value, nil)},
	}}}
	return marshalRecord(t, node, seq, at, msg)
}

func marshalRecord(t *testing.T, node string, seq uint64, at time.Time, msg *sensorpb.StreamRequest) mesh.NodeSnapshot {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return mesh.NodeSnapshot{Node: node, Seq: seq, At: at, Snapshot: b}
}

func requests(t *testing.T, h *Hub, node string) (float64, time.Time) {
	t.Helper()
	set, at, err := h.SetAt(metricset.RoleEBPF, TargetPrefix+node)
	if err != nil {
		t.Fatal(err)
	}
	m := set.Get("requests")
	if m == nil || len(m.Points) != 1 {
		t.Fatalf("requests = %+v", m)
	}
	return m.Points[0].Value, at
}

func newTestHub(now time.Time) *Hub {
	return NewHub(HubOptions{Log: logr.Discard(), now: func() time.Time { return now }})
}

func TestMerge_NewestRecordWins(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	h := newTestHub(t0.Add(time.Second))

	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 5, t0, 10)})
	if got, at := requests(t, h, "node-a"); got != 10 || !at.Equal(t0) {
		t.Fatalf("first record: requests %v at %v", got, at)
	}

	// Older by receive time, even with a higher seq (a restarted stream on
	// another replica counts from 1 again, so seq alone orders nothing).
	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 9, t0.Add(-time.Second), 99)})
	if got, _ := requests(t, h, "node-a"); got != 10 {
		t.Fatalf("an older record replaced a newer one: requests %v", got)
	}

	// Same receive time: the higher seq wins, the lower one does not.
	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 6, t0, 11)})
	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 4, t0, 12)})
	if got, _ := requests(t, h, "node-a"); got != 11 {
		t.Fatalf("seq tie-break: requests %v, want 11", got)
	}

	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 1, t0.Add(500*time.Millisecond), 20)})
	if got, at := requests(t, h, "node-a"); got != 20 || !at.Equal(t0.Add(500*time.Millisecond)) {
		t.Fatalf("a newer record was not applied: requests %v at %v", got, at)
	}
	if targets := h.Targets(metricset.RoleEBPF); len(targets) != 1 || targets[0] != TargetPrefix+"node-a" {
		t.Fatalf("targets = %v", targets)
	}
}

func TestMerge_IgnoresGarbageAndUnknownClose(t *testing.T) {
	h := newTestHub(time.Now())
	h.Merge([]mesh.NodeSnapshot{
		{Node: "node-a", Seq: 1, At: time.Now(), Snapshot: []byte{0xff, 0xff}},
		marshalRecord(t, "node-b", 1, time.Now(), &sensorpb.StreamRequest{Seq: 1}),
		record(t, "", 1, time.Now(), 1),
	})
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("nodes = %+v, want none", n)
	}
}

func TestMerge_LocalStreamIsNotClobbered(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	h := newTestHub(t0)

	// node-a streams to this replica.
	sid, _ := h.open(&sensorpb.Hello{Node: "node-a", Pod: "sensor-local", Roles: []string{metricset.RoleEBPF}}, 1, nil)
	local := &sensorpb.StreamRequest{Seq: 2, Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
		Role: metricset.RoleEBPF, ResourceMetrics: []*metricspb.ResourceMetrics{counter("requests", 1, nil)},
	}}}
	if _, ok := h.receive("node-a", sid, local, 1, nil); !ok {
		t.Fatal("local message refused")
	}

	// A peer's record is newer by its clock, yet the local stream wins: it
	// is this replica's own, live view of the node.
	h.Merge([]mesh.NodeSnapshot{
		record(t, "node-a", 50, t0.Add(time.Minute), 999),
		marshalRecord(t, "node-a", 50, t0.Add(time.Minute), &sensorpb.StreamRequest{Seq: 50, Body: &sensorpb.StreamRequest_Hello{Hello: &sensorpb.Hello{Node: "node-a", Pod: "sensor-peer"}}}),
	})
	if got, _ := requests(t, h, "node-a"); got != 1 {
		t.Fatalf("a peer record overwrote the local stream: requests %v", got)
	}
	if n := h.Nodes(); len(n) != 1 || n[0].Pod != "sensor-local" || !n[0].Connected {
		t.Fatalf("nodes = %+v", n)
	}
	if recs := h.Local(); len(recs) != 0 {
		t.Fatalf("Local with gossip off keeps no raw records, got %d", len(recs))
	}

	// Once the local stream closes, a newer peer record (the sensor moved
	// to another replica) is applied.
	h.close("node-a", sid)
	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 3, t0.Add(time.Second), 7)})
	if got, _ := requests(t, h, "node-a"); got != 7 {
		t.Fatalf("after the local stream closed: requests %v, want 7", got)
	}
	if n := h.Nodes(); len(n) != 1 || !n[0].Connected {
		t.Fatalf("a node a peer reports is connected: %+v", n)
	}
}

func TestMerge_ConnectedFollowsThePeers(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	now := t0
	h := NewHub(HubOptions{Log: logr.Discard(), Stale: time.Minute, now: func() time.Time { return now }})
	hello := &sensorpb.StreamRequest{Seq: 1, Body: &sensorpb.StreamRequest_Hello{Hello: &sensorpb.Hello{
		Node: "node-a", Pod: "sensor-a", Version: "test", Roles: []string{metricset.RoleEBPF},
	}}}
	h.Merge([]mesh.NodeSnapshot{marshalRecord(t, "node-a", 1, t0, hello)})
	n := h.Nodes()
	if len(n) != 1 || !n[0].Connected || n[0].Pod != "sensor-a" || n[0].Version != "test" {
		t.Fatalf("after a peer's hello: %+v", n)
	}

	// The peer's stream closed: the node is gone from the list.
	h.Merge([]mesh.NodeSnapshot{marshalRecord(t, "node-a", 1, t0.Add(time.Second), &sensorpb.StreamRequest{Seq: 1})})
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("after the peer's close record: %+v", n)
	}

	// Reconnected; then the peer goes silent (it died with the stream).
	h.Merge([]mesh.NodeSnapshot{record(t, "node-a", 2, t0.Add(2*time.Second), 1)})
	if n := h.Nodes(); len(n) != 1 || !n[0].Connected {
		t.Fatalf("after a fresh peer record: %+v", n)
	}
	now = t0.Add(5 * time.Minute)
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("a silent peer's node is forgotten: %+v", n)
	}
}

// TestGossip_FailoverNewLeaderHasState runs two replicas end to end: a
// sensor streams to replica A only, A gossips over a real mesh, and replica
// B (the next leader) holds the node, both from the replay when A first
// connects and from the live stream after, and keeps it once A is gone.
func TestGossip_FailoverNewLeaderHasState(t *testing.T) {
	certPEM, keyPEM, err := newSelfSigned(testSANs)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	mt, err := mesh.NewTLS(pair, "localhost")
	if err != nil {
		t.Fatal(err)
	}

	// A's mesh starts after its sensor does; until then A publishes nowhere.
	var meshA atomic.Pointer[mesh.Mesh]
	hubA, addrA, caFile := startHubWith(t, fakeAuth{node: "node-a"}, certPEM, keyPEM, func(r mesh.NodeSnapshot) {
		if m := meshA.Load(); m != nil {
			m.Publish(r)
		}
	})
	hubB, _, _ := startHubWith(t, fakeAuth{}, certPEM, keyPEM, func(mesh.NodeSnapshot) {})

	// The sensor is up on A before the replicas mesh, so B first learns of
	// it from A's replay.
	runSensor(t, addrA, caFile, "good-token", "node-a")
	waitFor(t, "node-a on A", func() bool { return len(hubA.Targets(metricset.RoleEBPF)) == 1 })

	meshB := mesh.New(mesh.Config{Self: "replica-b", TLS: mt, Apply: hubB.Merge, Replay: hubB.Local, Log: logr.Discard()})
	srvB := httptest.NewUnstartedServer(meshB.Handler())
	srvB.TLS = mt.Server.Clone()
	srvB.StartTLS()
	t.Cleanup(srvB.Close)
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelB)
	go func() { _ = meshB.Start(ctxB) }()

	a := mesh.New(mesh.Config{
		Self: "replica-a", TLS: mt, Apply: hubA.Merge, Replay: hubA.Local, Log: logr.Discard(),
		StaticPeers: []string{"wss://" + strings.TrimPrefix(srvB.URL, "https://") + mesh.Path},
	})
	meshA.Store(a)
	ctxA, cancelA := context.WithCancel(context.Background())
	go func() { _ = a.Start(ctxA) }()

	waitFor(t, "node-a on B", func() bool { return len(hubB.Targets(metricset.RoleEBPF)) == 1 })
	_, firstAt := requests(t, hubB, "node-a")
	waitFor(t, "a live snapshot on B", func() bool {
		_, at := requests(t, hubB, "node-a")
		return at.After(firstAt)
	})
	waitFor(t, "node-a's state on B", func() bool {
		n := hubB.Nodes()
		return len(n) == 1 && n[0].Connected && n[0].State != nil && n[0].Pod == "sensor-a"
	})

	// A goes away (its pod is deleted); B, now the leader, still reads
	// node-a's latest snapshot.
	cancelA()
	if got, _ := requests(t, hubB, "node-a"); got != 42 {
		t.Fatalf("B after A left: requests %v, want 42", got)
	}
	if targets := hubB.Targets(metricset.RoleEBPF); len(targets) != 1 {
		t.Fatalf("B after A left: targets %v", targets)
	}
}

func TestNodes_ForgetsANodeThatIsNotConnected(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	now := t0
	h := NewHub(HubOptions{Log: logr.Discard(), Stale: time.Minute, now: func() time.Time { return now }})
	hello := &sensorpb.StreamRequest{Seq: 1, Body: &sensorpb.StreamRequest_Hello{Hello: &sensorpb.Hello{
		Node: "node-a", Pod: "sensor-a", Version: "test", Roles: []string{metricset.RoleEBPF},
	}}}
	h.Merge([]mesh.NodeSnapshot{marshalRecord(t, "node-a", 1, t0, hello)})
	h.Merge([]mesh.NodeSnapshot{marshalRecord(t, "node-a", 1, t0.Add(time.Second), &sensorpb.StreamRequest{Seq: 1})})

	// The peer's last word (the disconnect record) is fresh: not connected,
	// so the node is gone from the list at once. A node with no sensor is a
	// node that left the cluster, not a node to watch.
	now = t0.Add(10 * time.Second)
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("a node with no connected sensor is forgotten: %+v", n)
	}
	// The same once the peer record itself is stale.
	h.Merge([]mesh.NodeSnapshot{marshalRecord(t, "node-a", 1, t0, hello)})
	now = t0.Add(2 * time.Minute)
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("a node whose peer record went stale is forgotten: %+v", n)
	}
}

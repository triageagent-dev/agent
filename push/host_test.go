package push

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func newTestHostPusher(endpoint string) *Pusher {
	return sharedTestCore().joinedTo(NewHost(Options{
		Endpoint:     endpoint,
		ClusterName:  "web-01",
		Tenant:       "acme",
		Namespaces:   []string{"host"},
		SensorStream: true,
		Heartbeat:    100 * time.Millisecond,
	}, logr.Discard()))
}

func TestNewHost_DisabledWithoutEndpoint(t *testing.T) {
	if p := NewHost(Options{Tenant: "acme"}, logr.Discard()); p != nil {
		t.Fatalf("NewHost with no endpoint = %v, want nil", p)
	}
}

// TestNewHost_ScopeAndRouting pins the host contract: the synthetic namespace
// is the whole scope, every item files under the one tenant, and nothing is
// derived from namespace annotations (tenant mode, never cluster mode).
func TestNewHost_ScopeAndRouting(t *testing.T) {
	p := newTestHostPusher("ws://example.internal/events/ws")
	if !p.NamespaceInScope("host") {
		t.Error("the synthetic namespace must be in scope")
	}
	if p.NamespaceInScope("other") {
		t.Error("a namespace other than the synthetic one must be out of scope")
	}
	if p.clusterMode() {
		t.Error("a host agent routes by --target-tenant, not by namespace annotation")
	}
	if got, ok := p.resolveTenant("host"); !ok || got != "acme" {
		t.Errorf("resolveTenant(host) = (%q, %v), want (acme, true)", got, ok)
	}
	if got := p.declaredNamespaces(); !slices.Equal(got, []string{"host"}) {
		t.Errorf("declared namespaces = %v, want [host]", got)
	}
	if p.vapps != nil || p.cache != nil || p.client != nil {
		t.Error("a host Pusher must hold no virtual-app state, cache or client")
	}
}

// TestNewHost_StartRunsTransportWithoutKubernetes starts a host Pusher with no
// manager, cache or informers and checks what the core sees: the hello with
// the synthetic namespace and the host capability, an empty snapshot, a
// heartbeat and the sensor_state frame.
func TestNewHost_StartRunsTransportWithoutKubernetes(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestHostPusher(wsURL(srv))
	p.SendSensorState(1720000000, []SensorStatus{{Node: "web-01", Connected: true}}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	waitForStampedKind(t, hub, KindSensorState, "", 3*time.Second)
	waitForStampedKind(t, hub, KindHeartbeat, "", 3*time.Second)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	kinds := hub.kinds()
	if len(kinds) < 3 || kinds[0] != KindHello || kinds[1] != KindSnapshotEnd {
		t.Fatalf("frames = %v, want hello then an empty snapshot (snapshot_end only)", kinds)
	}
	if slices.Contains(kinds, KindSnapshot) {
		t.Errorf("frames = %v: a host with no app source must send no snapshot chunk", kinds)
	}
	hub.mu.Lock()
	hello := hub.frames[0].Hello
	hub.mu.Unlock()
	if hello.ClusterName != "web-01" || hello.Tenant != "acme" {
		t.Errorf("hello cluster/tenant = %q/%q, want web-01/acme", hello.ClusterName, hello.Tenant)
	}
	if !slices.Equal(hello.Namespaces, []string{"host"}) {
		t.Errorf("hello namespaces = %v, want [host]", hello.Namespaces)
	}
	for _, c := range []string{"host", "sensor_state"} {
		if !slices.Contains(hello.Capabilities, c) {
			t.Errorf("hello capabilities = %v, want to contain %q", hello.Capabilities, c)
		}
	}
}

// TestPusher_ClusterHelloHasNoHostCapability keeps the cluster hello as it
// was: "host" only ever comes from NewHost.
func TestPusher_ClusterHelloHasNoHostCapability(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindHello, "", 2*time.Second)
	cancel()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if slices.Contains(hub.frames[0].Hello.Capabilities, "host") {
		t.Errorf("cluster hello capabilities = %v, must not contain host", hub.frames[0].Hello.Capabilities)
	}
}

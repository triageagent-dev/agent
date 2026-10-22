package push

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

// newTestNoKubePusher builds a cluster Pusher with NoKubernetes set and no
// cache or client: startKubernetes would dereference the nil cache, so a Start
// that returns normally proves it registered no informer.
func newTestNoKubePusher(endpoint string) *Pusher {
	return sharedTestCore().joinedTo(&Pusher{
		opts: Options{
			Endpoint:      endpoint,
			ClusterName:   "cluster-a",
			Tenant:        "example-tenant",
			NoKubernetes:  true,
			SensorStream:  true,
			Heartbeat:     100 * time.Millisecond,
			SnapshotChunk: 50,
		},
		log:          logr.Discard(),
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
		svcIndex:     newServiceIndex(logr.Discard()),
	})
}

// TestNoKubernetes_StartRegistersNoInformers starts the cluster Pusher with
// NoKubernetes and checks the core still sees a normal agent: the hello, an
// empty snapshot, a heartbeat and the sensor_state frame, with no "host"
// capability (it is a cluster agent) and no workload frames.
func TestNoKubernetes_StartRegistersNoInformers(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestNoKubePusher(wsURL(srv))
	p.SendSensorState(1720000000, []SensorStatus{{Node: "otlp", Connected: true}}, nil)

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
		t.Errorf("frames = %v: with no Kubernetes inputs there is no snapshot chunk", kinds)
	}
	hub.mu.Lock()
	hello := hub.frames[0].Hello
	hub.mu.Unlock()
	if slices.Contains(hello.Capabilities, "host") {
		t.Errorf("hello capabilities = %v, a cluster agent must not advertise host", hello.Capabilities)
	}
}

// TestNoKubernetes_RoutesByTenantAndReadsNothing pins the rest of the
// contract: every namespace files under the primary tenant, virtual apps are
// off, and the pod-exclusion hook answers without reading the cluster (the
// bare Pusher has no client, so a read would panic).
func TestNoKubernetes_RoutesByTenantAndReadsNothing(t *testing.T) {
	p := newTestNoKubePusher("ws://example.invalid/events/ws")
	if p.clusterMode() {
		t.Error("NoKubernetes routes by --target-tenant, never by namespace annotation")
	}
	for _, ns := range []string{"any", "other"} {
		if !p.NamespaceInScope(ns) {
			t.Errorf("namespace %q out of scope, want every namespace in scope under the tenant", ns)
		}
		if got, ok := p.resolveTenant(ns); !ok || got != "example-tenant" {
			t.Errorf("resolveTenant(%q) = (%q, %v), want (example-tenant, true)", ns, got, ok)
		}
	}
	if p.vapps != nil {
		t.Error("NoKubernetes must hold no virtual-app state")
	}
	p.opts.Namespaces = []string{"only"}
	if !p.NamespaceInScope("only") || p.NamespaceInScope("other") {
		t.Error("--target-namespaces must still narrow the scope")
	}
	p.opts.Namespaces = nil
	if p.PodExcluded(context.Background(), "any", "pod-1") {
		t.Error("PodExcluded must report false without reading the cluster")
	}
}

// TestNoKubernetes_AppSourceBuildsAppsPerNamespace pins how a NoKubernetes
// agent gets applications: an app source names each service's namespace, every
// namespace is in scope, and the snapshot carries one app per (namespace,
// service).
func TestNoKubernetes_AppSourceBuildsAppsPerNamespace(t *testing.T) {
	p := newTestNoKubePusher("ws://example.invalid/events/ws")
	if err := p.SetAppSource(newFakeAppSource()); err != nil {
		t.Fatalf("SetAppSource on a NoKubernetes Pusher = %v, want nil", err)
	}
	p.applyAppEvent(AppEvent{Name: "cart", Namespace: "shop", Ready: true})
	p.applyAppEvent(AppEvent{Name: "cart", Namespace: "staging", Ready: true})
	p.applyAppEvent(AppEvent{Name: "ad", Namespace: "shop", Ready: true})

	if _, n := p.snapshotFramesForTenant(context.Background(), "example-tenant"); n != 3 {
		t.Fatalf("snapshot apps = %d, want 3 (cart in two namespaces, ad)", n)
	}
	got := map[string]bool{}
	for _, a := range p.vapps.snapshotApps() {
		got[a.Namespace+"/"+a.Name] = a.Status.ComponentsReady == "1/1"
	}
	for _, want := range []string{"shop/cart", "staging/cart", "shop/ad"} {
		if !got[want] {
			t.Errorf("apps = %v, want %s ready", got, want)
		}
	}

	p.applyAppEvent(AppEvent{Name: "cart", Namespace: "staging", Deleted: true})
	p.vapps.recomputeNow(context.Background())
	for _, a := range p.vapps.snapshotApps() {
		if a.Namespace == "staging" {
			t.Errorf("deleted staging/%s still in the snapshot", a.Name)
		}
	}
}

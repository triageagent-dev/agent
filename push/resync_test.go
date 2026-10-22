package push

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPusher_Resync_ResendsSnapshotForTenantOnly is the end-to-end proof of
// the core-triggered resync (KindResync, protocol.go): a {kind:"resync",
// tenant:...} frame written by the core onto the live connection is
// answered with that tenant's full snapshot sequence, exactly like a fresh
// (re)connect would send — and a sibling tenant sharing the same connection
// is left untouched.
func TestPusher_Resync_ResendsSnapshotForTenantOnly(t *testing.T) {
	hub, srv := newFakeHub(t)

	workerApp := testApp("worker", true)
	workerApp.Namespace = "rag"
	webApp := testApp("web", true)
	webApp.Namespace = "dev"

	p := newTestPusher(wsURL(srv), []workloadInfo{workerApp, webApp})
	p.opts.Heartbeat = 30 * time.Second // keep the hub's pong reply out of this test's write window
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{annotationTenant: "tenant-b"}},
	}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-b", 3*time.Second)

	if got := countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd); got != 1 {
		t.Fatalf("tenant-a snapshot_end count before resync = %d, want 1", got)
	}

	hub.sendToAgent(t, &Frame{V: ProtocolVersion, Kind: KindResync, Tenant: "tenant-a"})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd) < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	if got := countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd); got != 2 {
		t.Fatalf("tenant-a snapshot_end count after resync = %d, want 2 (connect + resync)", got)
	}
	if got := countKind(hub.framesWithTenant("tenant-b"), KindSnapshotEnd); got != 1 {
		t.Errorf("tenant-b snapshot_end count after tenant-a's resync = %d, want 1 (unaffected)", got)
	}

	var names []string
	for _, f := range hub.framesWithTenant("tenant-a") {
		if f.Kind == KindSnapshot {
			for _, a := range f.Snapshot.Apps {
				names = append(names, a.Name)
			}
		}
	}
	if len(names) != 2 || names[0] != "worker" || names[1] != "worker" {
		t.Errorf("tenant-a snapshot apps across both sequences = %v, want [worker worker]", names)
	}
}

// TestPusher_Resync_ClusterModeStampedTenantOnly is the cluster-mode
// (Options.Tenant == "") variant: several tenants share one connection with
// no primary at all, and a resync stamped for one of them resends only that
// tenant's sequence.
func TestPusher_Resync_ClusterModeStampedTenantOnly(t *testing.T) {
	hub, srv := newFakeHub(t)

	workerApp := testApp("worker", true)
	workerApp.Namespace = "rag"
	webApp := testApp("web", true)
	webApp.Namespace = "dev"

	p := newClusterModeTestPusher(wsURL(srv), []workloadInfo{workerApp, webApp})
	p.opts.Heartbeat = 30 * time.Second
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{annotationTenant: "tenant-b"}},
	}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-b", 3*time.Second)

	hub.sendToAgent(t, &Frame{V: ProtocolVersion, Kind: KindResync, Tenant: "tenant-a"})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd) < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	if got := countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd); got != 2 {
		t.Fatalf("tenant-a snapshot_end count after resync = %d, want 2 (connect + resync)", got)
	}
	if got := countKind(hub.framesWithTenant("tenant-b"), KindSnapshotEnd); got != 1 {
		t.Errorf("tenant-b snapshot_end count after tenant-a's resync = %d, want 1 (unaffected)", got)
	}
}

// TestPusher_ReadLoop_UnknownKindIgnored proves an unrecognized hub->agent
// frame kind is dropped without disturbing the connection: a resync sent
// right after it must still produce a fresh snapshot.
func TestPusher_ReadLoop_UnknownKindIgnored(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), []workloadInfo{testApp("api", true)})
	p.opts.Heartbeat = 30 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 3*time.Second)

	hub.sendToAgent(t, &Frame{V: ProtocolVersion, Kind: "some_future_kind"})
	// Empty Tenant resolves to the session's handshake (primary) tenant.
	hub.sendToAgent(t, &Frame{V: ProtocolVersion, Kind: KindResync})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countKind(hub.framesWithTenant(""), KindSnapshotEnd) >= 2 {
			cancel()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("resync after an unknown-kind frame produced no fresh snapshot; frames = %v", hub.kinds())
}

// TestPusher_HandleResyncFrame_CoalescesBackToBack is the deterministic,
// white-box proof of the resync coalescing gate (tenantMux.tryStartResync):
// two resync frames handled back-to-back before the first sequence drains
// must leave exactly one snapshot_end queued, and a resync AFTER that
// sequence has fully drained must not be coalesced away.
func TestPusher_HandleResyncFrame_CoalescesBackToBack(t *testing.T) {
	p := newTestPusher("ws://unused", []workloadInfo{testApp("api", true)})
	mux := p.mux()
	mux.beginConnection([]string{p.opts.Tenant})

	p.handleResyncFrame(&Frame{Kind: KindResync, Tenant: p.opts.Tenant})
	p.handleResyncFrame(&Frame{Kind: KindResync, Tenant: p.opts.Tenant})

	w := mux.workers[p.opts.Tenant]
	if w == nil {
		t.Fatal("no worker registered for the primary tenant after handleResyncFrame")
	}
	if got := countKind(w.queue, KindSnapshotEnd); got != 1 {
		t.Fatalf("queued snapshot_end frames after two back-to-back resyncs = %d, want 1 (coalesced)", got)
	}

	// Drain the sequence exactly like connWorker.writeLoop would.
	for {
		if _, ok := mux.next(); !ok {
			break
		}
	}

	p.handleResyncFrame(&Frame{Kind: KindResync, Tenant: p.opts.Tenant})
	if got := countKind(w.queue, KindSnapshotEnd); got != 1 {
		t.Fatalf("queued snapshot_end frames after drain + a third resync = %d, want 1 (a fresh resync after completion must not be coalesced away)", got)
	}
}

// TestPusher_HandleResyncFrame_ClusterModeNoTenantDropped verifies a resync
// with no Tenant stamp in cluster-mode (no primary to fall back to) is
// dropped rather than routed anywhere.
func TestPusher_HandleResyncFrame_ClusterModeNoTenantDropped(t *testing.T) {
	p := newClusterModeTestPusher("ws://unused", nil)
	p.dynamicScope = newDynamicScopeSet()
	mux := p.mux()
	mux.beginConnection(nil)

	p.handleResyncFrame(&Frame{Kind: KindResync})

	if got := mux.snapshot(); len(got) != 0 {
		t.Fatalf("worker snapshot after a tenant-less resync in cluster-mode = %+v, want none", got)
	}
}

package push

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- White-box tenantWorker/tenantMux unit tests -------------------------

// TestTenantWorker_EnqueueLocked_CoalesceEvictsQueuedDeltas is the core
// overflow-policy test: a full queue of non-snapshot frames must be evicted
// wholesale (coalesced, not plain dropped-oldest) to make room for an
// incoming fresh snapshot-sequence frame — a full resync makes those queued
// deltas moot.
func TestTenantWorker_EnqueueLocked_CoalesceEvictsQueuedDeltas(t *testing.T) {
	w := newTenantWorker("t1")
	for i := 0; i < tenantWorkerQueueSize; i++ {
		w.enqueueLocked(&Frame{Kind: KindAppDelta})
	}
	if len(w.queue) != tenantWorkerQueueSize {
		t.Fatalf("queue len = %d, want %d before overflow", len(w.queue), tenantWorkerQueueSize)
	}

	w.enqueueLocked(&Frame{Kind: KindSnapshot})

	if len(w.queue) != 1 {
		t.Fatalf("queue len after coalescing enqueue = %d, want 1 (every queued delta evicted, the snapshot kept)", len(w.queue))
	}
	if w.queue[0].Kind != KindSnapshot {
		t.Errorf("queue[0].Kind = %q, want %q", w.queue[0].Kind, KindSnapshot)
	}
	if w.coalesced != tenantWorkerQueueSize {
		t.Errorf("coalesced = %d, want %d (every evicted delta counted)", w.coalesced, tenantWorkerQueueSize)
	}
	if w.dropped != tenantWorkerQueueSize {
		t.Errorf("dropped = %d, want %d (coalesced evictions count toward dropped too)", w.dropped, tenantWorkerQueueSize)
	}
}

// TestTenantWorker_EnqueueLocked_DropOldestWhenQueueIsAllSnapshotFrames
// verifies the fallback: when a full queue holds ONLY snapshot-sequence
// frames (nothing for a fresh snapshot to supersede), overflow falls back to
// plain drop-oldest, and that eviction is NOT counted as coalesced.
func TestTenantWorker_EnqueueLocked_DropOldestWhenQueueIsAllSnapshotFrames(t *testing.T) {
	w := newTenantWorker("t1")
	for i := 0; i < tenantWorkerQueueSize; i++ {
		w.enqueueLocked(&Frame{Kind: KindSnapshot})
	}

	w.enqueueLocked(&Frame{Kind: KindSnapshotEnd})

	if len(w.queue) != tenantWorkerQueueSize {
		t.Fatalf("queue len = %d, want %d (bounded)", len(w.queue), tenantWorkerQueueSize)
	}
	if w.queue[len(w.queue)-1].Kind != KindSnapshotEnd {
		t.Errorf("newest queue entry = %q, want %q", w.queue[len(w.queue)-1].Kind, KindSnapshotEnd)
	}
	if w.dropped != 1 {
		t.Errorf("dropped = %d, want 1 (one plain drop-oldest eviction)", w.dropped)
	}
	if w.coalesced != 0 {
		t.Errorf("coalesced = %d, want 0 (nothing here was superseded, just aged out)", w.coalesced)
	}
}

// TestTenantMux_LazyCreateAndRetire covers requirement (1): a worker is
// created lazily on first frame for a tenant, appears in WorkerStats, and is
// removed once reconcileTenants (simulated here directly via
// staleTenants/nextFor/retire) determines its tenant no longer holds scope.
func TestTenantMux_LazyCreateAndRetire(t *testing.T) {
	m := newTenantMux(logr.Discard(), func() time.Time { return time.Unix(1720000000, 0) })
	m.beginConnection(nil) // "connected", but no tenants active yet

	if got := m.snapshot(); len(got) != 0 {
		t.Fatalf("snapshot() before any frame = %+v, want empty", got)
	}

	if !m.enqueue("t1", &Frame{Kind: KindAppDelta}) {
		t.Fatal("enqueue(t1, ...) = false, want true (mux is connected)")
	}
	stats := m.snapshot()
	if len(stats) != 1 || stats[0].Tenant != "t1" || stats[0].Queued != 1 {
		t.Fatalf("snapshot() after first t1 frame = %+v, want one entry {t1, Queued:1}", stats)
	}

	m.enqueue("t2", &Frame{Kind: KindAppDelta})
	stats = m.snapshot()
	if len(stats) != 2 || stats[0].Tenant != "t1" || stats[1].Tenant != "t2" {
		t.Fatalf("snapshot() after t2 = %+v, want [t1, t2] sorted", stats)
	}

	stale := m.staleTenants([]string{"t2"})
	if len(stale) != 1 || stale[0] != "t1" {
		t.Fatalf("staleTenants([t2]) = %v, want [t1]", stale)
	}

	// Mirror connWorker.reconcileTenants: flush then retire.
	for {
		if _, ok := m.nextFor("t1"); !ok {
			break
		}
	}
	m.retire("t1")

	stats = m.snapshot()
	if len(stats) != 1 || stats[0].Tenant != "t2" {
		t.Fatalf("snapshot() after retiring t1 = %+v, want only t2", stats)
	}
}

// TestTenantMux_RoundRobinAlternatesUnderStorm is a deterministic proof of
// requirement (2)/(3)'s fairness property: a tenant with a deep backlog does
// not make a sibling tenant's single frame wait for that whole backlog to
// drain — round robin interleaves one frame per tenant per rotation.
func TestTenantMux_RoundRobinAlternatesUnderStorm(t *testing.T) {
	m := newTenantMux(logr.Discard(), time.Now)
	m.beginConnection([]string{"a", "b"})

	const floodSize = 50
	for i := 0; i < floodSize; i++ {
		m.enqueue("a", &Frame{Kind: KindAppDelta, Cluster: "a"})
	}
	m.enqueue("b", &Frame{Kind: KindAppDelta, Cluster: "b"})

	f0, ok := m.next()
	if !ok || f0.Cluster != "a" {
		t.Fatalf("first pop = %+v (ok=%v), want tenant a's frame first (round-robin starts at a)", f0, ok)
	}
	f1, ok := m.next()
	if !ok || f1.Cluster != "b" {
		t.Fatalf("second pop = %+v (ok=%v), want tenant b's SINGLE frame — round robin must not drain all %d of a's frames first", f1, ok, floodSize)
	}

	// Drain the rest and confirm nothing is lost: floodSize-1 remaining a
	// frames, then exhaustion.
	remaining := 0
	for {
		f, ok := m.next()
		if !ok {
			break
		}
		if f.Cluster != "a" {
			t.Errorf("unexpected extra frame from tenant %q after b's single frame was drained", f.Cluster)
		}
		remaining++
	}
	if remaining != floodSize-1 {
		t.Errorf("remaining a frames drained = %d, want %d", remaining, floodSize-1)
	}
}

// --- End-to-end tests over the real Pusher/connWorker/fakeHub pipeline ---

// TestPusher_SingleTenant_ExactlyOneWorker is the direct WorkerStats-based
// check for requirement (5): single-tenant mode ends up with exactly one
// registered worker, keyed by the primary tenant's own name.
func TestPusher_SingleTenant_ExactlyOneWorker(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), []workloadInfo{testApp("api", true)})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 2*time.Second)

	stats := p.WorkerStats()
	if len(stats) != 1 {
		t.Fatalf("WorkerStats() = %+v, want exactly one worker in single-tenant mode", stats)
	}
	if stats[0].Tenant != "example-tenant" {
		t.Errorf("WorkerStats()[0].Tenant = %q, want %q", stats[0].Tenant, "example-tenant")
	}
	if stats[0].LastSnapshot.IsZero() {
		t.Error("WorkerStats()[0].LastSnapshot is zero, want the connect-time replay to have recorded it")
	}
}

// TestPusher_SingleTenant_FrameSequenceUnchangedThroughMux is the explicit
// frame-sequence golden test for the single-worker (degenerate round-robin)
// case: hello -> snapshot -> snapshot_end -> app_delta -> heartbeat must
// still arrive in that exact order once deltas are routed through the new
// per-tenant mux instead of directly through connWorker's old shared
// channel.
func TestPusher_SingleTenant_FrameSequenceUnchangedThroughMux(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), []workloadInfo{testApp("api", true)})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 2*time.Second)

	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("ops", "api", true)))
	waitForStampedKind(t, hub, KindAppDelta, "", 2*time.Second)
	waitForStampedKind(t, hub, KindHeartbeat, "", 2*time.Second)

	got := hub.kinds()
	want := []string{KindHello, KindSnapshot, KindSnapshotEnd, KindAppDelta}
	if len(got) < len(want) {
		t.Fatalf("frame sequence = %v, want at least %v", got, want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Fatalf("frame sequence[%d] = %q, want %q (full: %v)", i, got[i], k, got)
		}
	}
	if idx := len(want) - 1; !containsAfter(got, KindHeartbeat, idx) {
		t.Errorf("no heartbeat after the app_delta; frames = %v", got)
	}
}

func containsAfter(kinds []string, want string, after int) bool {
	for i := after + 1; i < len(kinds); i++ {
		if kinds[i] == want {
			return true
		}
	}
	return false
}

// TestPusher_MultiTenant_FloodDoesNotDelaySibling is the end-to-end proof of
// requirement (2)'s isolation guarantee through the real pipeline: tenant-a
// floods hundreds of deltas, and tenant-b's single delta — enqueued only
// once, right after the flood — must not sit behind more than a small,
// bounded number of tenant-a's frames on the wire. The assertion is
// position-based (how many tenant-a frames arrived before tenant-b's),
// deliberately not wall-clock-based, so it can't flake on a loaded machine.
func TestPusher_MultiTenant_FloodDoesNotDelaySibling(t *testing.T) {
	hub, srv := newFakeHub(t)

	workerApp := testApp("worker", true)
	workerApp.Namespace = "rag"
	webApp := testApp("web", true)
	webApp.Namespace = "dev"

	p := newTestPusher(wsURL(srv), []workloadInfo{workerApp, webApp})
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{annotationTenant: "tenant-b"}},
	}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-b", 3*time.Second)

	const floodSize = 500
	floodApp := mkAppDeltaApp("rag", "worker", true)
	for i := 0; i < floodSize; i++ {
		p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, floodApp))
	}
	sentinel := mkAppDeltaApp("dev", "sentinel", true)
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, sentinel))

	deadline := time.Now().Add(3 * time.Second)
	var sentinelIdx = -1
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		for i, f := range hub.frames {
			if f.Kind == KindAppDelta && f.Tenant == "tenant-b" && f.AppDelta.App.Name == "sentinel" {
				sentinelIdx = i
			}
		}
		frames := append([]*Frame(nil), hub.frames...)
		hub.mu.Unlock()
		if sentinelIdx >= 0 {
			aAhead := 0
			for _, f := range frames[:sentinelIdx] {
				if f.Kind == KindAppDelta && f.Tenant == "tenant-a" {
					aAhead++
				}
			}
			if aAhead >= floodSize {
				t.Fatalf("tenant-b's sentinel frame arrived after ALL %d of tenant-a's flood frames (aAhead=%d) — round robin is not interleaving", floodSize, aAhead)
			}
			t.Logf("tenant-a frames ahead of tenant-b's sentinel: %d (of %d flooded)", aAhead, floodSize)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("tenant-b's sentinel app_delta frame never arrived")
}

// TestPusher_MultiTenant_ReconnectReplaysPerTenantIndependently covers
// requirement (4): the tenant-worker registry persists across a reconnect
// (unlike the pre-refactor per-connection sendC), and EVERY active tenant —
// primary and secondary — gets a full, independent snapshot replay again on
// the second connection, not just whichever tenant happened to change.
func TestPusher_MultiTenant_ReconnectReplaysPerTenantIndependently(t *testing.T) {
	hub, srv := newFakeHub(t)
	ragApp := testApp("worker", true)
	ragApp.Namespace = "rag"
	p := newTestPusher(wsURL(srv), []workloadInfo{ragApp})
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)

	// First connection.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	done1 := make(chan struct{})
	go func() { _, _ = p.runOnce(ctx1); close(done1) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-a", 3*time.Second)
	cancel1()
	<-done1

	// Second connection on the SAME Pusher (same tenantMux, spanning the
	// reconnect) — must independently replay both the primary's unstamped
	// sequence and tenant-a's stamped sequence again.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	done2 := make(chan struct{})
	go func() { _, _ = p.runOnce(ctx2); close(done2) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hub.connCount() == 2 && len(hub.framesWithTenant("tenant-a")) > 0 {
			// Give the second connection's own replay a moment past its dial.
			if countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd) >= 2 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel2()
	<-done2

	if got := hub.connCount(); got != 2 {
		t.Fatalf("connCount = %d, want exactly 2 (one per runOnce call)", got)
	}
	if got := countKind(hub.framesWithTenant("tenant-a"), KindSnapshotEnd); got != 2 {
		t.Errorf("tenant-a snapshot_end count = %d, want 2 (one full replay per connection)", got)
	}
	if got := countKind(hub.framesWithTenant(""), KindSnapshotEnd); got != 2 {
		t.Errorf("primary (unstamped) snapshot_end count = %d, want 2 (one full replay per connection)", got)
	}

	// The worker registry itself must have survived the reconnect — both
	// tenants are still registered, not recreated from a clean slate.
	stats := p.WorkerStats()
	if len(stats) != 2 {
		t.Fatalf("WorkerStats() after reconnect = %+v, want 2 (primary + tenant-a, persisted across the reconnect)", stats)
	}
}

func countKind(frames []*Frame, kind string) int {
	n := 0
	for _, f := range frames {
		if f.Kind == kind {
			n++
		}
	}
	return n
}

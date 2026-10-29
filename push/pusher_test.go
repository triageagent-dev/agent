package push

import (
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fakeHub is a WS server that records received frames and can send a pong.
// It tolerates any number of connections (so a reconnecting worker never
// wedges the harness) and merges every connection's frames into the
// aggregate frames/kinds() fields in arrival order. Multi-tenant push runs
// over ONE connection with per-frame Tenant stamps, so multi-tenant tests
// assert on connCount() plus each frame's Tenant field (framesWithTenant/
// kindTenantPairs) rather than on per-connection streams. The hub is a core
// that speaks the join handshake (the shared test core's CA), so a Pusher from
// newTestPusher connects with the certificate it was issued; hellos holds the
// hello of every connection that authenticated.
type fakeHub struct {
	mu     sync.Mutex
	frames []*Frame
	hellos []*HelloPayload

	wsConns []*websocket.Conn // live connections, for tests that write hub->agent frames
}

// firstHello returns the hello of the first authenticated connection, or nil
// before one has arrived.
func (h *fakeHub) firstHello() *HelloPayload {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.hellos) == 0 {
		return nil
	}
	return h.hellos[0]
}

func (h *fakeHub) record(f *Frame) {
	h.mu.Lock()
	h.frames = append(h.frames, f)
	h.mu.Unlock()
}

func (h *fakeHub) kinds() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.frames))
	for i, f := range h.frames {
		out[i] = f.Kind
	}
	return out
}

// connCount reports how many connections have authenticated so far.
func (h *fakeHub) connCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.hellos)
}

// latestConn returns the most recently authenticated connection, waiting up to 2s
// for at least one — for tests that write a hub->agent frame (e.g. resync)
// directly onto the live socket, as the core would.
func (h *fakeHub) latestConn(t *testing.T) *websocket.Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		var c *websocket.Conn
		if n := len(h.wsConns); n > 0 {
			c = h.wsConns[n-1]
		}
		h.mu.Unlock()
		if c != nil {
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no connection authenticated within 2s")
	return nil
}

// sendToAgent writes f directly onto the latest connection, as the core
// would for a hub->agent frame. Callers exercising this should set a long
// Options.Heartbeat so the hub's own pong reply (on receiving a heartbeat)
// never races this write on the same connection.
func (h *fakeHub) sendToAgent(t *testing.T, f *Frame) {
	t.Helper()
	data, err := encode(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := h.latestConn(t).WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write to agent: %v", err)
	}
}

// framesWithTenant returns a copy of every received frame whose Tenant
// envelope field equals wireTenant — "" selects the primary tenant's
// (unstamped) frames.
func (h *fakeHub) framesWithTenant(wireTenant string) []*Frame {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*Frame
	for _, f := range h.frames {
		if f.Tenant == wireTenant {
			out = append(out, f)
		}
	}
	return out
}

// kindTenantPair is one received frame's (kind, tenant-stamp) projection,
// for order-sensitive assertions on the single connection's stream.
type kindTenantPair struct {
	kind, tenant string
}

// kindTenantPairs returns every received frame projected to (Kind, Tenant),
// in arrival order, keeping only the given kinds (all kinds when none are
// given).
func (h *fakeHub) kindTenantPairs(kinds ...string) []kindTenantPair {
	keep := func(k string) bool {
		if len(kinds) == 0 {
			return true
		}
		return slices.Contains(kinds, k)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []kindTenantPair
	for _, f := range h.frames {
		if keep(f.Kind) {
			out = append(out, kindTenantPair{kind: f.Kind, tenant: f.Tenant})
		}
	}
	return out
}

func newFakeHub(t *testing.T) (*fakeHub, *httptest.Server) {
	t.Helper()
	h := &fakeHub{}
	srv := sharedTestCore().serveSession(t, func(conn *websocket.Conn, hello *Frame) {
		h.mu.Lock()
		h.hellos = append(h.hellos, hello.Hello)
		h.wsConns = append(h.wsConns, conn)
		h.mu.Unlock()
		h.record(hello)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			f, err := decode(data)
			if err != nil {
				continue
			}
			h.record(f)
			if f.Kind == KindHeartbeat {
				pong, _ := encode(&Frame{V: 1, Kind: KindPong, Cluster: f.Cluster})
				_ = conn.WriteMessage(websocket.TextMessage, pong)
			}
		}
	})
	return h, srv
}

// newTestPusher builds a Pusher wired against endpoint whose snapshot/delta
// apps come from virtual-app synthesis (vapps mode "on") over the given
// workload fixtures — every generic frame-pipeline test in this file needs a
// Pusher that pushes SOME set of apps without exercising virtual-app grouping
// rules itself, and virtual apps are the only source of pushed apps now that
// the Application custom resource is gone.
func newTestPusher(endpoint string, workloads []workloadInfo) *Pusher {
	return sharedTestCore().joinedTo(newUnjoinedTestPusher(endpoint, workloads))
}

// newUnjoinedTestPusher is newTestPusher with no join identity, for the tests
// that drive the join itself.
func newUnjoinedTestPusher(endpoint string, workloads []workloadInfo) *Pusher {
	p := &Pusher{
		opts: Options{
			Endpoint:      endpoint,
			ClusterName:   "cluster-a",
			Tenant:        "example-tenant",
			Heartbeat:     150 * time.Millisecond,
			SnapshotChunk: 50,
			VirtualApps:   VirtualAppsOn,
		},
		log:          logr.Discard(),
		skipHandlers: true,
	}
	p.vapps = newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), p.enqueue)
	p.vapps.ctx = context.Background()
	for _, w := range workloads {
		info := w
		p.vapps.applyWorkload(workloadKey(info.Kind, info.Namespace, info.Name), &info, false)
	}
	return p
}

// testApp is the workload-driven equivalent of the old *appv1.Application
// mkApp fixture: a single-member group named name in namespace "ops", ready
// or not, that virtual-app synthesis turns into one vapp with that exact
// name.
func testApp(name string, ready bool) workloadInfo {
	return workloadInfo{Kind: workloadDeployment, Namespace: "ops", Name: name, Ready: ready}
}

// mkAppDeltaApp synthesizes a one-member VirtualApp fixture through the real
// synthesis path, for tests that enqueue an app_delta frame directly without
// driving the full workload/recompute pipeline.
func mkAppDeltaApp(ns, name string, ready bool) *VirtualApp {
	w := testApp(name, ready)
	w.Namespace = ns
	return synthesizeApplication(groupKey{Namespace: ns, Name: name}, []workloadInfo{w})
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestPusher_HelloSnapshotHeartbeat drives one connection and asserts the frame
// sequence the receiver expects: hello → snapshot → snapshot_end → heartbeat, with
// a certificate hello (the certificate and a proof, no shared key) that names the
// cluster and tenant.
func TestPusher_HelloSnapshotHeartbeat(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), []workloadInfo{testApp("api", true), testApp("web", false)})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	// Wait until we have at least hello, snapshot, snapshot_end, and one heartbeat.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		k := hub.kinds()
		if len(k) >= 4 && slices.Contains(k, KindHeartbeat) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()

	k := hub.kinds()
	if len(k) < 3 || k[0] != KindHello || k[1] != KindSnapshot || k[2] != KindSnapshotEnd {
		t.Fatalf("frame sequence = %v, want hello, snapshot, snapshot_end, ...", k)
	}
	if !slices.Contains(k, KindHeartbeat) {
		t.Errorf("no heartbeat sent; frames = %v", k)
	}
	hello := hub.firstHello()
	if hello == nil {
		t.Fatal("no authenticated hello")
	}
	if len(hello.Cert) == 0 || len(hello.Proof) == 0 {
		t.Errorf("hello carries cert %d bytes, proof %d bytes, want both", len(hello.Cert), len(hello.Proof))
	}
	if hello.Tenant != "example-tenant" {
		t.Errorf("hello.Tenant = %q, want %q", hello.Tenant, "example-tenant")
	}
	if hello.ClusterName != "cluster-a" {
		t.Errorf("hello.ClusterName = %q, want %q", hello.ClusterName, "cluster-a")
	}
}

// TestPusher_ReadLoopUnblocksOnCancel is the regression test for BUG 2
// (AUDIT.md): readLoop used to ignore its ctx parameter entirely, so the only
// way a blocked conn.ReadMessage() ever returned was the ~3*Heartbeat read
// deadline — meaning runOnce's own wg.Wait() (which every exit path,
// including a plain parent-ctx cancellation once writeLoop has already
// returned, goes through before runOnce can return) stalled for that whole
// deadline on an otherwise-idle connection.
//
// Heartbeat is set to 30s here (readTimeout = 90s) SPECIFICALLY so this test
// cannot pass by accident on a short default deadline: if the ctx-ignoring
// bug were reintroduced, this test would fail on the 2s timeout below rather
// than the read deadline coincidentally already being close to expiring.
func TestPusher_ReadLoopUnblocksOnCancel(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.opts.Heartbeat = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = p.runOnce(ctx)
		close(done)
	}()

	// Wait for the connection to fully establish (hello + snapshot done)
	// before cancelling, so the test exercises a genuinely open, idle
	// connection — readLoop parked in ReadMessage with nothing incoming —
	// rather than the dial/hello/snapshot phase.
	waitDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(waitDeadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()

	select {
	case <-done:
		// runOnce returned promptly — the closer goroutine unblocked readLoop.
	case <-time.After(2 * time.Second):
		t.Fatal("runOnce did not return within 2s of ctx cancel; readLoop is still blocked on the ~90s read deadline (BUG 2 regression)")
	}
}

// TestPusher_ReadLimitClosesOnOversizedFrame verifies the read limit set on
// the outbound push connection: a single inbound frame larger than
// maxInboundReadBytes must end the connection promptly rather than being
// buffered without bound.
func TestPusher_ReadLimitClosesOnOversizedFrame(t *testing.T) {
	// The hello has already been read when the session starts, so the write
	// below isn't racing the client's own snapshot/heartbeat traffic.
	srv := sharedTestCore().serveSession(t, func(conn *websocket.Conn, _ *Frame) {
		_ = conn.WriteMessage(websocket.BinaryMessage, make([]byte, maxInboundReadBytes+1))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	p := newTestPusher(wsURL(srv), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = p.runOnce(ctx)
		close(done)
	}()

	select {
	case <-done:
		// runOnce returned — the oversized frame ended the connection.
	case <-time.After(2 * time.Second):
		t.Fatal("connection still open 2s after an oversized inbound frame — the read limit is not enforced")
	}
}

// TestPusher_SendSnapshot_IncludesPreexistingVirtualApp is the end-to-end
// regression test for the production bug observed on cluster-a: a
// virtual app synthesized from a workload that was already steady when the
// process started (no further informer events ever arrive for it, so nothing
// re-triggers synthesis later) was never announced to the hub, because
// sendSnapshot ran before virtualAppsState's 2s applyWorkload debounce had a
// chance to populate v.current — the very first (and, for a steady workload,
// only) snapshot went out as "apps=0".
//
// This drives the real Pusher.runOnce -> sendSnapshot path over a fake WS hub,
// mirroring registerInformers' synthetic ADD for a pre-existing Deployment,
// and asserts the snapshot the hub actually receives carries the app.
func TestPusher_SendSnapshot_IncludesPreexistingVirtualApp(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	// Simulate registerInformers' synthetic ADD callback for a workload that
	// was already Ready when the process started (the cluster-a observation:
	// agentregistry-inventory-controller).
	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "agentregistry-inventory-controller", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	p.vapps.applyWorkload(workloadKey(workloadDeployment, "ops", "agentregistry-inventory-controller"), &info, false)

	// runOnce dials and calls sendSnapshot immediately — well before
	// virtualAppsDebounce (2s) would fire on its own.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	hub.mu.Lock()
	defer hub.mu.Unlock()
	var gotApps []*VirtualApp
	sawSnapshot := false
	for _, f := range hub.frames {
		if f.Kind == KindSnapshot {
			sawSnapshot = true
			gotApps = append(gotApps, f.Snapshot.Apps...)
		}
	}
	if !sawSnapshot {
		t.Fatalf("no snapshot frame received; frames = %v", hub.frames)
	}
	if len(gotApps) != 1 || gotApps[0].Name != "agentregistry-inventory-controller" {
		t.Fatalf("snapshot apps = %+v, want exactly one synthesized virtual app named agentregistry-inventory-controller (sendSnapshot must recomputeNow before reading the virtual-app set)", gotApps)
	}
}

// TestPusher_DeltaEnqueued verifies onApp enqueues an app_delta that gets sent.
func TestPusher_DeltaEnqueued(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	// Wait for the connection to establish (snapshot_end seen), then enqueue a delta.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("ops", "api", false)))

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindAppDelta) {
			cancel()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("app_delta not received; frames = %v", hub.kinds())
}

// TestPusher_SendLogMetrics verifies SendLogMetrics enqueues a log_metrics frame that
// reaches the hub with its payload intact, and that it is chunked at
// MaxLogMetricsServices services per frame.
func TestPusher_SendLogMetrics(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	services := make([]ServiceLogMetrics, 150) // > MaxLogMetricsServices, must split
	for i := range services {
		services[i] = ServiceLogMetrics{Namespace: "ops", Service: "svc", ErrorCount: int64(i)}
	}
	p.SendLogMetrics(1720000000, 60, services)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		hub.mu.Lock()
		for _, f := range hub.frames {
			if f.Kind == KindLogMetrics {
				count++
			}
		}
		hub.mu.Unlock()
		if count == 2 {
			cancel()
			hub.mu.Lock()
			defer hub.mu.Unlock()
			var total int
			for _, f := range hub.frames {
				if f.Kind == KindLogMetrics {
					if f.LogMetrics.WindowStart != 1720000000 || f.LogMetrics.WindowSec != 60 {
						t.Errorf("window = (%d, %d), want (1720000000, 60)", f.LogMetrics.WindowStart, f.LogMetrics.WindowSec)
					}
					total += len(f.LogMetrics.Services)
				}
			}
			if total != 150 {
				t.Errorf("total services across log_metrics frames = %d, want 150", total)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected 2 log_metrics frames (150 services split at 100); frames = %v", hub.kinds())
}

// TestPusher_SendLogMetrics_DisconnectedIsNoop verifies that calling SendLogMetrics
// before a connection is established (no send channel yet) does not panic and simply
// drops — the counters are cumulative, so the next successful tick recovers them.
func TestPusher_SendLogMetrics_DisconnectedIsNoop(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil) // never connected: runOnce not called
	p.SendLogMetrics(1720000000, 60, []ServiceLogMetrics{{Namespace: "ops", Service: "api", ErrorCount: 99}})
	// No assertion beyond "did not panic" — sendC is nil, so this exercises the guard.
}

// TestPusher_SendLogMetrics_NilPusherIsNoop verifies a nil *Pusher (push mode
// disabled entirely) tolerates SendLogMetrics — main.go wires the collector
// unconditionally and relies on this rather than an extra nil check at the call site.
func TestPusher_SendLogMetrics_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	p.SendLogMetrics(1720000000, 60, []ServiceLogMetrics{{Namespace: "ops", Service: "api", ErrorCount: 99}})
}

// TestPusher_SendRedMetrics verifies SendRedMetrics enqueues a single red_metrics
// frame that reaches the hub with its payload intact. Unlike SendLogMetrics, this
// never chunks (redsource.Collector already caps at MaxRedMetricsServices before
// calling it).
func TestPusher_SendRedMetrics(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	services := []ServiceRedMetrics{
		{Namespace: "ops", Service: "api", RequestCount: 1000, ErrorCount: 42, Sample: "GET /api/orders -> 502 x41"},
	}
	p.SendRedMetrics(1720000000, 60, services)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		var found *RedMetricsPayload
		for _, f := range hub.frames {
			if f.Kind == KindRedMetrics {
				found = f.RedMetrics
			}
		}
		hub.mu.Unlock()
		if found != nil {
			cancel()
			if found.WindowStart != 1720000000 || found.WindowSec != 60 {
				t.Errorf("window = (%d, %d), want (1720000000, 60)", found.WindowStart, found.WindowSec)
			}
			if len(found.Services) != 1 || found.Services[0].Service != "api" || found.Services[0].ErrorCount != 42 {
				t.Errorf("services = %+v, want one api/42 entry", found.Services)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected a red_metrics frame; frames = %v", hub.kinds())
}

// TestPusher_SendRedMetrics_DisconnectedIsNoop mirrors
// TestPusher_SendLogMetrics_DisconnectedIsNoop for the red_metrics path.
func TestPusher_SendRedMetrics_DisconnectedIsNoop(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil)
	p.SendRedMetrics(1720000000, 60, []ServiceRedMetrics{{Namespace: "ops", Service: "api", ErrorCount: 99}})
}

// TestPusher_SendRedMetrics_NilPusherIsNoop mirrors
// TestPusher_SendLogMetrics_NilPusherIsNoop for the red_metrics path.
func TestPusher_SendRedMetrics_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	p.SendRedMetrics(1720000000, 60, []ServiceRedMetrics{{Namespace: "ops", Service: "api", ErrorCount: 99}})
}

// TestPusher_SendRedSummary verifies SendRedSummary enqueues a single red_summary
// frame that reaches the hub with its payload intact — same never-chunks posture
// as SendRedMetrics (redsource.Collector already caps at MaxRedMetricsServices
// before calling it).
func TestPusher_SendRedSummary(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	services := []ServiceRedMetrics{
		{Namespace: "ops", Service: "api", RequestCount: 1000, ErrorCount: 0, P99Ms: 42.5},
	}
	p.SendRedSummary(1720000000, 60, services)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		var found *RedSummaryPayload
		for _, f := range hub.frames {
			if f.Kind == KindRedSummary {
				found = f.RedSummary
			}
		}
		hub.mu.Unlock()
		if found != nil {
			cancel()
			if found.WindowStart != 1720000000 || found.WindowSec != 60 {
				t.Errorf("window = (%d, %d), want (1720000000, 60)", found.WindowStart, found.WindowSec)
			}
			if len(found.Services) != 1 || found.Services[0].Service != "api" || found.Services[0].RequestCount != 1000 {
				t.Errorf("services = %+v, want one api/requestCount=1000 entry", found.Services)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected a red_summary frame; frames = %v", hub.kinds())
}

// TestPusher_SendRedSummary_DisconnectedIsNoop mirrors
// TestPusher_SendRedMetrics_DisconnectedIsNoop for the red_summary path.
func TestPusher_SendRedSummary_DisconnectedIsNoop(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil)
	p.SendRedSummary(1720000000, 60, []ServiceRedMetrics{{Namespace: "ops", Service: "api", RequestCount: 99}})
}

// TestPusher_SendRedSummary_NilPusherIsNoop mirrors
// TestPusher_SendRedMetrics_NilPusherIsNoop for the red_summary path.
func TestPusher_SendRedSummary_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	p.SendRedSummary(1720000000, 60, []ServiceRedMetrics{{Namespace: "ops", Service: "api", RequestCount: 99}})
}

// TestPusher_SendUseSummary mirrors TestPusher_SendRedSummary for the
// use_summary path: verifies SendUseSummary enqueues a single use_summary
// frame that reaches the hub with its payload intact.
func TestPusher_SendUseSummary(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	services := []ServiceUseSummary{
		{Namespace: "ops", Service: "api", CPUPsiPct: 12.5, MemPsiPct: 34, IOPsiPct: 5, OOMKills: 1},
	}
	p.SendUseSummary(1720000000, 60, services)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		var found *UseSummaryPayload
		for _, f := range hub.frames {
			if f.Kind == KindUseSummary {
				found = f.UseSummary
			}
		}
		hub.mu.Unlock()
		if found != nil {
			cancel()
			if found.WindowStart != 1720000000 || found.WindowSec != 60 {
				t.Errorf("window = (%d, %d), want (1720000000, 60)", found.WindowStart, found.WindowSec)
			}
			if len(found.Services) != 1 || found.Services[0].Service != "api" || found.Services[0].MemPsiPct != 34 {
				t.Errorf("services = %+v, want one api/memPsiPct=34 entry", found.Services)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected a use_summary frame; frames = %v", hub.kinds())
}

// TestPusher_SendUseSummary_DisconnectedIsNoop mirrors
// TestPusher_SendRedSummary_DisconnectedIsNoop for the use_summary path.
func TestPusher_SendUseSummary_DisconnectedIsNoop(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil)
	p.SendUseSummary(1720000000, 60, []ServiceUseSummary{{Namespace: "ops", Service: "api", MemPsiPct: 99}})
}

// TestPusher_SendUseSummary_NilPusherIsNoop mirrors
// TestPusher_SendRedSummary_NilPusherIsNoop for the use_summary path.
func TestPusher_SendUseSummary_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	p.SendUseSummary(1720000000, 60, []ServiceUseSummary{{Namespace: "ops", Service: "api", MemPsiPct: 99}})
}

// TestPusher_SendSensorState_NilPusherIsNoop mirrors
// TestPusher_SendUseSummary_NilPusherIsNoop for the sensor_state path
// (ADR-0015 phase 1).
func TestPusher_SendSensorState_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	p.SendSensorState(1720000000, []SensorStatus{{Node: "node-1", Connected: true}}, nil, nil)
}

// TestPusher_SendSensorState_EmptyIsNoop mirrors the other Send* methods:
// nothing to report is a no-op, not an empty frame stored for the wire.
func TestPusher_SendSensorState_EmptyIsNoop(t *testing.T) {
	p := newRoutingTestPusher()
	p.SendSensorState(1720000000, nil, nil, nil)
	if f := p.sensorState.Load(); f != nil {
		t.Fatalf("sensorState = %+v, want nil (empty call is a no-op)", f)
	}
}

// The intake names senders, not tenants, and is worth sending alone: a sender
// whose every export is refused has no source, yet is what the operator
// needs to see.
func TestPusher_SendSensorState_IntakeAloneIsSent(t *testing.T) {
	p := newRoutingTestPusher()
	p.SendSensorState(1720000000, nil, nil, []OTLPIntakeStatus{{Sender: "obs/collector", Window: 60, RefusedExports: 3}})
	f := p.sensorState.Load()
	if f == nil || len(f.SensorState.Intake) != 1 || f.SensorState.Intake[0].RefusedExports != 3 {
		t.Fatalf("sensorState = %+v, want the intake stored", f)
	}
}

// Each OTLP source carries its namespace's tenant; one without a namespace
// (kubelet cAdvisor) or with an unmapped one stays unstamped in cluster mode.
func TestPusher_SendSensorState_StampsOTLPTenant(t *testing.T) {
	p := newRoutingTestPusher()
	p.opts.Tenant = "" // cluster mode
	p.dynamicScope.add("shop", "tenant-a")
	p.dynamicScope.add("ops", "tenant-b")
	in := []OTLPSourceStatus{{Key: "a", Namespace: "shop"}, {Key: "b", Namespace: "ops"}, {Key: "c"}, {Key: "d", Namespace: "loose"}}
	p.SendSensorState(1720000000, nil, in, nil)
	got := p.sensorState.Load().SensorState.OTLP
	want := []string{"tenant-a", "tenant-b", "", ""}
	for i, o := range got {
		if o.Tenant != want[i] {
			t.Errorf("%s tenant = %q, want %q", o.Key, o.Tenant, want[i])
		}
	}
	if in[0].Tenant != "" {
		t.Fatal("the caller's slice was modified")
	}
}

// TestPusher_SendSensorState_StoresLatest verifies SendSensorState is now a
// session-level store (ADR-0015 phase 1, revised): the frame is held on the
// Pusher for connworker.go's writeLoop to write unstamped on the next
// heartbeat tick, rather than enqueued onto any per-tenant queue.
func TestPusher_SendSensorState_StoresLatest(t *testing.T) {
	p := newRoutingTestPusher()
	p.SendSensorState(1720000000, []SensorStatus{{Node: "node-1", Connected: true}}, nil, nil)
	f := p.sensorState.Load()
	if f == nil || f.SensorState == nil || len(f.SensorState.Sensors) != 1 {
		t.Fatalf("sensorState = %+v, want the sensor stored", f)
	}
}

// TestPusher_SendSensorState_ReachesWireUnstampedOnHeartbeatTick verifies the
// session-level rewrite (ADR-0015 phase 1, revised): the frame SendSensorState
// stores reaches the wire unstamped, on connworker.go's heartbeat tick,
// exactly once — the swap-to-nil in writeLoop must not resend it every tick.
func TestPusher_SendSensorState_ReachesWireUnstampedOnHeartbeatTick(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.SendSensorState(1720000000, []SensorStatus{{Node: "node-1", Connected: true}}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	waitForStampedKind(t, hub, KindSensorState, "", 2*time.Second)
	// newTestPusher's heartbeat interval is 150ms; give several more ticks a
	// chance to fire before checking the frame was not resent.
	time.Sleep(400 * time.Millisecond)
	cancel()

	var frame *Frame
	count := 0
	for _, f := range hub.framesWithTenant("") {
		if f.Kind == KindSensorState {
			count++
			frame = f
		}
	}
	if count != 1 {
		t.Fatalf("sensor_state frames on the wire = %d, want exactly 1 (no per-tick resend)", count)
	}
	if frame.SensorState == nil || len(frame.SensorState.Sensors) != 1 {
		t.Fatalf("sensor_state frame = %+v, want the sensor carried through", frame)
	}
}

// TestPusher_HelloAdvertisesRedMetricsCapability verifies the hello frame carries
// "red_metrics" in Capabilities only when Options.RedMetrics is true.
func TestPusher_HelloAdvertisesRedMetricsCapability(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.opts.RedMetrics = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		for _, f := range hub.frames {
			if f.Kind == KindHello {
				cancel()
				hub.mu.Unlock()
				if !slices.Contains(f.Hello.Capabilities, "red_metrics") {
					t.Errorf("hello capabilities = %v, want to contain %q", f.Hello.Capabilities, "red_metrics")
				}
				return
			}
		}
		hub.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("hello frame never received")
}

// TestPusher_HelloAdvertisesSensorStreamCapability verifies the hello frame
// carries "sensor_state" in Capabilities only when Options.SensorStream is
// true (ADR-0015 phase 1) — same metadata-only pattern as red_metrics above.
func TestPusher_HelloAdvertisesSensorStreamCapability(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.opts.SensorStream = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		for _, f := range hub.frames {
			if f.Kind == KindHello {
				cancel()
				hub.mu.Unlock()
				if !slices.Contains(f.Hello.Capabilities, "sensor_state") {
					t.Errorf("hello capabilities = %v, want to contain %q", f.Hello.Capabilities, "sensor_state")
				}
				return
			}
		}
		hub.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("hello frame never received")
}

func TestPusher_SendTopologySnapshot(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	nodes := []TopologyNode{{Namespace: "rag", Workload: "app"}, {Namespace: "rag", Workload: "qdrant"}}
	edges := []TopologyEdge{{SrcNamespace: "rag", SrcWorkload: "app", DstNamespace: "rag", DstWorkload: "qdrant", Calls: 7, Protocol: "grpc"}}
	if !p.SendTopologySnapshot(1720000000, 600, nodes, edges) {
		t.Fatal("SendTopologySnapshot on a connected pusher = false, want true (enqueued)")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		var found *TopologySnapshotPayload
		for _, f := range hub.frames {
			if f.Kind == KindTopologySnapshot {
				found = f.TopologySnapshot
			}
		}
		hub.mu.Unlock()
		if found != nil {
			cancel()
			if found.CapturedAt != 1720000000 || found.WindowSec != 600 {
				t.Errorf("window = (%d, %d), want (1720000000, 600)", found.CapturedAt, found.WindowSec)
			}
			if len(found.Nodes) != 2 || len(found.Edges) != 1 || found.Edges[0].Calls != 7 {
				t.Errorf("nodes/edges = %+v/%+v, want 2 nodes, one edge with Calls=7", found.Nodes, found.Edges)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected a topology_snapshot frame; frames = %v", hub.kinds())
}

// TestPusher_SendTopologySnapshot_EmptyIsStillSent verifies the deliberate
// deviation from SendLogMetrics/SendRedMetrics/SendUseMetrics: an empty
// nodes+edges result is still enqueued, because a topology_snapshot frame is
// full level state (docs/adr/0011-topology-snapshot.md) — "nothing observed
// this window" is itself meaningful, unlike a gate that never tripped.
func TestPusher_SendTopologySnapshot_EmptyIsStillSent(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !p.SendTopologySnapshot(1720000000, 600, nil, nil) {
		t.Fatal("SendTopologySnapshot on a connected pusher = false, want true (enqueued)")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindTopologySnapshot) {
			cancel()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatalf("expected an empty topology_snapshot frame to still be sent; frames = %v", hub.kinds())
}

// TestPusher_SendTopologySnapshot_DisconnectedIsNoop mirrors
// TestPusher_SendRedMetrics_DisconnectedIsNoop for the topology_snapshot path,
// and additionally asserts the bool return: a disconnected Pusher must report
// the frame as NOT enqueued so redsource.Collector.flushTopology knows to
// retain its accumulators rather than clear them.
func TestPusher_SendTopologySnapshot_DisconnectedIsNoop(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil)
	if got := p.SendTopologySnapshot(1720000000, 600, []TopologyNode{{Namespace: "ops", Workload: "api"}}, nil); got {
		t.Error("SendTopologySnapshot on a disconnected pusher = true, want false")
	}
}

// TestPusher_SendTopologySnapshot_NilPusherIsNoop mirrors
// TestPusher_SendRedMetrics_NilPusherIsNoop for the topology_snapshot path,
// and additionally asserts a nil *Pusher reports the frame as NOT enqueued.
func TestPusher_SendTopologySnapshot_NilPusherIsNoop(t *testing.T) {
	var p *Pusher
	if got := p.SendTopologySnapshot(1720000000, 600, nil, nil); got {
		t.Error("SendTopologySnapshot on a nil pusher = true, want false")
	}
}

// TestPusher_HelloAdvertisesTopologyCapability verifies the hello frame carries
// "topology" in Capabilities only when Options.Topology is true.
func TestPusher_HelloAdvertisesTopologyCapability(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.opts.Topology = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		for _, f := range hub.frames {
			if f.Kind == KindHello {
				cancel()
				hub.mu.Unlock()
				if !slices.Contains(f.Hello.Capabilities, "topology") {
					t.Errorf("hello capabilities = %v, want to contain %q", f.Hello.Capabilities, "topology")
				}
				return
			}
		}
		hub.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("hello frame never received")
}

func TestValidateEndpoint(t *testing.T) {
	tests := []struct {
		endpoint       string
		allowPlaintext bool
		wantErr        bool
	}{
		{"wss://triage/v1/cluster-agent/ws", false, false}, // secure → ok
		{"wss://triage/v1/cluster-agent/ws", true, false},  // secure, plaintext allowed → ok
		{"ws://triage/v1/cluster-agent/ws", false, true},   // plaintext, not opted in → reject
		{"ws://triage/v1/cluster-agent/ws", true, false},   // plaintext, explicitly allowed → ok
		{"http://triage/foo", false, true},                 // wrong scheme → reject
		{"https://triage/foo", false, true},                // wrong scheme → reject
		{"triage:8080", false, true},                       // no ws scheme → reject
		{"://bad", false, true},                            // unparseable → reject
	}
	for _, tt := range tests {
		err := ValidateEndpoint(tt.endpoint, tt.allowPlaintext)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateEndpoint(%q, allowPlaintext=%v) err=%v, wantErr=%v", tt.endpoint, tt.allowPlaintext, err, tt.wantErr)
		}
	}
}

// TestPusher_OnNamespace_Deleted removes the namespace's defaults from the
// store so a later CR/workload no longer inherits from it.
func TestPusher_OnNamespace_Deleted(t *testing.T) {
	p := newTestPusher("ws://unused.invalid", nil)
	p.nsDefaults = newNsDefaultsStore()
	p.nsDefaults.set("ops", namespaceDefaults{Domain: "commerce"})

	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ops"}}, true)

	if _, ok := p.nsDefaults.get("ops"); ok {
		t.Error("namespace deletion must remove its stored defaults")
	}
}

// --- Namespace-scope-by-annotation (docs/todo/virtual-apps.md
// namespace-scope-by-annotation addendum, 2026-08-25): Pusher.onNamespace's
// dynamicScope maintenance, exercised end to end against a wired
// virtualAppsState (a bare Pusher struct + a manually-wired virtualAppsState,
// same convention as the rest of this file). ---

// newDynamicScopeTestPusher builds a Pusher scoped to the static list "ops"
// (non-empty and deliberately excluding "rag" — the dynamic scope is what
// must pull "rag" in) with a *virtualAppsState wired the way Pusher.New
// wires one, and a cachedDeployment already "resident in the informer
// cache" for "rag" via an overridden listWorkloadsFn — simulating a
// workload that existed before "rag" ever entered scope, so no informer
// event will ever fire for it again.
func newDynamicScopeTestPusher(t *testing.T) (p *Pusher, frames *[]*Frame, mu *sync.Mutex) {
	t.Helper()
	mu = &sync.Mutex{}
	frames = &[]*Frame{}
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		*frames = append(*frames, f)
	}
	vapps := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), enqueue)
	vapps.ctx = context.Background()
	cachedDeployment := toUnstructuredObj(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	vapps.listWorkloadsFn = func(_ context.Context, ns string) ([]rawWorkload, error) {
		if ns == "rag" {
			return []rawWorkload{{Obj: cachedDeployment, Kind: "Deployment"}}, nil
		}
		return nil, nil
	}

	p = &Pusher{
		opts:         Options{ClusterName: "cluster-a", Tenant: "tenant-a", Namespaces: []string{"ops"}},
		log:          logr.Discard(),
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
		vapps:        vapps,
		skipHandlers: true,
	}
	vapps.dynamicScope = p.dynamicScope
	return p, frames, mu
}

// TestPusher_OnNamespace_DynamicScope_AddBackfillsCachedWorkloads is the
// crux of the feature: a namespace annotated with a matching triageagent.dev/tenant
// AFTER a workload has gone steady in it must still pull that workload into
// vapp synthesis immediately — not just for the workload's next informer
// event, which may never come.
func TestPusher_OnNamespace_DynamicScope_AddBackfillsCachedWorkloads(t *testing.T) {
	p, frames, mu := newDynamicScopeTestPusher(t)

	if p.inScope("rag") {
		t.Fatal("rag must start out of scope: it's neither in the static list nor annotated yet")
	}

	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)

	if !p.inScope("rag") {
		t.Fatalf("rag must be in scope after a matching %s annotation", annotationTenant)
	}

	apps := p.vapps.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "worker" {
		t.Fatalf("snapshotApps = %+v, want one app named worker (cached steady workload must be backfilled)", apps)
	}

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, f := range *frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpAdd && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	if !found {
		t.Errorf("no OpAdd app_delta frame for worker; frames = %+v", *frames)
	}
}

// TestPusher_OnNamespace_DynamicScope_DifferentTenantJoinsUnderItsOwnTenant
// replaces the old equals-primary-only admission test: the multi-tenant-push
// generalization widens the admission predicate from "annotation equals this
// agent's own Options.Tenant" to "any non-empty annotation value" — a
// namespace annotated for a tenant other than primary now DOES enter scope,
// under that tenant, and still gets backfilled (backfill is a scope
// question, not a stamping one — see namespace_scope.go's addDynamicScope).
// The tenant also becomes an active secondary tenant (whose stamped
// snapshot sequence every reconnect sends), though this test's harness (a
// locally-wired virtualAppsState.enqueue, not Pusher.enqueue) does not
// exercise frame STAMPING for it — that is covered end to end by the
// TestPusher_MultiTenant_* tests below.
func TestPusher_OnNamespace_DynamicScope_DifferentTenantJoinsUnderItsOwnTenant(t *testing.T) {
	p, frames, mu := newDynamicScopeTestPusher(t)

	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "other-tenant"}},
	}, false)

	if !p.inScope("rag") {
		t.Fatal("rag annotated for ANY tenant must now enter scope (admission predicate widened from equals-primary to any-tenant)")
	}
	if got, ok := p.dynamicScope.tenantFor("rag"); !ok || got != "other-tenant" {
		t.Errorf("dynamicScope.tenantFor(rag) = (%q,%v), want (other-tenant,true)", got, ok)
	}

	apps := p.vapps.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "worker" {
		t.Fatalf("snapshotApps = %+v, want one app named worker (backfill is tenant-agnostic)", apps)
	}

	if got := p.activeSecondaryTenants(); !slices.Contains(got, "other-tenant") {
		t.Errorf("activeSecondaryTenants() = %v, want to contain other-tenant", got)
	}

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, f := range *frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpAdd && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	if !found {
		t.Errorf("no OpAdd app_delta frame for worker; frames = %+v", *frames)
	}
}

// TestPusher_OnNamespace_DynamicScope_RemoveEvictsWorkloads proves that once
// a namespace's triageagent.dev/tenant annotation is removed (or changed to another
// tenant), its previously-backfilled workloads actually leave the
// synthesized set — they don't just stop being scoped at ingress; they are
// evicted from the flat workload set and the group is deleted.
func TestPusher_OnNamespace_DynamicScope_RemoveEvictsWorkloads(t *testing.T) {
	p, frames, mu := newDynamicScopeTestPusher(t)

	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	if len(p.vapps.snapshotApps()) != 1 {
		t.Fatal("setup: worker must be synthesized after entering scope")
	}
	mu.Lock()
	*frames = nil // discard the setup OpAdd
	mu.Unlock()

	// Annotation removed entirely.
	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "rag"}}, false)

	if p.inScope("rag") {
		t.Errorf("rag must leave scope once its %s annotation is gone", annotationTenant)
	}
	if len(p.vapps.snapshotApps()) != 0 {
		t.Error("worker's group must be gone: its namespace left dynamic scope")
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, f := range *frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	if !found {
		t.Errorf("no OpDelete app_delta frame for worker; frames = %+v", *frames)
	}
}

// TestPusher_OnNamespace_DynamicScope_EmptyStaticAlsoEvictsOnRemoval is the
// scope-union rule's other half: an empty static --target-namespaces list
// means the triageagent.dev/tenant-annotated set IS the whole scope, not watch-all —
// so a namespace whose annotation was its only path into scope must be
// evicted exactly like TestPusher_OnNamespace_DynamicScope_RemoveEvictsWorkloads
// above, whether or not the static list happens to be empty.
func TestPusher_OnNamespace_DynamicScope_EmptyStaticAlsoEvictsOnRemoval(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}
	vapps := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil /* empty static = watch all */, logr.Discard(), enqueue)
	vapps.ctx = context.Background()

	p := &Pusher{
		opts:         Options{ClusterName: "cluster-a", Tenant: "tenant-a"},
		log:          logr.Discard(),
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
		vapps:        vapps,
		skipHandlers: true,
	}
	vapps.dynamicScope = p.dynamicScope

	// A workload placed directly, independent of any annotation — the same
	// setup shape TestPusher_OnNamespace_DynamicScope_RemoveEvictsWorkloads
	// above uses.
	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	vapps.applyWorkload(workloadKey(workloadDeployment, "rag", "worker"), &info, false)
	vapps.recomputeNow(context.Background())
	if len(vapps.snapshotApps()) != 1 {
		t.Fatal("setup: worker must be synthesized")
	}
	mu.Lock()
	frames = nil // discard the setup OpAdd
	mu.Unlock()

	// Annotate rag for this tenant, then remove the annotation entirely —
	// with no --target-namespaces list, that annotation was rag's ONLY path
	// into scope, so losing it must evict everything backfilled under it.
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "rag"}}, false)

	if p.inScope("rag") {
		t.Errorf("rag must leave scope once its %s annotation is gone — the empty static list gives it no other path in", annotationTenant)
	}
	if apps := vapps.snapshotApps(); len(apps) != 0 {
		t.Fatalf("worker's group must be gone: rag left scope entirely; got %+v", apps)
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, f := range frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	if !found {
		t.Errorf("no OpDelete app_delta frame for worker; frames = %+v", frames)
	}
}

// --- Multi-tenant push (docs/todo/virtual-apps.md multi-tenant-push
// addendum, 2026-08-25): ONE outbound connection per cluster, per-frame
// Tenant stamping (tenant_routing.go). ---

// TestPusher_SingleTenant_ExactlyOneConnection is the "byte-identical"
// requirement's direct assertion: a deployment with no annotated namespaces
// at all opens exactly one connection, same as before this feature existed.
func TestPusher_SingleTenant_ExactlyOneConnection(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), []workloadInfo{testApp("api", true)})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(hub.kinds(), KindSnapshotEnd) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	time.Sleep(50 * time.Millisecond) // let the connection settle before counting
	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1 for a single-tenant deployment with no annotations", got)
	}
}

// TestPusher_MultiTenant_TwoActiveTenants_OneConnection is the core test of
// the one-socket model (requirements a + c): two namespaces annotated to
// two different (secondary) tenants share the SINGLE connection — one
// handshake, declaring the primary tenant — and the connect-time stream is
// the primary tenant's UNSTAMPED snapshot sequence followed by one stamped
// sequence per active tenant (sorted order), each snapshot_end stamped
// exactly like its chunks, each sequence carrying only its own tenant's
// apps.
func TestPusher_MultiTenant_TwoActiveTenants_OneConnection(t *testing.T) {
	hub, srv := newFakeHub(t)

	apiApp := testApp("api", true) // ns "ops" — never annotated, stays primary
	workerApp := testApp("worker", true)
	workerApp.Namespace = "rag"
	webApp := testApp("web", true)
	webApp.Namespace = "dev"

	p := newTestPusher(wsURL(srv), []workloadInfo{apiApp, workerApp, webApp})
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()

	// Both namespaces annotated BEFORE the connection dials, so the connect
	// path sends every tenant's sequence.
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{annotationTenant: "tenant-b"}},
	}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	// tenant-b sorts last, so its snapshot_end means every sequence arrived.
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-b", 3*time.Second)

	// (a) exactly ONE connection with two active secondary tenants, and the
	// handshake still declares the primary tenant.
	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1 (multi-tenant rides one socket)", got)
	}
	if h := hub.firstHello(); h == nil || h.Tenant != "example-tenant" {
		t.Errorf("hello = %+v, want the primary tenant example-tenant (handshake unchanged)", h)
	}

	// (c) the connect-time (kind, tenant-stamp) sequence.
	want := []kindTenantPair{
		{KindHello, ""},
		{KindSnapshot, ""}, {KindSnapshotEnd, ""},
		{KindSnapshot, "tenant-a"}, {KindSnapshotEnd, "tenant-a"},
		{KindSnapshot, "tenant-b"}, {KindSnapshotEnd, "tenant-b"},
	}
	got := hub.kindTenantPairs(KindHello, KindSnapshot, KindSnapshotEnd)
	if len(got) < len(want) {
		t.Fatalf("kind/tenant sequence = %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("kind/tenant sequence[%d] = %v, want %v (full: %v)", i, got[i], w, got)
		}
	}

	// Each tenant's snapshot carries only its own namespaces' apps.
	checkApps := func(wireTenant, wantApp string) {
		t.Helper()
		var names []string
		for _, f := range hub.framesWithTenant(wireTenant) {
			if f.Kind == KindSnapshot {
				for _, a := range f.Snapshot.Apps {
					names = append(names, a.Name)
				}
			}
		}
		if len(names) != 1 || names[0] != wantApp {
			t.Errorf("tenant stamp %q snapshot apps = %v, want [%s]", wireTenant, names, wantApp)
		}
	}
	checkApps("", "api")
	checkApps("tenant-a", "worker")
	checkApps("tenant-b", "web")
}

// waitForStampedKind polls until a frame of kind carrying the given Tenant
// stamp ("" = the primary tenant's unstamped frames) is seen, or fails the
// test after timeout.
func waitForStampedKind(t *testing.T, hub *fakeHub, kind, wireTenant string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, f := range hub.framesWithTenant(wireTenant) {
			if f.Kind == kind {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %s frame with tenant stamp %q received within %s; frames = %v", kind, wireTenant, timeout, hub.kinds())
}

// TestPusher_MultiTenant_LogMetricsSplitStamped is requirement (e): one
// SendLogMetrics call carrying services from three tenants' namespaces
// (two annotated + one primary) must arrive as three separate log_metrics
// frames on the SAME connection — one per tenant, each carrying only its
// own tenant's service, the annotated ones stamped and the primary's
// unstamped.
func TestPusher_MultiTenant_LogMetricsSplitStamped(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 3*time.Second)

	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ops", Annotations: map[string]string{annotationTenant: "tenant-a"}}}, false)
	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{annotationTenant: "tenant-b"}}}, false)

	p.SendLogMetrics(1720000000, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 5},
		{Namespace: "dev", Service: "web", ErrorCount: 9},
		{Namespace: "plain", Service: "job", ErrorCount: 1},
	})

	checkOne := func(wireTenant, wantNS, wantSvc string, wantErrs int64) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			for _, f := range hub.framesWithTenant(wireTenant) {
				if f.Kind == KindLogMetrics {
					if len(f.LogMetrics.Services) != 1 {
						t.Errorf("tenant stamp %q log_metrics services = %+v, want exactly 1 (the frame must SPLIT, not duplicate)", wireTenant, f.LogMetrics.Services)
						return
					}
					got := f.LogMetrics.Services[0]
					if got.Namespace != wantNS || got.Service != wantSvc || got.ErrorCount != wantErrs {
						t.Errorf("tenant stamp %q log_metrics service = %+v, want {%s %s %d}", wireTenant, got, wantNS, wantSvc, wantErrs)
					}
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Errorf("tenant stamp %q: no log_metrics frame received", wireTenant)
	}
	checkOne("tenant-a", "ops", "api", 5)
	checkOne("tenant-b", "dev", "web", 9)
	checkOne("", "plain", "job", 1)

	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1 (all three splits ride one socket)", got)
	}
}

// TestPusher_MultiTenant_AnnotationWhileConnected_StampedSnapshotOnLiveSocket
// is requirement (d): a namespace annotated to a NEW tenant while the
// connection is up must produce that tenant's full stamped snapshot
// sequence on the live socket immediately (sendTenantSnapshotOnLive) — no
// reconnect, no second connection.
func TestPusher_MultiTenant_AnnotationWhileConnected_StampedSnapshotOnLiveSocket(t *testing.T) {
	hub, srv := newFakeHub(t)
	ragApp := testApp("worker", true)
	ragApp.Namespace = "rag"
	p := newTestPusher(wsURL(srv), []workloadInfo{ragApp})
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 3*time.Second)

	// Annotate while connected.
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)

	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-a", 3*time.Second)

	var names []string
	for _, f := range hub.framesWithTenant("tenant-a") {
		if f.Kind == KindSnapshot {
			for _, a := range f.Snapshot.Apps {
				names = append(names, a.Name)
			}
		}
	}
	if len(names) != 1 || names[0] != "worker" {
		t.Errorf("tenant-a live snapshot apps = %v, want [worker]", names)
	}
	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1 (the stamped snapshot rides the existing socket)", got)
	}
}

// TestPusher_MultiTenant_DeltaStamping is requirement (b): frames for an
// annotated namespace carry the Tenant stamp, frames for a primary
// (unannotated) namespace carry none — and once the annotation is removed,
// a tenant's last namespace leaving needs nothing special: subsequent
// frames for that namespace simply go back to unstamped/primary on the same
// socket.
func TestPusher_MultiTenant_DeltaStamping(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newTestPusher(wsURL(srv), nil)
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()
	// Both namespaces are named explicitly: an empty list is the annotated
	// set, not everything, so a single-tenant deployment must name what it
	// watches. "rag" is listed too because this test de-annotates it below
	// and still expects its frames — being listed is what keeps it observed;
	// the annotation only decides which tenant they are stamped with. This
	// test is about tenant STAMPING, not scope.
	p.opts.Namespaces = []string{"ops", "rag"}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()
	waitForStampedKind(t, hub, KindSnapshotEnd, "", 3*time.Second)

	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)
	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-a", 3*time.Second)

	ragApp := mkAppDeltaApp("rag", "worker", true)
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, ragApp))                            // annotated → stamped tenant-a
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("ops", "api", true))) // ns "ops", unannotated → unstamped

	waitForDelta := func(wireTenant, wantApp string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			for _, f := range hub.framesWithTenant(wireTenant) {
				if f.Kind == KindAppDelta && f.AppDelta.App.Name == wantApp {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("no app_delta for %q with tenant stamp %q received", wantApp, wireTenant)
	}
	waitForDelta("tenant-a", "worker")
	waitForDelta("", "api")
	// The primary's delta must not also appear stamped.
	for _, f := range hub.framesWithTenant("tenant-a") {
		if f.Kind == KindAppDelta && f.AppDelta.App.Name == "api" {
			t.Errorf("primary-namespace app_delta unexpectedly stamped tenant-a: %+v", f)
		}
	}

	// Remove the annotation — the tenant's last namespace leaving needs no
	// teardown; the namespace's frames just resolve to primary again.
	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "rag"}}, false)
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, ragApp))
	waitForDelta("", "worker")

	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1 throughout", got)
	}
}

// TestPusher_MultiTenant_PreStartAnnotation_StampedSnapshotOnFirstConnect
// replaces the old pre-Start worker-adoption regression test (there are no
// secondary workers left to adopt): a namespace ALREADY annotated when the
// process starts — the agent-restart production path, where the Namespace
// informer replays its Add before Start ever dials — must have its tenant's
// stamped snapshot sequence included on the FIRST connect, right after the
// primary tenant's unstamped sequence, on the one and only connection.
func TestPusher_MultiTenant_PreStartAnnotation_StampedSnapshotOnFirstConnect(t *testing.T) {
	hub, srv := newFakeHub(t)
	ragApp := testApp("worker", true)
	ragApp.Namespace = "rag"
	p := newTestPusher(wsURL(srv), []workloadInfo{ragApp})
	p.dynamicScope = newDynamicScopeSet()
	p.nsDefaults = newNsDefaultsStore()

	// Annotated before Start (and therefore before any connection exists).
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "rag", Annotations: map[string]string{annotationTenant: "tenant-a"}},
	}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = p.Start(ctx) }()

	waitForStampedKind(t, hub, KindSnapshotEnd, "tenant-a", 3*time.Second)

	// Primary sequence first (zero apps — rag now belongs to tenant-a — so
	// just an unstamped snapshot_end), then tenant-a's stamped sequence.
	want := []kindTenantPair{
		{KindSnapshotEnd, ""},
		{KindSnapshot, "tenant-a"}, {KindSnapshotEnd, "tenant-a"},
	}
	got := hub.kindTenantPairs(KindSnapshot, KindSnapshotEnd)
	if len(got) < len(want) {
		t.Fatalf("kind/tenant sequence = %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("kind/tenant sequence[%d] = %v, want %v (full: %v)", i, got[i], w, got)
		}
	}

	var names []string
	for _, f := range hub.framesWithTenant("tenant-a") {
		if f.Kind == KindSnapshot {
			for _, a := range f.Snapshot.Apps {
				names = append(names, a.Name)
			}
		}
	}
	if len(names) != 1 || names[0] != "worker" {
		t.Errorf("tenant-a first-connect snapshot apps = %v, want [worker]", names)
	}
	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1", got)
	}
}

// --- splitFrameByTenant / splitByTenant: unit-level routing coverage,
// independent of any live connection. ---

func newRoutingTestPusher() *Pusher {
	return &Pusher{
		opts:         Options{ClusterName: "cluster-a", Tenant: "primary"},
		log:          logr.Discard(),
		dynamicScope: newDynamicScopeSet(),
	}
}

func TestSplitFrameByTenant_NoDynamicScopeRoutesEverythingToPrimary(t *testing.T) {
	p := newRoutingTestPusher() // dynamicScope present but empty

	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 1},
		{Namespace: "dev", Service: "web", ErrorCount: 2},
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 1 {
		t.Fatalf("splitFrameByTenant with no dynamic scope = %d tenants, want 1 (primary)", len(got))
	}
	frame, ok := got["primary"]
	if !ok || len(frame.LogMetrics.Services) != 2 {
		t.Errorf("got = %+v, want single primary-tenant frame carrying both services", got)
	}
}

func TestSplitFrameByTenant_AppDeltaRoutesByNamespace(t *testing.T) {
	p := newRoutingTestPusher()
	p.dynamicScope.add("rag", "tenant-a")

	f := newAppDelta("cluster-a", OpUpdate, &VirtualApp{ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker"}})
	got := p.splitFrameByTenant(f)
	if len(got) != 1 {
		t.Fatalf("got %d tenants, want 1", len(got))
	}
	if _, ok := got["tenant-a"]; !ok {
		t.Errorf("got = %v, want routed to tenant-a", got)
	}

	// An un-annotated namespace's app_delta routes to primary.
	f2 := newAppDelta("cluster-a", OpUpdate, &VirtualApp{ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"}})
	got2 := p.splitFrameByTenant(f2)
	if _, ok := got2["primary"]; !ok {
		t.Errorf("got2 = %v, want routed to primary", got2)
	}
}

func TestSplitFrameByTenant_LogMetricsSplitsTwoWays(t *testing.T) {
	p := newRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")
	p.dynamicScope.add("dev", "tenant-b")

	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 5},
		{Namespace: "dev", Service: "web", ErrorCount: 9},
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 2 {
		t.Fatalf("splitFrameByTenant = %d tenants, want 2, got %+v", len(got), got)
	}
	a, ok := got["tenant-a"]
	if !ok || len(a.LogMetrics.Services) != 1 || a.LogMetrics.Services[0].Service != "api" {
		t.Errorf("tenant-a split = %+v, want exactly the api service", a)
	}
	b, ok := got["tenant-b"]
	if !ok || len(b.LogMetrics.Services) != 1 || b.LogMetrics.Services[0].Service != "web" {
		t.Errorf("tenant-b split = %+v, want exactly the web service", b)
	}
	// Window fields must be preserved on every split.
	if a.LogMetrics.WindowStart != 1 || a.LogMetrics.WindowSec != 60 {
		t.Errorf("tenant-a window = (%d,%d), want (1,60)", a.LogMetrics.WindowStart, a.LogMetrics.WindowSec)
	}
}

func TestSplitFrameByTenant_TopologySplitsNodesAndEdgesBySourceNamespace(t *testing.T) {
	p := newRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")
	p.dynamicScope.add("dev", "tenant-b")

	f := newTopologySnapshot("cluster-a", 1720000000, 600,
		[]TopologyNode{{Namespace: "ops", Workload: "api"}, {Namespace: "dev", Workload: "web"}},
		[]TopologyEdge{
			{SrcNamespace: "ops", SrcWorkload: "api", DstNamespace: "dev", DstWorkload: "web", Calls: 7},
		},
	)
	got := p.splitFrameByTenant(f)
	if len(got) != 2 {
		t.Fatalf("got %d tenants, want 2, got %+v", len(got), got)
	}
	a := got["tenant-a"]
	if a == nil || len(a.TopologySnapshot.Nodes) != 1 || a.TopologySnapshot.Nodes[0].Workload != "api" {
		t.Errorf("tenant-a nodes = %+v, want exactly the ops/api node", a)
	}
	if a == nil || len(a.TopologySnapshot.Edges) != 1 || a.TopologySnapshot.Edges[0].DstWorkload != "web" {
		t.Errorf("tenant-a edges = %+v, want the cross-tenant edge (owned by its SOURCE namespace's tenant)", a)
	}
	b := got["tenant-b"]
	if b == nil || len(b.TopologySnapshot.Nodes) != 1 || b.TopologySnapshot.Nodes[0].Workload != "web" {
		t.Errorf("tenant-b nodes = %+v, want exactly the dev/web node", b)
	}
	if b != nil && len(b.TopologySnapshot.Edges) != 0 {
		t.Errorf("tenant-b edges = %+v, want none (the edge's SOURCE is ops, not dev)", b.TopologySnapshot.Edges)
	}
}

func TestSplitFrameByTenant_TopologyEmptySnapshotRoutesToPrimary(t *testing.T) {
	p := newRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")

	f := newTopologySnapshot("cluster-a", 1720000000, 600, nil, nil)
	got := p.splitFrameByTenant(f)
	if len(got) != 1 {
		t.Fatalf("got %d tenants, want 1 (empty snapshot always routes to primary)", len(got))
	}
	if _, ok := got["primary"]; !ok {
		t.Errorf("got = %v, want routed to primary", got)
	}
}

// --- Cluster-mode (Options.Tenant == "", docs/todo/virtual-apps.md
// "Cluster-mode push" addendum, 2026-08-27): no primary tenant at all — an
// agent identified only by ClusterName, every frame's tenant derived purely
// from triageagent.dev/tenant namespace annotations, an unmapped namespace's items
// dropped rather than defaulted. ---

func newClusterModeRoutingTestPusher() *Pusher {
	return &Pusher{
		opts:         Options{ClusterName: "cluster-a"}, // Tenant left "" deliberately
		log:          logr.Discard(),
		dynamicScope: newDynamicScopeSet(),
	}
}

// newClusterModeTestPusher mirrors newTestPusher but with Options.Tenant
// left empty — a live-connection cluster-mode agent for the integration
// tests below.
func newClusterModeTestPusher(endpoint string, workloads []workloadInfo) *Pusher {
	p := &Pusher{
		opts: Options{
			Endpoint:      endpoint,
			ClusterName:   "cluster-a",
			JoinTenants:   []string{"tenant-a"}, // a cluster-mode agent lists every tenant it serves
			Heartbeat:     150 * time.Millisecond,
			SnapshotChunk: 50,
			VirtualApps:   VirtualAppsOn,
		},
		log:          logr.Discard(),
		skipHandlers: true,
	}
	p.vapps = newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), p.enqueue)
	p.vapps.ctx = context.Background()
	for _, w := range workloads {
		info := w
		p.vapps.applyWorkload(workloadKey(info.Kind, info.Namespace, info.Name), &info, false)
	}
	return sharedTestCore().joinedTo(p)
}

func TestSplitFrameByTenant_ClusterMode_NoDynamicScopeDropsEverything(t *testing.T) {
	p := newClusterModeRoutingTestPusher() // dynamicScope present but empty

	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 1},
		{Namespace: "dev", Service: "web", ErrorCount: 2},
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 0 {
		t.Fatalf("splitFrameByTenant in cluster-mode with no dynamic scope = %d tenants, want 0 (nothing has a mapping, nothing to invent)", len(got))
	}
	if n := p.noTenantDropped.Load(); n != 2 {
		t.Errorf("noTenantDropped = %d, want 2 (both services dropped)", n)
	}
}

func TestSplitFrameByTenant_ClusterMode_AppDeltaMappedRoutesUnmappedDrops(t *testing.T) {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("rag", "tenant-a")

	f := newAppDelta("cluster-a", OpUpdate, &VirtualApp{ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker"}})
	got := p.splitFrameByTenant(f)
	if len(got) != 1 {
		t.Fatalf("got %d tenants, want 1", len(got))
	}
	if _, ok := got["tenant-a"]; !ok {
		t.Errorf("got = %v, want routed to tenant-a", got)
	}

	// An un-annotated namespace's app_delta has no tenant in cluster-mode —
	// dropped, not routed anywhere (there is no primary to fall back to).
	f2 := newAppDelta("cluster-a", OpUpdate, &VirtualApp{ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"}})
	got2 := p.splitFrameByTenant(f2)
	if len(got2) != 0 {
		t.Errorf("got2 = %v, want dropped (0 tenants) — ops has no %s mapping in cluster-mode", got2, annotationTenant)
	}
}

func TestSplitFrameByTenant_ClusterMode_LogMetricsDropsUnmappedKeepsMapped(t *testing.T) {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")
	p.dynamicScope.add("dev", "tenant-b")

	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "ops", Service: "api", ErrorCount: 5},
		{Namespace: "dev", Service: "web", ErrorCount: 9},
		{Namespace: "plain", Service: "job", ErrorCount: 1}, // no triageagent.dev/tenant mapping
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 2 {
		t.Fatalf("splitFrameByTenant = %d tenants, want 2 (plain's service must be dropped, not given its own primary slot), got %+v", len(got), got)
	}
	a, ok := got["tenant-a"]
	if !ok || len(a.LogMetrics.Services) != 1 || a.LogMetrics.Services[0].Service != "api" {
		t.Errorf("tenant-a split = %+v, want exactly the api service", a)
	}
	b, ok := got["tenant-b"]
	if !ok || len(b.LogMetrics.Services) != 1 || b.LogMetrics.Services[0].Service != "web" {
		t.Errorf("tenant-b split = %+v, want exactly the web service", b)
	}
	if _, ok := got[""]; ok {
		t.Errorf("got an unstamped/empty-tenant split %+v — cluster-mode must never send one", got[""])
	}
	if n := p.noTenantDropped.Load(); n != 1 {
		t.Errorf("noTenantDropped = %d, want 1 (plain/job)", n)
	}
}

func TestSplitFrameByTenant_ClusterMode_TopologyEmptySnapshotDropped(t *testing.T) {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")

	f := newTopologySnapshot("cluster-a", 1720000000, 600, nil, nil)
	got := p.splitFrameByTenant(f)
	if len(got) != 0 {
		t.Fatalf("got %d tenants, want 0 — an empty snapshot has no tenant to attribute it to in cluster-mode, and must not be invented onto one", len(got))
	}
}

func TestSplitFrameByTenant_ClusterMode_TopologyUnmappedNamespaceDropped(t *testing.T) {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("ops", "tenant-a")

	f := newTopologySnapshot("cluster-a", 1720000000, 600,
		[]TopologyNode{{Namespace: "ops", Workload: "api"}, {Namespace: "plain", Workload: "job"}},
		[]TopologyEdge{{SrcNamespace: "plain", SrcWorkload: "job", DstNamespace: "ops", DstWorkload: "api", Calls: 3}},
	)
	got := p.splitFrameByTenant(f)
	if len(got) != 1 {
		t.Fatalf("got %d tenants, want 1 (only tenant-a; plain's node and the plain-sourced edge must be dropped), got %+v", len(got), got)
	}
	a := got["tenant-a"]
	if a == nil || len(a.TopologySnapshot.Nodes) != 1 || a.TopologySnapshot.Nodes[0].Workload != "api" {
		t.Errorf("tenant-a nodes = %+v, want exactly the ops/api node (plain/job dropped)", a)
	}
	if a == nil || len(a.TopologySnapshot.Edges) != 0 {
		t.Errorf("tenant-a edges = %+v, want none — the edge's SOURCE (plain) has no tenant mapping", a.TopologySnapshot.Edges)
	}
}

func TestPusher_ClusterMode_HelloDeclaresEmptyTenant(t *testing.T) {
	hub, srv := newFakeHub(t)
	p := newClusterModeTestPusher(wsURL(srv), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _, _ = p.runOnce(ctx) }()

	// No apps and no dynamic scope means NO snapshot sequence at all in
	// cluster-mode (no primary to send one for) — wait for the heartbeat
	// instead, which confirms the connection completed its startup sequence
	// (hello, then straight to streaming) and is still alive.
	waitForStampedKind(t, hub, KindHeartbeat, "", 2*time.Second)

	hub.mu.Lock()
	var helloFrame *Frame
	for _, f := range hub.frames {
		if f.Kind == KindHello {
			helloFrame = f
			break
		}
	}
	hub.mu.Unlock()

	if helloFrame == nil {
		t.Fatal("no hello frame received")
	}
	if helloFrame.Hello.Tenant != "" {
		t.Errorf("hello.Tenant = %q, want empty in cluster-mode", helloFrame.Hello.Tenant)
	}
	if len(helloFrame.Hello.Cert) == 0 || len(helloFrame.Hello.Proof) == 0 {
		t.Error("cluster-mode hello must be a certificate hello like any other: cert and proof present")
	}
}

// TestPusher_ClusterMode_NoUnstampedSnapshotSequence is the cluster-mode
// analog of TestPusher_MultiTenant_TwoActiveTenants_OneConnection: two
// namespaces annotated to two tenants share the single connection, but
// UNLIKE single-tenant mode there is no primary sequence at all — only the
// two tenants' stamped sequences — and a third, unannotated namespace's app
// never appears anywhere on the wire.
func TestPusher_ClusterMode_NoUnstampedSnapshotSequence(t *testing.T) {
	hub, srv := newFakeHub(t)

	workerApp := testApp("worker", true)
	workerApp.Namespace = "rag"
	webApp := testApp("web", true)
	webApp.Namespace = "dev"
	plainApp := testApp("job", true)
	plainApp.Namespace = "plain" // never annotated — must be dropped, not defaulted

	p := newClusterModeTestPusher(wsURL(srv), []workloadInfo{workerApp, webApp, plainApp})
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
	cancel()
	time.Sleep(50 * time.Millisecond) // let the connection settle before asserting the final frame set

	if got := hub.connCount(); got != 1 {
		t.Errorf("connCount = %d, want exactly 1", got)
	}

	// No unstamped (primary-style) Snapshot/SnapshotEnd at all in cluster-mode.
	for _, f := range hub.framesWithTenant("") {
		if f.Kind == KindSnapshot || f.Kind == KindSnapshotEnd {
			t.Errorf("unexpected unstamped %s frame in cluster-mode: %+v", f.Kind, f)
		}
	}

	got := hub.kindTenantPairs(KindSnapshot, KindSnapshotEnd)
	want := []kindTenantPair{
		{KindSnapshot, "tenant-a"}, {KindSnapshotEnd, "tenant-a"},
		{KindSnapshot, "tenant-b"}, {KindSnapshotEnd, "tenant-b"},
	}
	if len(got) != len(want) {
		t.Fatalf("kind/tenant sequence = %v, want %v (exactly two stamped sequences, nothing else)", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("kind/tenant sequence[%d] = %v, want %v (full: %v)", i, got[i], w, got)
		}
	}

	// plain/job never appears in ANY tenant's snapshot.
	for _, wireTenant := range []string{"tenant-a", "tenant-b", ""} {
		for _, f := range hub.framesWithTenant(wireTenant) {
			if f.Kind != KindSnapshot {
				continue
			}
			for _, a := range f.Snapshot.Apps {
				if a.Name == "job" {
					t.Errorf("plain/job must never be pushed in cluster-mode (unmapped namespace): found under tenant stamp %q", wireTenant)
				}
			}
		}
	}
}

func TestParseNamespaces(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"ops", []string{"ops"}},
		{"ops,dev", []string{"ops", "dev"}},
		{"ops, dev", []string{"ops", "dev"}},    // spaces after comma
		{" ops , dev ", []string{"ops", "dev"}}, // spaces everywhere
		{"ops, ,dev,", []string{"ops", "dev"}},  // empty entries dropped
		{"  ", nil},                             // all blank → nil
		{",,", nil},                             // only separators → nil
	}
	for _, tt := range tests {
		got := ParseNamespaces(tt.in)
		if len(got) != len(tt.want) {
			t.Errorf("ParseNamespaces(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("ParseNamespaces(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}

// runOnce runs the primary connection worker once (connworker.go).
func (p *Pusher) runOnce(ctx context.Context) (connected bool, err error) {
	return p.primaryWorker().runOnce(ctx)
}

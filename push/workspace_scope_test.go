package push

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const scopeTenant = "example-tenant" // newTestPusher's primary tenant

type frameCapture struct {
	mu     sync.Mutex
	frames []*Frame
}

func (c *frameCapture) Observe(_ string, f *Frame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames = append(c.frames, f)
}

func (c *frameCapture) kind(k string) []*Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*Frame
	for _, f := range c.frames {
		if f.Kind == k {
			out = append(out, f)
		}
	}
	return out
}

func TestWorkspaceScope_NoFrameYetAllowsEverything(t *testing.T) {
	p := newTestPusher("", nil)
	if !p.WorkspaceNamespaceInScope(scopeTenant, "anything") || !p.WorkspaceNamespaceInScope("other", "x") {
		t.Fatal("before the first limits frame every namespace is in scope")
	}
	var nilPusher *Pusher
	if !nilPusher.WorkspaceNamespaceInScope(scopeTenant, "x") {
		t.Fatal("a nil pusher allows everything")
	}
}

func TestWorkspaceScope_SetAbsentAndEmptyListMeanAll(t *testing.T) {
	p := newTestPusher("", nil)
	p.scope.set(map[string][]string{"t1": {"a", "b"}, "t2": {}})
	if !p.WorkspaceNamespaceInScope("t1", "a") || p.WorkspaceNamespaceInScope("t1", "c") {
		t.Fatal("t1 takes only a and b")
	}
	if !p.WorkspaceNamespaceInScope("t2", "anything") {
		t.Fatal("an empty list takes every namespace")
	}
	if !p.WorkspaceNamespaceInScope("t3", "anything") {
		t.Fatal("an absent tenant takes every namespace")
	}
	if !p.WorkspaceNamespaceInScope("t1", "") {
		t.Fatal("a source with no namespace is never excluded")
	}
}

func TestWorkspaceScope_UpdateReplaces(t *testing.T) {
	p := newTestPusher("", nil)
	p.scope.set(map[string][]string{"t1": {"a"}})
	p.scope.set(map[string][]string{"t1": {"b"}})
	if p.WorkspaceNamespaceInScope("t1", "a") || !p.WorkspaceNamespaceInScope("t1", "b") {
		t.Fatal("the second frame replaces the first")
	}
	p.scope.set(nil) // a frame without the field
	if !p.WorkspaceNamespaceInScope("t1", "a") {
		t.Fatal("a frame with no namespaces lifts the scope")
	}
}

func TestWorkspaceScope_JoinBindingsSeedUntilALimitsFrame(t *testing.T) {
	p := newTestPusher("", nil)
	p.scope.seed([]JoinBinding{{Tenant: "t1", Namespaces: []string{"a"}}, {Tenant: "t2"}})
	if p.WorkspaceNamespaceInScope("t1", "b") || !p.WorkspaceNamespaceInScope("t2", "b") {
		t.Fatalf("seeded scope = %v", p.scope.snapshot())
	}
	p.scope.set(map[string][]string{"t1": {"b"}})
	p.scope.seed([]JoinBinding{{Tenant: "t1", Namespaces: []string{"a"}}})
	if !p.WorkspaceNamespaceInScope("t1", "b") || p.WorkspaceNamespaceInScope("t1", "a") {
		t.Fatal("a limits frame wins over a later seed")
	}
}

func TestWorkspaceScope_LimitsFrameSetsScope(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		b, _ := encode(&Frame{V: ProtocolVersion, Kind: KindLimits, Limits: &LimitsPayload{
			EventsPerDay: map[string]int64{scopeTenant: 1000},
			Namespaces:   map[string][]string{scopeTenant: {"ops", "shop"}},
		}})
		_ = c.WriteMessage(websocket.TextMessage, b)
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	p := newTestPusher("", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newConnWorker(p).readLoop(ctx, cancel, conn, 5*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for p.WorkspaceNamespaceInScope(scopeTenant, "other") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.WorkspaceNamespaceInScope(scopeTenant, "other") || !p.WorkspaceNamespaceInScope(scopeTenant, "shop") {
		t.Fatalf("scope after the limits frame = %v", p.scope.snapshot())
	}
	if got := p.scope.snapshot()[scopeTenant]; !reflect.DeepEqual(got, []string{"ops", "shop"}) {
		t.Fatalf("scope = %v", got)
	}
}

func TestWorkspaceScope_PushDropsAndCountsOutOfScopeItems(t *testing.T) {
	p := newTestPusher("", nil)
	obs := &frameCapture{}
	p.SetObserver(obs)
	p.scope.set(map[string][]string{scopeTenant: {"in"}})

	// app delta
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("out", "api", true)))
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("in", "api", true)))
	if got := len(obs.kind(KindAppDelta)); got != 1 {
		t.Fatalf("app deltas sent = %d, want 1", got)
	}
	if got := p.Link().OutOfScopeDropped; got != 1 {
		t.Fatalf("dropped after the delta = %d, want 1", got)
	}

	// a metric frame with one service in and one out is trimmed
	p.enqueue(newLogMetrics(p.opts.ClusterName, 1, 60, []ServiceLogMetrics{
		{Namespace: "in", Service: "a", ErrorCount: 1}, {Namespace: "out", Service: "b", ErrorCount: 1},
	}))
	lm := obs.kind(KindLogMetrics)
	if len(lm) != 1 || len(lm[0].LogMetrics.Services) != 1 || lm[0].LogMetrics.Services[0].Namespace != "in" {
		t.Fatalf("log metrics frames = %+v", lm)
	}

	// topology: nodes by namespace, edges by source namespace
	p.enqueue(newTopologySnapshot(p.opts.ClusterName, 1, 60,
		[]TopologyNode{{Namespace: "in", Workload: "a"}, {Namespace: "out", Workload: "b"}},
		[]TopologyEdge{{SrcNamespace: "in", SrcWorkload: "a", External: "db:5432"}, {SrcNamespace: "out", SrcWorkload: "b", External: "db:5432"}}))
	tp := obs.kind(KindTopologySnapshot)
	if len(tp) != 1 || len(tp[0].TopologySnapshot.Nodes) != 1 || len(tp[0].TopologySnapshot.Edges) != 1 {
		t.Fatalf("topology frames = %+v", tp)
	}

	// A frame whose every item is out of scope sends nothing.
	p.enqueue(newLogMetrics(p.opts.ClusterName, 2, 60, []ServiceLogMetrics{{Namespace: "out", Service: "b", ErrorCount: 1}}))
	if got := len(obs.kind(KindLogMetrics)); got != 1 {
		t.Fatalf("log metrics frames = %d, want still 1", got)
	}
	if got := p.Link().OutOfScopeDropped; got != 5 {
		t.Fatalf("dropped = %d, want 5 (delta, service, node, edge, service)", got)
	}
}

func TestWorkspaceScope_EmptyScopeDropsNothing(t *testing.T) {
	p := newTestPusher("", nil)
	obs := &frameCapture{}
	p.SetObserver(obs)
	p.scope.set(map[string][]string{scopeTenant: {}})
	p.enqueue(newAppDelta(p.opts.ClusterName, OpUpdate, mkAppDeltaApp("any", "api", true)))
	if len(obs.kind(KindAppDelta)) != 1 || p.Link().OutOfScopeDropped != 0 {
		t.Fatal("an empty list takes every namespace")
	}
}

func TestWorkspaceScope_SnapshotSkipsOutOfScopeApps(t *testing.T) {
	in, out := testApp("api", true), testApp("web", true)
	in.Namespace, out.Namespace = "in", "out"
	p := newTestPusher("", []workloadInfo{in, out})
	p.scope.set(map[string][]string{scopeTenant: {"in"}})
	p.vapps.recomputeNow(context.Background()) // may enqueue deltas, which count too
	before := p.Link().OutOfScopeDropped
	frames, n := p.snapshotFramesForTenant(context.Background(), scopeTenant)
	if n != 1 {
		t.Fatalf("snapshot apps = %d, want 1 (frames %d)", n, len(frames))
	}
	if got := p.Link().OutOfScopeDropped - before; got < 1 {
		t.Fatalf("the out-of-scope app was not counted: %d", got)
	}
}

func TestWorkspaceScope_RestoredScopeStandsUntilALimitsFrame(t *testing.T) {
	p := newTestPusher("", nil)
	p.scope.restore(map[string][]string{"t1": {"a"}})
	if p.WorkspaceNamespaceInScope("t1", "b") || !p.WorkspaceNamespaceInScope("t2", "b") {
		t.Fatalf("restored scope = %v", p.scope.snapshot())
	}
	if p.scope.needsRestore() {
		t.Fatal("a restored scope is not read again")
	}
	p.scope.set(map[string][]string{"t1": {"b"}})
	p.scope.restore(map[string][]string{"t1": {"a"}})
	if !p.WorkspaceNamespaceInScope("t1", "b") || p.WorkspaceNamespaceInScope("t1", "a") {
		t.Fatal("a limits frame wins over a later restore")
	}
	if p.scope.dropRestored() {
		t.Fatal("a confirmed scope is not lifted")
	}
}

func TestWorkspaceScope_RestoredScopeLiftedWithoutAFrame(t *testing.T) {
	old := scopeLimitsGrace
	scopeLimitsGrace = 30 * time.Millisecond
	defer func() { scopeLimitsGrace = old }()

	p := newTestPusher("", nil)
	p.scope.restore(map[string][]string{"t1": {"a"}})
	dropped := make(chan struct{})
	stop := p.scope.armGrace(func() { close(dropped) })
	defer stop()
	select {
	case <-dropped:
	case <-time.After(2 * time.Second):
		t.Fatal("the restored scope was not lifted")
	}
	if !p.WorkspaceNamespaceInScope("t1", "b") {
		t.Fatal("a lifted scope takes every namespace")
	}

	// A scope the core confirmed in time stays, and so does a session with
	// nothing restored.
	p = newTestPusher("", nil)
	p.scope.restore(map[string][]string{"t1": {"a"}})
	stop = p.scope.armGrace(func() { t.Error("lifted") })
	p.scope.set(map[string][]string{"t1": {"a"}})
	time.Sleep(100 * time.Millisecond)
	stop()
	if p.WorkspaceNamespaceInScope("t1", "b") {
		t.Fatal("the confirmed scope was lifted")
	}
	p = newTestPusher("", nil)
	p.scope.armGrace(func() { t.Error("nothing was restored") })()
}

func TestWorkspaceScope_EncodedIgnoresNoScope(t *testing.T) {
	var s workspaceScope
	if _, changed := s.encoded(); changed {
		t.Fatal("an agent that never had a scope has nothing to save")
	}
	s.set(map[string][]string{"t1": {"b", "a"}})
	data, changed := s.encoded()
	if !changed || string(data) != `{"t1":["a","b"]}` {
		t.Fatalf("encoded = %s, %v", data, changed)
	}
	s.markSaved(data)
	if _, changed := s.encoded(); changed {
		t.Fatal("a saved scope is not saved again")
	}
	s.set(nil)
	if data, changed := s.encoded(); !changed || string(data) != `{}` {
		t.Fatalf("a lifted scope must be saved: %s, %v", data, changed)
	}
}

// The sensor_state frame lists OTLP sources; like every data frame it leaves
// out a source of an unannotated namespace (cluster mode) and one outside its
// tenant's workspace scope, but keeps one with no namespace of its own.
func TestWorkspaceScope_SensorStateSourcesFollowTheScope(t *testing.T) {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("shop", "t1")
	p.dynamicScope.add("vault", "t1")
	p.dynamicScope.add("other", "t2")
	p.scope.set(map[string][]string{"t1": {"shop"}})

	out := p.stampOTLPTenants([]OTLPSourceStatus{
		{Key: "in", Namespace: "shop"},
		{Key: "out", Namespace: "vault"},
		{Key: "unmapped", Namespace: "ops"},
		{Key: "unscoped", Namespace: "other"},
		{Key: "kubelet"},
	})
	var keys []string
	for _, o := range out {
		keys = append(keys, o.Key)
	}
	if want := []string{"in", "unscoped", "kubelet"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("sources = %v, want %v", keys, want)
	}
	if out[0].Tenant != "t1" || out[1].Tenant != "t2" {
		t.Fatalf("tenants = %+v", out)
	}
	if n := p.outOfScopeDropped.Load(); n != 0 {
		t.Fatalf("a listing was counted as %d data drops", n)
	}
}

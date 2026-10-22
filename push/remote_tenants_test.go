package push

import (
	"testing"
	"time"
)

func newRemoteTestPusher() *Pusher {
	p := newClusterModeRoutingTestPusher()
	p.dynamicScope.add("rag", "local-tenant")
	return p
}

func TestRemoteTenant_QualifiedNamespaceRoutesToItsTenant(t *testing.T) {
	p := newRemoteTestPusher()
	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "acme~default", Service: "cart", ErrorCount: 1},
		{Namespace: "globex~default", Service: "cart", ErrorCount: 2},
		{Namespace: "rag", Service: "worker", ErrorCount: 3},
		{Namespace: "unmapped", Service: "x", ErrorCount: 4},
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 3 {
		t.Fatalf("tenants = %d, want acme, globex and the local one: %+v", len(got), got)
	}
	for tenant, want := range map[string]string{"acme": "acme~default", "globex": "globex~default", "local-tenant": "rag"} {
		fr := got[tenant]
		if fr == nil || len(fr.LogMetrics.Services) != 1 || fr.LogMetrics.Services[0].Namespace != want {
			t.Fatalf("tenant %s frame = %+v", tenant, fr)
		}
	}
	if n := p.noTenantDropped.Load(); n != 1 {
		t.Fatalf("noTenantDropped = %d, want 1 (only the unmapped local namespace)", n)
	}
}

func TestRemoteTenant_LocalNamespaceCannotImpersonate(t *testing.T) {
	// A Kubernetes namespace cannot contain "~", and a plain name never
	// resolves to a remote tenant.
	p := newRemoteTestPusher()
	if got := p.tenantForNamespace("acme"); got != "" {
		t.Fatalf("plain namespace %q resolved to %q", "acme", got)
	}
	if _, ok := p.remoteSeen.Load("acme"); ok {
		t.Fatal("a plain namespace marked a remote tenant active")
	}
}

func TestRemoteTenant_EnqueueStripsQualificationAndStamps(t *testing.T) {
	p := newRemoteTestPusher()
	p.mux().beginConnection(nil)

	p.enqueue(newRedMetrics("cluster-a", 1, 60, []ServiceRedMetrics{{Namespace: "acme~shop", Service: "cart"}}))
	p.enqueue(newTopologySnapshot("cluster-a", 1, 60,
		[]TopologyNode{{Namespace: "acme~shop", Workload: "cart"}, {Namespace: "acme~shop", Workload: "db"}},
		[]TopologyEdge{{SrcNamespace: "acme~shop", SrcWorkload: "cart", DstNamespace: "acme~shop", DstWorkload: "db", Calls: 3}}))

	var red, topo *Frame
	for {
		f, ok := p.mux().next()
		if !ok {
			break
		}
		switch f.Kind {
		case KindRedMetrics:
			red = f
		case KindTopologySnapshot:
			topo = f
		}
	}
	if red == nil || red.Tenant != "acme" || red.RedMetrics.Services[0].Namespace != "shop" {
		t.Fatalf("red frame = %+v", red)
	}
	if topo == nil || topo.Tenant != "acme" {
		t.Fatalf("topology frame = %+v", topo)
	}
	tp := topo.TopologySnapshot
	if tp.Nodes[0].Namespace != "shop" || tp.Edges[0].SrcNamespace != "shop" || tp.Edges[0].DstNamespace != "shop" {
		t.Fatalf("topology still qualified: %+v", tp)
	}
}

func TestRemoteTenant_ActiveWhileDataFlowsAndExpires(t *testing.T) {
	p := newRemoteTestPusher()
	p.tenantForNamespace("acme~shop")
	if got := p.activeSecondaryTenants(); !contains(got, "acme") {
		t.Fatalf("activeSecondaryTenants = %v, want acme", got)
	}
	p.remoteSeen.Store("acme", time.Now().Add(-2*remoteTenantTTL))
	if got := p.activeSecondaryTenants(); contains(got, "acme") {
		t.Fatalf("activeSecondaryTenants = %v, acme should have expired", got)
	}
}

func TestRemoteTenant_FirstDataSendsSnapshotSequence(t *testing.T) {
	p := newRemoteTestPusher()
	p.mux().beginConnection(nil)
	p.tenantForNamespace("acme~shop")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for {
			f, ok := p.mux().next()
			if !ok {
				break
			}
			if f.Kind == KindSnapshotEnd && f.Tenant == "acme" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no snapshot_end for the new remote tenant")
}

func TestRemoteTenant_InScope(t *testing.T) {
	p := newRemoteTestPusher()
	if !p.inScope("acme~shop") || !p.NamespaceInScope("acme~shop") {
		t.Fatal("a remote tenant's namespace is out of scope")
	}
	if p.inScope("elsewhere") {
		t.Fatal("an unannotated local namespace is in scope")
	}
}

func TestRemoteTenant_StatusStrippedAndStamped(t *testing.T) {
	p := newRemoteTestPusher()
	out := p.stampOTLPTenants([]OTLPSourceStatus{{Key: "k", Namespace: "acme~shop"}, {Key: "n", Namespace: "acme~"}})
	if out[0].Tenant != "acme" || out[0].Namespace != "shop" {
		t.Fatalf("source = %+v", out[0])
	}
	if out[1].Tenant != "acme" || out[1].Namespace != "" {
		t.Fatalf("namespace-less source = %+v", out[1])
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestRemoteTenant_NamespacelessSourceIsDropped(t *testing.T) {
	p := newRemoteTestPusher()
	f := newLogMetrics("cluster-a", 1, 60, []ServiceLogMetrics{
		{Namespace: "acme~", Service: "node", ErrorCount: 1},
		{Namespace: "acme~shop", Service: "cart", ErrorCount: 2},
	})
	got := p.splitFrameByTenant(f)
	if len(got) != 1 || len(got["acme"].LogMetrics.Services) != 1 || got["acme"].LogMetrics.Services[0].Service != "cart" {
		t.Fatalf("split = %+v, want only acme's cart", got)
	}
	if n := p.noTenantDropped.Load(); n != 1 {
		t.Fatalf("noTenantDropped = %d, want 1", n)
	}
}

func TestRemoteTenant_SnapshotIsQueuedBeforeTheFirstData(t *testing.T) {
	p := newRemoteTestPusher()
	p.mux().beginConnection(nil)
	p.enqueue(newRedMetrics("cluster-a", 1, 60, []ServiceRedMetrics{{Namespace: "acme~shop", Service: "cart"}}))

	var order []string
	for {
		f, ok := p.mux().next()
		if !ok {
			break
		}
		order = append(order, f.Kind)
	}
	if len(order) < 2 || order[0] != KindSnapshotEnd && order[0] != KindSnapshot || order[len(order)-1] != KindRedMetrics {
		t.Fatalf("frame order = %v, want the snapshot sequence then red_metrics", order)
	}
}

func TestRemoteTenant_NoSecondSnapshotForALocalTenant(t *testing.T) {
	p := newRemoteTestPusher() // local-tenant already holds namespace rag
	p.mux().beginConnection(nil)
	p.tenantForNamespace("local-tenant~shop")
	if f, ok := p.mux().next(); ok {
		t.Fatalf("a snapshot was queued for a tenant that already has one live: %+v", f)
	}
}

// TestUnqualifyFrame_AppsGetPlainNamespaceOnACopy: a virtual app built from a
// remote sender's OTLP reaches the core under its own namespace, and the
// agent's state keeps the qualified one.
func TestUnqualifyFrame_AppsGetPlainNamespaceOnACopy(t *testing.T) {
	app := &VirtualApp{}
	app.Name, app.Namespace = "cart", "t1~shop"

	snap := newSnapshot("cluster-a", []*VirtualApp{app})
	unqualifyFrame(snap, "t1")
	if got := snap.Snapshot.Apps[0].Namespace; got != "shop" {
		t.Fatalf("snapshot app namespace = %q, want shop", got)
	}

	delta := newAppDelta("cluster-a", "upsert", app)
	unqualifyFrame(delta, "t1")
	if got := delta.AppDelta.App.Namespace; got != "shop" {
		t.Fatalf("app_delta namespace = %q, want shop", got)
	}

	if app.Namespace != "t1~shop" {
		t.Fatalf("the agent's own app changed to %q", app.Namespace)
	}

	other := newAppDelta("cluster-a", "upsert", app)
	unqualifyFrame(other, "t2")
	if other.AppDelta.App != app {
		t.Fatal("another tenant's split must leave the app as it is")
	}
}

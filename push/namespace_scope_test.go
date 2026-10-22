package push

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- dynamicScopeSet ---

func TestDynamicScopeSet_AddReportsTransitionOnly(t *testing.T) {
	s := newDynamicScopeSet()
	if prev, changed := s.add("ops", "tenant-a"); !changed || prev != "" {
		t.Errorf("first add() = (%q,%v), want (\"\",true)", prev, changed)
	}
	if prev, changed := s.add("ops", "tenant-a"); changed || prev != "tenant-a" {
		t.Errorf("second add() of the same (ns,tenant) = (%q,%v), want (tenant-a,false)", prev, changed)
	}
	if !s.has("ops") {
		t.Error("has() must report true after add()")
	}
	if got, ok := s.tenantFor("ops"); !ok || got != "tenant-a" {
		t.Errorf("tenantFor(ops) = (%q,%v), want (tenant-a,true)", got, ok)
	}
}

func TestDynamicScopeSet_AddReassignsTenantAsATransition(t *testing.T) {
	s := newDynamicScopeSet()
	s.add("ops", "tenant-a")
	prev, changed := s.add("ops", "tenant-b")
	if !changed || prev != "tenant-a" {
		t.Errorf("re-annotating ops to a different tenant = (%q,%v), want (tenant-a,true)", prev, changed)
	}
	if got, ok := s.tenantFor("ops"); !ok || got != "tenant-b" {
		t.Errorf("tenantFor(ops) after reassignment = (%q,%v), want (tenant-b,true)", got, ok)
	}
}

func TestDynamicScopeSet_RemoveReportsTransitionOnly(t *testing.T) {
	s := newDynamicScopeSet()
	if _, changed := s.remove("ops"); changed {
		t.Error("remove() of a namespace never added must report no transition (false)")
	}
	s.add("ops", "tenant-a")
	if tenant, changed := s.remove("ops"); !changed || tenant != "tenant-a" {
		t.Errorf("remove() of a present namespace = (%q,%v), want (tenant-a,true)", tenant, changed)
	}
	if s.has("ops") {
		t.Error("has() must report false after remove()")
	}
	if _, changed := s.remove("ops"); changed {
		t.Error("second remove() must report no transition (false)")
	}
}

func TestDynamicScopeSet_TenantCounts(t *testing.T) {
	s := newDynamicScopeSet()
	s.add("ops", "tenant-a")
	s.add("dev", "tenant-a")
	s.add("rag", "tenant-b")

	counts := s.tenantCounts()
	if counts["tenant-a"] != 2 {
		t.Errorf("tenantCounts()[tenant-a] = %d, want 2", counts["tenant-a"])
	}
	if counts["tenant-b"] != 1 {
		t.Errorf("tenantCounts()[tenant-b] = %d, want 1", counts["tenant-b"])
	}

	s.remove("dev")
	counts = s.tenantCounts()
	if counts["tenant-a"] != 1 {
		t.Errorf("after removing dev, tenantCounts()[tenant-a] = %d, want 1", counts["tenant-a"])
	}

	s.remove("ops")
	counts = s.tenantCounts()
	if _, ok := counts["tenant-a"]; ok {
		t.Errorf("tenant-a must be absent from tenantCounts() once it owns no namespace, got %v", counts)
	}
}

// --- effectiveInScope: the scope-union rule ---

func TestEffectiveInScope_EmptyStaticIsTheAnnotatedSet(t *testing.T) {
	// Not configured is not "everything": with no --target-namespaces the
	// annotated set IS the scope. This used to assert watch-all, which is how
	// an agent with a primary tenant and no list came to push every namespace
	// on the cluster.
	dyn := newDynamicScopeSet()
	dyn.add("rag", "tenant-a")
	if !effectiveInScope("rag", nil, dyn) {
		t.Error("annotated namespace must be in scope")
	}
	if effectiveInScope("anything-at-all", nil, dyn) {
		t.Error("unannotated namespace must not be in scope")
	}
	// A nil dynamic set means the watcher was never wired (bare struct
	// literals), which is not a deployment — keep the permissive answer.
	if !effectiveInScope("anything-at-all", nil, nil) {
		t.Error("nil dynamic set must stay permissive")
	}
}

func TestEffectiveInScope_NonEmptyStaticUnionsWithDynamic(t *testing.T) {
	static := []string{"ops"}
	dyn := newDynamicScopeSet()
	dyn.add("rag", "tenant-a")

	if !effectiveInScope("ops", static, dyn) {
		t.Error("ops is in the static list — must be in scope")
	}
	if !effectiveInScope("rag", static, dyn) {
		t.Error("rag is in the dynamic set — must be in scope (static ∪ dynamic)")
	}
	if effectiveInScope("dev", static, dyn) {
		t.Error("dev is in neither — must be out of scope")
	}
}

func TestEffectiveInScope_NonEmptyStaticNilDynamicIsStaticOnly(t *testing.T) {
	static := []string{"ops"}
	if !effectiveInScope("ops", static, nil) {
		t.Error("ops is in the static list — must be in scope even with a nil dynamic set")
	}
	if effectiveInScope("dev", static, nil) {
		t.Error("dev is out of the static list and there is no dynamic set — must be out of scope")
	}
}

// --- Pusher.NamespaceInScope: the exported dynamic-scope hook the metric
// collectors' Options.NamespaceAllowed is wired to ---

func TestPusherNamespaceInScope_HonorsDynamicScopeUnion(t *testing.T) {
	dyn := newDynamicScopeSet()
	dyn.add("annotated", "tenant-b")
	p := &Pusher{opts: Options{Namespaces: []string{"static-ns"}}, dynamicScope: dyn}

	if !p.NamespaceInScope("static-ns") {
		t.Error("static-ns is in the static list — must be in scope")
	}
	if !p.NamespaceInScope("annotated") {
		t.Error("annotated is in the dynamic set — must be in scope (static ∪ dynamic)")
	}
	if p.NamespaceInScope("unknown") {
		t.Error("unknown is in neither set — must be out of scope when the static list is non-empty")
	}
}

// A PRIMARY TENANT answers "which tenant does this belong to", never "observe
// the whole cluster". With no --target-namespaces the annotated set is still
// the scope; the primary only decides what an in-scope but unannotated
// namespace is filed under.
func TestPusherNamespaceInScope_PrimaryTenantDoesNotWidenScope(t *testing.T) {
	dyn := newDynamicScopeSet()
	dyn.add("annotated", "tenant-b")
	p := &Pusher{opts: Options{Tenant: "primary"}, dynamicScope: dyn}
	if !p.NamespaceInScope("annotated") {
		t.Error("annotated namespace must be in scope")
	}
	if p.NamespaceInScope("anything-at-all") {
		t.Error("a primary tenant must not put every namespace in scope")
	}
	// An explicit list is how a single-tenant deployment names what it watches.
	p2 := &Pusher{opts: Options{Tenant: "primary", Namespaces: []string{"ops"}}, dynamicScope: dyn}
	if !p2.NamespaceInScope("ops") {
		t.Error("listed namespace must be in scope")
	}
}

// In CLUSTER MODE the annotated set IS the scope. resolveTenant already drops
// an unannotated namespace's items, so watching it produces nothing while
// costing a cluster-wide cache and cluster-wide RBAC.
func TestPusherNamespaceInScope_ClusterModeNarrowsToAnnotated(t *testing.T) {
	dyn := newDynamicScopeSet()
	dyn.add("annotated", "tenant-b")
	p := &Pusher{opts: Options{}, dynamicScope: dyn}
	if !p.NamespaceInScope("annotated") {
		t.Error("an annotated namespace must be in scope in cluster mode")
	}
	if p.NamespaceInScope("unannotated") {
		t.Error("cluster mode must not watch an unannotated namespace — its items are dropped anyway")
	}
}

// A nil dynamic set means the annotation watcher was never wired. Scoping to
// nothing would be a silent blackout, so this falls back to watch-all.
func TestPusherNamespaceInScope_ClusterModeNilDynamicFallsBackToAll(t *testing.T) {
	p := &Pusher{opts: Options{}}
	if !p.NamespaceInScope("anything-at-all") {
		t.Error("cluster mode with no dynamic set must fall back to watch-all, not blackout")
	}
}

func TestPusherNamespaceInScope_NilPusherIsSafe(t *testing.T) {
	var p *Pusher
	if p.NamespaceInScope("anything") {
		t.Error("a nil *Pusher must answer false, not panic")
	}
}

// --- NamespaceTenant ---
//
// Replaces the old tenantScopeMatch (equals-primary-tenant only) predicate:
// ANY non-empty annotationkeys.Tenant value now admits a namespace into
// dynamic scope, under that exact tenant — not just a value matching this
// agent's own Options.Tenant.

func TestNamespaceTenant(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantTenant  string
		wantOK      bool
	}{
		{"primary tenant value", map[string]string{annotationTenant: "tenant-a"}, "tenant-a", true},
		{"a different tenant value is ALSO admitted now", map[string]string{annotationTenant: "other-tenant"}, "other-tenant", true},
		{"absent annotation", nil, "", false},
		{"empty annotation value never admits", map[string]string{annotationTenant: ""}, "", false},
		{"whitespace-only annotation value never admits", map[string]string{annotationTenant: "   "}, "", false},
		{"surrounding whitespace is trimmed", map[string]string{annotationTenant: "  tenant-a  "}, "tenant-a", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Annotations: tt.annotations}}
			gotTenant, gotOK := NamespaceTenant(ns)
			if gotOK != tt.wantOK || gotTenant != tt.wantTenant {
				t.Errorf("NamespaceTenant() = (%q,%v), want (%q,%v)", gotTenant, gotOK, tt.wantTenant, tt.wantOK)
			}
		})
	}
}

// --- Pusher.dropDynamicScope: the guard gating namespace-leaves-scope
// cleanup ---
//
// dropDynamicScope decides whether losing a namespace's triageagent.dev/tenant
// annotation must delete that namespace's virtual apps from the hub, by
// probing what the namespace's scope will be once the dynamic mapping is
// gone. These tests drive that decision through the SAME path production
// frames travel — virtualAppsState's own enqueue for the vapp delete —
// rather than calling the cleanup functions directly, so a regression in the
// GUARD itself (not just in whichever function it calls) fails a test here.

// newDropScopeTestPusher builds a Pusher with one virtual-app-eligible
// workload already resident in ns, static becoming Options.Namespaces (and
// virtualAppsState.namespaces, mirroring how New wires both from the same
// opts.Namespaces). The returned vappFrames pointer accumulates every frame
// virtualAppsState enqueues.
func newDropScopeTestPusher(t *testing.T, static []string, ns string) (p *Pusher, vappFrames *[]*Frame) {
	t.Helper()

	p = &Pusher{
		opts:         Options{ClusterName: "cluster-a", Namespaces: static},
		log:          logr.Discard(),
		nsDefaults:   newNsDefaultsStore(),
		dynamicScope: newDynamicScopeSet(),
	}

	var frames []*Frame
	vapps := newVirtualAppsState(VirtualAppsOn, "cluster-a", static, logr.Discard(),
		func(f *Frame) { frames = append(frames, f) })
	vapps.ctx = context.Background()
	// Shared, not copied — exactly as Pusher.New wires it, and required for
	// dropDynamicScope's own dynamicScope.tenantFor(ns) check to agree with
	// what evictNamespaceAndRecompute's caller already knows.
	vapps.dynamicScope = p.dynamicScope
	p.vapps = vapps

	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	vapps.applyWorkload(workloadKey(workloadDeployment, ns, "worker"), &info, false)
	vapps.recomputeNow(context.Background())
	if len(vapps.snapshotApps()) != 1 {
		t.Fatalf("setup: virtual app for %s/worker must be synthesized before the test", ns)
	}
	frames = nil // discard the setup OpAdd

	return p, &frames
}

// This is the live bug, reproduced without a cluster: the SHIPPING
// configuration runs with no --target-namespaces (Options.Namespaces empty),
// so the annotated set is the whole static+dynamic scope. Losing the
// annotation must leave the namespace out of scope entirely and delete its
// virtual app.
func TestPusher_DropDynamicScope_EmptyStaticNamespaces_DeletesVirtualApp(t *testing.T) {
	p, vappFrames := newDropScopeTestPusher(t, nil, "rag")
	p.dynamicScope.add("rag", "tenant-a")

	p.dropDynamicScope("rag")

	if p.dynamicScope.has("rag") {
		t.Error("rag must no longer be dynamically scoped after dropDynamicScope")
	}

	foundVappDelete := false
	for _, f := range *vappFrames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "worker" {
			foundVappDelete = true
		}
	}
	if !foundVappDelete {
		t.Errorf("virtual-app OpDelete missing: with no --target-namespaces, losing the only annotation that put rag in scope must delete it; frames = %+v", *vappFrames)
	}
}

// A non-empty static list that still names ns keeps it in scope even after
// the dynamic mapping is gone — the annotation was never its only path in,
// so nothing must be deleted.
func TestPusher_DropDynamicScope_StaticListStillCoversNamespace_NoDelete(t *testing.T) {
	p, vappFrames := newDropScopeTestPusher(t, []string{"rag"}, "rag")
	p.dynamicScope.add("rag", "tenant-a")

	p.dropDynamicScope("rag")

	if p.dynamicScope.has("rag") {
		t.Error("rag must no longer be dynamically scoped after dropDynamicScope — the mapping removal itself is unconditional")
	}
	if len(*vappFrames) != 0 {
		t.Errorf("virtual app must not be deleted: rag stays in scope via the static list; frames = %+v", *vappFrames)
	}
}

// A non-empty static list that does NOT name ns must still delete on drop —
// the same outcome as the empty-static case, proving the guard's answer
// depends on membership, not on whether the list happens to be empty.
func TestPusher_DropDynamicScope_StaticListExcludesNamespace_DeletesAnyway(t *testing.T) {
	p, vappFrames := newDropScopeTestPusher(t, []string{"ops"}, "rag")
	p.dynamicScope.add("rag", "tenant-a")

	p.dropDynamicScope("rag")

	if len(*vappFrames) != 1 || (*vappFrames)[0].Kind != KindAppDelta || (*vappFrames)[0].AppDelta.Op != OpDelete {
		t.Errorf("virtual app must be deleted: rag is in neither the static list nor (after removal) the dynamic set; frames = %+v", *vappFrames)
	}
}

// A namespace dropDynamicScope is asked about but which was never
// dynamically scoped (static-list-only, or never annotated) must be a
// complete no-op — nothing to remove.
func TestPusher_DropDynamicScope_NamespaceNeverDynamicallyScoped_NoOp(t *testing.T) {
	p, vappFrames := newDropScopeTestPusher(t, nil, "rag")
	// rag intentionally never added to p.dynamicScope.

	p.dropDynamicScope("rag")

	if len(*vappFrames) != 0 {
		t.Errorf("dropping a namespace that was never dynamically scoped must not touch virtual apps; frames = %+v", *vappFrames)
	}
}

// effectiveInScope is effectiveInScopeMode outside cluster mode.
func effectiveInScope(ns string, static []string, dynamic *dynamicScopeSet) bool {
	return effectiveInScopeMode(ns, static, dynamic, false)
}

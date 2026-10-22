package push

import (
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- PodLabels propagation (docs/todo/service-graph-from-obi.md) ---

func TestWorkloadFromDeployment_PodLabelsFromTemplate(t *testing.T) {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "vappsim",
			Name:      "orders-app-api",
			Labels:    map[string]string{labelInstance: "orders"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "orders"}},
			},
		},
	}
	info := workloadFromDeployment(d)
	if info.Labels[labelInstance] != "orders" {
		t.Errorf("Labels = %v, object labels must be preserved", info.Labels)
	}
	if info.PodLabels["app"] != "orders" {
		t.Errorf("PodLabels = %v, want app=orders from the pod template", info.PodLabels)
	}
}

func TestWorkloadFromStatefulSet_PodLabelsFromTemplate(t *testing.T) {
	s := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vappsim", Name: "db"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32p(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "db"}},
			},
		},
	}
	info := workloadFromStatefulSet(s)
	if info.PodLabels["app"] != "db" {
		t.Errorf("PodLabels = %v, want app=db", info.PodLabels)
	}
}

func TestWorkloadFromDaemonSet_PodLabelsFromTemplate(t *testing.T) {
	d := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vappsim", Name: "agent"},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "agent"}},
			},
		},
	}
	info := workloadFromDaemonSet(d)
	if info.PodLabels["app"] != "agent" {
		t.Errorf("PodLabels = %v, want app=agent", info.PodLabels)
	}
}

// --- Identity index (rebuildIdentityIndex / identityFor / workloadForSelector) ---

func newTestVappsState() *virtualAppsState {
	return newVirtualAppsState(VirtualAppsOn, "cluster", nil, logr.Discard(), nil)
}

func TestIdentityFor_ResolvesThroughGroupNameFor(t *testing.T) {
	v := newTestVappsState()
	v.rebuildIdentityIndex([]workloadInfo{
		wl(workloadDeployment, "vappsim", "orders-app-api", map[string]string{labelInstance: "orders"}),
	})
	identity, ok := v.identityFor("vappsim", "orders-app-api")
	if !ok || identity != "orders" {
		t.Fatalf("identityFor = (%q, %v), want (\"orders\", true)", identity, ok)
	}
}

// TestIdentityFor_ResolvesRolloutName proves the identity index is generic
// across kinds: a Rollout-owned workload resolves through GroupNameFor
// exactly like a Deployment-owned one.
func TestIdentityFor_ResolvesRolloutName(t *testing.T) {
	v := newTestVappsState()
	v.rebuildIdentityIndex([]workloadInfo{
		wl(workloadRollout, "ops", "checkout-rollout", map[string]string{labelInstance: "checkout"}),
	})
	identity, ok := v.identityFor("ops", "checkout-rollout")
	if !ok || identity != "checkout" {
		t.Fatalf("identityFor = (%q, %v), want (\"checkout\", true) for a Rollout-owned workload", identity, ok)
	}
}

func TestIdentityFor_UnknownOwnerIsNotOK(t *testing.T) {
	v := newTestVappsState()
	v.rebuildIdentityIndex(nil)
	if _, ok := v.identityFor("vappsim", "nonexistent"); ok {
		t.Error("identityFor on an untracked owner should report ok=false")
	}
}

func TestIdentityFor_RespectsGroupOverrideAnnotation(t *testing.T) {
	v := newTestVappsState()
	w := wl(workloadDeployment, "ops", "neo4j-shared", map[string]string{labelInstance: "ignored"})
	w.GroupOverride = "neo4j"
	v.rebuildIdentityIndex([]workloadInfo{w})
	identity, ok := v.identityFor("ops", "neo4j-shared")
	if !ok || identity != "neo4j" {
		t.Errorf("identityFor = (%q, %v), want (\"neo4j\", true) — %s must outrank labels", identity, ok, annotationVirtualAppGroup)
	}
}

func TestWorkloadForSelector_ExactMatch(t *testing.T) {
	v := newTestVappsState()
	w := wl(workloadDeployment, "vappsim", "orders-app-api", nil)
	w.PodLabels = map[string]string{"app": "orders"}
	v.rebuildIdentityIndex([]workloadInfo{w})
	owner, ok := v.workloadForSelector("vappsim", map[string]string{"app": "orders"})
	if !ok || owner != "orders-app-api" {
		t.Fatalf("workloadForSelector = (%q, %v), want (\"orders-app-api\", true)", owner, ok)
	}
}

func TestWorkloadForSelector_SupersetLabelsStillMatch(t *testing.T) {
	v := newTestVappsState()
	w := wl(workloadDeployment, "vappsim", "orders-app-api", nil)
	w.PodLabels = map[string]string{"app": "orders", "extra": "label"}
	v.rebuildIdentityIndex([]workloadInfo{w})
	owner, ok := v.workloadForSelector("vappsim", map[string]string{"app": "orders"})
	if !ok || owner != "orders-app-api" {
		t.Fatalf("workloadForSelector = (%q, %v), want (\"orders-app-api\", true) — a superset pod-label set still satisfies the selector", owner, ok)
	}
}

func TestWorkloadForSelector_NoMatch(t *testing.T) {
	v := newTestVappsState()
	w := wl(workloadDeployment, "vappsim", "orders-app-api", nil)
	w.PodLabels = map[string]string{"app": "orders"}
	v.rebuildIdentityIndex([]workloadInfo{w})
	if _, ok := v.workloadForSelector("vappsim", map[string]string{"app": "billing"}); ok {
		t.Error("workloadForSelector should report ok=false when no workload's pod labels satisfy the selector")
	}
}

func TestWorkloadForSelector_EmptySelectorNeverMatches(t *testing.T) {
	v := newTestVappsState()
	w := wl(workloadDeployment, "vappsim", "orders-app-api", nil)
	w.PodLabels = map[string]string{"app": "orders"}
	v.rebuildIdentityIndex([]workloadInfo{w})
	if _, ok := v.workloadForSelector("vappsim", nil); ok {
		t.Error("an empty/nil selector (headless Service) should never match")
	}
}

func TestWorkloadForSelector_AmbiguousPrefersExactLabelSetMatch(t *testing.T) {
	v := newTestVappsState()
	exact := wl(workloadDeployment, "vappsim", "orders-app-api", nil)
	exact.PodLabels = map[string]string{"app": "orders"}
	superset := wl(workloadDeployment, "vappsim", "orders-app-api-canary", nil)
	superset.PodLabels = map[string]string{"app": "orders", "track": "canary"}
	v.rebuildIdentityIndex([]workloadInfo{superset, exact})

	owner, ok := v.workloadForSelector("vappsim", map[string]string{"app": "orders"})
	if !ok || owner != "orders-app-api" {
		t.Fatalf("workloadForSelector = (%q, %v), want the EXACT label-set match (\"orders-app-api\"), not the superset one", owner, ok)
	}
}

func TestWorkloadForSelector_AmbiguousNoExactMatchPicksLexicallyFirst(t *testing.T) {
	v := newTestVappsState()
	b := wl(workloadDeployment, "vappsim", "b-app", nil)
	b.PodLabels = map[string]string{"app": "shared", "x": "1"}
	a := wl(workloadDeployment, "vappsim", "a-app", nil)
	a.PodLabels = map[string]string{"app": "shared", "y": "2"}
	v.rebuildIdentityIndex([]workloadInfo{b, a})

	owner, ok := v.workloadForSelector("vappsim", map[string]string{"app": "shared"})
	if !ok || owner != "a-app" {
		t.Fatalf("workloadForSelector = (%q, %v), want the lexically-first name (\"a-app\") when no candidate matches exactly", owner, ok)
	}
}

func TestRebuildIdentityIndex_RebuildsFromScratchEachCall(t *testing.T) {
	v := newTestVappsState()
	v.rebuildIdentityIndex([]workloadInfo{
		wl(workloadDeployment, "vappsim", "orders-app-api", map[string]string{labelInstance: "orders"}),
	})
	if _, ok := v.identityFor("vappsim", "orders-app-api"); !ok {
		t.Fatal("expected orders-app-api to resolve after the first rebuild")
	}

	// A workload that has since disappeared (deleted, or moved out of scope)
	// must not survive into the next rebuild — this is a REPLACE, not a merge.
	v.rebuildIdentityIndex([]workloadInfo{
		wl(workloadDeployment, "vappsim", "billing-app-api", map[string]string{labelInstance: "billing"}),
	})
	if _, ok := v.identityFor("vappsim", "orders-app-api"); ok {
		t.Error("orders-app-api should no longer resolve after a rebuild that dropped it")
	}
	if identity, ok := v.identityFor("vappsim", "billing-app-api"); !ok || identity != "billing" {
		t.Errorf("identityFor(billing-app-api) = (%q, %v), want (\"billing\", true)", identity, ok)
	}
}

// --- *Pusher passthrough nil-safety ---

func TestPusher_IdentityFor_NilWhenVirtualAppsOff(t *testing.T) {
	p := &Pusher{} // bare literal — vapps is nil, as with --virtual-apps=off
	if _, ok := p.IdentityFor("vappsim", "orders-app-api"); ok {
		t.Error("IdentityFor must report ok=false when virtual-app tracking is off")
	}
	if _, ok := p.WorkloadForSelector("vappsim", map[string]string{"app": "orders"}); ok {
		t.Error("WorkloadForSelector must report ok=false when virtual-app tracking is off")
	}
}

func TestPusher_ServiceLookups_NilWhenIndexNotWired(t *testing.T) {
	p := &Pusher{} // bare literal — svcIndex nil, as with a pre-existing test/no New()
	if _, ok := p.ServiceByName("ns", "svc"); ok {
		t.Error("ServiceByName must report ok=false when the index was never registered")
	}
	if _, _, ok := p.ServiceByClusterIP("10.0.0.1"); ok {
		t.Error("ServiceByClusterIP must report ok=false when the index was never registered")
	}
	if _, _, ok := p.IngressBackend("example.com"); ok {
		t.Error("IngressBackend must report ok=false when the index was never registered")
	}
}

// Tests for the virtual-app-identity addendum (docs/todo/virtual-apps.md,
// the annotationkeys.VirtualAppIdentity section): GroupNameFor resolves
// every workload in a namespace to its own name — absent the annotation, or
// with it set to the explicit "workload" — ignoring the part-of/instance/name
// label chain entirely; a namespace annotated triageagent.dev/virtual-app-identity:
// "labels" opts into that chain instead. namespaceAnnotations' own parsing
// rules are covered in namespace_defaults_test.go; GroupNameFor's mode
// precedence (workload mode ignoring labels, group override still winning,
// labels mode using the chain) is covered in virtualapps_test.go. This file
// covers synthesis, the identity index, and the runtime transition.
package push

import (
	"context"
	"sort"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-logr/logr"
)

// threeDeploymentsSharingInstance builds three Ready Deployments in ns, all
// carrying the same app.kubernetes.io/instance label — the "a whole Helm
// release stamps one instance label on dozens of workloads" shape the
// virtual-app-identity addendum exists for.
func threeDeploymentsSharingInstance(ns, instance string) []workloadInfo {
	var out []workloadInfo
	for _, name := range []string{"svc1", "svc2", "svc3"} {
		out = append(out, workloadInfo{
			Kind: workloadDeployment, Namespace: ns, Name: name, Ready: true,
			Labels: map[string]string{labelInstance: instance},
		})
	}
	return out
}

func vappNames(apps []*VirtualApp) []string {
	names := make([]string, len(apps))
	for i, a := range apps {
		names[i] = a.Name
	}
	sort.Strings(names)
	return names
}

// --- Synthesis: workload mode yields one vapp per workload; labels mode
// folds them into the shared instance-derived group ---

func TestSynthesis_WorkloadMode_OneVappPerWorkload(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{IdentityMode: IdentityModeWorkload}, true
	}
	v := &virtualAppsState{nsDefaultsFn: nsFn, workloads: map[string]workloadInfo{}}
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		v.workloads[workloadKey(w.Kind, w.Namespace, w.Name)] = w
	}

	groups := groupWorkloads(v.workloadsSnapshot())
	synthesized := buildSynthesized(groups)

	if len(synthesized) != 3 {
		t.Fatalf("synthesized = %d groups, want 3 (one per workload); got %+v", len(synthesized), synthesized)
	}
	for _, name := range []string{"svc1", "svc2", "svc3"} {
		if _, ok := synthesized[groupKey{Namespace: "ops", Name: name}]; !ok {
			t.Errorf("missing vapp named %q; got %+v", name, synthesized)
		}
	}
}

func TestSynthesis_LabelsMode_OneVappForTheSharedInstance(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{IdentityMode: IdentityModeLabels}, true
	}
	v := &virtualAppsState{nsDefaultsFn: nsFn, workloads: map[string]workloadInfo{}}
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		v.workloads[workloadKey(w.Kind, w.Namespace, w.Name)] = w
	}

	groups := groupWorkloads(v.workloadsSnapshot())
	synthesized := buildSynthesized(groups)

	if len(synthesized) != 1 {
		t.Fatalf("synthesized = %d groups, want 1 (folded by the shared instance label); got %+v", len(synthesized), synthesized)
	}
	app, ok := synthesized[groupKey{Namespace: "ops", Name: "myapp"}]
	if !ok {
		t.Fatalf("missing the folded vapp named myapp; got %+v", synthesized)
	}
	if len(app.Status.Objects) != 3 {
		t.Errorf("folded vapp has %d components, want 3", len(app.Status.Objects))
	}
}

// TestSynthesis_DefaultMode_NoAnnotation_OneVappPerWorkload proves the
// default — no triageagent.dev/virtual-app-identity annotation at all — behaves
// exactly like the explicit workload mode above, even though the namespace
// was actually observed (nsFn returns ok=true with a zero-value
// namespaceDefaults, the shape namespaceAnnotations produces for an
// unannotated namespace).
func TestSynthesis_DefaultMode_NoAnnotation_OneVappPerWorkload(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) { return namespaceDefaults{}, true }
	v := &virtualAppsState{nsDefaultsFn: nsFn, workloads: map[string]workloadInfo{}}
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		v.workloads[workloadKey(w.Kind, w.Namespace, w.Name)] = w
	}

	groups := groupWorkloads(v.workloadsSnapshot())
	synthesized := buildSynthesized(groups)

	if len(synthesized) != 3 {
		t.Fatalf("synthesized = %d groups, want 3 (default identity names by workload, ignoring the shared instance label); got %+v", len(synthesized), synthesized)
	}
	for _, name := range []string{"svc1", "svc2", "svc3"} {
		if _, ok := synthesized[groupKey{Namespace: "ops", Name: name}]; !ok {
			t.Errorf("missing vapp named %q; got %+v", name, synthesized)
		}
	}
}

// TestSynthesis_DefaultMode_NoNamespaceInformerWired_OneVappPerWorkload
// covers the other "default" shape: no nsDefaultsFn at all (namespace
// informer not wired, or --virtual-apps used before this feature existed) —
// every workload's IdentityMode stays at its own zero value, which resolves
// identically to the explicit workload mode.
func TestSynthesis_DefaultMode_NoNamespaceInformerWired_OneVappPerWorkload(t *testing.T) {
	v := &virtualAppsState{workloads: map[string]workloadInfo{}}
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		v.workloads[workloadKey(w.Kind, w.Namespace, w.Name)] = w
	}

	groups := groupWorkloads(v.workloadsSnapshot())
	synthesized := buildSynthesized(groups)

	if len(synthesized) != 3 {
		t.Fatalf("synthesized = %d groups, want 3 (no namespace informer wired: zero-value IdentityMode still defaults to workload); got %+v", len(synthesized), synthesized)
	}
}

// TestSynthesis_InvalidIdentityAnnotation_BehavesAsWorkload proves an
// unparsable triageagent.dev/virtual-app-identity value (parsed end to end through
// namespaceAnnotations, the same path Pusher.onNamespace uses) synthesizes
// exactly like the default: one vapp per workload, ignoring the shared
// instance label — invalid is never treated as the opt-in "labels" chain.
func TestSynthesis_InvalidIdentityAnnotation_BehavesAsWorkload(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceAnnotations(map[string]string{annotationVirtualAppIdentity: "garbage"}), true
	}
	v := &virtualAppsState{nsDefaultsFn: nsFn, workloads: map[string]workloadInfo{}}
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		v.workloads[workloadKey(w.Kind, w.Namespace, w.Name)] = w
	}

	groups := groupWorkloads(v.workloadsSnapshot())
	synthesized := buildSynthesized(groups)

	if len(synthesized) != 3 {
		t.Fatalf("synthesized = %d groups, want 3 (invalid annotation value falls back to workload identity, not the label chain); got %+v", len(synthesized), synthesized)
	}
}

// --- Identity index agrees with synthesis in workload mode ---

func TestIdentityIndex_AgreesWithSynthesis_WorkloadMode(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{IdentityMode: IdentityModeWorkload}, true
	}
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), func(*Frame) {})
	v.nsDefaultsFn = nsFn
	v.ctx = context.Background()
	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		info := w
		v.applyWorkload(workloadKey(w.Kind, w.Namespace, w.Name), &info, false)
	}
	v.recomputeNow(context.Background())

	apps := v.snapshotApps()
	if len(vappNames(apps)) != 3 {
		t.Fatalf("setup: snapshotApps = %+v, want 3 vapps", apps)
	}
	for _, name := range []string{"svc1", "svc2", "svc3"} {
		identity, ok := v.identityFor("ops", name)
		if !ok {
			t.Errorf("identityFor(ops, %s) ok=false, want true", name)
			continue
		}
		if identity != name {
			t.Errorf("identityFor(ops, %s) = %q, want %q (must agree with the workload-mode vapp name)", name, identity, name)
		}
	}
}

// --- Runtime transition: flipping labels -> workload deletes the folded
// vapp and adds the per-workload ones (docs/todo/virtual-apps.md's "renames
// = delete + add" over the existing delta path) ---

func TestPusher_OnNamespace_IdentityModeFlip_LabelsToWorkload_EmitsDeleteAndAdds(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}
	vapps := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), enqueue)
	vapps.ctx = context.Background()

	p := &Pusher{
		opts:       Options{ClusterName: "cluster-a", Tenant: "primary", Namespaces: []string{"ops"}},
		log:        logr.Discard(),
		nsDefaults: newNsDefaultsStore(),
		vapps:      vapps,
	}
	vapps.nsDefaultsFn = p.nsDefaults.get

	// Seed the starting state explicitly: labels mode, so the shared instance
	// label folds all three Deployments into one vapp before the flip below.
	// The default (no annotation) is workload mode, which would already
	// yield three separate vapps and defeat the point of this test.
	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ops", Annotations: map[string]string{annotationVirtualAppIdentity: "labels"}},
	}, false)

	for _, w := range threeDeploymentsSharingInstance("ops", "myapp") {
		info := w
		vapps.applyWorkload(workloadKey(w.Kind, w.Namespace, w.Name), &info, false)
	}
	vapps.recomputeNow(context.Background())
	if got := vappNames(vapps.snapshotApps()); len(got) != 1 || got[0] != "myapp" {
		t.Fatalf("setup: snapshotApps = %v, want exactly [myapp]", got)
	}
	mu.Lock()
	frames = nil // discard the setup OpAdd
	mu.Unlock()

	p.onNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ops", Annotations: map[string]string{annotationVirtualAppIdentity: "workload"}},
	}, false)
	vapps.recomputeNow(context.Background())

	got := vappNames(vapps.snapshotApps())
	want := []string{"svc1", "svc2", "svc3"}
	if len(got) != len(want) {
		t.Fatalf("snapshotApps after flip = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("snapshotApps after flip = %v, want %v", got, want)
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()
	var deletedMyapp, addedPerWorkload int
	for _, f := range frames {
		if f.Kind != KindAppDelta {
			continue
		}
		if f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "myapp" {
			deletedMyapp++
		}
		if f.AppDelta.Op == OpAdd {
			for _, name := range want {
				if f.AppDelta.App.Name == name {
					addedPerWorkload++
				}
			}
		}
	}
	if deletedMyapp != 1 {
		t.Errorf("want exactly one OpDelete for the folded vapp myapp, got %d; frames = %+v", deletedMyapp, frames)
	}
	if addedPerWorkload != 3 {
		t.Errorf("want exactly 3 OpAdd for svc1/svc2/svc3, got %d; frames = %+v", addedPerWorkload, frames)
	}

	if d, ok := p.nsDefaults.get("ops"); !ok || d.IdentityMode != IdentityModeWorkload {
		t.Errorf("nsDefaults.get(ops) = (%+v, %v), want IdentityMode=workload", d, ok)
	}
}

package push

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/triageagent-dev/agent/internal/annotationkeys"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

func int32p(v int32) *int32 { return &v }

// conditionTrue reports whether app's condition of type condType is True —
// a test-only helper now that nothing in production ever reads a condition
// back off a VirtualApp it just built.
func conditionTrue(app *VirtualApp, condType ConditionType) bool {
	for _, c := range app.Status.Conditions {
		if c.Type == condType {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// toUnstructuredObj converts obj to unstructured form via the same
// DefaultUnstructuredConverter production code uses. Panics on a conversion
// error — every call site in this test suite builds fixtures directly, which
// are always convertible.
func toUnstructuredObj(obj any) *unstructured.Unstructured {
	m, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: m}
}

// workloadFromDeployment/workloadFromStatefulSet/workloadFromDaemonSet are
// test-only shims preserving this suite's typed call sites: convert the
// typed fixture to unstructured (as a real informer event would deliver it)
// and adapt it through the one production adapter, workloadFromUnstructured.
func workloadFromDeployment(d *appsv1.Deployment) workloadInfo {
	return workloadFromUnstructured(toUnstructuredObj(d), string(workloadDeployment))
}

func workloadFromStatefulSet(s *appsv1.StatefulSet) workloadInfo {
	return workloadFromUnstructured(toUnstructuredObj(s), string(workloadStatefulSet))
}

func workloadFromDaemonSet(d *appsv1.DaemonSet) workloadInfo {
	return workloadFromUnstructured(toUnstructuredObj(d), string(workloadDaemonSet))
}

// --- ValidateVirtualAppsMode ---

func TestValidateVirtualAppsMode(t *testing.T) {
	tests := []struct {
		mode    string
		wantErr bool
	}{
		{"", false},
		{"off", false},
		{"shadow", true},
		{"on", false},
		{"ON", true},
		{"bogus", true},
	}
	for _, tt := range tests {
		err := ValidateVirtualAppsMode(tt.mode)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateVirtualAppsMode(%q) err=%v, wantErr=%v", tt.mode, err, tt.wantErr)
		}
	}
}

// --- Grouping ---

// wl builds a workloadInfo in the explicit IdentityModeLabels mode — every
// caller in this suite that passes part-of/instance/name labels is testing
// the label chain itself, not the default identity mode (covered separately
// in virtual_app_identity_test.go).
func wl(kind workloadKind, ns, name string, labels map[string]string) workloadInfo {
	return workloadInfo{Kind: kind, Namespace: ns, Name: name, Labels: labels, IdentityMode: IdentityModeLabels}
}

func TestGroupNameFor(t *testing.T) {
	tests := []struct {
		name string
		w    workloadInfo
		want string
	}{
		{
			name: "part-of wins over instance and name",
			w: wl(workloadDeployment, "ops", "api-deploy", map[string]string{
				labelPartOf: "checkout", labelInstance: "checkout-blue", labelName: "api",
			}),
			want: "checkout",
		},
		{
			name: "instance wins over name when part-of absent",
			w:    wl(workloadDeployment, "ops", "api-deploy", map[string]string{labelInstance: "checkout-blue", labelName: "api"}),
			want: "checkout-blue",
		},
		{
			name: "name used when part-of/instance absent",
			w:    wl(workloadDeployment, "ops", "api-deploy", map[string]string{labelName: "api"}),
			want: "api",
		},
		{
			name: "self fallback with no grouping labels",
			w:    wl(workloadDeployment, "ops", "solo-worker", nil),
			want: "solo-worker",
		},
		{
			name: "blank labels treated as absent",
			w:    wl(workloadDeployment, "ops", "solo-worker", map[string]string{labelPartOf: "  ", labelInstance: "", labelName: "api"}),
			want: "api",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupNameFor(tt.w); got != tt.want {
				t.Errorf("groupNameFor() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Annotation control: exclusion + group-membership override ---

func TestGroupNameFor_GroupAnnotationWins(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels:        map[string]string{labelPartOf: "checkout", labelInstance: "checkout-blue", labelName: "api"},
		GroupOverride: "billing",
	}
	if got := groupNameFor(w); got != "billing" {
		t.Errorf("groupNameFor() = %q, want %q (group annotation beats part-of/instance/name)", got, "billing")
	}
}

func TestGroupNameFor_BlankGroupAnnotationTreatedAsAbsent(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels:        map[string]string{labelPartOf: "checkout"},
		GroupOverride: "   ",
		IdentityMode:  IdentityModeLabels,
	}
	if got := groupNameFor(w); got != "checkout" {
		t.Errorf("groupNameFor() = %q, want %q (blank group annotation falls back to part-of)", got, "checkout")
	}
}

// --- IdentityMode: triageagent.dev/virtual-app-identity (virtual-app-identity addendum) ---

func TestGroupNameFor_WorkloadMode_IgnoresLabels(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels:       map[string]string{labelPartOf: "checkout", labelInstance: "checkout-blue", labelName: "api"},
		IdentityMode: IdentityModeWorkload,
	}
	if got := groupNameFor(w); got != "api-deploy" {
		t.Errorf("groupNameFor() = %q, want %q (workload mode ignores part-of/instance/name)", got, "api-deploy")
	}
}

func TestGroupNameFor_WorkloadMode_GroupOverrideStillWins(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		GroupOverride: "billing",
		IdentityMode:  IdentityModeWorkload,
	}
	if got := groupNameFor(w); got != "billing" {
		t.Errorf("groupNameFor() = %q, want %q (group override outranks workload mode too)", got, "billing")
	}
}

func TestGroupNameFor_LabelsMode_UsesTheChain(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels:       map[string]string{labelInstance: "checkout-blue"},
		IdentityMode: IdentityModeLabels,
	}
	if got := groupNameFor(w); got != "checkout-blue" {
		t.Errorf("groupNameFor() = %q, want %q (explicit labels mode opts into the part-of/instance/name chain)", got, "checkout-blue")
	}
}

func TestGroupNameFor_LabelsMode_GroupOverrideStillWins(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels:        map[string]string{labelInstance: "checkout-blue"},
		GroupOverride: "billing",
		IdentityMode:  IdentityModeLabels,
	}
	if got := groupNameFor(w); got != "billing" {
		t.Errorf("groupNameFor() = %q, want %q (group override outranks labels mode too)", got, "billing")
	}
}

// TestGroupNameFor_ZeroValueMode_IsWorkload proves the zero value of
// workloadInfo.IdentityMode ("", an unannotated namespace) behaves
// identically to the explicit IdentityModeWorkload — the default ignores
// labels entirely, even when a chain label is present.
func TestGroupNameFor_ZeroValueMode_IsWorkload(t *testing.T) {
	w := workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api-deploy",
		Labels: map[string]string{labelPartOf: "checkout", labelInstance: "checkout-blue", labelName: "api"},
	}
	if got := groupNameFor(w); got != "api-deploy" {
		t.Errorf("groupNameFor() = %q, want %q (zero-value mode is the workload default, ignoring labels)", got, "api-deploy")
	}
}

func TestGroupWorkloads_GroupAnnotation_SplitsReleaseAggregate(t *testing.T) {
	// Two workloads share instance=prod-ecosystem but each declares its own vapp
	// group via the annotation — they must land in two separate, non-colliding
	// groups instead of the one instance-derived group.
	workloads := []workloadInfo{
		{Kind: workloadDeployment, Namespace: "ops", Name: "api",
			Labels: map[string]string{labelInstance: "prod-ecosystem"}, GroupOverride: "checkout-api"},
		{Kind: workloadDeployment, Namespace: "ops", Name: "worker",
			Labels: map[string]string{labelInstance: "prod-ecosystem"}, GroupOverride: "checkout-worker"},
	}
	groups := groupWorkloads(workloads)
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2 (each annotation value is its own group)", len(groups))
	}
	if _, ok := groups[groupKey{Namespace: "ops", Name: "checkout-api"}]; !ok {
		t.Error("missing group ops/checkout-api")
	}
	if _, ok := groups[groupKey{Namespace: "ops", Name: "checkout-worker"}]; !ok {
		t.Error("missing group ops/checkout-worker")
	}
	if _, ok := groups[groupKey{Namespace: "ops", Name: "prod-ecosystem"}]; ok {
		t.Error("instance-derived group ops/prod-ecosystem should not exist once both members declare their own group")
	}
}

// TestGroupWorkloads_GroupAnnotation_SharedResourceJoinsOneVapp is the
// docs/todo/virtual-apps.md shared-resource example: a neo4j Deployment used by
// several charts in one namespace, each annotated
// triageagent.dev/virtual-app-group: neo4j, folds into ONE shared vapp — same group value
// means same group, this is membership, not a display label. Two otherwise
// unrelated workloads (disagreeing part-of) still land together.
func TestGroupWorkloads_GroupAnnotation_SharedResourceJoinsOneVapp(t *testing.T) {
	workloads := []workloadInfo{
		{Kind: workloadDeployment, Namespace: "ops", Name: "api", Labels: map[string]string{labelPartOf: "alpha"}, GroupOverride: "neo4j"},
		{Kind: workloadStatefulSet, Namespace: "ops", Name: "db", Labels: map[string]string{labelPartOf: "beta"}, GroupOverride: "neo4j"},
	}
	groups := groupWorkloads(workloads)
	if len(groups) != 1 {
		t.Fatalf("len(groups) = %d, want 1 (both declare the same group)", len(groups))
	}
	members := groups[groupKey{Namespace: "ops", Name: "neo4j"}]
	if len(members) != 2 {
		t.Fatalf("neo4j members = %d, want 2", len(members))
	}
}

func TestGroupWorkloads_ExcludedWorkloadDropped(t *testing.T) {
	workloads := []workloadInfo{
		wl(workloadDeployment, "ops", "api", map[string]string{labelPartOf: "checkout"}),
		{Kind: workloadStatefulSet, Namespace: "ops", Name: "db", Labels: map[string]string{labelPartOf: "checkout"}, Excluded: true},
		{Kind: workloadDeployment, Namespace: "ops", Name: "solo-excluded", Excluded: true},
	}
	groups := groupWorkloads(workloads)

	checkout := groups[groupKey{Namespace: "ops", Name: "checkout"}]
	if len(checkout) != 1 {
		t.Fatalf("ops/checkout members = %d, want 1 (excluded db must not count toward membership)", len(checkout))
	}
	if checkout[0].Name != "api" {
		t.Errorf("ops/checkout sole member = %q, want api", checkout[0].Name)
	}
	if _, ok := groups[groupKey{Namespace: "ops", Name: "solo-excluded"}]; ok {
		t.Error("a wholly-excluded single-workload group must not exist at all")
	}
}

func TestGroupWorkloads_ExcludedNotReadinessCounted(t *testing.T) {
	// An excluded, not-ready member must not drag down the group's readiness —
	// it is dropped before synthesis ever sees it.
	workloads := []workloadInfo{
		{Kind: workloadDeployment, Namespace: "ops", Name: "api", Labels: map[string]string{labelPartOf: "checkout"}, IdentityMode: IdentityModeLabels, Ready: true},
		{Kind: workloadStatefulSet, Namespace: "ops", Name: "db", Labels: map[string]string{labelPartOf: "checkout"}, IdentityMode: IdentityModeLabels, Ready: false, Excluded: true},
	}
	groups := groupWorkloads(workloads)
	app := synthesizeApplication(groupKey{Namespace: "ops", Name: "checkout"}, groups[groupKey{Namespace: "ops", Name: "checkout"}])
	if !conditionTrue(app, conditionReady) {
		t.Error("Ready condition = False, want True (the not-ready member is excluded and must not count)")
	}
	if app.Status.ComponentsReady != "1/1" {
		t.Errorf("componentsReady = %q, want 1/1 (excluded member dropped from denominator)", app.Status.ComponentsReady)
	}
}

func TestGroupWorkloads_GroupAnnotationAndExclusion_ExclusionWins(t *testing.T) {
	// A workload can carry both annotations at once; exclusion must take
	// precedence — the group annotation must not resurrect an excluded workload
	// into a group of its own or anyone else's.
	workloads := []workloadInfo{
		{Kind: workloadDeployment, Namespace: "ops", Name: "api", GroupOverride: "billing", Excluded: true},
		{Kind: workloadStatefulSet, Namespace: "ops", Name: "db", GroupOverride: "billing", Excluded: false},
	}
	groups := groupWorkloads(workloads)
	billing := groups[groupKey{Namespace: "ops", Name: "billing"}]
	if len(billing) != 1 {
		t.Fatalf("billing members = %d, want 1 (excluded member must not join even under its own group annotation)", len(billing))
	}
	if billing[0].Name != "db" {
		t.Errorf("billing sole member = %q, want db", billing[0].Name)
	}
}

func TestWorkloadFromDeployment_AnnotationsReadFromObjectNotPodTemplate(t *testing.T) {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ops",
			Name:        "api",
			Annotations: map[string]string{annotationVirtualApp: "false", annotationVirtualAppGroup: "billing"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(1),
			Template: corev1.PodTemplateSpec{
				// Pod-template annotations must be ignored entirely.
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationVirtualApp: "true", annotationVirtualAppGroup: "wrong"}},
			},
		},
	}
	info := workloadFromDeployment(d)
	if !info.Excluded {
		t.Error("Excluded = false, want true (object annotation, not pod-template, must win)")
	}
	if info.GroupOverride != "billing" {
		t.Errorf("GroupOverride = %q, want %q (object annotation, not pod-template, must win)", info.GroupOverride, "billing")
	}
}

func TestWorkloadFromDeployment_TrueValueDoesNotExclude(t *testing.T) {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ops",
			Name:        "api",
			Annotations: map[string]string{annotationVirtualApp: "true"},
		},
		Spec: appsv1.DeploymentSpec{Replicas: int32p(1)},
	}
	info := workloadFromDeployment(d)
	if info.Excluded {
		t.Error("Excluded = true, want false (only the literal value \"false\" excludes)")
	}
}

func TestGroupWorkloads(t *testing.T) {
	workloads := []workloadInfo{
		wl(workloadDeployment, "ops", "api", map[string]string{labelPartOf: "checkout"}),
		wl(workloadStatefulSet, "ops", "db", map[string]string{labelPartOf: "checkout"}),
		wl(workloadDeployment, "ops", "solo", nil),
		wl(workloadDeployment, "dev", "api", map[string]string{labelPartOf: "checkout"}), // different namespace: separate group
	}
	groups := groupWorkloads(workloads)

	if len(groups) != 3 {
		t.Fatalf("len(groups) = %d, want 3 (ops/checkout, ops/solo, dev/checkout)", len(groups))
	}
	opsCheckout := groups[groupKey{Namespace: "ops", Name: "checkout"}]
	if len(opsCheckout) != 2 {
		t.Fatalf("ops/checkout members = %d, want 2", len(opsCheckout))
	}
	// Deterministic (Kind, Name) order: Deployment < StatefulSet.
	if opsCheckout[0].Kind != workloadDeployment || opsCheckout[1].Kind != workloadStatefulSet {
		t.Errorf("ops/checkout member order = %+v, want [Deployment, StatefulSet]", opsCheckout)
	}
	opsSolo := groups[groupKey{Namespace: "ops", Name: "solo"}]
	if len(opsSolo) != 1 {
		t.Fatalf("ops/solo members = %d, want 1 (single-workload fallback)", len(opsSolo))
	}
	devCheckout := groups[groupKey{Namespace: "dev", Name: "checkout"}]
	if len(devCheckout) != 1 {
		t.Fatalf("dev/checkout members = %d, want 1 (namespace-scoped grouping)", len(devCheckout))
	}
}

// --- Synthesis across kinds (docs/todo/virtual-apps.md "Workload resources") ---

// workloadRollout is a plain workloadKind value for a CRD-owned kind this
// repo carries no typed Go struct for — grouping/synthesis/identity never
// branch on kind, so an arbitrary string is exactly as valid as the three
// DefaultWorkloadResources constants.
const workloadRollout workloadKind = "Rollout"

// TestGroupWorkloads_DeploymentAndRolloutShareGroup proves grouping is
// generic across kinds: a Deployment and a Rollout sharing part-of join the
// SAME group exactly as two Deployments would.
func TestGroupWorkloads_DeploymentAndRolloutShareGroup(t *testing.T) {
	workloads := []workloadInfo{
		wl(workloadDeployment, "ops", "api", map[string]string{labelPartOf: "checkout"}),
		wl(workloadRollout, "ops", "api-canary", map[string]string{labelPartOf: "checkout"}),
	}
	groups := groupWorkloads(workloads)
	if len(groups) != 1 {
		t.Fatalf("len(groups) = %d, want 1 (Deployment + Rollout sharing part-of must group together)", len(groups))
	}
	members := groups[groupKey{Namespace: "ops", Name: "checkout"}]
	if len(members) != 2 {
		t.Fatalf("checkout members = %+v, want 2", members)
	}
	// Deterministic (Kind, Name) order: Deployment < Rollout.
	if members[0].Kind != workloadDeployment || members[1].Kind != workloadRollout {
		t.Errorf("checkout member order = %+v, want [Deployment, Rollout]", members)
	}
}

// TestSynthesizeApplication_RolloutAloneNamedByIdentityChain: a lone
// Rollout, with no group override and the explicit labels identity mode, is
// named exactly the way a lone Deployment would be — by the
// part-of/instance/name label chain.
func TestSynthesizeApplication_RolloutAloneNamedByIdentityChain(t *testing.T) {
	w := wl(workloadRollout, "ops", "checkout-rollout", map[string]string{labelInstance: "checkout"})
	w.Ready = true
	groups := groupWorkloads([]workloadInfo{w})
	key := groupKey{Namespace: "ops", Name: "checkout"}
	members, ok := groups[key]
	if !ok || len(members) != 1 {
		t.Fatalf("groups = %+v, want one group named %q (instance label chain)", groups, key.Name)
	}
	app := synthesizeApplication(key, members)
	if app.Name != "checkout" {
		t.Errorf("synthesized app name = %q, want %q", app.Name, "checkout")
	}
	if len(app.Status.Objects) != 1 || app.Status.Objects[0].Kind != "Rollout" {
		t.Errorf("component list = %+v, want one Rollout component", app.Status.Objects)
	}
}

// TestSynthesizeApplication_RolloutAloneNamedByWorkloadUnderIdentityModeWorkload
// is the same lone Rollout under the namespace's triageagent.dev/virtual-app-identity:
// "workload" mode — it must group under its OWN name, ignoring labels
// entirely, exactly as a lone Deployment would under that mode.
func TestSynthesizeApplication_RolloutAloneNamedByWorkloadUnderIdentityModeWorkload(t *testing.T) {
	w := workloadInfo{
		Kind: workloadRollout, Namespace: "ops", Name: "checkout-rollout",
		Labels: map[string]string{labelInstance: "checkout"}, IdentityMode: IdentityModeWorkload, Ready: true,
	}
	groups := groupWorkloads([]workloadInfo{w})
	key := groupKey{Namespace: "ops", Name: "checkout-rollout"}
	if _, ok := groups[key]; !ok {
		t.Fatalf("groups = %+v, want a group named %q (the workload's own name, ignoring labels)", groups, key.Name)
	}
}

// --- Readiness / generic adapter (workloadFromUnstructured) ---

// rolloutObj builds a synthetic Argo Rollout-shaped unstructured object —
// this repo carries no typed Rollout Go struct, so its spec.replicas/
// status.readyReplicas are set directly as a map, matching the JSON shape a
// real Rollout decodes to (spec.replicas and status.readyReplicas are the
// same field names and semantics as Deployment's).
func rolloutObj(namespace, name string, replicas, readyReplicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": namespace, "name": name},
		"spec":     map[string]any{"replicas": replicas},
		"status":   map[string]any{"readyReplicas": readyReplicas},
	}}
}

// customKindObj builds a bare unstructured object of an arbitrary custom
// kind — no spec.replicas, no status replica-count fields — optionally
// carrying a status.conditions entry.
func customKindObj(namespace, name string, conditions ...map[string]any) *unstructured.Unstructured {
	obj := map[string]any{"metadata": map[string]any{"namespace": namespace, "name": name}}
	if len(conditions) > 0 {
		raw := make([]any, len(conditions))
		for i, c := range conditions {
			raw[i] = c
		}
		obj["status"] = map[string]any{"conditions": raw}
	}
	return &unstructured.Unstructured{Object: obj}
}

func TestWorkloadFromUnstructured_Adapter(t *testing.T) {
	tests := []struct {
		name         string
		obj          *unstructured.Unstructured
		kind         string
		wantReady    bool
		wantExcluded bool
	}{
		{
			name: "Deployment: readyReplicas meets spec.replicas",
			obj: toUnstructuredObj(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"},
				Spec:       appsv1.DeploymentSpec{Replicas: int32p(3)},
				Status:     appsv1.DeploymentStatus{ReadyReplicas: 3},
			}),
			kind:      "Deployment",
			wantReady: true,
		},
		{
			name: "Deployment: readyReplicas short of spec.replicas",
			obj: toUnstructuredObj(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"},
				Spec:       appsv1.DeploymentSpec{Replicas: int32p(3)},
				Status:     appsv1.DeploymentStatus{ReadyReplicas: 2},
			}),
			kind:      "Deployment",
			wantReady: false,
		},
		{
			name: "Deployment: nil replicas defaults desired to 1",
			obj: toUnstructuredObj(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"},
				Status:     appsv1.DeploymentStatus{ReadyReplicas: 1},
			}),
			kind:      "Deployment",
			wantReady: true,
		},
		{
			name: "Deployment: scaled to zero is excluded, not a spurious Ready 1/1",
			obj: toUnstructuredObj(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "api"},
				Spec:       appsv1.DeploymentSpec{Replicas: int32p(0)},
				Status:     appsv1.DeploymentStatus{ReadyReplicas: 0},
			}),
			kind:         "Deployment",
			wantReady:    false,
			wantExcluded: true,
		},
		{
			name: "DaemonSet: numberReady meets status.desiredNumberScheduled",
			obj: toUnstructuredObj(&appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "agent"},
				Status:     appsv1.DaemonSetStatus{NumberReady: 4, DesiredNumberScheduled: 4},
			}),
			kind:      "DaemonSet",
			wantReady: true,
		},
		{
			name: "DaemonSet: numberReady short of status.desiredNumberScheduled",
			obj: toUnstructuredObj(&appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "agent"},
				Status:     appsv1.DaemonSetStatus{NumberReady: 3, DesiredNumberScheduled: 4},
			}),
			kind:      "DaemonSet",
			wantReady: false,
		},
		{
			name: "DaemonSet: scaled to zero is excluded",
			obj: toUnstructuredObj(&appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "agent"},
				Status:     appsv1.DaemonSetStatus{NumberReady: 0, DesiredNumberScheduled: 0},
			}),
			kind:         "DaemonSet",
			wantReady:    false,
			wantExcluded: true,
		},
		{
			name:      "Rollout: readyReplicas meets spec.replicas",
			obj:       rolloutObj("ops", "checkout", 2, 2),
			kind:      "Rollout",
			wantReady: true,
		},
		{
			name:      "Rollout: readyReplicas short of spec.replicas",
			obj:       rolloutObj("ops", "checkout", 2, 1),
			kind:      "Rollout",
			wantReady: false,
		},
		{
			name:      "custom kind: only a True Ready condition",
			obj:       customKindObj("ops", "sandbox", map[string]any{"type": "Ready", "status": "True"}),
			kind:      "Sandbox",
			wantReady: true,
		},
		{
			name:      "custom kind: only a True Available condition",
			obj:       customKindObj("ops", "sandbox", map[string]any{"type": "Available", "status": "True"}),
			kind:      "Sandbox",
			wantReady: true,
		},
		{
			name:      "custom kind with nothing: desired 1, ready 0",
			obj:       customKindObj("ops", "sandbox"),
			kind:      "Sandbox",
			wantReady: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := workloadFromUnstructured(tt.obj, tt.kind)
			if info.Ready != tt.wantReady {
				t.Errorf("Ready = %v, want %v", info.Ready, tt.wantReady)
			}
			if info.Excluded != tt.wantExcluded {
				t.Errorf("Excluded = %v, want %v", info.Excluded, tt.wantExcluded)
			}
			if info.Kind != workloadKind(tt.kind) {
				t.Errorf("Kind = %q, want %q", info.Kind, tt.kind)
			}
		})
	}
}

// --- Version resolution ---

func TestImageTag(t *testing.T) {
	tests := []struct {
		image string
		want  string
	}{
		{"nginx:1.25", "1.25"},
		{"registry.example.com:5000/team/api:v2.3.1", "v2.3.1"},
		{"registry.example.com:5000/team/api", ""},
		{"nginx@sha256:abcdef", ""},          // digest-pinned, no tag
		{"nginx:1.25@sha256:abcdef", "1.25"}, // tag AND digest: digest is dropped first
		{"nginx", ""},
	}
	for _, tt := range tests {
		if got := imageTag(tt.image); got != tt.want {
			t.Errorf("imageTag(%q) = %q, want %q", tt.image, got, tt.want)
		}
	}
}

func TestResolveGroupVersion(t *testing.T) {
	tests := []struct {
		name    string
		members []workloadInfo
		want    string
	}{
		{
			name: "uniform version label wins",
			members: []workloadInfo{
				{Labels: map[string]string{labelVersion: "1.2.3"}, ImageTag: "1.2.3"},
				{Labels: map[string]string{labelVersion: "1.2.3"}, ImageTag: "1.2.4"}, // image tag disagrees; label still wins
			},
			want: "1.2.3",
		},
		{
			name: "disagreeing version label falls back to uniform image tag",
			members: []workloadInfo{
				{Labels: map[string]string{labelVersion: "1.2.3"}, ImageTag: "v9"},
				{Labels: map[string]string{labelVersion: "1.2.4"}, ImageTag: "v9"},
			},
			want: "v9",
		},
		{
			name: "no version label, uniform image tag",
			members: []workloadInfo{
				{Labels: nil, ImageTag: "v9"},
				{Labels: nil, ImageTag: "v9"},
			},
			want: "v9",
		},
		{
			name: "nothing uniform yields empty",
			members: []workloadInfo{
				{Labels: nil, ImageTag: "v9"},
				{Labels: nil, ImageTag: "v10"},
			},
			want: "",
		},
		{
			name:    "no members",
			members: nil,
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveGroupVersion(tt.members); got != tt.want {
				t.Errorf("resolveGroupVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Metadata annotations: extraction (docs/todo/virtual-apps.md annotations
// Phase 2 addendum) ---

func TestWorkloadAnnotations_MetadataFields(t *testing.T) {
	tests := []struct {
		name            string
		annotations     map[string]string
		wantDomain      string
		wantDescription string
		wantTier        int
		wantHasTier     bool
		wantTierInvalid bool
	}{
		{
			name:            "domain, description, and a valid tier",
			annotations:     map[string]string{annotationDomain: "commerce", annotationDescription: "Checkout service", annotationTier: "2"},
			wantDomain:      "commerce",
			wantDescription: "Checkout service",
			wantTier:        2,
			wantHasTier:     true,
		},
		{
			name:        "no annotations at all",
			annotations: nil,
		},
		{
			name:        "tier 0 is a valid, declared value",
			annotations: map[string]string{annotationTier: "0"},
			wantTier:    0,
			wantHasTier: true,
		},
		{
			name:        "tier 9 is the top of the valid range",
			annotations: map[string]string{annotationTier: "9"},
			wantTier:    9,
			wantHasTier: true,
		},
		{
			name:            "tier out of range (10) is unparsable, not just clamped",
			annotations:     map[string]string{annotationTier: "10"},
			wantHasTier:     false,
			wantTierInvalid: true,
		},
		{
			name:            "negative tier is unparsable",
			annotations:     map[string]string{annotationTier: "-1"},
			wantHasTier:     false,
			wantTierInvalid: true,
		},
		{
			name:            "non-numeric tier is unparsable",
			annotations:     map[string]string{annotationTier: "critical"},
			wantHasTier:     false,
			wantTierInvalid: true,
		},
		{
			name:            "whitespace-padded tier is unparsable (strict)",
			annotations:     map[string]string{annotationTier: " 3 "},
			wantHasTier:     false,
			wantTierInvalid: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, domain, description, tier, hasTier, tierInvalid, _, _, _ := workloadAnnotations(tt.annotations)
			if domain != tt.wantDomain {
				t.Errorf("domain = %q, want %q", domain, tt.wantDomain)
			}
			if description != tt.wantDescription {
				t.Errorf("description = %q, want %q", description, tt.wantDescription)
			}
			if hasTier != tt.wantHasTier {
				t.Errorf("hasTier = %v, want %v", hasTier, tt.wantHasTier)
			}
			if hasTier && tier != tt.wantTier {
				t.Errorf("tier = %d, want %d", tier, tt.wantTier)
			}
			if tierInvalid != tt.wantTierInvalid {
				t.Errorf("tierInvalid = %v, want %v", tierInvalid, tt.wantTierInvalid)
			}
		})
	}
}

// TestWorkloadAnnotations_WeightField exercises triageagent.dev/weight's strict parse
// (docs/todo/service-weight.md §8): strconv.ParseFloat, finite, and within
// [0.1, 10] inclusive — anything else is treated identically to "not
// declared".
func TestWorkloadAnnotations_WeightField(t *testing.T) {
	tests := []struct {
		name              string
		annotations       map[string]string
		wantWeight        float64
		wantHasWeight     bool
		wantWeightInvalid bool
	}{
		{
			name:          "a valid mid-range weight",
			annotations:   map[string]string{annotationWeight: "2.0"},
			wantWeight:    2.0,
			wantHasWeight: true,
		},
		{
			name:          "0.1 is the bottom of the valid range",
			annotations:   map[string]string{annotationWeight: "0.1"},
			wantWeight:    0.1,
			wantHasWeight: true,
		},
		{
			name:          "10 is the top of the valid range",
			annotations:   map[string]string{annotationWeight: "10"},
			wantWeight:    10,
			wantHasWeight: true,
		},
		{
			name:              "0 is below the minimum and unparsable, not just clamped",
			annotations:       map[string]string{annotationWeight: "0"},
			wantHasWeight:     false,
			wantWeightInvalid: true,
		},
		{
			name:              "11 is above the maximum",
			annotations:       map[string]string{annotationWeight: "11"},
			wantHasWeight:     false,
			wantWeightInvalid: true,
		},
		{
			name:              "non-numeric weight is unparsable",
			annotations:       map[string]string{annotationWeight: "abc"},
			wantHasWeight:     false,
			wantWeightInvalid: true,
		},
		{
			name:              "NaN parses but is rejected as non-finite",
			annotations:       map[string]string{annotationWeight: "NaN"},
			wantHasWeight:     false,
			wantWeightInvalid: true,
		},
		{
			name:              "Inf parses but is rejected as non-finite",
			annotations:       map[string]string{annotationWeight: "Inf"},
			wantHasWeight:     false,
			wantWeightInvalid: true,
		},
		{
			name:        "no annotations at all",
			annotations: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, _, _, _, weight, hasWeight, weightInvalid := workloadAnnotations(tt.annotations)
			if hasWeight != tt.wantHasWeight {
				t.Errorf("hasWeight = %v, want %v", hasWeight, tt.wantHasWeight)
			}
			if hasWeight && weight != tt.wantWeight {
				t.Errorf("weight = %v, want %v", weight, tt.wantWeight)
			}
			if weightInvalid != tt.wantWeightInvalid {
				t.Errorf("weightInvalid = %v, want %v", weightInvalid, tt.wantWeightInvalid)
			}
		})
	}
}

func TestWorkloadFromDeployment_MetadataAnnotationsReadFromObjectNotPodTemplate(t *testing.T) {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ops",
			Name:      "api",
			Annotations: map[string]string{
				annotationDomain:      "commerce",
				annotationDescription: "Checkout API",
				annotationTier:        "1",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(1),
			Template: corev1.PodTemplateSpec{
				// Pod-template annotations must be ignored entirely, same rule
				// as the existing exclude/group-override annotations.
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					annotationDomain: "wrong-domain", annotationDescription: "wrong description", annotationTier: "9",
				}},
			},
		},
	}
	info := workloadFromDeployment(d)
	if info.Domain != "commerce" {
		t.Errorf("Domain = %q, want %q", info.Domain, "commerce")
	}
	if info.Description != "Checkout API" {
		t.Errorf("Description = %q, want %q", info.Description, "Checkout API")
	}
	if !info.HasTier || info.Tier != 1 {
		t.Errorf("Tier/HasTier = %d/%v, want 1/true", info.Tier, info.HasTier)
	}
	if info.TierInvalid {
		t.Error("TierInvalid = true, want false for a valid tier")
	}
}

// --- Metadata annotations: per-group aggregation ---

func TestResolveGroupDomain(t *testing.T) {
	tests := []struct {
		name    string
		members []workloadInfo
		want    string
	}{
		{
			name:    "uniform domain wins",
			members: []workloadInfo{{Domain: "commerce"}, {Domain: "commerce"}},
			want:    "commerce",
		},
		{
			name:    "conflicting domain yields empty",
			members: []workloadInfo{{Domain: "commerce"}, {Domain: "payments"}},
			want:    "",
		},
		{
			name:    "one member silent on domain yields empty",
			members: []workloadInfo{{Domain: "commerce"}, {Domain: ""}},
			want:    "",
		},
		{
			name:    "no members",
			members: nil,
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveGroupDomain(tt.members); got != tt.want {
				t.Errorf("resolveGroupDomain() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveGroupDescription(t *testing.T) {
	tests := []struct {
		name    string
		members []workloadInfo
		want    string
	}{
		{
			name:    "uniform description wins",
			members: []workloadInfo{{Description: "Checkout service"}, {Description: "Checkout service"}},
			want:    "Checkout service",
		},
		{
			name:    "conflicting description yields empty",
			members: []workloadInfo{{Description: "Checkout API"}, {Description: "Checkout worker"}},
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveGroupDescription(tt.members); got != tt.want {
				t.Errorf("resolveGroupDescription() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveGroupTier(t *testing.T) {
	tests := []struct {
		name     string
		members  []workloadInfo
		wantTier int
		wantOK   bool
	}{
		{
			name:     "minimum across declaring members wins",
			members:  []workloadInfo{{Tier: 5, HasTier: true}, {Tier: 2, HasTier: true}, {Tier: 7, HasTier: true}},
			wantTier: 2,
			wantOK:   true,
		},
		{
			name:     "non-declaring members are ignored, not treated as 0",
			members:  []workloadInfo{{HasTier: false}, {Tier: 4, HasTier: true}},
			wantTier: 4,
			wantOK:   true,
		},
		{
			name:    "no member declares a tier",
			members: []workloadInfo{{HasTier: false}, {HasTier: false}},
			wantOK:  false,
		},
		{
			name:    "no members at all",
			members: nil,
			wantOK:  false,
		},
		{
			name:     "declared tier 0 beats a non-declaring member and is not lost",
			members:  []workloadInfo{{HasTier: false}, {Tier: 0, HasTier: true}},
			wantTier: 0,
			wantOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTier, gotOK := resolveGroupTier(tt.members)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotOK && gotTier != tt.wantTier {
				t.Errorf("tier = %d, want %d", gotTier, tt.wantTier)
			}
		})
	}
}

// TestResolveGroupWeight mirrors TestResolveGroupTier at the opposite end of
// the scale: the MAXIMUM across declaring members wins
// (docs/todo/service-weight.md §4's reducer).
func TestResolveGroupWeight(t *testing.T) {
	tests := []struct {
		name       string
		members    []workloadInfo
		wantWeight float64
		wantOK     bool
	}{
		{
			name:       "maximum across declaring members wins",
			members:    []workloadInfo{{Weight: 2, HasWeight: true}, {Weight: 5, HasWeight: true}, {Weight: 1, HasWeight: true}},
			wantWeight: 5,
			wantOK:     true,
		},
		{
			name:       "non-declaring members are ignored",
			members:    []workloadInfo{{HasWeight: false}, {Weight: 3, HasWeight: true}},
			wantWeight: 3,
			wantOK:     true,
		},
		{
			name:    "no member declares a weight",
			members: []workloadInfo{{HasWeight: false}, {HasWeight: false}},
			wantOK:  false,
		},
		{
			name:    "no members at all",
			members: nil,
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWeight, gotOK := resolveGroupWeight(tt.members)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotOK && gotWeight != tt.wantWeight {
				t.Errorf("weight = %v, want %v", gotWeight, tt.wantWeight)
			}
		})
	}
}

// --- Synthesis shape ---

func TestSynthesizeApplication(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "checkout"}
	members := []workloadInfo{
		{Kind: workloadDeployment, Name: "api", Ready: true, Labels: map[string]string{labelVersion: "v1"}, ImageTag: "v1"},
		{Kind: workloadStatefulSet, Name: "db", Ready: false, Labels: map[string]string{labelVersion: "v1"}, ImageTag: "v1"},
	}

	app := synthesizeApplication(key, members)

	if app.Name != "checkout" || app.Namespace != "ops" {
		t.Fatalf("metadata = %s/%s, want ops/checkout", app.Namespace, app.Name)
	}
	if app.Labels[labelName] != "checkout" {
		t.Errorf("labels[%s] = %q, want %q", labelName, app.Labels[labelName], "checkout")
	}
	if app.Labels[labelManagedBy] != managedByVirtualApps {
		t.Errorf("labels[%s] = %q, want %q", labelManagedBy, app.Labels[labelManagedBy], managedByVirtualApps)
	}
	if app.Spec.Descriptor.Version != "v1" {
		t.Errorf("version = %q, want v1", app.Spec.Descriptor.Version)
	}
	if app.Status.ComponentsReady != "1/2" {
		t.Errorf("componentsReady = %q, want 1/2", app.Status.ComponentsReady)
	}
	if conditionTrue(app, conditionReady) {
		// One member (db) is not ready -> overall Ready must be False.
		t.Error("Ready condition = True, want False (one member not ready)")
	}
	if len(app.Status.Objects) != 2 {
		t.Fatalf("objects = %d, want 2", len(app.Status.Objects))
	}
	wantObjects := []ObjectStatus{
		{Kind: "Deployment", Name: "api", Status: "Ready"},
		{Kind: "StatefulSet", Name: "db", Status: "InProgress"},
	}
	if !reflect.DeepEqual(app.Status.Objects, wantObjects) {
		t.Errorf("objects = %+v, want %+v", app.Status.Objects, wantObjects)
	}
}

func TestSynthesizeApplication_AllReady(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "solo"}
	members := []workloadInfo{{Kind: workloadDeployment, Name: "api", Ready: true}}
	app := synthesizeApplication(key, members)
	if !conditionTrue(app, conditionReady) {
		t.Error("Ready condition = False, want True (all members ready)")
	}
	if app.Status.ComponentsReady != "1/1" {
		t.Errorf("componentsReady = %q, want 1/1", app.Status.ComponentsReady)
	}
}

func TestSynthesizeApplication_EmptyGroupNotReady(t *testing.T) {
	app := synthesizeApplication(groupKey{Namespace: "ops", Name: "empty"}, nil)
	if conditionTrue(app, conditionReady) {
		t.Error("Ready condition = True, want False for an empty member set")
	}
	if app.Status.ComponentsReady != "0/0" {
		t.Errorf("componentsReady = %q, want 0/0", app.Status.ComponentsReady)
	}
}

// TestSynthesizeApplication_MetadataAnnotations is the wire-contract test: all
// three triageagent.dev/* annotations land on the synthesized Application's own
// metadata.annotations, and description additionally reaches
// spec.descriptor.description (docs/todo/virtual-apps.md annotations Phase 2
// addendum — the triage hub reads the metadata.annotations copies).
func TestSynthesizeApplication_MetadataAnnotations(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "checkout"}
	members := []workloadInfo{
		{Kind: workloadDeployment, Name: "api", Ready: true, Domain: "commerce", Description: "Checkout service", Tier: 3, HasTier: true, Weight: 2, HasWeight: true},
		{Kind: workloadStatefulSet, Name: "db", Ready: true, Domain: "commerce", Description: "Checkout service", Tier: 5, HasTier: true, Weight: 5, HasWeight: true},
	}

	app := synthesizeApplication(key, members)

	if got := app.Annotations[annotationDomain]; got != "commerce" {
		t.Errorf("annotations[%s] = %q, want %q", annotationDomain, got, "commerce")
	}
	if got := app.Annotations[annotationDescription]; got != "Checkout service" {
		t.Errorf("annotations[%s] = %q, want %q", annotationDescription, got, "Checkout service")
	}
	if got := app.Annotations[annotationTier]; got != "3" {
		t.Errorf("annotations[%s] = %q, want %q (min across members)", annotationTier, got, "3")
	}
	if got := app.Annotations[annotationWeight]; got != "5" {
		t.Errorf("annotations[%s] = %q, want %q (max across members)", annotationWeight, got, "5")
	}
	if app.Spec.Descriptor.Description != "Checkout service" {
		t.Errorf("spec.descriptor.description = %q, want %q", app.Spec.Descriptor.Description, "Checkout service")
	}
}

// TestSynthesizeApplication_WeightAnnotation_OnlyWhenDeclared checks that
// triageagent.dev/weight lands on the synthesized Application only when at least one
// member declares it, and is absent (never a fabricated default) otherwise —
// mirroring tier's "sole declaring member wins" and no-declaration cases.
func TestSynthesizeApplication_WeightAnnotation_OnlyWhenDeclared(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "checkout"}

	declaring := []workloadInfo{
		{Kind: workloadDeployment, Name: "api", Ready: true},
		{Kind: workloadStatefulSet, Name: "db", Ready: true, Weight: 2.5, HasWeight: true},
	}
	app := synthesizeApplication(key, declaring)
	if got := app.Annotations[annotationWeight]; got != "2.5" {
		t.Errorf("annotations[%s] = %q, want %q (sole declaring member wins)", annotationWeight, got, "2.5")
	}

	none := []workloadInfo{{Kind: workloadDeployment, Name: "api", Ready: true}}
	app = synthesizeApplication(key, none)
	if v, ok := app.Annotations[annotationWeight]; ok {
		t.Errorf("annotations[%s] = %q, want absent entirely (no member declared a weight)", annotationWeight, v)
	}
}

// TestSynthesizeApplication_ConflictingMetadata_OmitsThoseAnnotations checks
// that a conflicting domain/description falls back to the uniform-or-empty
// rule (no annotation key at all, not an empty-string value), while a tier
// declared by only one member still wins the group.
func TestSynthesizeApplication_ConflictingMetadata_OmitsThoseAnnotations(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "checkout"}
	members := []workloadInfo{
		{Kind: workloadDeployment, Name: "api", Ready: true, Domain: "commerce", Description: "Checkout API", Tier: 4, HasTier: true},
		{Kind: workloadStatefulSet, Name: "db", Ready: true, Domain: "payments", Description: "Checkout DB"},
	}

	app := synthesizeApplication(key, members)

	if _, ok := app.Annotations[annotationDomain]; ok {
		t.Errorf("annotations[%s] present = %q, want absent (conflicting domain)", annotationDomain, app.Annotations[annotationDomain])
	}
	if _, ok := app.Annotations[annotationDescription]; ok {
		t.Errorf("annotations[%s] present = %q, want absent (conflicting description)", annotationDescription, app.Annotations[annotationDescription])
	}
	if app.Spec.Descriptor.Description != "" {
		t.Errorf("spec.descriptor.description = %q, want empty (conflicting description)", app.Spec.Descriptor.Description)
	}
	if got := app.Annotations[annotationTier]; got != "4" {
		t.Errorf("annotations[%s] = %q, want %q (sole declaring member wins)", annotationTier, got, "4")
	}
}

// TestSynthesizeApplication_NoMetadataAnnotations_ProducesNoTriageKeys is the
// negative case: a group whose members declare none of the three annotations
// gets none of the three keys on the synthesized Application at all — absent,
// never a fabricated empty string (docs/todo/virtual-apps.md annotations
// Phase 2 addendum).
func TestSynthesizeApplication_NoMetadataAnnotations_ProducesNoTriageKeys(t *testing.T) {
	key := groupKey{Namespace: "ops", Name: "checkout"}
	members := []workloadInfo{{Kind: workloadDeployment, Name: "api", Ready: true}}

	app := synthesizeApplication(key, members)

	for _, k := range []string{annotationDomain, annotationDescription, annotationTier, annotationWeight} {
		if v, ok := app.Annotations[k]; ok {
			t.Errorf("annotations[%s] = %q, want absent entirely", k, v)
		}
	}
	if app.Spec.Descriptor.Description != "" {
		t.Errorf("spec.descriptor.description = %q, want empty", app.Spec.Descriptor.Description)
	}
}

// --- Diff/delta emission ---

func TestEmitDeltas(t *testing.T) {
	var mu sync.Mutex
	var got []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, f)
	}
	v := &virtualAppsState{cluster: "cluster-a", enqueue: enqueue}

	appA := synthesizeApplication(groupKey{Namespace: "ops", Name: "a"}, []workloadInfo{{Kind: workloadDeployment, Name: "a", Ready: true}})
	appAUpdated := synthesizeApplication(groupKey{Namespace: "ops", Name: "a"}, []workloadInfo{{Kind: workloadDeployment, Name: "a", Ready: false}})
	appB := synthesizeApplication(groupKey{Namespace: "ops", Name: "b"}, []workloadInfo{{Kind: workloadDeployment, Name: "b", Ready: true}})
	appC := synthesizeApplication(groupKey{Namespace: "ops", Name: "c"}, []workloadInfo{{Kind: workloadDeployment, Name: "c", Ready: true}})

	old := map[groupKey]*VirtualApp{
		{Namespace: "ops", Name: "a"}: appA,
		{Namespace: "ops", Name: "b"}: appB, // will vanish
	}
	updated := map[groupKey]*VirtualApp{
		{Namespace: "ops", Name: "a"}: appAUpdated, // changed
		{Namespace: "ops", Name: "c"}: appC,        // new
	}

	v.emitDeltas(old, updated)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("frames = %d, want 3 (update a, add c, delete b)", len(got))
	}
	ops := map[string]int{}
	for _, f := range got {
		if f.Kind != KindAppDelta {
			t.Errorf("frame kind = %q, want %q", f.Kind, KindAppDelta)
		}
		ops[f.AppDelta.Op+":"+f.AppDelta.App.Name]++
	}
	want := map[string]int{"update:a": 1, "add:c": 1, "delete:b": 1}
	if !reflect.DeepEqual(ops, want) {
		t.Errorf("ops = %v, want %v", ops, want)
	}
}

func TestEmitDeltas_NoChangeNoFrames(t *testing.T) {
	var got []*Frame
	v := &virtualAppsState{cluster: "cluster-a", enqueue: func(f *Frame) { got = append(got, f) }}
	app := synthesizeApplication(groupKey{Namespace: "ops", Name: "a"}, []workloadInfo{{Kind: workloadDeployment, Name: "a", Ready: true}})
	same := map[groupKey]*VirtualApp{{Namespace: "ops", Name: "a"}: app}
	v.emitDeltas(same, same)
	if len(got) != 0 {
		t.Errorf("frames = %d, want 0 (nothing changed)", len(got))
	}
}

// --- Engine integration: applyWorkload -> debounced recompute -> mode behavior ---

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestVirtualAppsState_OnMode_PushesCRLessGroup(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()

	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	v.applyWorkload(workloadKey(workloadDeployment, "ops", "worker"), &info, false)

	waitFor(t, virtualAppsDebounce+2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(frames) == 1
	})

	mu.Lock()
	defer mu.Unlock()
	if frames[0].AppDelta.Op != OpAdd || frames[0].AppDelta.App.Name != "worker" {
		t.Errorf("frame = %+v, want add/worker", frames[0].AppDelta)
	}

	apps := v.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "worker" {
		t.Errorf("snapshotApps = %+v, want one app named worker", apps)
	}
}

// TestVirtualAppsState_RecomputeNow_PopulatesBeforeDebounceFires is the
// regression test for the "sent snapshot apps=0" bug: a workload that was
// already steady when applyWorkload first sees it (the synthetic ADD an
// informer fires for a pre-existing object) only lands in v.current once the
// 2s virtualAppsDebounce timer fires. Pusher.sendSnapshot runs well before
// that on first connect, so a plain snapshotApps() call there read an empty
// set. recomputeNow (called by sendSnapshot immediately before snapshotApps)
// must make the synthesized app visible immediately, without waiting for the
// debounce — and must cancel the pending timer so it doesn't fire a second,
// redundant recompute+emitDeltas afterward.
func TestVirtualAppsState_RecomputeNow_PopulatesBeforeDebounceFires(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()

	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "agentregistry-inventory-controller", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	v.applyWorkload(workloadKey(workloadDeployment, "ops", "agentregistry-inventory-controller"), &info, false)

	// Before the debounce fires, a snapshot taken immediately (mirroring
	// sendSnapshot running ~1s after Start) must be empty — this is the bug.
	if apps := v.snapshotApps(); len(apps) != 0 {
		t.Fatalf("snapshotApps before recomputeNow = %d apps, want 0 (debounce has not fired yet)", len(apps))
	}

	v.recomputeNow(context.Background())

	apps := v.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "agentregistry-inventory-controller" {
		t.Fatalf("snapshotApps after recomputeNow = %+v, want one app named agentregistry-inventory-controller", apps)
	}

	// The pending debounce timer must have been cancelled by recomputeNow, so
	// it never fires again later and double-emits.
	time.Sleep(virtualAppsDebounce + 500*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 1 {
		t.Errorf("frames = %d, want exactly 1 (recomputeNow must cancel the pending debounce timer)", len(frames))
	}
}

// TestVirtualAppsState_ExcludedWorkload_NeverSynthesizedOrPushed drives the full
// applyWorkload -> debounced recompute pipeline (not just the pure grouping
// functions) with a triageagent.dev/virtual-app: "false" annotated workload.
func TestVirtualAppsState_ExcludedWorkload_NeverSynthesizedOrPushed(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()

	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ops",
			Name:        "worker",
			Generation:  1,
			Annotations: map[string]string{annotationVirtualApp: "false"},
		},
		Spec:   appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	v.applyWorkload(workloadKey(workloadDeployment, "ops", "worker"), &info, false)

	time.Sleep(virtualAppsDebounce + 500*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 0 {
		t.Errorf("frames = %d, want 0 (excluded workload must never be synthesized/pushed)", len(frames))
	}
	if len(v.snapshotApps()) != 0 {
		t.Error("snapshotApps should be empty: the only workload is excluded")
	}
}

// TestVirtualAppsState_Namespaces_OutOfScopeIgnored verifies onDeployment/etc.
// (via the shared inScope check exercised here directly, since registerInformers
// needs a real cache) drop workloads outside Options.Namespaces.
func TestVirtualAppsState_Namespaces_OutOfScope(t *testing.T) {
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), nil)
	if v.inScope("ops") != true {
		t.Error("ops should be in scope")
	}
	if v.inScope("dev") != false {
		t.Error("dev should be out of scope")
	}
}

func TestGroupKeyString(t *testing.T) {
	k := groupKey{Namespace: "ops", Name: "checkout"}
	if got := k.String(); got != "ops/checkout" {
		t.Errorf("groupKey.String() = %q, want %q", got, "ops/checkout")
	}
}

// --- Namespace-level annotation inheritance (docs/todo/virtual-apps.md
// namespace-inheritance addendum, 2026-08-24): workloadsSnapshot's overlay,
// exercised together with grouping/aggregation. ---

func TestWorkloadsSnapshot_NilNsDefaultsFn_NoOverlay(t *testing.T) {
	v := &virtualAppsState{
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {Kind: workloadDeployment, Namespace: "ops", Name: "api"},
		},
	}
	out := v.workloadsSnapshot()
	if len(out) != 1 || out[0].Domain != "" {
		t.Errorf("workloadsSnapshot() = %+v, want the one workload untouched (no nsDefaultsFn wired)", out)
	}
}

func TestWorkloadsSnapshot_WorkloadInheritsNamespaceDefaults(t *testing.T) {
	nsFn := func(ns string) (namespaceDefaults, bool) {
		if ns == "ops" {
			return namespaceDefaults{Domain: "commerce", Description: "Checkout namespace", Tier: 3, HasTier: true}, true
		}
		return namespaceDefaults{}, false
	}
	v := &virtualAppsState{
		nsDefaultsFn: nsFn,
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {Kind: workloadDeployment, Namespace: "ops", Name: "api"},
		},
	}
	out := v.workloadsSnapshot()
	if len(out) != 1 {
		t.Fatalf("workloadsSnapshot() len = %d, want 1", len(out))
	}
	w := out[0]
	if w.Domain != "commerce" || w.Description != "Checkout namespace" || !w.HasTier || w.Tier != 3 {
		t.Errorf("workload = %+v, want inherited commerce/Checkout namespace/tier 3", w)
	}
}

func TestWorkloadsSnapshot_WorkloadOwnValueWinsOverNamespaceDefault(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{Domain: "commerce", Tier: 5, HasTier: true}, true
	}
	v := &virtualAppsState{
		nsDefaultsFn: nsFn,
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {
				Kind: workloadDeployment, Namespace: "ops", Name: "api",
				Domain: "payments", Tier: 1, HasTier: true,
			},
		},
	}
	out := v.workloadsSnapshot()
	w := out[0]
	if w.Domain != "payments" {
		t.Errorf("Domain = %q, want %q (workload's own value must win)", w.Domain, "payments")
	}
	if w.Tier != 1 {
		t.Errorf("Tier = %d, want %d (workload's own value must win)", w.Tier, 1)
	}
}

// TestWorkloadsSnapshot_GroupAggregation_AllInheritResolves is the
// group-aggregation case named in the addendum: two members declare nothing
// of their own, both inherit the same namespace default, and the group
// resolves that value exactly as if every member had declared it themselves
// — the uniform-or-empty aggregation logic in resolveGroupDomain/
// resolveGroupDescription/resolveGroupTier is untouched, it just sees
// already-inherited values.
func TestWorkloadsSnapshot_GroupAggregation_AllInheritResolves(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{IdentityMode: IdentityModeLabels, Domain: "commerce", Description: "Checkout", Tier: 3, HasTier: true}, true
	}
	v := &virtualAppsState{
		nsDefaultsFn: nsFn,
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {Kind: workloadDeployment, Namespace: "ops", Name: "api", Labels: map[string]string{labelPartOf: "checkout"}, Ready: true},
			"ops/StatefulSet/db": {Kind: workloadStatefulSet, Namespace: "ops", Name: "db", Labels: map[string]string{labelPartOf: "checkout"}, Ready: true},
		},
	}
	groups := groupWorkloads(v.workloadsSnapshot())
	members := groups[groupKey{Namespace: "ops", Name: "checkout"}]
	if len(members) != 2 {
		t.Fatalf("ops/checkout members = %d, want 2", len(members))
	}
	if got := resolveGroupDomain(members); got != "commerce" {
		t.Errorf("resolveGroupDomain() = %q, want %q (both members inherit the same namespace default)", got, "commerce")
	}
	if got := resolveGroupDescription(members); got != "Checkout" {
		t.Errorf("resolveGroupDescription() = %q, want %q", got, "Checkout")
	}
	if tier, ok := resolveGroupTier(members); !ok || tier != 3 {
		t.Errorf("resolveGroupTier() = (%d,%v), want (3,true)", tier, ok)
	}

	app := synthesizeApplication(groupKey{Namespace: "ops", Name: "checkout"}, members)
	if app.Annotations[annotationDomain] != "commerce" {
		t.Errorf("synthesized annotations[%s] = %q, want %q", annotationDomain, app.Annotations[annotationDomain], "commerce")
	}
}

// TestWorkloadsSnapshot_GroupAggregation_WorkloadOverrideCreatesConflict is
// the addendum's other named case: one member overrides the inherited
// default with its own, disagreeing value — the group must fall back to the
// existing uniform-or-empty conflict behavior exactly as it would for any
// other disagreeing per-workload annotation.
func TestWorkloadsSnapshot_GroupAggregation_WorkloadOverrideCreatesConflict(t *testing.T) {
	nsFn := func(string) (namespaceDefaults, bool) {
		return namespaceDefaults{IdentityMode: IdentityModeLabels, Domain: "commerce"}, true
	}
	v := &virtualAppsState{
		nsDefaultsFn: nsFn,
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {Kind: workloadDeployment, Namespace: "ops", Name: "api", Labels: map[string]string{labelPartOf: "checkout"}, Domain: "payments"},
			"ops/StatefulSet/db": {Kind: workloadStatefulSet, Namespace: "ops", Name: "db", Labels: map[string]string{labelPartOf: "checkout"}},
		},
	}
	groups := groupWorkloads(v.workloadsSnapshot())
	members := groups[groupKey{Namespace: "ops", Name: "checkout"}]
	if got := resolveGroupDomain(members); got != "" {
		t.Errorf("resolveGroupDomain() = %q, want empty (one member's own domain conflicts with the other's inherited default)", got)
	}
}

// TestWorkloadsSnapshot_NamespaceExcluded_SuppressesWholeNamespace is the
// namespace-level triageagent.dev/virtual-app: "false" case: EVERY workload in the
// excluded namespace is dropped at grouping time regardless of its own
// annotation, while an unrelated namespace is completely unaffected.
func TestWorkloadsSnapshot_NamespaceExcluded_SuppressesWholeNamespace(t *testing.T) {
	nsFn := func(ns string) (namespaceDefaults, bool) {
		if ns == "ops" {
			return namespaceDefaults{Excluded: true}, true
		}
		return namespaceDefaults{}, false
	}
	v := &virtualAppsState{
		nsDefaultsFn: nsFn,
		workloads: map[string]workloadInfo{
			"ops/Deployment/api": {Kind: workloadDeployment, Namespace: "ops", Name: "api"},
			"ops/Deployment/web": {Kind: workloadDeployment, Namespace: "ops", Name: "web"},
			"dev/Deployment/api": {Kind: workloadDeployment, Namespace: "dev", Name: "api"},
		},
	}
	groups := groupWorkloads(v.workloadsSnapshot())
	if _, ok := groups[groupKey{Namespace: "ops", Name: "api"}]; ok {
		t.Error("ops/api must be suppressed: its namespace is excluded")
	}
	if _, ok := groups[groupKey{Namespace: "ops", Name: "web"}]; ok {
		t.Error("ops/web must be suppressed: its namespace is excluded")
	}
	if _, ok := groups[groupKey{Namespace: "dev", Name: "api"}]; !ok {
		t.Error("dev/api must be unaffected by ops's namespace-level exclusion")
	}
}

// TestScheduleRecompute_TriggersDebouncedRecompute proves the namespace-change
// entry point (Pusher.onNamespace calls this) arms the same debounce timer
// applyWorkload uses.
func TestScheduleRecompute_TriggersDebouncedRecompute(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()
	v.mu.Lock()
	v.workloads["ops/Deployment/api"] = workloadInfo{
		Kind: workloadDeployment, Namespace: "ops", Name: "api", Ready: true,
	}
	v.mu.Unlock()

	v.scheduleRecompute()

	waitFor(t, virtualAppsDebounce+2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(frames) == 1
	})
}

// --- Namespace-scope-by-annotation: backfillNamespace / evictNamespace
// (docs/todo/virtual-apps.md namespace-scope-by-annotation addendum,
// 2026-08-25). Unit-level coverage of virtualAppsState's own half of the
// feature; push/pusher_test.go covers the Pusher.onNamespace wiring end to
// end. ---

func TestVirtualAppsState_BackfillNamespace_PullsCachedSteadyWorkload(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), enqueue)
	v.ctx = context.Background()
	v.listWorkloadsFn = func(_ context.Context, ns string) ([]rawWorkload, error) {
		if ns != "rag" {
			return nil, nil
		}
		return []rawWorkload{{
			Obj: toUnstructuredObj(&appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
				Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
				Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
			}),
			Kind: "Deployment",
		}}, nil
	}

	// Before backfill: nothing known about "rag" at all — as if onWorkload
	// never fired an event for it because "rag" was out of scope when the
	// Deployment settled.
	if len(v.workloadsSnapshot()) != 0 {
		t.Fatal("setup: v.workloads must start empty")
	}

	v.backfillNamespace("rag")

	apps := v.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "worker" {
		t.Fatalf("snapshotApps = %+v, want one app named worker", apps)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 1 || frames[0].AppDelta.Op != OpAdd || frames[0].AppDelta.App.Name != "worker" {
		t.Errorf("frames = %+v, want exactly one OpAdd for worker", frames)
	}
}

// TestVirtualAppsState_BackfillNamespace_NoCacheAndNoOverrideIsNoop covers a
// virtualAppsState with no cache wired (registerInformers never ran, as in
// most of this file's other unit tests) and no list*Fn override — the
// default listCached* implementations must return (nil, nil), not panic.
func TestVirtualAppsState_BackfillNamespace_NoCacheAndNoOverrideIsNoop(t *testing.T) {
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), func(*Frame) {})
	v.ctx = context.Background()

	v.backfillNamespace("rag") // must not panic

	if len(v.snapshotApps()) != 0 {
		t.Error("nothing should have been backfilled with no cache and no override wired")
	}
}

func TestVirtualAppsState_EvictNamespace_RemovesOnlyThatNamespace(t *testing.T) {
	v := &virtualAppsState{
		workloads: map[string]workloadInfo{
			"rag/Deployment/worker": {Kind: workloadDeployment, Namespace: "rag", Name: "worker"},
			"ops/Deployment/api":    {Kind: workloadDeployment, Namespace: "ops", Name: "api"},
		},
	}
	if !v.evictNamespace("rag") {
		t.Error("evictNamespace(rag) must report a change")
	}
	if _, ok := v.workloads["rag/Deployment/worker"]; ok {
		t.Error("rag's workload must be gone")
	}
	if _, ok := v.workloads["ops/Deployment/api"]; !ok {
		t.Error("ops's workload must be untouched")
	}
	if v.evictNamespace("rag") {
		t.Error("evicting an already-evicted namespace must report no change")
	}
}

func TestVirtualAppsState_EvictNamespaceAndRecompute_EmitsDelete(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()

	info := workloadFromDeployment(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	})
	v.applyWorkload(workloadKey(workloadDeployment, "rag", "worker"), &info, false)
	v.recomputeNow(context.Background())
	if len(v.snapshotApps()) != 1 {
		t.Fatal("setup: worker must be synthesized before eviction")
	}
	mu.Lock()
	frames = nil // discard the setup OpAdd
	mu.Unlock()

	v.evictNamespaceAndRecompute("rag")

	if len(v.snapshotApps()) != 0 {
		t.Error("worker's group must be gone after eviction")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 1 || frames[0].AppDelta.Op != OpDelete || frames[0].AppDelta.App.Name != "worker" {
		t.Errorf("frames = %+v, want exactly one OpDelete for worker", frames)
	}
}

func TestVirtualAppsState_EvictNamespaceAndRecompute_NoopWhenNothingRemoved(t *testing.T) {
	frameCount := 0
	enqueue := func(*Frame) { frameCount++ }

	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), enqueue)
	v.ctx = context.Background()

	v.evictNamespaceAndRecompute("never-seen")

	if frameCount != 0 {
		t.Error("evicting a namespace that contributed no workload must not trigger a recompute at all")
	}
}

// The three kinds shipped in DefaultWorkloadResources.
const (
	workloadDeployment  workloadKind = "Deployment"
	workloadStatefulSet workloadKind = "StatefulSet"
	workloadDaemonSet   workloadKind = "DaemonSet"
)

// --- Scaled to zero after running (seenRunning) ---

func zeroTestDeploy(name string, replicas, ready int32, annotations map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: name, Annotations: annotations},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(replicas)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

// zeroTestHarness returns a synchronous state plus helpers: apply a
// Deployment/StatefulSet and recompute, and read the ops emitted since the last call.
func zeroTestHarness(t *testing.T) (v *virtualAppsState, apply func(info workloadInfo, deleted bool), drain func() []string) {
	t.Helper()
	var mu sync.Mutex
	var frames []*Frame
	v = newVirtualAppsState(VirtualAppsOn, "cluster-a", nil, logr.Discard(), func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	})
	v.ctx = context.Background()
	apply = func(info workloadInfo, deleted bool) {
		v.applyWorkload(workloadKey(info.Kind, info.Namespace, info.Name), &info, deleted)
		v.recomputeNow(context.Background())
	}
	drain = func() []string {
		mu.Lock()
		defer mu.Unlock()
		var ops []string
		for _, f := range frames {
			ops = append(ops, f.AppDelta.Op)
		}
		frames = nil
		return ops
	}
	t.Cleanup(func() {
		v.mu.Lock()
		if v.timer != nil {
			v.timer.Stop()
		}
		v.mu.Unlock()
	})
	return v, apply, drain
}

func TestVirtualApps_ScaledToZero_FirstSeenAtZeroStaysExcluded(t *testing.T) {
	v, apply, drain := zeroTestHarness(t)
	apply(workloadFromDeployment(zeroTestDeploy("parked", 0, 0, nil)), false)
	if apps := v.snapshotApps(); len(apps) != 0 {
		t.Fatalf("apps = %+v, want none", apps)
	}
	if ops := drain(); len(ops) != 0 {
		t.Errorf("deltas = %v, want none", ops)
	}
	if v.seenRunning["ops/Deployment/parked"] {
		t.Error("never-running workload must not be in seenRunning")
	}
}

func TestVirtualApps_ScaledToZero_AfterRunningStaysDownThenRecovers(t *testing.T) {
	v, apply, drain := zeroTestHarness(t)
	apply(workloadFromDeployment(zeroTestDeploy("currency", 1, 1, nil)), false)
	if ops := drain(); len(ops) != 1 || ops[0] != OpAdd {
		t.Fatalf("deltas = %v, want [add]", ops)
	}

	apply(workloadFromDeployment(zeroTestDeploy("currency", 0, 0, nil)), false)
	if ops := drain(); len(ops) != 1 || ops[0] != OpUpdate {
		t.Fatalf("deltas after scale to 0 = %v, want [update]", ops)
	}
	apps := v.snapshotApps()
	if len(apps) != 1 {
		t.Fatalf("apps = %+v, want one", apps)
	}
	c := apps[0].Status.Conditions[0]
	if c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "scaled to zero (0 desired replicas)") {
		t.Errorf("condition = %+v, want False with scaled-to-zero message", c)
	}
	if apps[0].Status.ComponentsReady != "0/1" {
		t.Errorf("ComponentsReady = %q, want 0/1", apps[0].Status.ComponentsReady)
	}

	apply(workloadFromDeployment(zeroTestDeploy("currency", 1, 1, nil)), false)
	if ops := drain(); len(ops) != 1 || ops[0] != OpUpdate {
		t.Fatalf("deltas after scale up = %v, want [update]", ops)
	}
	c = v.snapshotApps()[0].Status.Conditions[0]
	if c.Status != corev1.ConditionTrue || c.Message != "" {
		t.Errorf("condition = %+v, want True and no message", c)
	}
}

func TestVirtualApps_ScaledToZero_StatefulSetAndAnnotationExclusion(t *testing.T) {
	v, apply, _ := zeroTestHarness(t)
	sts := func(r int32, ann map[string]string) workloadInfo {
		return workloadFromStatefulSet(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "db", Annotations: ann},
			Spec:       appsv1.StatefulSetSpec{Replicas: int32p(r)},
			Status:     appsv1.StatefulSetStatus{ReadyReplicas: r},
		})
	}
	apply(sts(1, nil), false)
	apply(sts(0, nil), false)
	if apps := v.snapshotApps(); len(apps) != 1 || apps[0].Status.Conditions[0].Status != corev1.ConditionFalse {
		t.Fatalf("statefulset at 0 after running: apps = %+v, want one not-ready", apps)
	}
	// An explicit virtual-app: "false" still wins.
	apply(sts(0, map[string]string{annotationkeys.VirtualApp: "false"}), false)
	if apps := v.snapshotApps(); len(apps) != 0 {
		t.Errorf("annotation-excluded workload must stay out, got %+v", apps)
	}
}

func TestVirtualApps_ScaledToZero_DeleteForgetsAndSendsDelete(t *testing.T) {
	v, apply, drain := zeroTestHarness(t)
	apply(workloadFromDeployment(zeroTestDeploy("currency", 1, 1, nil)), false)
	drain()
	d := workloadFromDeployment(zeroTestDeploy("currency", 1, 1, nil))
	apply(d, true)
	if ops := drain(); len(ops) != 1 || ops[0] != OpDelete {
		t.Fatalf("deltas = %v, want [delete]", ops)
	}
	if len(v.seenRunning) != 0 {
		t.Errorf("seenRunning = %v, want empty", v.seenRunning)
	}
	// Re-created at 0: first seen at zero again, so excluded.
	apply(workloadFromDeployment(zeroTestDeploy("currency", 0, 0, nil)), false)
	if apps := v.snapshotApps(); len(apps) != 0 {
		t.Errorf("apps = %+v, want none", apps)
	}
}

func TestVirtualApps_ScaledToZero_OneMemberOfTwo(t *testing.T) {
	v, apply, _ := zeroTestHarness(t)
	lbl := map[string]string{annotationkeys.VirtualAppGroup: "checkout"}
	apply(workloadFromDeployment(zeroTestDeploy("api", 1, 1, lbl)), false)
	apply(workloadFromDeployment(zeroTestDeploy("worker", 1, 1, lbl)), false)
	if a := v.snapshotApps(); len(a) != 1 || a[0].Status.ComponentsReady != "2/2" {
		t.Fatalf("apps = %+v, want one 2/2", a)
	}
	apply(workloadFromDeployment(zeroTestDeploy("worker", 0, 0, lbl)), false)
	apps := v.snapshotApps()
	if len(apps) != 1 || apps[0].Status.ComponentsReady != "1/2" || apps[0].Status.Conditions[0].Status != corev1.ConditionFalse {
		t.Fatalf("apps = %+v, want one 1/2 not ready", apps)
	}
	if m := apps[0].Status.Conditions[0].Message; !strings.Contains(m, "Deployment/worker: scaled to zero") {
		t.Errorf("message = %q", m)
	}
}

// TestWorkloadFromUnstructured_ReplicaCounts: a workload's pod counts reach
// its component so the core can tell one replica down from all of them; a
// workload judged by its conditions carries none, and a surge is capped.
func TestWorkloadFromUnstructured_ReplicaCounts(t *testing.T) {
	rollout := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Rollout",
		"metadata": map[string]any{"namespace": "dev", "name": "persona"},
		"spec":     map[string]any{"replicas": int64(2)},
		"status":   map[string]any{"replicas": int64(2)},
	}}
	for _, tc := range []struct {
		name                 string
		obj                  *unstructured.Unstructured
		kind                 string
		wantDesired, wantRdy int64
	}{
		{"one of two ready", toUnstructuredObj(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "dev", Name: "api"},
			Spec:       appsv1.DeploymentSpec{Replicas: int32p(2)},
			Status:     appsv1.DeploymentStatus{Replicas: 2, ReadyReplicas: 1},
		}), "Deployment", 2, 1},
		{"surge capped at desired", toUnstructuredObj(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "dev", Name: "api"},
			Spec:       appsv1.DeploymentSpec{Replicas: int32p(2)},
			Status:     appsv1.DeploymentStatus{Replicas: 3, ReadyReplicas: 3},
		}), "Deployment", 2, 2},
		{"absent ready count with a replica count is none ready", rollout, "Rollout", 2, 0},
		{"conditions only carry no counts", &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"namespace": "dev", "name": "db"},
			"status": map[string]any{"conditions": []any{
				map[string]any{"type": "Ready", "status": "True"},
			}},
		}}, "Cluster", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := workloadFromUnstructured(tc.obj, tc.kind)
			if w.Replicas != tc.wantDesired || w.ReadyReplicas != tc.wantRdy {
				t.Fatalf("replicas %d/%d, want %d/%d", w.ReadyReplicas, w.Replicas, tc.wantRdy, tc.wantDesired)
			}
			app := synthesizeApplication(groupKey{Namespace: "dev", Name: "x"}, []workloadInfo{w})
			o := app.Status.Objects[0]
			if o.Replicas != tc.wantDesired || o.ReadyReplicas != tc.wantRdy {
				t.Fatalf("component replicas %d/%d, want %d/%d", o.ReadyReplicas, o.Replicas, tc.wantRdy, tc.wantDesired)
			}
		})
	}
}

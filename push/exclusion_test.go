package push

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

var off = map[string]string{annotationkeys.Triage: "false"}

// deploymentObj builds an unstructured Deployment with optional own and
// pod-template annotations and pod-template labels.
func deploymentObj(ns, name string, own, template, podLabels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"spec":       map[string]any{"replicas": int64(1)},
		"status":     map[string]any{"readyReplicas": int64(1)},
	}}
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetAnnotations(own)
	if template != nil {
		_ = unstructured.SetNestedStringMap(u.Object, template, "spec", "template", "metadata", "annotations")
	}
	if podLabels != nil {
		_ = unstructured.SetNestedStringMap(u.Object, podLabels, "spec", "template", "metadata", "labels")
	}
	return u
}

// exclusionPusher is newTestPusher with the namespace store, a fake client
// for owner reads, a movable clock, and a connected mux whose frames the
// test drains.
func exclusionPusher(t *testing.T, objs ...client.Object) (*Pusher, *time.Time) {
	t.Helper()
	p := newTestPusher("ws://unused", nil)
	clock := time.Unix(1720000000, 0)
	p.opts.now = func() time.Time { return clock }
	p.nsDefaults = newNsDefaultsStore()
	p.vapps.nsDefaultsFn = p.nsDefaults.get
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	p.client = fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	p.mux().beginConnection([]string{p.opts.Tenant})
	return p, &clock
}

// addWorkload feeds an unstructured workload through the real informer
// callback and recomputes, as a live informer event would.
func addWorkload(p *Pusher, u *unstructured.Unstructured) {
	p.vapps.onWorkload(u, u.GetKind(), false)
	p.vapps.recomputeNow(context.Background())
}

func setNamespace(p *Pusher, name string, annotations map[string]string) {
	p.onNamespace(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}, false)
}

// drain pops every queued frame.
func drain(p *Pusher) []*Frame {
	var out []*Frame
	for {
		f, ok := p.mux().next()
		if !ok {
			return out
		}
		out = append(out, f)
	}
}

func TestTriageExcluded_Value(t *testing.T) {
	for _, tc := range []struct {
		ann  map[string]string
		want bool
	}{
		{nil, false},
		{map[string]string{}, false},
		{map[string]string{annotationkeys.Triage: "false"}, true},
		{map[string]string{annotationkeys.Triage: "true"}, false},
		{map[string]string{annotationkeys.Triage: "False"}, false},
		{map[string]string{annotationkeys.Triage: ""}, false},
		{map[string]string{annotationkeys.VirtualApp: "false"}, false},
	} {
		if got := TriageExcluded(tc.ann); got != tc.want {
			t.Errorf("TriageExcluded(%v) = %v, want %v", tc.ann, got, tc.want)
		}
	}
}

func TestWorkloadFromUnstructured_TriageExcluded(t *testing.T) {
	for _, tc := range []struct {
		name          string
		own, template map[string]string
		want          bool
	}{
		{"absent", nil, nil, false},
		{"workload metadata", off, nil, true},
		{"pod template", nil, off, true},
		{"virtual-app false only", map[string]string{annotationkeys.VirtualApp: "false"}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := workloadFromUnstructured(deploymentObj("shop", "lg", tc.own, tc.template, nil), "Deployment")
			if w.TriageExcluded != tc.want {
				t.Errorf("TriageExcluded = %v, want %v", w.TriageExcluded, tc.want)
			}
			if tc.want && (!w.Excluded || !w.annotationExcluded) {
				t.Errorf("a triage-excluded workload must also be excluded from virtual-app synthesis: %+v", w)
			}
		})
	}
	cj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "batch/v1", "kind": "CronJob"}}
	_ = unstructured.SetNestedStringMap(cj.Object, off, "spec", "jobTemplate", "spec", "template", "metadata", "annotations")
	if !objectTriageExcluded(cj) {
		t.Error("a CronJob's jobTemplate pod-template annotation must exclude it")
	}
}

func TestPredicate_WorkloadPodTemplateNamespaceAbsent(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, map[string]string{labelName: "loadgen"}))
	addWorkload(p, deploymentObj("shop", "worker", nil, off, nil))
	addWorkload(p, deploymentObj("shop", "checkout", nil, nil, nil))
	setNamespace(p, "shop", nil)
	setNamespace(p, "test", off)

	for _, tc := range []struct {
		ns, svc string
		want    bool
	}{
		{"shop", "load-generator", true}, // workload metadata
		{"shop", "loadgen", true},        // its pod template's app.kubernetes.io/name (log attribution)
		{"shop", "worker", true},         // pod template
		{"shop", "checkout", false},      // absent
		{"shop", "unknown", false},
		{"test", "anything", true}, // namespace
		{"other", "load-generator", false},
	} {
		if got := p.ServiceExcluded(tc.ns, tc.svc); got != tc.want {
			t.Errorf("ServiceExcluded(%s, %s) = %v, want %v", tc.ns, tc.svc, got, tc.want)
		}
	}
	if !p.NamespaceExcluded("test") || p.NamespaceExcluded("shop") {
		t.Error("NamespaceExcluded must follow the Namespace annotation")
	}
	for _, a := range p.vapps.snapshotApps() {
		if a.Name == "load-generator" || a.Name == "worker" {
			t.Errorf("excluded workload %s must not be synthesized as an app", a.Name)
		}
	}
}

// A Namespace "false" is not overridden by a workload "true", the same OR
// rule annotationkeys.VirtualApp follows.
func TestPredicate_NamespaceFalseBeatsWorkloadTrue(t *testing.T) {
	p, _ := exclusionPusher(t)
	setNamespace(p, "test", off)
	addWorkload(p, deploymentObj("test", "api", map[string]string{annotationkeys.Triage: "true"}, nil, nil))
	if !p.ServiceExcluded("test", "api") {
		t.Error("workload \"true\" must not override namespace \"false\"")
	}
	if apps := p.vapps.snapshotApps(); len(apps) != 0 {
		t.Errorf("no app may be synthesized in an excluded namespace, got %d", len(apps))
	}
}

// A name an included workload also answers to is never dropped.
func TestPredicate_SharedNameStaysIn(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "lg", off, nil, map[string]string{labelName: "shop"}))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, map[string]string{labelName: "shop"}))
	if p.ServiceExcluded("shop", "shop") {
		t.Error("a label shared with an included workload must not be excluded")
	}
	if !p.ServiceExcluded("shop", "lg") {
		t.Error("the excluded workload's own name must be")
	}
}

func TestPredicate_LiveChange(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "load-generator", nil, nil, nil))
	setNamespace(p, "shop", nil)
	_ = drain(p)
	if p.ServiceExcluded("shop", "load-generator") {
		t.Fatal("not annotated yet")
	}

	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, nil))
	if !p.ServiceExcluded("shop", "load-generator") {
		t.Fatal("annotation added later must exclude from then on")
	}
	// The app it had is deleted on the core.
	var deleted bool
	for _, f := range drain(p) {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "load-generator" {
			deleted = true
		}
	}
	if !deleted {
		t.Error("excluding a running workload must send its app's delete")
	}

	addWorkload(p, deploymentObj("shop", "load-generator", nil, nil, nil))
	if p.ServiceExcluded("shop", "load-generator") {
		t.Fatal("annotation removed must include it again")
	}

	setNamespace(p, "shop", off)
	if !p.ServiceExcluded("shop", "load-generator") || !p.NamespaceExcluded("shop") {
		t.Fatal("namespace annotation added later must exclude from then on")
	}
	setNamespace(p, "shop", nil)
	if p.NamespaceExcluded("shop") {
		t.Fatal("namespace annotation removed must include it again")
	}
}

func ctrlRef(apiVersion, kind, name string) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name, UID: types.UID("uid-" + name), Controller: &yes}}
}

func TestPodExcluded_OwnerWalk(t *testing.T) {
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "load-generator-7f9c4", OwnerReferences: ctrlRef("apps/v1", "Deployment", "load-generator")}}
	lgPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "load-generator-7f9c4-abcde", OwnerReferences: ctrlRef("apps/v1", "ReplicaSet", "load-generator-7f9c4")}}
	apiPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "api-6d8b5-fghij", OwnerReferences: ctrlRef("apps/v1", "ReplicaSet", "api-6d8b5")}}
	apiRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "api-6d8b5", OwnerReferences: ctrlRef("apps/v1", "Deployment", "api")}}
	annotatedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "solo", Annotations: off}}
	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke", Annotations: off}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke-2890", OwnerReferences: ctrlRef("batch/v1", "CronJob", "smoke")}}
	jobPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke-2890-xyz12", OwnerReferences: ctrlRef("batch/v1", "Job", "smoke-2890")}}

	p, _ := exclusionPusher(t, rs, lgPod, apiPod, apiRS, annotatedPod, cj, job, jobPod)
	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, nil))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, nil))
	ctx := context.Background()

	for _, tc := range []struct {
		pod  string
		want bool
	}{
		{"load-generator-7f9c4-abcde", true}, // Pod → ReplicaSet → excluded Deployment
		{"api-6d8b5-fghij", false},
		{"solo", true},                       // the pod's own (pod-template) annotation
		{"smoke-2890-xyz12", true},           // Pod → Job → CronJob own metadata
		{"load-generator-7f9c4-zzzzz", true}, // gone: generated-name fallback
		{"api-6d8b5-zzzzz", false},
		{"nobody-knows", false},
	} {
		if got := p.PodExcluded(ctx, "shop", tc.pod); got != tc.want {
			t.Errorf("PodExcluded(%s) = %v, want %v", tc.pod, got, tc.want)
		}
	}
	setNamespace(p, "shop", off)
	if !p.PodExcluded(ctx, "shop", "api-6d8b5-fghij") {
		t.Error("a Namespace \"false\" must exclude every pod in it")
	}
}

// An owner the virtual-app index does not track (a CronJob) is re-read once
// its cached read expires, so an annotation added later applies.
func TestPodExcluded_LiveChangeOnUntrackedOwner(t *testing.T) {
	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke-2890", OwnerReferences: ctrlRef("batch/v1", "CronJob", "smoke")}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "smoke-2890-xyz12", OwnerReferences: ctrlRef("batch/v1", "Job", "smoke-2890")}}
	p, clock := exclusionPusher(t, cj, job, pod)
	ctx := context.Background()
	if p.PodExcluded(ctx, "shop", pod.Name) {
		t.Fatal("not annotated yet")
	}
	cj.Annotations = off
	if err := p.client.Update(ctx, cj); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(ownerLinkTTL + time.Second)
	if !p.PodExcluded(ctx, "shop", pod.Name) {
		t.Error("an annotation added to the CronJob must exclude its pods once the cached read expires")
	}
}

// --- emitter paths ---

func warning(ns, kind, name string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: name + ".1"},
		Type:           corev1.EventTypeWarning,
		Reason:         "BackOff",
		InvolvedObject: corev1.ObjectReference{Namespace: ns, APIVersion: "v1", Kind: kind, Name: name},
	}
}

func TestEmit_K8sEventsDropped(t *testing.T) {
	lgPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "load-generator-7f9c4-abcde", OwnerReferences: ctrlRef("apps/v1", "ReplicaSet", "load-generator-7f9c4")}}
	p, _ := exclusionPusher(t, lgPod)
	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, nil))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, nil))
	setNamespace(p, "test", off)
	_ = drain(p)

	p.onEvent(warning("shop", "Pod", "load-generator-7f9c4-abcde")) // OOMKilled pod
	p.onEvent(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "rs.1"}, Type: corev1.EventTypeWarning,
		InvolvedObject: corev1.ObjectReference{Namespace: "shop", APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "load-generator-7f9c4"}})
	p.onEvent(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "d.1"}, Type: corev1.EventTypeWarning,
		InvolvedObject: corev1.ObjectReference{Namespace: "shop", APIVersion: "apps/v1", Kind: "Deployment", Name: "load-generator"}})
	p.onEvent(warning("test", "Pod", "x-1"))
	p.onEvent(warning("shop", "Pod", "api-6d8b5-fghij"))

	frames := drain(p)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want only the included pod's event", len(frames))
	}
	if got := frames[0].K8sEvent.Event.InvolvedObject.Name; got != "api-6d8b5-fghij" {
		t.Errorf("sent event for %s, want api-6d8b5-fghij", got)
	}
}

func TestEmit_CollectorFramesDropped(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, map[string]string{labelName: "loadgen"}))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, nil))
	setNamespace(p, "test", off)
	_ = drain(p)

	in := []ServiceRedMetrics{{Namespace: "shop", Service: "load-generator"}, {Namespace: "shop", Service: "api"}, {Namespace: "test", Service: "x"}}
	p.SendLogMetrics(1, 60, []ServiceLogMetrics{{Namespace: "shop", Service: "loadgen", ErrorCount: 9}, {Namespace: "shop", Service: "api", ErrorCount: 9}})
	p.SendRedMetrics(1, 60, in)
	p.SendRedSummary(1, 60, in)
	p.SendUseMetrics(1, 60, []ServiceUseMetrics{{Namespace: "shop", Service: "load-generator", OOMKills: 3}, {Namespace: "shop", Service: "api"}})
	p.SendUseSummary(1, 60, []ServiceUseSummary{{Namespace: "shop", Service: "load-generator", OOMKills: 3}, {Namespace: "shop", Service: "api"}})
	// Every entry excluded: no frame at all.
	p.SendUseMetrics(1, 60, []ServiceUseMetrics{{Namespace: "shop", Service: "load-generator", OOMKills: 3}})

	got := map[string][]string{}
	for _, f := range drain(p) {
		var svcs []string
		switch f.Kind {
		case KindLogMetrics:
			for _, s := range f.LogMetrics.Services {
				svcs = append(svcs, s.Service)
			}
		case KindRedMetrics:
			for _, s := range f.RedMetrics.Services {
				svcs = append(svcs, s.Service)
			}
		case KindRedSummary:
			for _, s := range f.RedSummary.Services {
				svcs = append(svcs, s.Service)
			}
		case KindUseMetrics:
			for _, s := range f.UseMetrics.Services {
				svcs = append(svcs, s.Service)
			}
		case KindUseSummary:
			for _, s := range f.UseSummary.Services {
				svcs = append(svcs, s.Service)
			}
		}
		got[f.Kind] = append(got[f.Kind], svcs...)
	}
	for _, kind := range []string{KindLogMetrics, KindRedMetrics, KindRedSummary, KindUseMetrics, KindUseSummary} {
		if len(got[kind]) != 1 || got[kind][0] != "api" {
			t.Errorf("%s services = %v, want [api]", kind, got[kind])
		}
	}
	// The collector's own slice is untouched.
	if len(in) != 3 || in[0].Service != "load-generator" {
		t.Errorf("caller's slice was modified: %+v", in)
	}
}

func TestEmit_TopologyDropped(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "load-generator", off, nil, nil))
	addWorkload(p, deploymentObj("shop", "frontend", nil, nil, nil))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, nil))
	_ = drain(p)

	ok := p.SendTopologySnapshot(1, 600,
		[]TopologyNode{{Namespace: "shop", Workload: "load-generator"}, {Namespace: "shop", Workload: "frontend"}, {Namespace: "shop", Workload: "api"}},
		[]TopologyEdge{
			{SrcNamespace: "shop", SrcWorkload: "load-generator", DstNamespace: "shop", DstWorkload: "frontend", Calls: 5},
			{SrcNamespace: "shop", SrcWorkload: "frontend", DstNamespace: "shop", DstWorkload: "load-generator", Calls: 1},
			{SrcNamespace: "shop", SrcWorkload: "frontend", DstNamespace: "shop", DstWorkload: "api", Calls: 5},
			{SrcNamespace: "shop", SrcWorkload: "frontend", External: "example.com:443", Calls: 2},
		})
	if !ok {
		t.Fatal("topology snapshot must still be sent")
	}
	frames := drain(p)
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	ts := frames[0].TopologySnapshot
	for _, n := range ts.Nodes {
		if n.Workload == "load-generator" {
			t.Error("excluded node sent")
		}
	}
	if len(ts.Nodes) != 2 {
		t.Errorf("nodes = %+v, want frontend and api", ts.Nodes)
	}
	if len(ts.Edges) != 2 {
		t.Errorf("edges = %+v, want frontend→api and frontend→external", ts.Edges)
	}
	for _, e := range ts.Edges {
		if e.SrcWorkload == "load-generator" || e.DstWorkload == "load-generator" {
			t.Errorf("edge touching an excluded workload sent: %+v", e)
		}
	}
}

func TestEmit_SnapshotOmitsExcludedApps(t *testing.T) {
	p, _ := exclusionPusher(t)
	addWorkload(p, deploymentObj("shop", "load-generator", nil, off, nil))
	addWorkload(p, deploymentObj("shop", "api", nil, nil, nil))
	frames, n := p.snapshotFramesForTenant(context.Background(), p.opts.Tenant)
	if n != 1 {
		t.Fatalf("snapshot apps = %d, want 1", n)
	}
	for _, f := range frames {
		if f.Snapshot == nil {
			continue
		}
		for _, a := range f.Snapshot.Apps {
			if a.Name != "api" {
				t.Errorf("snapshot carries %s", a.Name)
			}
		}
	}
}

// A bare Pusher (no vapps, no namespace store, no client) excludes nothing
// and never panics.
func TestPredicate_NilSafe(t *testing.T) {
	var nilP *Pusher
	if nilP.ServiceExcluded("a", "b") || nilP.NamespaceExcluded("a") || nilP.PodExcluded(context.Background(), "a", "b") {
		t.Error("nil Pusher must exclude nothing")
	}
	p := &Pusher{log: logr.Discard()}
	if p.ServiceExcluded("a", "b") || p.PodExcluded(context.Background(), "a", "b") {
		t.Error("bare Pusher must exclude nothing")
	}
	if f := p.dropExcluded(newRedMetrics("c", 1, 60, []ServiceRedMetrics{{Namespace: "a", Service: "b"}})); f == nil || len(f.RedMetrics.Services) != 1 {
		t.Error("bare Pusher must pass frames through")
	}
}

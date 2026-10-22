package push

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// toUnstructuredTyped converts obj to unstructured form and stamps it with
// apiVersion/kind — needed (unlike toUnstructuredObj in virtualapps_test.go)
// whenever the object is handed to a fake dynamic client, whose tracker
// resolves an object's GVR/GVK from the object itself.
func toUnstructuredTyped(t *testing.T, obj any, apiVersion, kind string) *unstructured.Unstructured {
	t.Helper()
	u := toUnstructuredObj(obj)
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	return u
}

func noopHandlers() dynamicWorkloadHandlers {
	return dynamicWorkloadHandlers{
		onWorkload: func(any, string, bool) {},
	}
}

// testDeploymentGVR/testStatefulSetGVR/testDaemonSetGVR and
// testWorkloadResources stand in for resolveWorkloadResources' discovery
// gate throughout this file's tests, which exercise post-discovery behavior
// (TestResolveWorkloadResources, in workload_resources_test.go, covers
// discovery itself).
var (
	testDeploymentGVR  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	testStatefulSetGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	testDaemonSetGVR   = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
)

func testWorkloadResources() []workloadResource {
	return []workloadResource{
		{GVR: testDeploymentGVR, Kind: "Deployment"},
		{GVR: testStatefulSetGVR, Kind: "StatefulSet"},
		{GVR: testDaemonSetGVR, Kind: "DaemonSet"},
	}
}

// newWorkloadScheme registers apps/v1 (Deployment/StatefulSet/DaemonSet)
// into a fresh scheme — what a fake dynamic client's tracker needs to
// resolve a GVR's List Kind for the built-in kinds these tests use.
func newWorkloadScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return scheme
}

// --- dynamicInformerManager.ensure ---

func TestDynamicInformerManager_EnsureNoClientDegrades(t *testing.T) {
	m := newDynamicInformerManager(nil, nil, 0, logr.Discard(), noopHandlers())

	err := m.ensure(context.Background(), "rag")
	if err == nil {
		t.Fatal("ensure() with no dynamic client must return an error, not silently succeed")
	}
	if !errors.Is(err, errNoClientset) {
		t.Errorf("ensure() error = %v, want it to wrap errNoClientset", err)
	}
	if m.running("rag") {
		t.Error("no factory should be recorded as running after a failed ensure()")
	}
}

func TestDynamicInformerManager_EnsureIsIdempotent(t *testing.T) {
	dep := toUnstructuredTyped(t, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	}, "apps/v1", "Deployment")
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t), dep)
	m := newDynamicInformerManager(nil, dyn, 0, logr.Discard(), noopHandlers())
	m.workloadResources = testWorkloadResources()

	if err := m.ensure(context.Background(), "rag"); err != nil {
		t.Fatalf("first ensure() = %v, want nil", err)
	}
	if !m.running("rag") {
		t.Fatal("factory must be running after ensure()")
	}

	// A second ensure() call for the same namespace (annotation flap: add,
	// add again) must be a pure no-op — it must not create a second factory,
	// and the map must still have exactly one entry.
	if err := m.ensure(context.Background(), "rag"); err != nil {
		t.Fatalf("second ensure() = %v, want nil (idempotent)", err)
	}
	m.mu.Lock()
	n := len(m.namespaces)
	m.mu.Unlock()
	if n != 1 {
		t.Errorf("namespaces map has %d entries, want exactly 1 (no double-registration)", n)
	}
}

func TestDynamicInformerManager_EnsureSyncsExistingObjects(t *testing.T) {
	dep := toUnstructuredTyped(t, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	}, "apps/v1", "Deployment")
	sts := toUnstructuredTyped(t, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "db"},
	}, "apps/v1", "StatefulSet")
	ds := toUnstructuredTyped(t, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "agent"},
	}, "apps/v1", "DaemonSet")
	// A same-named Deployment in a DIFFERENT namespace proves the factory is
	// actually namespace-scoped, not cluster-wide.
	other := toUnstructuredTyped(t, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "worker"},
	}, "apps/v1", "Deployment")
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t), dep, sts, ds, other)
	m := newDynamicInformerManager(nil, dyn, 0, logr.Discard(), noopHandlers())
	m.workloadResources = testWorkloadResources()

	if err := m.ensure(context.Background(), "rag"); err != nil {
		t.Fatalf("ensure() = %v, want nil", err)
	}

	raws, err := m.listWorkloads("rag")
	if err != nil {
		t.Fatalf("listWorkloads(rag): %v", err)
	}
	got := make(map[string]bool, len(raws))
	for _, rw := range raws {
		got[rw.Kind+"/"+rw.Obj.GetName()] = true
	}
	for _, want := range []string{"Deployment/worker", "StatefulSet/db", "DaemonSet/agent"} {
		if !got[want] {
			t.Errorf("listWorkloads(rag) = %+v, missing %s", raws, want)
		}
	}
	if len(raws) != 3 {
		t.Errorf("listWorkloads(rag) returned %d items, want exactly 3", len(raws))
	}

	opsRaws, err := m.listWorkloads("ops")
	if err != nil || len(opsRaws) != 0 {
		t.Errorf("listWorkloads(ops) = %+v, %v; want none — no factory was ever started for ops", opsRaws, err)
	}
}

func TestDynamicInformerManager_ListOnUnstartedNamespaceIsNoop(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t))
	m := newDynamicInformerManager(nil, dyn, 0, logr.Discard(), noopHandlers())
	m.workloadResources = testWorkloadResources()

	raws, err := m.listWorkloads("never-started")
	if err != nil || raws != nil {
		t.Errorf("listWorkloads on a namespace with no factory = %+v, %v; want (nil, nil)", raws, err)
	}
}

// --- dynamicInformerManager.stop ---

func TestDynamicInformerManager_StopClosesChannelAndIsIdempotent(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t))
	m := newDynamicInformerManager(nil, dyn, 0, logr.Discard(), noopHandlers())

	if err := m.ensure(context.Background(), "rag"); err != nil {
		t.Fatalf("ensure() = %v, want nil", err)
	}
	m.mu.Lock()
	f := m.namespaces["rag"]
	m.mu.Unlock()
	if f == nil {
		t.Fatal("setup: factory must be recorded")
	}

	if !m.stop("rag") {
		t.Error("first stop() must report a transition (true)")
	}
	select {
	case _, open := <-f.stopCh:
		if open {
			t.Error("stopCh must be closed after stop()")
		}
	default:
		t.Error("stopCh must be closed (non-blocking receive should succeed) after stop()")
	}
	if m.running("rag") {
		t.Error("running() must be false after stop()")
	}
	if m.stop("rag") {
		t.Error("second stop() must report no transition (false) — idempotent")
	}
}

func TestDynamicInformerManager_StopNeverStartedIsNoop(t *testing.T) {
	m := newDynamicInformerManager(nil, nil, 0, logr.Discard(), noopHandlers())
	if m.stop("never-scoped") {
		t.Error("stop() of a namespace that never had a factory must report no transition (false)")
	}
}

// --- virtualAppsState integration: cacheCovers routing ---

func TestVirtualAppsState_CacheCovers(t *testing.T) {
	v := &virtualAppsState{cacheNamespaces: nil}
	if !v.cacheCovers("anything") {
		t.Error("empty cacheNamespaces must mean cluster-wide cache — everything covered")
	}
	v.cacheNamespaces = []string{"ops"}
	if !v.cacheCovers("ops") {
		t.Error("ops is in cacheNamespaces — must be covered")
	}
	if v.cacheCovers("rag") {
		t.Error("rag is not in cacheNamespaces — must not be covered")
	}
}

// TestVirtualAppsState_BackfillNamespace_CoveredNamespaceNeverStartsFactory is
// the "a dynamic ns already covered by CacheNamespaces does NOT create a
// factory" case: even though dynInformers is wired, a namespace the manager
// cache already covers must go through the manager-cache path unchanged, and
// never touch dynInformers at all.
func TestVirtualAppsState_BackfillNamespace_CoveredNamespaceNeverStartsFactory(t *testing.T) {
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), func(*Frame) {})
	v.ctx = context.Background()
	v.cacheNamespaces = []string{"ops"}
	v.workloadResources = testWorkloadResources()
	dep := toUnstructuredTyped(t, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	}, "apps/v1", "Deployment")
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t), dep)
	v.dynInformers = newDynamicInformerManager(nil, dyn, 0, logr.Discard(), dynamicWorkloadHandlers{
		onWorkload: v.onWorkload,
	})
	v.dynInformers.workloadResources = v.workloadResources
	// No manager cache wired (v.cache stays nil) — "ops" is cache-covered per
	// cacheNamespaces, so backfillNamespace must take the manager-cache path
	// (which degrades to nil/nil with no cache, same as
	// TestVirtualAppsState_BackfillNamespace_NoCacheAndNoOverrideIsNoop) and
	// must NEVER fall back to dynInformers for it.

	v.backfillNamespace("ops")

	if v.dynInformers.running("ops") {
		t.Error("ops is covered by the manager cache — a runtime factory must never have been started for it")
	}
	if len(v.snapshotApps()) != 0 {
		t.Error("no manager cache was wired, so nothing should have been backfilled")
	}
}

// TestVirtualAppsState_BackfillNamespace_UncoveredNamespaceUsesRuntimeFactory
// is the crux of the scoped-cache fix end to end: a namespace NOT covered by
// the manager cache must fall back to a runtime per-namespace informer
// factory, and a Deployment already steady in that namespace (as if it
// existed before the namespace ever entered dynamic scope) must be
// discovered through that factory's own listers and backfilled into vapp
// synthesis — exactly the "annotate -> factory delivers cached steady
// Deployment -> synthesized vapp appears" scenario.
func TestVirtualAppsState_BackfillNamespace_UncoveredNamespaceUsesRuntimeFactory(t *testing.T) {
	var mu sync.Mutex
	var frames []*Frame
	enqueue := func(f *Frame) {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, f)
	}
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), enqueue)
	v.ctx = context.Background()
	// The manager cache only covers "ops" — "rag" is outside it, exactly the
	// bug's repro: a scoped cache that a triageagent.dev/tenant annotation on "rag"
	// cannot widen at runtime.
	v.cacheNamespaces = []string{"ops"}
	v.workloadResources = testWorkloadResources()

	cachedDeployment := toUnstructuredTyped(t, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "rag", Name: "worker", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32p(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1, ObservedGeneration: 1},
	}, "apps/v1", "Deployment")
	dyn := dynamicfake.NewSimpleDynamicClient(newWorkloadScheme(t), cachedDeployment)
	v.dynInformers = newDynamicInformerManager(nil, dyn, 0, logr.Discard(), dynamicWorkloadHandlers{
		onWorkload: v.onWorkload,
	})
	v.dynInformers.workloadResources = v.workloadResources

	if len(v.workloadsSnapshot()) != 0 {
		t.Fatal("setup: nothing must be known about rag yet")
	}

	// Mirrors Pusher.addDynamicScope's call once dynamicScope.add("rag")
	// transitions true — dynamicScope membership itself is exercised in
	// pusher_test.go; this test drives virtualAppsState's own half directly.
	v.backfillNamespace("rag")

	if !v.dynInformers.running("rag") {
		t.Error("a runtime informer factory must have been started for the uncovered namespace rag")
	}
	apps := v.snapshotApps()
	if len(apps) != 1 || apps[0].Name != "worker" {
		t.Fatalf("snapshotApps = %+v, want one app named worker (discovered via the runtime factory's listers)", apps)
	}
	mu.Lock()
	found := false
	for _, f := range frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpAdd && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	mu.Unlock()
	if !found {
		t.Errorf("no OpAdd app_delta frame for worker; frames = %+v", frames)
	}

	// --- Removal: annotation gone (or namespace deleted) evicts the
	// workload AND tears down the runtime factory, mirroring
	// Pusher.dropDynamicScope -> evictNamespaceAndRecompute. ---
	mu.Lock()
	frames = nil
	mu.Unlock()

	v.evictNamespaceAndRecompute("rag")

	if v.dynInformers.running("rag") {
		t.Error("the runtime informer factory for rag must be stopped once it leaves scope")
	}
	if len(v.snapshotApps()) != 0 {
		t.Error("worker's group must be gone after eviction")
	}
	mu.Lock()
	defer mu.Unlock()
	found = false
	for _, f := range frames {
		if f.Kind == KindAppDelta && f.AppDelta.Op == OpDelete && f.AppDelta.App.Name == "worker" {
			found = true
		}
	}
	if !found {
		t.Errorf("no OpDelete app_delta frame for worker; frames = %+v", frames)
	}
}

// TestVirtualAppsState_BackfillNamespace_NoClientsetDegradesGracefully proves
// the documented degrade path: dynInformers wired with a nil dynamic client
// (as Options.DynamicClient would be if main.go failed to build one) must
// log and return, never panic, for an out-of-cache namespace.
func TestVirtualAppsState_BackfillNamespace_NoClientsetDegradesGracefully(t *testing.T) {
	v := newVirtualAppsState(VirtualAppsOn, "cluster-a", []string{"ops"}, logr.Discard(), func(*Frame) {})
	v.ctx = context.Background()
	v.cacheNamespaces = []string{"ops"}
	v.workloadResources = testWorkloadResources()
	v.dynInformers = newDynamicInformerManager(nil, nil, 0, logr.Discard(), dynamicWorkloadHandlers{
		onWorkload: v.onWorkload,
	})

	v.backfillNamespace("rag") // must not panic

	if len(v.snapshotApps()) != 0 {
		t.Error("nothing should have been backfilled with no dynamic client configured")
	}
}

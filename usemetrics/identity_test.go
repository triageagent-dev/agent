package usemetrics

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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/push"
)

// newIdentityTestCollector builds a Collector whose identity resolver reads a fake
// client seeded with objects — no sensor stream or push wiring, since
// resolveServiceIdentity is the only thing under test in this file. Wired to
// the explicit labels identity mode: TestResolveServiceIdentity's table
// exercises the part-of/instance/name chain against realistic k8s label
// patterns, not the default workload identity (covered separately in
// TestResolveOwnerIdentity_WorkloadModeReturnsOwnerName).
func newIdentityTestCollector(t *testing.T, objects ...client.Object) *Collector {
	t.Helper()
	scheme := newTestScheme(t)
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return &Collector{log: logr.Discard(), opts: Options{now: time.Now}, identity: &OwnerIdentityResolver{
		Client:  fc,
		ModeFor: func(string) string { return push.IdentityModeLabels },
	}}
}

// controllerRef builds a controller owner reference — the shape
// metav1.GetControllerOfNoCopy requires to recognize an owner at all. UID is
// left blank; tests that need to exercise the owner-identity cache (which
// keys on UID) use controllerRefUID instead.
func controllerRef(apiVersion, kind, name string) metav1.OwnerReference {
	isController := true
	return metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		Controller: &isController,
	}
}

// controllerRefUID is controllerRef with an explicit UID — needed wherever a
// test cares about the (namespace, UID) owner-identity cache key, e.g. to
// prove a cache hit, or to keep two distinct owners from colliding on a
// blank UID within the same Collector.
func controllerRefUID(apiVersion, kind, name, uid string) metav1.OwnerReference {
	ref := controllerRef(apiVersion, kind, name)
	ref.UID = types.UID(uid)
	return ref
}

// unstructuredOwner builds a minimal unstructured object for a CRD/operator
// owner kind with no Go struct in this repo (e.g. an Argo Rollout or a
// kagent Sandbox) — resolveOwnerIdentity's generic walk fetches these the
// same way it fetches a typed apps/v1 object.
func unstructuredOwner(apiVersion, kind, namespace, name string, labels map[string]string, owners ...metav1.OwnerReference) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	u.SetNamespace(namespace)
	u.SetName(name)
	if labels != nil {
		u.SetLabels(labels)
	}
	if len(owners) > 0 {
		u.SetOwnerReferences(owners)
	}
	return u
}

// TestResolveServiceIdentity covers one case per recognized owner kind, the
// ReplicaSet -> Deployment and Job -> CronJob two-hop chains, a standalone
// ReplicaSet, a ReplicaSet's further owner resolved generically (a Rollout —
// no allowlist of controller kinds), a CRD owning a pod directly, the
// group-override annotation, and every fallback path (pod gone, no owner,
// an owner whose object can't be fetched at all, and a ReplicaSet whose
// further owner is known by name but can't be fetched).
func TestResolveServiceIdentity(t *testing.T) {
	tests := []struct {
		name         string
		namespace    string
		podName      string
		objects      []client.Object
		wantOK       bool
		wantIdentity string
	}{
		{
			// Real labels observed on tenant-a-gke (tenant-a): instance disagrees
			// with name/app, and the instance label is what virtual-app
			// synthesis (and therefore the core's graph) names this workload.
			name:      "StatefulSet-owned pod with real tenant-a-gke labels resolves to the instance label",
			namespace: "tenant-a",
			podName:   "tenant-a-qdrant-0",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "tenant-a-qdrant-0", Namespace: "tenant-a",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "tenant-a-qdrant")},
					},
				},
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "tenant-a-qdrant", Namespace: "tenant-a",
						Labels: map[string]string{
							"app.kubernetes.io/instance": "tenant-a-qdrant",
							"app.kubernetes.io/name":     "qdrant",
							"app":                        "qdrant",
						},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "tenant-a-qdrant",
		},
		{
			// Real labels observed on tenant-a-gke (tenant-a): a Deployment reached
			// through its ReplicaSet, the two-hop chain.
			name:      "Deployment-owned pod via ReplicaSet resolves to the instance label",
			namespace: "tenant-a",
			podName:   "tenant-a-tenant-a-backend-7f9c8d-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "tenant-a-tenant-a-backend-7f9c8d-abcde", Namespace: "tenant-a",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "ReplicaSet", "tenant-a-tenant-a-backend-7f9c8d")},
					},
				},
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "tenant-a-tenant-a-backend-7f9c8d", Namespace: "tenant-a",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "tenant-a-tenant-a-backend")},
					},
				},
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name: "tenant-a-tenant-a-backend", Namespace: "tenant-a",
						Labels: map[string]string{
							"app.kubernetes.io/instance": "tenant-a-tenant-a-backend",
							"app.kubernetes.io/name":     "tenant-a-backend",
						},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "tenant-a-tenant-a-backend",
		},
		{
			// Real labels observed on tenant-a-gke: no app.kubernetes.io/* labels at
			// all, only the plain "app" label — GroupNameFor's fallback rule
			// resolves this to the StatefulSet's OWN NAME, "neo4j", not the
			// "app" label's value (which groupNameFor never reads at all).
			// They happen to agree here only because the StatefulSet itself
			// is named neo4j.
			name:      "StatefulSet-owned pod with no app.kubernetes.io labels resolves to the StatefulSet's own name",
			namespace: "tenant-a",
			podName:   "neo4j-0",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "neo4j-0", Namespace: "tenant-a",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "neo4j")},
					},
				},
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "neo4j", Namespace: "tenant-a",
						Labels: map[string]string{"app": "neo4j"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "neo4j",
		},
		{
			name:      "DaemonSet-owned pod resolves off the DaemonSet object",
			namespace: "triage",
			podName:   "triage-node-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "triage-node-abcde", Namespace: "triage",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "DaemonSet", "triage-node")},
					},
				},
				&appsv1.DaemonSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "triage-node", Namespace: "triage",
						Labels: map[string]string{"app.kubernetes.io/name": "triage-node"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "triage-node",
		},
		{
			name:      "standalone Job-owned pod resolves off the Job object",
			namespace: "ops",
			podName:   "migrate-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "migrate-abcde", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("batch/v1", "Job", "migrate")},
					},
				},
				&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name: "migrate", Namespace: "ops",
						Labels: map[string]string{"app.kubernetes.io/name": "migrate"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "migrate",
		},
		{
			name:      "CronJob-owned Job pod walks the two-hop chain to the CronJob",
			namespace: "ops",
			podName:   "nightly-28372910-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nightly-28372910-abcde", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("batch/v1", "Job", "nightly-28372910")},
					},
				},
				&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nightly-28372910", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("batch/v1", "CronJob", "nightly")},
					},
				},
				&batchv1.CronJob{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nightly", Namespace: "ops",
						Labels: map[string]string{"app.kubernetes.io/instance": "nightly-prod"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "nightly-prod",
		},
		{
			name:      "standalone ReplicaSet with no controller of its own resolves off the ReplicaSet object",
			namespace: "ops",
			podName:   "manual-rs-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "manual-rs-abcde", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "ReplicaSet", "manual-rs")},
					},
				},
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "manual-rs", Namespace: "ops",
						Labels: map[string]string{"app.kubernetes.io/name": "manual-rs"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "manual-rs",
		},
		{
			// The core bug this walk fixes: a ReplicaSet's controller is an
			// Argo Rollout, not a Deployment — a kind this repo has no Go
			// struct for at all. The generic walk fetches it the same way it
			// fetches a Deployment, by the ownerReference's own apiVersion/
			// kind, and names the pod after the Rollout, never the
			// ReplicaSet's own pod-template-hash name.
			name:      "ReplicaSet owned by an Argo Rollout resolves generically to the Rollout",
			namespace: "ops",
			podName:   "canary-6f8b7c-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "canary-6f8b7c-abcde", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "ReplicaSet", "canary-6f8b7c")},
					},
				},
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "canary-6f8b7c", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("argoproj.io/v1alpha1", "Rollout", "canary")},
					},
				},
				unstructuredOwner("argoproj.io/v1alpha1", "Rollout", "ops", "canary",
					map[string]string{"app.kubernetes.io/instance": "canary-rollout"}),
			},
			wantOK:       true,
			wantIdentity: "canary-rollout",
		},
		{
			// A CRD/operator kind owning the pod DIRECTLY (no ReplicaSet/Job
			// hop at all) — the same generic fetch, one hop.
			name:      "pod owned directly by a custom Sandbox kind resolves by its name and labels",
			namespace: "agents",
			podName:   "sandbox-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sandbox-abcde", Namespace: "agents",
						OwnerReferences: []metav1.OwnerReference{controllerRef("agents.x-k8s.io/v1alpha1", "Sandbox", "sandbox-1")},
					},
				},
				unstructuredOwner("agents.x-k8s.io/v1alpha1", "Sandbox", "agents", "sandbox-1",
					map[string]string{"app.kubernetes.io/name": "sandbox-1"}),
			},
			wantOK:       true,
			wantIdentity: "sandbox-1",
		},
		{
			name:      "annotationkeys.VirtualAppGroup annotation on the owning workload outranks its labels",
			namespace: "ops",
			podName:   "shared-0",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "shared-0", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "shared")},
					},
				},
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "shared", Namespace: "ops",
						Labels:      map[string]string{"app.kubernetes.io/name": "shared"},
						Annotations: map[string]string{annotationkeys.VirtualAppGroup: "neo4j"},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "neo4j",
		},
		{
			name:      "pod not found falls back",
			namespace: "ops",
			podName:   "gone",
			wantOK:    false,
		},
		{
			name:      "pod with no owner reference falls back",
			namespace: "ops",
			podName:   "bare",
			objects: []client.Object{
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bare", Namespace: "ops"}},
			},
			wantOK: false,
		},
		{
			// No kind allowlist any more: an owner kind the fake client has
			// nothing seeded for still resolves — degraded to the pod's own
			// labels (none here) plus the owner reference's own name, never
			// an outright failure.
			name:      "unrecognized owner kind with no object to fetch falls back to the owner name",
			namespace: "ops",
			podName:   "weird",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "weird", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("v1", "ReplicationController", "legacy")},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "legacy",
		},
		{
			name:      "owner object itself missing falls back to the owner reference's name",
			namespace: "ops",
			podName:   "orphaned",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "orphaned", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "does-not-exist")},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "does-not-exist",
		},
		{
			// Item 4's exact scenario: the ReplicaSet itself IS fetched, so
			// its Rollout controller's real name ("missing-rollout") is
			// known for certain — but the Rollout object can't be fetched
			// (deleted/RBAC). The fallback must use that Rollout name's
			// pod-label chain, and must NEVER collapse back to the
			// ReplicaSet's own pod-template-hash name ("orphan-rs-abc123").
			name:      "ReplicaSet resolved but its Rollout can't be fetched falls back via the pod's own labels, never the ReplicaSet hash",
			namespace: "ops",
			podName:   "orphan-rollout-abcde",
			objects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "orphan-rollout-abcde", Namespace: "ops",
						Labels:          map[string]string{"app.kubernetes.io/name": "orphan-app"},
						OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "ReplicaSet", "orphan-rs-abc123")},
					},
				},
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name: "orphan-rs-abc123", Namespace: "ops",
						OwnerReferences: []metav1.OwnerReference{controllerRef("argoproj.io/v1alpha1", "Rollout", "missing-rollout")},
					},
				},
			},
			wantOK:       true,
			wantIdentity: "orphan-app",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newIdentityTestCollector(t, tc.objects...)
			got, ok := c.resolveServiceIdentity(context.Background(), tc.namespace, tc.podName)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (identity=%q)", ok, tc.wantOK, got)
			}
			if ok && got != tc.wantIdentity {
				t.Errorf("identity = %q, want %q", got, tc.wantIdentity)
			}
		})
	}
}

// TestResolveServiceIdentity_DefaultModeIgnoresLabels proves the default —
// no Options.IdentityModeFor wired at all, the shape a Collector gets when
// push.Pusher.IdentityModeFor is never threaded through, or when a namespace
// carries no triageagent.dev/virtual-app-identity annotation — resolves by the
// owning workload's own name even when part-of/instance labels are present,
// exactly like the explicit workload mode.
func TestResolveServiceIdentity_DefaultModeIgnoresLabels(t *testing.T) {
	scheme := newTestScheme(t)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant-a-qdrant", Namespace: "tenant-a",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "not-this-name",
				"app.kubernetes.io/name":     "also-not-this-name",
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tenant-a-qdrant-0", Namespace: "tenant-a",
			OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "StatefulSet", "tenant-a-qdrant")},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, pod).Build()
	c := &Collector{log: logr.Discard(), opts: Options{now: time.Now}, identity: &OwnerIdentityResolver{Client: fc}}

	got, ok := c.resolveServiceIdentity(context.Background(), "tenant-a", "tenant-a-qdrant-0")
	if !ok || got != "tenant-a-qdrant" {
		t.Fatalf("resolveServiceIdentity = (%q, %v), want (%q, true) — default mode names by the workload, ignoring its labels", got, ok, "tenant-a-qdrant")
	}
}

// TestResolveServiceIdentity_NilResolveClientFallsBack covers a Collector
// built without New (e.g. an older call site, or a test that never wires
// resolveClient) — resolution must degrade to the fallback, never panic.
func TestResolveServiceIdentity_NilResolveClientFallsBack(t *testing.T) {
	c := &Collector{log: logr.Discard()}
	if _, ok := c.resolveServiceIdentity(context.Background(), "ops", "any"); ok {
		t.Fatal("resolveServiceIdentity with a nil resolveClient must return ok=false")
	}
}

// TestResolveOwnerIdentity_WorkloadModeReturnsOwnerName pins item 5: in
// IdentityModeWorkload, the top owner's own name IS the identity, ignoring
// its labels entirely — the same rule GroupNameFor already enforces,
// exercised here through the generic owner-fetch path.
func TestResolveOwnerIdentity_WorkloadModeReturnsOwnerName(t *testing.T) {
	scheme := newTestScheme(t)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "billing-worker", Namespace: "ops",
			// A misleading label that would win under IdentityModeLabels —
			// proving workload mode really does ignore it.
			Labels: map[string]string{"app.kubernetes.io/instance": "not-this-name"},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()
	r := &OwnerIdentityResolver{Client: fc, ModeFor: func(string) string { return push.IdentityModeWorkload }}
	owner := controllerRef("apps/v1", "StatefulSet", "billing-worker")
	got := r.resolveOwnerIdentity(context.Background(), "ops", &owner, map[string]string{"app.kubernetes.io/instance": "also-not-this"})
	if got != "billing-worker" {
		t.Fatalf("identity = %q, want the owner's own name %q", got, "billing-worker")
	}
}

// countingReader wraps a client.Reader and counts Get calls by object name —
// used to prove the owner-identity cache actually elides the second walk's
// Gets rather than merely returning the same answer coincidentally.
type countingReader struct {
	client.Reader
	gets map[string]int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if r.gets == nil {
		r.gets = make(map[string]int)
	}
	r.gets[key.Name]++
	return r.Reader.Get(ctx, key, obj, opts...)
}

// TestResolveOwnerIdentity_CacheHitAvoidsSecondGet pins item 3: two pods
// owned by the SAME ReplicaSet (same UID) resolve through one walk — the
// second pod's resolution must not re-Get the ReplicaSet or the Deployment
// it resolves to.
func TestResolveOwnerIdentity_CacheHitAvoidsSecondGet(t *testing.T) {
	scheme := newTestScheme(t)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "api-7f9c8d", Namespace: "ops",
			OwnerReferences: []metav1.OwnerReference{controllerRef("apps/v1", "Deployment", "api")},
		},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "api", Namespace: "ops",
			Labels: map[string]string{"app.kubernetes.io/name": "api"},
		},
	}
	pod1 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-7f9c8d-aaaaa", Namespace: "ops",
		OwnerReferences: []metav1.OwnerReference{controllerRefUID("apps/v1", "ReplicaSet", "api-7f9c8d", "rs-uid-1")}}}
	pod2 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-7f9c8d-bbbbb", Namespace: "ops",
		OwnerReferences: []metav1.OwnerReference{controllerRefUID("apps/v1", "ReplicaSet", "api-7f9c8d", "rs-uid-1")}}}

	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, dep, pod1, pod2).Build()
	counting := &countingReader{Reader: fc}
	c := &Collector{log: logr.Discard(), opts: Options{now: time.Now}, identity: &OwnerIdentityResolver{Client: counting}}

	got1, ok1 := c.resolveServiceIdentity(context.Background(), "ops", "api-7f9c8d-aaaaa")
	if !ok1 || got1 != "api" {
		t.Fatalf("first resolution = (%q, %v), want (api, true)", got1, ok1)
	}
	if counting.gets["api-7f9c8d"] != 1 || counting.gets["api"] != 1 {
		t.Fatalf("after first resolution, gets = %+v, want exactly one Get each for api-7f9c8d and api", counting.gets)
	}

	got2, ok2 := c.resolveServiceIdentity(context.Background(), "ops", "api-7f9c8d-bbbbb")
	if !ok2 || got2 != "api" {
		t.Fatalf("second resolution = (%q, %v), want (api, true)", got2, ok2)
	}
	if counting.gets["api-7f9c8d"] != 1 || counting.gets["api"] != 1 {
		t.Fatalf("after second resolution (same owner UID), gets = %+v, want UNCHANGED counts (cache hit)", counting.gets)
	}
	// The second pod's own Get is real work the cache can't and shouldn't
	// elide — only the owner walk behind it.
	if counting.gets["api-7f9c8d-bbbbb"] != 1 {
		t.Fatalf("pod2's own Get count = %d, want 1", counting.gets["api-7f9c8d-bbbbb"])
	}
}

// TestStaticIdentityResolver pins the host resolver: a pod in the synthetic
// namespace is its own identity, anything else is left to the reported label.
func TestStaticIdentityResolver(t *testing.T) {
	r := StaticIdentityResolver{Namespace: "host"}
	if got, ok := r.ResolveIdentity(context.Background(), "host", "nginx"); !ok || got != "nginx" {
		t.Fatalf("host/nginx = (%q, %v), want (nginx, true)", got, ok)
	}
	if _, ok := r.ResolveIdentity(context.Background(), "other", "nginx"); ok {
		t.Fatal("a pod outside the synthetic namespace must not resolve")
	}
	if _, ok := r.ResolveIdentity(context.Background(), "host", ""); ok {
		t.Fatal("an empty pod name must not resolve")
	}
}

// TestNoIdentityResolver_ResolvesNothing pins the resolver of an agent that
// watches no Kubernetes objects: it never names an identity, so the collector
// keeps the exporter's own service label.
func TestNoIdentityResolver_ResolvesNothing(t *testing.T) {
	var r IdentityResolver = NoIdentityResolver{}
	if id, ok := r.ResolveIdentity(context.Background(), "ns", "pod-1"); ok || id != "" {
		t.Errorf("ResolveIdentity = (%q, %v), want (\"\", false)", id, ok)
	}
}

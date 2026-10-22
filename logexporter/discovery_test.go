package logexporter

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

func TestNamespaceSetAndAllowed(t *testing.T) {
	if s := namespaceSet(nil); s != nil {
		t.Fatalf("namespaceSet(nil) = %v, want nil", s)
	}
	if s := namespaceSet([]string{"", "  "}); s != nil {
		t.Fatalf("namespaceSet(blank-only) = %v, want nil", s)
	}
	set := namespaceSet([]string{"ops", " billing ", ""})
	if !namespaceAllowed(set, "ops") || !namespaceAllowed(set, "billing") {
		t.Fatal("expected ops and billing allowed")
	}
	if namespaceAllowed(set, "kube-system") {
		t.Fatal("expected kube-system NOT allowed")
	}
	if !namespaceAllowed(nil, "anything") {
		t.Fatal("nil set must allow everything")
	}
}

func TestServiceIdentity(t *testing.T) {
	yes := true
	owner := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &yes}}
	}
	cases := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{"name label without owner", corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "api", "app": "other"}}}, "api"},
		{"app fallback without owner", corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}}}, "worker"},
		{"unattributable", corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"env": "prod"}}}, ""},
		{"no labels", corev1.Pod{}, ""},
		{"daemonset owner beats the label", corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app.kubernetes.io/name": "ebpf"}, OwnerReferences: owner("DaemonSet", "triage-agent-ebpf")}}, "triage-agent-ebpf"},
		{"deployment behind a replicaset", corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"pod-template-hash": "7d9f8b6c4", "app": "x"}, OwnerReferences: owner("ReplicaSet", "api-7d9f8b6c4")}}, "api"},
		{"statefulset owner", corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: owner("StatefulSet", "db")}}, "db"},
		{"virtual app group wins", corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"triageagent.dev/virtual-app-group": "shop"}, OwnerReferences: owner("ReplicaSet", "api-7d9f8b6c4")}}, "shop"},
		{"non-controller owner ignored", corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "worker"}, OwnerReferences: []metav1.OwnerReference{{Kind: "Foo", Name: "foo"}}}}, "worker"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := serviceIdentity(&tc.pod); got != tc.want {
				t.Errorf("serviceIdentity = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveService(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-1",
			Namespace: "ops",
			Labels:    map[string]string{"app.kubernetes.io/name": "api"},
		},
	}
	clientset := fake.NewSimpleClientset(pod)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	lister := podInformer.Lister()

	if svc, ok := resolveService(lister, "ops", "api-1"); !ok || svc != "api" {
		t.Fatalf("resolveService = (%q, %v), want (api, true)", svc, ok)
	}
	if _, ok := resolveService(lister, "ops", "does-not-exist"); ok {
		t.Fatal("expected ok=false for an unknown pod")
	}
}

// TestResolveService_TriageExcludedPod: a pod whose template carries
// triageagent.dev/triage: "false" resolves to no service, so its log lines
// are never counted under any service.
func TestResolveService_TriageExcludedPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        "load-generator-1",
		Namespace:   "ops",
		Labels:      map[string]string{"app.kubernetes.io/name": "load-generator"},
		Annotations: map[string]string{annotationkeys.Triage: "false"},
	}}
	factory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(pod), 0)
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	svc, ok := resolveService(podInformer.Lister(), "ops", "load-generator-1")
	if !ok || svc != "" {
		t.Fatalf("resolveService = (%q, %v), want (\"\", true): resolved but unattributable", svc, ok)
	}
}

func TestScanLogDir(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"api-1_ops_api-cid1.log",
		"worker-1_billing_worker-cid2.log",
		"not-a-container-log.txt",
		"malformed.log",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A subdirectory must be skipped, not error the whole scan.
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	got := scanLogDir(dir, nil, nil)
	if len(got) != 2 {
		t.Fatalf("got %d files, want 2: %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, f := range got {
		seen[f.namespace+"/"+f.pod+"/"+f.container] = true
	}
	if !seen["ops/api-1/api"] || !seen["billing/worker-1/worker"] {
		t.Fatalf("unexpected discovered set: %+v", got)
	}

	// Namespace filter narrows the result.
	filtered := scanLogDir(dir, namespaceSet([]string{"ops"}), nil)
	if len(filtered) != 1 || filtered[0].namespace != "ops" {
		t.Fatalf("filtered = %+v, want only ops", filtered)
	}

	// A missing directory returns empty, not an error.
	if got := scanLogDir(filepath.Join(dir, "does-not-exist"), nil, nil); got != nil {
		t.Fatalf("expected nil for missing dir, got %+v", got)
	}
}

// TestScanLogDirDynamicGateOverridesStaticSet pins the rule the log-exporter
// exists to honour: a namespace with no triageagent.dev/tenant annotation is not read,
// and the live predicate outranks whatever static set was configured.
func TestScanLogDirDynamicGateOverridesStaticSet(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{
		"app-1_gitlab_web-abc.log",
		"app-2_pact-broker_db-def.log",
	} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Static set says allow everything; the dynamic gate says gitlab only.
	got := scanLogDir(dir, nil, func(ns string) bool { return ns == "gitlab" })
	if len(got) != 1 {
		t.Fatalf("want 1 file, got %d: %+v", len(got), got)
	}
	if got[0].namespace != "gitlab" {
		t.Errorf("read the wrong namespace: %q", got[0].namespace)
	}
}

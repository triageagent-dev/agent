package nodeexporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/usemetrics"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestExporter_HandleMetrics is an end-to-end test of Collect's OTLP output:
// a fake clientset seeds one Running pod, a cgroupfs-layout fixture
// tree on disk supplies its PSI/memory/cpu.stat readings, and Collect is called
// directly (bypassing New/Run — no rest.Config or sensor stream connection needed).
func TestExporter_HandleMetrics(t *testing.T) {
	cgroupRoot := t.TempDir()
	uid := types.UID("uid-1")
	// Guaranteed QoS, cgroupfs layout: root/kubepods/pod<uid>.
	dir := filepath.Join(cgroupRoot, "kubepods", "poduid-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "memory.pressure"), "some avg10=0.00 avg60=42.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
	writeFile(t, filepath.Join(dir, "memory.current"), "500\n")
	writeFile(t, filepath.Join(dir, "memory.max"), "1000\n")
	writeFile(t, filepath.Join(dir, "memory.events"), "low 0\nhigh 0\nmax 0\noom 1\noom_kill 1\noom_group_kill 0\n")
	writeFile(t, filepath.Join(dir, "cpu.stat"), "usage_usec 1\nuser_usec 1\nsystem_usec 1\nnr_periods 10\nnr_throttled 2\nthrottled_usec 1\n")
	// Deliberately no cpu.pressure/io.pressure files — exercises the CONFIG_PSI-off
	// graceful degrade: those two gauges must simply be absent from the output.

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-1",
			Namespace: "ops",
			UID:       uid,
			Labels:    map[string]string{"app.kubernetes.io/name": "api"},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			Phase:    corev1.PodRunning,
			QOSClass: corev1.PodQOSGuaranteed,
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

	e := &Exporter{
		opts:    Options{CgroupRoot: cgroupRoot},
		factory: factory,
		lister:  podInformer.Lister(),
		log:     logr.Discard(),
	}

	body := collectText(t, e)

	wantLines := []string{
		`use.mem.psi.avg60{namespace="ops",service="api",pod="api-1"} 42`,
		`use.mem.pct{namespace="ops",service="api",pod="api-1"} 50`,
		`use.oom.kill{namespace="ops",service="api",pod="api-1"} 1`,
		`use.cpu.nr.periods{namespace="ops",service="api",pod="api-1"} 10`,
		`use.cpu.nr.throttled{namespace="ops",service="api",pod="api-1"} 2`,
	}
	for _, want := range wantLines {
		if !strings.Contains(body, want) {
			t.Errorf("body missing line %q\nfull body:\n%s", want, body)
		}
	}
	// PSI-absent gauges must be omitted entirely, not emitted as 0.
	for _, absent := range []string{"use.cpu.psi.avg60{", "use.io.psi.avg60{"} {
		if strings.Contains(body, absent) {
			t.Errorf("body unexpectedly contains %q — PSI-absent gauges must be omitted, not zeroed", absent)
		}
	}
}

// TestExporter_HandleMetrics_SkipsPodWithNoCgroupFound verifies a pod whose cgroup
// directory can't be found under either layout is silently skipped, not an error
// for the whole collect.
func TestExporter_HandleMetrics_SkipsPodWithNoCgroupFound(t *testing.T) {
	cgroupRoot := t.TempDir() // empty — no pod cgroup directories exist at all

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ghost", Namespace: "ops", UID: types.UID("nope")},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, QOSClass: corev1.PodQOSBestEffort},
	}
	clientset := fake.NewSimpleClientset(pod)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	e := &Exporter{
		opts:    Options{CgroupRoot: cgroupRoot},
		factory: factory,
		lister:  podInformer.Lister(),
		log:     logr.Discard(),
	}

	body := collectText(t, e)
	if strings.Contains(body, "ghost") {
		t.Errorf("body unexpectedly mentions the skipped pod:\n%s", body)
	}
}

// TestExporter_HandleMetrics_NodeAndRequestMetrics verifies the four new
// gauge families (2026-08-29): node total memory/cores are repeated on the
// pod's own sample set from the Exporter's cached (New-time) resolution, and
// request bytes/cores are read straight from the pod spec.
func TestExporter_HandleMetrics_NodeAndRequestMetrics(t *testing.T) {
	cgroupRoot := t.TempDir()
	uid := types.UID("uid-2")
	dir := filepath.Join(cgroupRoot, "kubepods", "poduid-2")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "memory.current"), "500\n")
	writeFile(t, filepath.Join(dir, "memory.max"), "1000\n")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-2",
			Namespace: "ops",
			UID:       uid,
			Labels:    map[string]string{"app.kubernetes.io/name": "api"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			InitContainers: []corev1.Container{
				{
					Name: "init",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("128"), // bytes, matches the cgroup fixture's small numbers
							corev1.ResourceCPU:    resource.MustParse("250m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			// Guaranteed: matches the "kubepods/pod<uid>" cgroupfs fixture
			// layout below, which carries no qos-slice path segment.
			QOSClass: corev1.PodQOSGuaranteed,
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

	e := &Exporter{
		opts:              Options{CgroupRoot: cgroupRoot},
		factory:           factory,
		lister:            podInformer.Lister(),
		log:               logr.Discard(),
		nodeMemTotalBytes: 16 * 1024 * 1024 * 1024,
		nodeMemTotalOK:    true,
		nodeCPUCores:      8,
	}

	body := collectText(t, e)

	wantLines := []string{
		`use.mem.node.total.bytes{namespace="ops",service="api",pod="api-2"} 1.7179869184e+10`,
		`use.cpu.node.cores{namespace="ops",service="api",pod="api-2"} 8`,
		`use.mem.request.bytes{namespace="ops",service="api",pod="api-2"} 128`,
		`use.cpu.request.cores{namespace="ops",service="api",pod="api-2"} 0.25`,
	}
	for _, want := range wantLines {
		if !strings.Contains(body, want) {
			t.Errorf("body missing line %q\nfull body:\n%s", want, body)
		}
	}
}

// TestExporter_HandleMetrics_OmitsMemRequestBytesWhenNoneDeclared proves the
// family is omitted, not zeroed, for a pod whose regular containers declare no
// memory request at all — even though CPU is declared.
func TestExporter_HandleMetrics_OmitsMemRequestBytesWhenNoneDeclared(t *testing.T) {
	cgroupRoot := t.TempDir()
	uid := types.UID("uid-3")
	dir := filepath.Join(cgroupRoot, "kubepods", "poduid-3")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-3",
			Namespace: "ops",
			UID:       uid,
			Labels:    map[string]string{"app.kubernetes.io/name": "api"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
					},
				},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, QOSClass: corev1.PodQOSGuaranteed},
	}

	clientset := fake.NewSimpleClientset(pod)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	e := &Exporter{
		opts:         Options{CgroupRoot: cgroupRoot},
		factory:      factory,
		lister:       podInformer.Lister(),
		log:          logr.Discard(),
		nodeCPUCores: 8,
		// nodeMemTotalOK left false — also proves the node-mem gauge itself
		// omits gracefully when the exporter never resolved a node total.
	}

	body := collectText(t, e)

	for _, absent := range []string{"use.mem.request.bytes{", "use.mem.node.total.bytes{"} {
		if strings.Contains(body, absent) {
			t.Errorf("body unexpectedly contains %q — must be omitted, not zeroed", absent)
		}
	}
	if !strings.Contains(body, `use.cpu.request.cores{namespace="ops",service="api",pod="api-3"} 0.1`) {
		t.Errorf("body missing CPU request line\nfull body:\n%s", body)
	}
	if !strings.Contains(body, `use.cpu.node.cores{namespace="ops",service="api",pod="api-3"} 8`) {
		t.Errorf("body missing CPU node-cores line (must always be present)\nfull body:\n%s", body)
	}
}

func TestServiceIdentity(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{
			name: "app.kubernetes.io/name wins",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app.kubernetes.io/name": "api", "app": "other"},
			}},
			want: "api",
		},
		{
			name: "falls back to app label",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"app": "legacy-api"},
			}},
			want: "legacy-api",
		},
		{
			name: "falls back to owner name",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{Name: "api-7f9d8"}},
			}},
			want: "api-7f9d8",
		},
		{
			name: "no labels, no owner -> empty (unattributed)",
			pod:  &corev1.Pod{},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := serviceIdentity(tc.pod); got != tc.want {
				t.Errorf("serviceIdentity() = %q, want %q", got, tc.want)
			}
		})
	}
}

// collectText renders Collect's output one point per line as
// name{namespace="…",service="…",pod="…"} value, for readable assertions.
func collectText(t *testing.T, e *Exporter) string {
	t.Helper()
	rms, err := e.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	set := metricset.FromOTLP(rms)
	var names []string
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		for _, p := range set[n].Points {
			fmt.Fprintf(&b, "%s{namespace=%q,service=%q,pod=%q} %g\n", n,
				p.Attr(usemetrics.LabelNamespace), p.Attr(usemetrics.LabelService), p.Attr(usemetrics.LabelPod), p.Value)
		}
	}
	return b.String()
}

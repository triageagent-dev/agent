package logexporter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/triageagent-dev/agent/metricset"
)

// newTestExporter builds an Exporter wired to a fake clientset, bypassing New's
// rest.Config requirement — same technique nodeexporter's own exporter_test.go
// uses (constructing the struct directly around a fake informer factory).
func newTestExporter(t *testing.T, logDir string, pods ...*corev1.Pod) *Exporter {
	t.Helper()
	objs := make([]runtime.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	clientset := fake.NewSimpleClientset(objs...)
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	factory.WaitForCacheSync(stop)

	opts := Options{
		NodeName:       "node-1",
		LogDir:         logDir,
		RescanInterval: time.Hour, // test drives rescan() manually
		PollInterval:   5 * time.Millisecond,
	}
	opts.setDefaults()
	cls, err := newClassifier(opts.ErrorRegex, opts.WarnRegex, opts.SampleMaxLen, opts.MaxSamplesPerService, opts.SampleJSONFields)
	if err != nil {
		t.Fatal(err)
	}
	return &Exporter{
		opts:    opts,
		factory: factory,
		lister:  podInformer.Lister(),
		nsAllow: namespaceSet(opts.IncludeNamespaces),
		cls:     cls,
		reg:     newSeriesRegistry(),
		self:    &selfMetrics{},
		log:     logr.Discard(),
		tracked: make(map[string]*trackedFile),
	}
}

// waitFor polls cond every 5ms up to 2s, failing the test on timeout — standard
// pattern for asserting on state mutated by another goroutine (here, a
// tailer's own poll loop) without a fixed sleep.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never became true within 2s")
}

func TestExporter_DiscoversTailsAndAttributesEndToEnd(t *testing.T) {
	dir := t.TempDir()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-1",
			Namespace: "ops",
			Labels:    map[string]string{"app.kubernetes.io/name": "api"},
		},
	}
	// Pre-existing content, written BEFORE this file is ever discovered — must
	// never be counted ("never re-read history").
	logPath := filepath.Join(dir, "api-1_ops_api-abc123.log")
	if err := os.WriteFile(logPath, []byte(criLine('F', "level=error pre-existing, must be skipped")), 0o644); err != nil {
		t.Fatal(err)
	}

	e := newTestExporter(t, dir, pod)
	e.rescan()

	e.mu.Lock()
	tf, ok := e.tracked[logPath]
	e.mu.Unlock()
	if !ok {
		t.Fatal("expected the log file to be tracked after rescan")
	}
	waitFor(t, func() bool { return tf.t.counters.Load() != nil })
	cs := tf.t.counters.Load()

	// Give the tailer's first tick a moment to open (from END) before we append.
	waitFor(t, func() bool { return tf.t.opened.Load() })
	if got := cs.total.Load(); got != 0 {
		t.Fatalf("total = %d, want 0 (pre-existing content must not be counted)", got)
	}

	appendToFile(t, logPath, criLine('F', "level=error fresh line"))
	waitFor(t, func() bool { return cs.totalErrors() == 1 })

	if got := e.self.filesTracked.Load(); got != 1 {
		t.Fatalf("filesTracked = %d, want 1", got)
	}

	// Removing the file from the directory must retire its tailer.
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	e.rescan()
	e.mu.Lock()
	_, stillTracked := e.tracked[logPath]
	e.mu.Unlock()
	if stillTracked {
		t.Fatal("expected the removed file's tailer to be retired")
	}
	waitFor(t, func() bool { return e.self.filesTracked.Load() == 0 })

	e.stopAll()
}

func TestExporter_NamespaceFilterSkipsFilesEntirely(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "w-1_billing_w-abc.log"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	e := newTestExporter(t, dir)
	e.nsAllow = namespaceSet([]string{"ops"})
	e.rescan()

	e.mu.Lock()
	n := len(e.tracked)
	e.mu.Unlock()
	if n != 0 {
		t.Fatalf("tracked = %d, want 0 (billing namespace must be filtered out)", n)
	}
	e.stopAll()
}

func TestExporter_UnattributablePodStaysUncounted(t *testing.T) {
	dir := t.TempDir()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "ops"}, // no labels at all
	}
	logPath := filepath.Join(dir, "api-1_ops_api-abc123.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	e := newTestExporter(t, dir, pod)
	e.rescan()

	e.mu.Lock()
	tf, ok := e.tracked[logPath]
	e.mu.Unlock()
	if !ok {
		t.Fatal("expected the file to still be tailed even though it is unattributable")
	}
	waitFor(t, func() bool { return tf.t.opened.Load() })

	appendToFile(t, logPath, criLine('F', "level=error nobody owns this"))
	waitFor(t, func() bool { return e.self.linesProcessed.Load() >= 1 })

	if tf.t.counters.Load() != nil {
		t.Fatal("expected counters to stay nil for an unattributable pod")
	}
	if got := e.reg.snapshot(); len(got) != 0 {
		t.Fatalf("expected no series in the registry, got %+v", got)
	}
	e.stopAll()
}

func TestExporter_CollectCarriesRegistryContent(t *testing.T) {
	e := newTestExporter(t, t.TempDir())
	cs := e.reg.getOrCreate("ops", "api")
	for i := 0; i < 7; i++ {
		cs.recordError("boom", []byte("boom"), DefaultSampleMaxLen, DefaultMaxSamplesPerService)
	}

	rms, err := e.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, p := range metricset.FromOTLP(rms).Get(MetricLogErrorsTotal).Points {
		if p.Attr(LabelSample) == "boom" {
			found = true
			if p.Value != 7 || p.Attr(LabelNamespace) != "ops" || p.Attr(LabelService) != "api" {
				t.Errorf("series = %v value %v", p.Attrs, p.Value)
			}
		}
	}
	if !found {
		t.Fatal("no boom series")
	}
}

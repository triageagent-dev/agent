// Package logexporter implements `triage-agent --mode=log-exporter`: a
// standalone per-node container-log counter (docs/adr/0014-log-exporter-in-
// agent-ds.md) that replaces Fluent Bit for the log_metrics signal family. It
// tails every container log file under /var/log/containers on its own node,
// classifies each line error/warn/neither with the SAME regexes Fluent Bit's
// log_to_metrics filter used, and reports cumulative OTLP counters (metrics.go)
// to the controller's logmetrics.Collector over the sensor stream.
//
// Deliberately NOT a controller-runtime manager.Runnable/reconciler: this mode
// has no Application reconciler, no leader election (every node runs its own
// copy), and no cache beyond the one node-scoped Pod informer it needs to map
// (namespace, pod) to a service label — same posture as nodeexporter.
package logexporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/triageagent-dev/agent/metricset"
	"k8s.io/client-go/rest"
)

// Options configures the Exporter.
type Options struct {
	// NodeName scopes the pod informer to this node only (fieldSelector
	// spec.nodeName=NodeName) — same idiom as nodeexporter.Options.NodeName.
	// Populated from the NODE_NAME env var (Kubernetes downward API) by the
	// caller — required.
	NodeName string
	// LogDir is the directory scanned for container log symlinks. Default
	// "/var/log/containers" — overridable for tests.
	LogDir string
	// IncludeNamespaces restricts tailing to these exact namespace names.
	// Empty = every namespace.
	IncludeNamespaces []string
	// NamespaceAllowed, when non-nil, OVERRIDES IncludeNamespaces with a live
	// predicate — the same hook logmetrics/usemetrics/redsource already expose,
	// so every subsystem answers "may I read this namespace" the same way.
	//
	// It matters more here than for those three. They emit metrics ABOUT a
	// namespace; this reads the application log CONTENT of every pod on the
	// node. A static list goes stale the moment a namespace is annotated or
	// un-annotated, and stale here means either silently missing a customer's
	// logs or silently reading someone else's.
	NamespaceAllowed func(string) bool
	// ErrorRegex/WarnRegex classify a line's content. Defaults are IDENTICAL to
	// the Fluent Bit pipeline's own defaults (DefaultErrorRegex/
	// DefaultWarnRegex, classify.go) — the classification contract this
	// exporter reproduces.
	ErrorRegex string
	WarnRegex  string
	// SampleMaxLen bounds two DIFFERENT strings to this many chars
	// (fingerprint.go's shared truncateSample): the masked+truncated
	// fingerprint that identifies an error series (unchanged since round 1),
	// and — since round 5 (docs/adr/0014-log-exporter-in-agent-ds.md's
	// identity/display split) — the RAW exemplar captured once at slot
	// creation that becomes the series' `sample` label value. Default 160 —
	// matches the sample length Fluent Bit's own pipeline used (ADR-0007). <= 0
	// disables truncation for both.
	SampleMaxLen int
	// MaxSamplesPerService caps DISTINCT error fingerprints tracked per
	// (namespace, service) — each is its own permanent, monotonic Prometheus
	// series (metrics.go's errorSlot, never evicted). Past the cap, further
	// distinct fingerprints fall into an unlabeled catch-all series. Default 8.
	MaxSamplesPerService int
	// SampleJSONFields is the priority-ordered list of top-level JSON keys
	// tried when an error line's content is a JSON object (jsonextract.go) —
	// the first present, non-empty string value wins (or, for an object
	// value, that object's own "message" sub-key). Default
	// DefaultSampleJSONFields ("message,msg,error,err").
	SampleJSONFields []string
	// RescanInterval is how often the log directory is re-listed for new/
	// removed files. Default 10s.
	RescanInterval time.Duration
	// PollInterval is how often each already-tracked file's tailer goroutine
	// checks for new data / rotation. Default 250ms — independent of
	// RescanInterval: a file already being tailed must not wait for the next
	// directory rescan to pick up new lines.
	PollInterval time.Duration
}

func (o *Options) setDefaults() {
	if o.LogDir == "" {
		o.LogDir = "/var/log/containers"
	}
	if o.ErrorRegex == "" {
		o.ErrorRegex = DefaultErrorRegex
	}
	if o.WarnRegex == "" {
		o.WarnRegex = DefaultWarnRegex
	}
	if o.SampleMaxLen <= 0 {
		o.SampleMaxLen = DefaultSampleMaxLen
	}
	if o.MaxSamplesPerService <= 0 {
		o.MaxSamplesPerService = DefaultMaxSamplesPerService
	}
	if len(o.SampleJSONFields) == 0 {
		o.SampleJSONFields = DefaultSampleJSONFields
	}
	if o.RescanInterval <= 0 {
		o.RescanInterval = 10 * time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 250 * time.Millisecond
	}
}

// trackedFile pairs a running tailer with the context that stops its goroutine.
type trackedFile struct {
	t      *tailer
	cancel context.CancelFunc
}

// Exporter is the per-node log counter.
type Exporter struct {
	opts      Options
	factory   informers.SharedInformerFactory
	lister    corelisters.PodLister
	nsAllow   map[string]bool
	nsAllowFn func(string) bool
	cls       *classifier
	reg       *seriesRegistry
	self      *selfMetrics
	log       logr.Logger

	mu      sync.Mutex
	tracked map[string]*trackedFile // log file path -> its tailer
}

// New builds an Exporter from a Kubernetes rest.Config (the same in-cluster/
// kubeconfig resolution every other mode uses, via ctrl.GetConfigOrDie — see
// main.go). It does not start the informer, the discovery loop, or the HTTP
// server; call Run for that.
func New(opts Options, cfg *rest.Config, log logr.Logger) (*Exporter, error) {
	opts.setDefaults()
	if opts.NodeName == "" {
		return nil, errors.New("NODE_NAME is required in --mode=log-exporter (set via the downward API, spec.nodeName)")
	}
	cls, err := newClassifier(opts.ErrorRegex, opts.WarnRegex, opts.SampleMaxLen, opts.MaxSamplesPerService, opts.SampleJSONFields)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	// Node-scoped at the API-server level, same reasoning as nodeexporter.New:
	// keeps the informer's watch — and its in-memory cache — down to "pods on
	// this node" on a DaemonSet where every node runs a copy.
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 10*time.Minute,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", opts.NodeName).String()
		}))
	podInformer := factory.Core().V1().Pods()
	_ = podInformer.Informer()

	return &Exporter{
		opts:      opts,
		factory:   factory,
		lister:    podInformer.Lister(),
		nsAllow:   namespaceSet(opts.IncludeNamespaces),
		nsAllowFn: opts.NamespaceAllowed,
		cls:       cls,
		reg:       newSeriesRegistry(),
		self:      &selfMetrics{},
		log:       log.WithName("logexporter"),
		tracked:   make(map[string]*trackedFile),
	}, nil
}

// Run starts the pod informer, waits for its initial sync, does the first
// directory scan (spawning a tailer per currently-present file), then
// periodically rescans the log directory until ctx is cancelled. It blocks
// until then. Collect reads the counters; nothing listens.
func (e *Exporter) Run(ctx context.Context) error {
	e.factory.Start(ctx.Done())
	for typ, ok := range e.factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cache sync failed for %v", typ)
		}
	}
	e.log.Info("pod informer synced", "node", e.opts.NodeName)

	e.rescan()

	ticker := time.NewTicker(e.opts.RescanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.stopAll()
			return nil
		case <-ticker.C:
			e.rescan()
		}
	}
}

// rescan lists the log directory, spawns a tailer for every newly-present
// file (starting it tailing from the file's current end — "never re-read
// history"), retries service resolution for any tracked file that isn't
// attributed yet, and stops+removes tailers for files that disappeared.
func (e *Exporter) rescan() {
	found := scanLogDir(e.opts.LogDir, e.nsAllow, e.nsAllowFn)
	present := make(map[string]bool, len(found))

	e.mu.Lock()
	for _, df := range found {
		present[df.path] = true
		if tf, ok := e.tracked[df.path]; ok {
			if tf.t.counters.Load() == nil {
				e.resolveInto(tf.t, df.namespace, df.pod)
			}
			continue
		}
		t := newTailer(df.path, df.namespace, df.pod, df.container, e.log)
		e.resolveInto(t, df.namespace, df.pod)
		tctx, cancel := context.WithCancel(context.Background())
		e.tracked[df.path] = &trackedFile{t: t, cancel: cancel}
		e.self.filesTracked.Add(1)
		go e.runTailer(tctx, t)
	}
	var removed []*trackedFile
	for path, tf := range e.tracked {
		if !present[path] {
			removed = append(removed, tf)
			delete(e.tracked, path)
		}
	}
	e.mu.Unlock()

	for _, tf := range removed {
		tf.cancel()
		e.self.filesTracked.Add(-1)
	}
}

// resolveInto looks up (namespace, pod)'s service identity and, if resolvable
// and non-empty, attaches the (namespace, service) counterSet to t. Leaves
// t.counters nil (unresolved or unattributable) otherwise — rescan retries
// unresolved files on every subsequent tick, cheaply (one lister.Get, not on
// the per-line hot path).
func (e *Exporter) resolveInto(t *tailer, namespace, pod string) {
	svc, ok := resolveService(e.lister, namespace, pod)
	if !ok || svc == "" {
		return
	}
	t.counters.Store(e.reg.getOrCreate(namespace, svc))
}

// runTailer is one file's dedicated goroutine: poll on PollInterval until ctx
// is cancelled (the file was removed from the directory listing). Isolation
// boundary — a tick that takes a long time (a hot file, a slow disk) only
// delays THIS goroutine's next tick, never another file's.
func (e *Exporter) runTailer(ctx context.Context, t *tailer) {
	defer t.closeFile()
	ticker := time.NewTicker(e.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.tick(e.cls, e.self)
		}
	}
}

// stopAll cancels every tracked tailer — called on Run's shutdown path.
func (e *Exporter) stopAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for path, tf := range e.tracked {
		tf.cancel()
		delete(e.tracked, path)
	}
	e.self.filesTracked.Store(0)
}

// Collect returns the counters as cumulative OTLP.
func (e *Exporter) Collect(_ context.Context) ([]*metricspb.ResourceMetrics, error) {
	b := metricset.NewBuilder("k8s.node.name", e.opts.NodeName)
	buildMetrics(b, e.reg, e.self)
	return b.ResourceMetrics(), nil
}

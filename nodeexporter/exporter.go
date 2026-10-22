// Package nodeexporter implements `triage-agent --mode=node-exporter`: a
// standalone per-node USE-metrics sensor (docs/adr/0010-use-metrics-
// node-exporter.md, docs/adr/0015-sensor-stream-grpc.md). It runs a node-scoped
// Pod informer (fieldSelector spec.nodeName=$NODE_NAME, populated via the
// downward API) and, on each collect, reads every locally-running pod's cgroup
// v2 files (PSI, memory, cpu.stat) plus its pod spec's own resource requests,
// to build per-(namespace,service,pod) gauges/counters that usemetrics.Collector
// reads back over the sensor stream and aggregates. It also
// resolves the NODE's own capacity (total memory from /proc/meminfo, online CPU
// core count from /sys/devices/system/cpu/online) once at startup and repeats it
// on every pod's sample set — the portable denominator that exists even for a
// pod with no resource limit at all, unlike the limit-relative figures above.
//
// Deliberately NOT a controller-runtime manager.Runnable/reconciler: this mode has
// no Application reconciler, no leader election (every node runs its own copy, so
// there is nothing to elect), and no cache beyond the one pod informer it needs.
// Everything it reads is read-only.
package nodeexporter

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/usemetrics"
)

// Options configures the Exporter.
type Options struct {
	// NodeName scopes the pod informer to this node only (fieldSelector
	// spec.nodeName=NodeName). Populated from the NODE_NAME env var (Kubernetes
	// downward API, spec.nodeName) by the caller — required.
	NodeName string
	// CgroupRoot is the root of the host cgroup v2 filesystem, mounted read-only
	// into this container. Default "/sys/fs/cgroup".
	CgroupRoot string
	// ProcRoot is the root of the host /proc, mounted read-only into this
	// container — used to read node-wide MemTotal (resolveNodeMemTotalBytes) and
	// as a fallback source for the node's CPU core count
	// (resolveNodeCPUCores). Default "/proc". Injectable so tests can fixture a
	// synthetic /proc tree instead of reading the real one.
	ProcRoot string
	// SysRoot is the root of the host /sys, mounted read-only into this
	// container — used to read the node's online CPU set
	// (/sys/devices/system/cpu/online, see resolveNodeCPUCores). Default "/sys".
	// Injectable for the same testing reason as ProcRoot.
	SysRoot string
}

func (o *Options) setDefaults() {
	if o.CgroupRoot == "" {
		o.CgroupRoot = "/sys/fs/cgroup"
	}
	if o.ProcRoot == "" {
		o.ProcRoot = "/proc"
	}
	if o.SysRoot == "" {
		o.SysRoot = "/sys"
	}
}

// Exporter is the per-node USE-metrics Prometheus exporter.
type Exporter struct {
	opts    Options
	factory informers.SharedInformerFactory
	lister  corelisters.PodLister
	log     logr.Logger

	// nodeMemTotalBytes/nodeMemTotalOK and nodeCPUCores are the node's capacity,
	// resolved ONCE in New (see resolveNodeMemTotalBytes/resolveNodeCPUCores) and
	// cached for the process lifetime — a node's memory and CPU core count do
	// not change, so re-reading /proc and /sys on every collect would be pure
	// waste. nodeCPUCores has no accompanying "ok": its own resolution chain
	// ends in runtime.NumCPU(), which never fails.
	nodeMemTotalBytes float64
	nodeMemTotalOK    bool
	nodeCPUCores      float64

	// kubepodsSlice locates the systemd kubepods slice hierarchy under
	// opts.CgroupRoot, resolved ONCE in New (see discoverKubepodsSlice) and
	// cached for the process lifetime — the kubelet's --cgroup-root doesn't
	// change while it's running, so re-probing it on every collect would be
	// pure waste, same reasoning as nodeMemTotalBytes/nodeCPUCores above.
	kubepodsSlice kubepodsSlice

	// oomStatus accumulates the per-pod OOMKilled termination count from pod
	// status — the second, cgroup-independent OOM source (see
	// oomStatusTracker). Its zero value is ready to use.
	oomStatus oomStatusTracker
}

// New builds an Exporter from a Kubernetes rest.Config (the same in-cluster/
// kubeconfig resolution the controller mode uses, via ctrl.GetConfigOrDie — see
// main.go). It does not start the informer or the HTTP server; call Run for that.
func New(opts Options, cfg *rest.Config, log logr.Logger) (*Exporter, error) {
	opts.setDefaults()
	if opts.NodeName == "" {
		return nil, errors.New("NODE_NAME is required in --mode=node-exporter (set via the downward API, spec.nodeName)")
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	// Node-scoped at the API-server level (field selector), not just filtered
	// client-side: this is what keeps the informer's watch — and its in-memory
	// cache — down to "pods on this node" rather than "every pod in the cluster",
	// which matters a lot on a DaemonSet where every node runs a copy.
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 10*time.Minute,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", opts.NodeName).String()
		}))
	podInformer := factory.Core().V1().Pods()
	// Referencing the Informer (not just the Lister) here is what registers it with
	// the factory so factory.Start actually starts it below.
	_ = podInformer.Informer()

	l := log.WithName("nodeexporter")

	// Node capacity is read from /proc and /sys, NOT the k8s Node object — the
	// entire portability point of this pipeline (see resolveNodeMemTotalBytes'
	// doc comment). Resolved once here rather than per-collect, and logged so a
	// wrong reading (e.g. a restrictive /sys forcing the runtime.NumCPU()
	// fallback) is diagnosable from the startup log alone, without needing to
	// correlate it against a surprising node-percentage reading later.
	memTotalBytes, memTotalOK := resolveNodeMemTotalBytes(opts.ProcRoot)
	cpuCores, cpuCoresSource := resolveNodeCPUCores(opts.SysRoot, opts.ProcRoot)
	l.Info("resolved node capacity",
		"memTotalBytes", memTotalBytes, "memTotalAvailable", memTotalOK,
		"cpuCores", cpuCores, "cpuCoresSource", cpuCoresSource)

	kpSlice := discoverKubepodsSlice(opts.CgroupRoot)
	l.Info("resolved kubepods slice layout", "dir", kpSlice.dir, "prefix", kpSlice.prefix)

	return &Exporter{
		opts:              opts,
		factory:           factory,
		lister:            podInformer.Lister(),
		log:               l,
		nodeMemTotalBytes: float64(memTotalBytes),
		nodeMemTotalOK:    memTotalOK,
		nodeCPUCores:      float64(cpuCores),
		kubepodsSlice:     kpSlice,
	}, nil
}

// Run starts the pod informer and waits for its initial sync, then keeps it
// running until ctx is cancelled. Collect reads from it; nothing listens.
func (e *Exporter) Run(ctx context.Context) error {
	e.factory.Start(ctx.Done())
	for typ, ok := range e.factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cache sync failed for %v", typ)
		}
	}
	e.log.Info("pod informer synced", "node", e.opts.NodeName)
	<-ctx.Done()
	return nil
}

// Collect lists this node's currently-known Running pods from the informer's
// local cache (already node-scoped server-side) and, for each, reads its cgroup
// files fresh — cheap given the bounded per-node pod count, and it avoids a second,
// separately-stale cache of computed values sitting behind the informer's own.
func (e *Exporter) Collect(_ context.Context) ([]*metricspb.ResourceMetrics, error) {
	pods, err := e.lister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	// Deterministic output order: not a correctness requirement, but it keeps
	// successive snapshots diffable and matches this repo's determinism discipline
	// (redsource's sample tie-break, usemetrics' order slice).
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})

	// Fold the WHOLE listing (every phase) into the status-derived OOM tracker
	// before the per-pod loop skips anything: a pod that is briefly not Running,
	// or whose cgroup dir is briefly not found below, must keep its baseline so a
	// kill during that gap is still counted when it reappears.
	e.oomStatus.observe(pods)

	b := metricset.NewBuilder("k8s.node.name", e.opts.NodeName)

	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		dir := podCgroupDir(e.opts.CgroupRoot, e.kubepodsSlice, pod.UID, pod.Status.QOSClass)
		if dir == "" {
			// Pod cgroup not found under either known layout — just terminated, or
			// an unrecognized layout. Skip gracefully rather than erroring the
			// whole collect over one pod.
			continue
		}
		lbl := metricset.KV(usemetrics.LabelNamespace, pod.Namespace, usemetrics.LabelService, serviceIdentity(pod), usemetrics.LabelPod, pod.Name)

		// PSI: absent files (CONFIG_PSI off) simply produce no line for that
		// gauge — graceful degrade, not a fabricated 0.
		if v, ok := readPSIAvg60(filepath.Join(dir, "cpu.pressure")); ok {
			b.Gauge(usemetrics.MetricCPUPsiAvg60, lbl, v)
		}
		if v, ok := readPSIAvg60(filepath.Join(dir, "memory.pressure")); ok {
			b.Gauge(usemetrics.MetricMemPsiAvg60, lbl, v)
		}
		if v, ok := readPSIAvg60(filepath.Join(dir, "io.pressure")); ok {
			b.Gauge(usemetrics.MetricIOPsiAvg60, lbl, v)
		}
		// Container limits come from the pod SPEC, not the cgroup tree: a pod
		// carrying an unlimited sidecar or init container has no pod-level
		// memory.max at all, and the spec is the only place the limit its real
		// workload does declare can still be read. See memLimitBytes.
		limits := regularContainerMemLimits(pod)
		if v, ok := readMemPct(dir, limits); ok {
			b.Gauge(usemetrics.MetricMemPct, lbl, v)
		}
		if v, ok := readMemWorkingSetPct(dir, limits); ok {
			b.Gauge(usemetrics.MetricMemWorkingSetPct, lbl, v)
		}
		if v, ok := readMemWorkingSetBytes(dir); ok {
			b.Gauge(usemetrics.MetricMemBytes, lbl, v)
		}
		if v, ok := memLimitBytes(dir, limits); ok {
			b.Gauge(usemetrics.MetricMemLimitBytes, lbl, float64(v))
		}
		if v, ok := readOOMKill(dir); ok {
			b.Counter(usemetrics.MetricOOMKillTotal, lbl, v)
		}
		// Second source: OOMKilled terminations the kubelet recorded in pod
		// status. Emitted for every pod that gets this far (0 until a kill is
		// seen) so the collector always holds a baseline to diff against.
		b.Counter(usemetrics.MetricOOMKillStatusTotal, lbl, e.oomStatus.kills(pod.UID))
		if stat, ok := readCPUStat(dir); ok {
			b.Counter(usemetrics.MetricCPUUsageUsecTotal, lbl, stat.usageUsec)
			b.Counter(usemetrics.MetricCPUNrPeriodsTotal, lbl, stat.nrPeriods)
			b.Counter(usemetrics.MetricCPUNrThrottledTotal, lbl, stat.nrThrottled)
		}
		if v, ok := readCPULimitCores(dir, regularContainerCPULimitsMilli(pod)); ok {
			b.Gauge(usemetrics.MetricCPULimitCores, lbl, v)
		}
		// Node capacity: portable denominators, resolved once at startup (see
		// New) and repeated on every pod's sample set rather than published as
		// a separate label-less series — this exporter's whole shape is
		// per-(namespace,service,pod) samples, and the collector joins on that
		// same key, so the node totals ride along on it too. CPU cores has no
		// "omitted" case (its resolution chain ends in runtime.NumCPU(), which
		// never fails); memory total is omitted on the rare host where
		// /proc/meminfo couldn't be read at all, matching the graceful-degrade
		// convention every other family here follows.
		if e.nodeMemTotalOK {
			b.Gauge(usemetrics.MetricMemNodeTotalBytes, lbl, e.nodeMemTotalBytes)
		}
		b.Gauge(usemetrics.MetricCPUNodeCores, lbl, e.nodeCPUCores)
		// Requests: the Kubernetes right-sizing denominator, read straight from
		// the pod spec — there is no cgroup file for a request, only for a
		// limit, so unlike memLimitBytes/readCPULimitCores there is nothing to
		// prefer over the spec value. Omitted entirely when no regular
		// container declares one (sumRegularRequests' ok=false), never
		// reported as a meaningless 0.
		if v, ok := sumRegularRequests(regularContainerMemRequests(pod)); ok {
			b.Gauge(usemetrics.MetricMemRequestBytes, lbl, float64(v))
		}
		if v, ok := sumRegularRequests(regularContainerCPURequestsMilli(pod)); ok {
			b.Gauge(usemetrics.MetricCPURequestCores, lbl, float64(v)/1000.0)
		}
	}

	return b.ResourceMetrics(), nil
}

// regularContainerMemLimits returns each regular container's memory limit in
// bytes, in spec order, using 0 for a container that declares none — the shape
// memLimitBytes needs to decide whether the pod is bounded at all.
//
// Init containers are excluded: a Running pod's init containers have terminated
// and hold no memory, so including them would inflate the denominator for every
// pod that has one. That they are nevertheless what SUPPRESSES the pod-level
// memory.max (kubelet requires a limit on every container, init included,
// before it sets one) is exactly why this fallback exists.
func regularContainerMemLimits(pod *corev1.Pod) []int64 {
	out := make([]int64, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		if q := pod.Spec.Containers[i].Resources.Limits.Memory(); q != nil {
			out = append(out, q.Value())
			continue
		}
		out = append(out, 0)
	}
	return out
}

// regularContainerCPULimitsMilli is the CPU counterpart of
// regularContainerMemLimits: each regular container's CPU limit in millicores,
// 0 where none is declared. Same init-container exclusion, same reason.
func regularContainerCPULimitsMilli(pod *corev1.Pod) []int64 {
	out := make([]int64, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		if q := pod.Spec.Containers[i].Resources.Limits.Cpu(); q != nil {
			out = append(out, q.MilliValue())
			continue
		}
		out = append(out, 0)
	}
	return out
}

// regularContainerMemRequests is the memory-REQUEST counterpart of
// regularContainerMemLimits: each regular container's memory request in bytes,
// in spec order, 0 for a container that declares none. Same init-container
// exclusion, same reason (a Running pod's init containers have terminated and
// hold no memory).
//
// Unlike memLimitBytes' fallback, there is no cgroup file to prefer over this:
// kubelet's cgroup tree reflects LIMITS (memory.max), never requests, so the
// pod spec is the only place a request can be read from at all — this is not a
// fallback, it is the sole source.
func regularContainerMemRequests(pod *corev1.Pod) []int64 {
	out := make([]int64, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		if q := pod.Spec.Containers[i].Resources.Requests.Memory(); q != nil {
			out = append(out, q.Value())
			continue
		}
		out = append(out, 0)
	}
	return out
}

// regularContainerCPURequestsMilli is the CPU counterpart of
// regularContainerMemRequests: each regular container's CPU request in
// millicores, 0 where none is declared. Same init-container exclusion, same
// reason.
func regularContainerCPURequestsMilli(pod *corev1.Pod) []int64 {
	out := make([]int64, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		if q := pod.Spec.Containers[i].Resources.Requests.Cpu(); q != nil {
			out = append(out, q.MilliValue())
			continue
		}
		out = append(out, 0)
	}
	return out
}

// sumRegularRequests sums per-container regular request values (bytes or
// millicores — the caller decides the unit) and reports whether the sum is
// usable at all: at least one container declared a nonzero request.
//
// This is deliberately NOT the all-or-nothing rule memLimitBytes/
// readCPULimitCores apply to their pod-spec FALLBACK (there, one unlimited
// container makes the pod's effective limit undefined, so the whole sum is
// discarded). A request has no such ambiguity: Kubernetes itself computes a
// pod's total request as the sum of whatever its containers declare, treating
// an absent request as 0 — so this simply omits the family when NOTHING was
// declared by any regular container, rather than reporting a meaningless 0
// that the collector's request-percentage derivation would otherwise divide
// by (see usemetrics/deltas.go's memReqPct/cpuReqPct).
func sumRegularRequests(vals []int64) (sum int64, ok bool) {
	for _, v := range vals {
		sum += v
	}
	return sum, sum > 0
}

// serviceIdentity resolves a pod's service identity: app.kubernetes.io/name label,
// falling back to the plain "app" label, falling back to the pod's owner's name —
// the same label convention logexporter.serviceIdentity resolves (discovery.go),
// extended one step further since this exporter has direct pod-object access (an
// informer, not a log line) where the log exporter does not. Returns "" (unattributed — see
// usemetrics.Collector's unattributed handling) when none of the three resolve.
func serviceIdentity(pod *corev1.Pod) string {
	if v := pod.Labels["app.kubernetes.io/name"]; v != "" {
		return v
	}
	if v := pod.Labels["app"]; v != "" {
		return v
	}
	if len(pod.OwnerReferences) > 0 {
		return pod.OwnerReferences[0].Name
	}
	return ""
}

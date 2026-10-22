// Triage exclusion: annotationkeys.Triage "false" on a workload's own
// metadata, on its pod template, or on its Namespace takes that workload (or
// every workload in the namespace) out of triage entirely. The agent then
// sends the core nothing about it: no virtual app (virtualapps.go folds the
// annotation into Excluded), no Kubernetes Warning events for its pods or
// owners (Pusher.onEvent), and no log/RED/USE/topology entries
// (Pusher.dropExcluded, applied to every collector frame on its way into
// enqueue). The eBPF role additionally never instruments a pod whose
// template carries it (the chart's and obiEssentialsYAML's
// exclude_instrument), and never instruments, or tails the logs of, a
// namespace that carries it (cmd/triage-agent's namespaceTenantWatch).
//
// Precedence follows annotationkeys.VirtualApp's existing rule
// (namespace_defaults.go): a Namespace "false" is ORed into every workload
// in it, so a workload-level "true" does not override it. Any value other
// than exactly "false", or absence, keeps the workload in.
//
// Every lookup is answered from state the agent already keeps live: the
// Namespace informer (nsDefaults), the virtual-app workload informers
// (virtualAppsState's workload index), and the manager's cached client for
// the pod → ReplicaSet/Job → Deployment/CronJob owner walk the USE collector
// already does (usemetrics/identity.go). An annotation added later takes
// effect from the next informer update on; owner reads that went to the
// API server are reused for at most ownerLinkTTL.
package push

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// TriageExcluded reports whether annotations carry annotationkeys.Triage set
// to exactly "false".
func TriageExcluded(annotations map[string]string) bool {
	return annotations[annotationkeys.Triage] == "false"
}

// podTemplateAnnotationPaths are where a workload kind nests the pod
// template's annotations: spec.template for Deployment/StatefulSet/
// DaemonSet/ReplicaSet/Job/Rollout, spec.jobTemplate.spec.template for a
// CronJob.
var podTemplateAnnotationPaths = [][]string{
	{"spec", "template", "metadata", "annotations"},
	{"spec", "jobTemplate", "spec", "template", "metadata", "annotations"},
}

// objectTriageExcluded reports whether a workload object opts out of triage
// on its own metadata or on its pod template.
func objectTriageExcluded(obj *unstructured.Unstructured) bool {
	if TriageExcluded(obj.GetAnnotations()) {
		return true
	}
	for _, path := range podTemplateAnnotationPaths {
		a, found, err := unstructured.NestedStringMap(obj.Object, path...)
		if err == nil && found && TriageExcluded(a) {
			return true
		}
	}
	return false
}

// workloadServiceNames are the names a workload's signals can arrive under
// in a collector frame's (namespace, service) or topology node: its own
// name (eBPF owner name, the default identity), its virtual-app identity
// (RED/USE/topology after identity resolution), and its pod template's
// app.kubernetes.io/name and app labels (the log exporter's attribution,
// logexporter.serviceIdentity).
func workloadServiceNames(w workloadInfo) []string {
	names := []string{w.Name, groupNameFor(w)}
	for _, key := range []string{labelName, "app"} {
		if v := strings.TrimSpace(w.PodLabels[key]); v != "" {
			names = append(names, v)
		}
	}
	return names
}

// triageExcludedNames returns, per namespace, every service name that
// belongs only to annotationkeys.Triage-excluded workloads. A name that a
// non-excluded workload in the same namespace also answers to is left out,
// so excluding one workload never drops a sibling's signal that happens to
// share its identity or app label.
func triageExcludedNames(workloads []workloadInfo) map[string]map[string]bool {
	excluded := map[string]map[string]bool{}
	kept := map[string]map[string]bool{}
	for _, w := range workloads {
		target := kept
		if w.TriageExcluded {
			target = excluded
		}
		if target[w.Namespace] == nil {
			target[w.Namespace] = map[string]bool{}
		}
		for _, n := range workloadServiceNames(w) {
			target[w.Namespace][n] = true
		}
	}
	for ns, names := range excluded {
		for n := range names {
			if kept[ns][n] {
				delete(names, n)
			}
		}
		if len(names) == 0 {
			delete(excluded, ns)
		}
	}
	return excluded
}

// serviceExcluded reports whether name, in namespace, belongs only to
// annotationkeys.Triage-excluded workloads (triageExcludedNames).
func (v *virtualAppsState) serviceExcluded(namespace, name string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.excludedNames[namespace][name]
}

// workloadTriageExcluded looks up a tracked workload by kind and name.
// known is false when the virtual-app index does not track it (another
// kind, out of scope, or not recomputed yet).
func (v *virtualAppsState) workloadTriageExcluded(namespace, kind, name string) (excluded, known bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, w := range v.workloadsByNamespace[namespace] {
		if string(w.Kind) == kind && w.Name == name {
			return w.TriageExcluded, true
		}
	}
	return false, false
}

// generatedSuffix matches what a controller appends to its workload's name
// for the objects it creates: a ReplicaSet's pod-template hash, a pod's
// random suffix, a ReplicaSet pod's hash plus suffix, a StatefulSet
// ordinal, or a CronJob's scheduled-time Job suffix.
var generatedSuffix = regexp.MustCompile(`^[a-z0-9]{1,10}(-[a-z0-9]{5})?$`)

// generatedNameExcluded is the fallback for an object that can no longer be
// read (a pod deleted before its Warning event arrived): it picks the
// longest tracked workload name in namespace that objName is generated from
// and returns that workload's verdict. matched is false when none fits.
func (v *virtualAppsState) generatedNameExcluded(namespace, objName string) (excluded, matched bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	best := ""
	for _, w := range v.workloadsByNamespace[namespace] {
		if len(w.Name) <= len(best) || !strings.HasPrefix(objName, w.Name+"-") {
			continue
		}
		if !generatedSuffix.MatchString(objName[len(w.Name)+1:]) {
			continue
		}
		best, excluded, matched = w.Name, w.TriageExcluded, true
	}
	return excluded, matched
}

// NamespaceExcluded reports whether namespace carries annotationkeys.Triage
// "false", from the Namespace informer. Nil-safe.
func (p *Pusher) NamespaceExcluded(namespace string) bool {
	if p == nil || p.nsDefaults == nil || namespace == "" {
		return false
	}
	d, _ := p.nsDefaults.get(namespace)
	return d.TriageExcluded
}

// ServiceExcluded reports whether a collector frame entry for (namespace,
// service) must be dropped: the namespace is excluded, or service names a
// workload that is (triageExcludedNames). Nil-safe.
func (p *Pusher) ServiceExcluded(namespace, service string) bool {
	if p == nil {
		return false
	}
	if p.NamespaceExcluded(namespace) {
		return true
	}
	return p.vapps != nil && service != "" && p.vapps.serviceExcluded(namespace, service)
}

// PodExcluded reports whether a pod is out of triage: its namespace, its own
// annotations (its pod template's, copied onto it), or any controller owner
// up the chain (ReplicaSet → Deployment/Rollout, Job → CronJob, or a
// StatefulSet/DaemonSet/Job directly) carries annotationkeys.Triage
// "false". Exported for the USE collector, which reads per pod. Nil-safe.
func (p *Pusher) PodExcluded(ctx context.Context, namespace, pod string) bool {
	if p == nil || p.opts.NoKubernetes {
		// No Kubernetes inputs: the pod and its owners are never read.
		return false
	}
	return p.objectExcluded(ctx, namespace, "v1", "Pod", pod)
}

// maxOwnerDepth bounds the owner walk; Pod → ReplicaSet → Deployment and
// Pod → Job → CronJob are three levels.
const maxOwnerDepth = 4

// ownerLinkTTL bounds how long one object's read (its own verdict plus its
// controller owner) is reused before being read again, so a Warning event
// burst for one pod costs one read rather than one per event, while an
// annotation edit on a kind the virtual-app index does not track (Job,
// CronJob, ReplicaSet) still takes effect within this window.
const ownerLinkTTL = 30 * time.Second

// ownerLink is one object's cached read.
type ownerLink struct {
	excluded bool
	owner    *metav1.OwnerReference
	expires  time.Time
}

// ownerLinkCache caches ownerLinks by namespace/apiVersion/kind/name. Safe
// for concurrent use: Warning events arrive on several informer goroutines.
type ownerLinkCache struct {
	mu      sync.Mutex
	entries map[string]ownerLink
}

func (c *ownerLinkCache) get(key string, now time.Time) (ownerLink, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.entries[key]
	if !ok || now.After(l.expires) {
		return ownerLink{}, false
	}
	return l, true
}

func (c *ownerLinkCache) set(key string, l ownerLink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]ownerLink)
	}
	// Bounded by the number of distinct objects with Warning events; drop
	// expired entries whenever it grows, rather than on a separate ticker.
	if len(c.entries) >= 4096 {
		for k, e := range c.entries {
			if l.expires.After(e.expires.Add(ownerLinkTTL)) {
				delete(c.entries, k)
			}
		}
	}
	c.entries[key] = l
}

// objectExcluded walks one object up its controller owner chain. At each
// level it answers from the virtual-app workload index when that tracks the
// object (live from its informer), else reads the object through the
// manager's client, and if the object is gone falls back to matching its
// generated name against tracked workloads.
func (p *Pusher) objectExcluded(ctx context.Context, namespace, apiVersion, kind, name string) bool {
	if namespace == "" || name == "" || kind == "" {
		return false
	}
	if p.NamespaceExcluded(namespace) {
		return true
	}
	for depth := 0; depth < maxOwnerDepth; depth++ {
		if p.vapps != nil {
			if excluded, known := p.vapps.workloadTriageExcluded(namespace, kind, name); known {
				return excluded
			}
			// A ReplicaSet is always named <owner>-<pod-template-hash> and
			// carries its owner's pod template, so a tracked Deployment or
			// Rollout answers for it without an API read.
			if kind == "ReplicaSet" {
				if excluded, matched := p.vapps.generatedNameExcluded(namespace, name); matched {
					return excluded
				}
			}
		}
		link, err := p.readOwnerLink(ctx, namespace, apiVersion, kind, name)
		if err != nil {
			if p.vapps != nil {
				if excluded, matched := p.vapps.generatedNameExcluded(namespace, name); matched {
					return excluded
				}
			}
			return false
		}
		if link.excluded {
			return true
		}
		if link.owner == nil {
			return false
		}
		apiVersion, kind, name = link.owner.APIVersion, link.owner.Kind, link.owner.Name
	}
	return false
}

// readOwnerLink reads one object through the manager's client: a Pod typed,
// which shares the Pod informer the USE collector's identity resolution
// already reads, and every other kind as unstructured, addressed by the
// apiVersion/kind the event or owner reference names — no allowlist of
// kinds, the same generic walk usemetrics/identity.go does.
func (p *Pusher) readOwnerLink(ctx context.Context, namespace, apiVersion, kind, name string) (ownerLink, error) {
	key := namespace + "/" + apiVersion + "/" + kind + "/" + name
	now := p.now()
	if l, ok := p.ownerLinks.get(key, now); ok {
		return l, nil
	}
	if p.client == nil {
		return ownerLink{}, errNoReader
	}
	var l ownerLink
	nn := types.NamespacedName{Namespace: namespace, Name: name}
	if kind == "Pod" && (apiVersion == "v1" || apiVersion == "") {
		var pod corev1.Pod
		if err := p.client.Get(ctx, nn, &pod); err != nil {
			return ownerLink{}, err
		}
		l.excluded = TriageExcluded(pod.Annotations)
		l.owner = metav1.GetControllerOfNoCopy(&pod)
	} else {
		gv, err := schema.ParseGroupVersion(apiVersion)
		if err != nil {
			return ownerLink{}, err
		}
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gv.WithKind(kind))
		if err := p.client.Get(ctx, nn, obj); err != nil {
			return ownerLink{}, err
		}
		l.excluded = objectTriageExcluded(obj)
		l.owner = metav1.GetControllerOfNoCopy(obj)
	}
	l.expires = now.Add(ownerLinkTTL)
	p.ownerLinks.set(key, l)
	return l, nil
}

// errNoReader is readOwnerLink's error on a Pusher built without a client
// (tests that construct *Pusher directly).
var errNoReader = errors.New("push: no client to read owner objects")

// now is opts.now with a time.Now fallback for a bare *Pusher literal.
func (p *Pusher) now() time.Time {
	if p.opts.now != nil {
		return p.opts.now()
	}
	return time.Now()
}

// eventLookupTimeout bounds the owner walk for one Warning event, so a read
// that cannot complete (an informer that never syncs) never stalls the
// event informer's handler goroutine.
const eventLookupTimeout = 5 * time.Second

// eventExcluded reports whether a Warning event is about an excluded
// workload: its involved object (a Pod, ReplicaSet, Deployment, Job, ...)
// resolved up the owner chain by objectExcluded.
func (p *Pusher) eventExcluded(e *corev1.Event) bool {
	ns := e.InvolvedObject.Namespace
	if ns == "" {
		ns = e.Namespace
	}
	if p.NamespaceExcluded(ns) {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventLookupTimeout)
	defer cancel()
	return p.objectExcluded(ctx, ns, e.InvolvedObject.APIVersion, e.InvolvedObject.Kind, e.InvolvedObject.Name)
}

// dropExcluded removes every entry about an excluded workload from a frame
// on its way into enqueue — the one choke point every collector frame
// (log/RED/USE metrics and summaries, topology) passes through, so no
// collector needs its own copy of the rule. A metrics frame left with no
// entries is dropped (nil). A topology snapshot is level state and is kept
// even when emptied, so the core learns the excluded nodes are gone. A
// k8s_event whose namespace is excluded is dropped here too, behind
// onEvent's own check. app_delta frames pass untouched: virtual-app
// synthesis already excludes these workloads, and its deletes are what
// remove an app that became excluded.
func (p *Pusher) dropExcluded(f *Frame) *Frame {
	if f == nil {
		return nil
	}
	switch f.Kind {
	case KindK8sEvent:
		if f.K8sEvent != nil && f.K8sEvent.Event != nil && p.NamespaceExcluded(f.K8sEvent.Event.Namespace) {
			return nil
		}
	case KindLogMetrics:
		if f.LogMetrics != nil {
			kept, changed := keepNotExcluded(p, f.LogMetrics.Services, func(s ServiceLogMetrics) (string, string) { return s.Namespace, s.Service })
			if changed {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.LogMetrics
				pl.Services, nf.LogMetrics = kept, &pl
				return &nf
			}
		}
	case KindRedMetrics:
		if f.RedMetrics != nil {
			kept, changed := keepNotExcluded(p, f.RedMetrics.Services, redKey)
			if changed {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.RedMetrics
				pl.Services, nf.RedMetrics = kept, &pl
				return &nf
			}
		}
	case KindRedSummary:
		if f.RedSummary != nil {
			kept, changed := keepNotExcluded(p, f.RedSummary.Services, redKey)
			if changed {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.RedSummary
				pl.Services, nf.RedSummary = kept, &pl
				return &nf
			}
		}
	case KindUseMetrics:
		if f.UseMetrics != nil {
			kept, changed := keepNotExcluded(p, f.UseMetrics.Services, func(s ServiceUseMetrics) (string, string) { return s.Namespace, s.Service })
			if changed {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.UseMetrics
				pl.Services, nf.UseMetrics = kept, &pl
				return &nf
			}
		}
	case KindUseSummary:
		if f.UseSummary != nil {
			kept, changed := keepNotExcluded(p, f.UseSummary.Services, func(s ServiceUseSummary) (string, string) { return s.Namespace, s.Service })
			if changed {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.UseSummary
				pl.Services, nf.UseSummary = kept, &pl
				return &nf
			}
		}
	case KindTopologySnapshot:
		if f.TopologySnapshot != nil {
			nodes, nodesChanged := keepNotExcluded(p, f.TopologySnapshot.Nodes, func(n TopologyNode) (string, string) { return n.Namespace, n.Workload })
			edges, edgesChanged := keepNotExcluded(p, f.TopologySnapshot.Edges, func(e TopologyEdge) (string, string) { return e.SrcNamespace, e.SrcWorkload })
			edges, dstChanged := keepNotExcluded(p, edges, func(e TopologyEdge) (string, string) { return e.DstNamespace, e.DstWorkload })
			if nodesChanged || edgesChanged || dstChanged {
				nf, pl := *f, *f.TopologySnapshot
				pl.Nodes, pl.Edges, nf.TopologySnapshot = nodes, edges, &pl
				return &nf
			}
		}
	}
	return f
}

func redKey(s ServiceRedMetrics) (string, string) { return s.Namespace, s.Service }

// keepNotExcluded returns items without the ones ServiceExcluded matches,
// and whether any were removed. The input slice is never modified: a
// collector may still hold it.
func keepNotExcluded[T any](p *Pusher, items []T, key func(T) (namespace, service string)) ([]T, bool) {
	drop := -1
	for i, it := range items {
		ns, svc := key(it)
		if ns != "" && p.ServiceExcluded(ns, svc) {
			drop = i
			break
		}
	}
	if drop < 0 {
		return items, false
	}
	out := make([]T, 0, len(items)-1)
	out = append(out, items[:drop]...)
	for _, it := range items[drop+1:] {
		ns, svc := key(it)
		if ns != "" && p.ServiceExcluded(ns, svc) {
			continue
		}
		out = append(out, it)
	}
	return out, true
}

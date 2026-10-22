package usemetrics

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/push"
)

// IdentityResolver resolves a pod to the service identity its samples are
// aggregated under. ok is false when it cannot, and the collector then keeps
// the exporter's own reported service label. The cluster resolver walks owner
// references (OwnerIdentityResolver); a Linux host names every pod by itself
// (StaticIdentityResolver).
type IdentityResolver interface {
	ResolveIdentity(ctx context.Context, namespace, pod string) (identity string, ok bool)
}

// StaticIdentityResolver is the host resolver: one unit is one pod is one
// service, so a pod in Namespace is its own identity. A pod outside Namespace
// is not resolved.
type StaticIdentityResolver struct{ Namespace string }

// ResolveIdentity implements IdentityResolver.
func (s StaticIdentityResolver) ResolveIdentity(_ context.Context, namespace, pod string) (string, bool) {
	if pod == "" || namespace != s.Namespace {
		return "", false
	}
	return pod, true
}

// NoIdentityResolver resolves nothing, so the collector keeps the exporter's
// own reported service label. It is the resolver of an agent that watches no
// Kubernetes objects (--kubernetes=false) and so cannot walk owner references.
type NoIdentityResolver struct{}

// ResolveIdentity implements IdentityResolver.
func (NoIdentityResolver) ResolveIdentity(context.Context, string, string) (string, bool) {
	return "", false
}

// OwnerIdentityResolver is the cluster resolver. Client reads the pod and its
// owners (the manager's cached client in a cluster); ModeFor returns a
// namespace's virtual-app identity mode (push.Pusher.IdentityModeFor), nil
// meaning the default; Now is injectable for tests. Not safe for concurrent
// use: the collector calls it from one goroutine.
type OwnerIdentityResolver struct {
	Client  client.Reader
	ModeFor func(namespace string) string
	Now     func() time.Time

	cache ownerIdentityCache
}

func (r *OwnerIdentityResolver) clock() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// GC reclaims cache entries past their TTL; the collector calls it once per tick.
func (r *OwnerIdentityResolver) GC(now time.Time) { r.cache.gc(now) }

// ResolveIdentity resolves a pod's service identity through
// the SAME chain virtual-app synthesis uses to name every Application
// (push.GroupNameFor): walk the pod's owner references to the workload that
// actually owns it, then resolve that workload's own labels, its
// annotationkeys.VirtualAppGroup annotation, its own name, and its
// namespace's annotationkeys.VirtualAppIdentity mode through
// push.GroupNameFor. This is what
// keeps a USE-metrics series bound to the exact graph node virtual-app
// synthesis (and therefore the core's Graph.resolveRedInstanceLocked) names
// for the same workload, instead of an independently-derived guess.
//
// ok is false only when the pod itself can't be read or carries no
// controller owner reference at all — resolveOwnerIdentity below always
// produces SOME identity once an owner reference exists, degrading to a
// weaker one rather than failing outright. Resolution is a best-effort
// enrichment either way, and the caller always has the exporter's own
// reported service= label to fall back to. A resolution failure must never
// drop a sample.
func (r *OwnerIdentityResolver) ResolveIdentity(ctx context.Context, namespace, podName string) (string, bool) {
	if r == nil || r.Client == nil {
		return "", false
	}
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: namespace, Name: podName}
	if err := r.Client.Get(ctx, key, &pod); err != nil {
		return "", false
	}
	owner := metav1.GetControllerOfNoCopy(&pod)
	if owner == nil {
		return "", false
	}
	return r.resolveOwnerIdentity(ctx, namespace, owner, pod.Labels), true
}

// resolveOwnerIdentity resolves one pod's controller owner reference to the
// TOP-level workload identity: a ReplicaSet or Job is walked one further hop
// to ITS OWN controller owner when it has one (a Deployment, a Rollout, a
// CronJob, or any other apiVersion/kind — never an allowlist of recognized
// controllers), and that further owner is the top owner; a ReplicaSet or Job
// with no controller of its own, and every other owner kind (StatefulSet,
// DaemonSet, or any operator/CRD kind owning the pod directly), IS the top
// owner already. The top owner is fetched generically — through
// r.Client using an unstructured.Unstructured addressed by the
// ownerReference's own apiVersion/kind, never a typed Get restricted to a
// fixed set of kinds — and its labels, annotations and name feed
// workloadGroupName the same way a Deployment's always have.
//
// A Get failure anywhere in the walk (RBAC, deleted, an apiVersion the
// cluster no longer serves) does not fail the resolution: it falls back to
// workloadGroupName built from the POD's own labels and the last owner name
// known for certain at that point in the walk — the true top owner's name
// when the walk got that far (e.g. the Rollout's name, once its owning
// ReplicaSet was read successfully), never a name one hop short of it. This
// is what keeps a Rollout (or any other unfetchable controller) from
// resolving as its ReplicaSet's own pod-template-hash name.
//
// Every result is cached per (namespace, owner UID) — see ownerIdentityCache
// — so the many pods one ReplicaSet/StatefulSet/DaemonSet/Job owns share a
// single walk instead of one per pod per window.
func (r *OwnerIdentityResolver) resolveOwnerIdentity(ctx context.Context, namespace string, owner *metav1.OwnerReference, podLabels map[string]string) string {
	mode := r.identityModeFor(namespace)
	now := r.clock()
	if identity, ok := r.cache.get(namespace, owner.UID, now); ok {
		return identity
	}

	top := owner
	if owner.Kind == "ReplicaSet" || owner.Kind == "Job" {
		obj, err := r.getOwnerObject(ctx, namespace, owner)
		if err != nil {
			identity := workloadGroupName(podLabels, nil, owner.Name, mode)
			r.cache.set(namespace, owner.UID, identity, now)
			return identity
		}
		if inner := metav1.GetControllerOfNoCopy(obj); inner != nil {
			top = inner
		} else {
			identity := workloadGroupName(obj.GetLabels(), obj.GetAnnotations(), obj.GetName(), mode)
			r.cache.set(namespace, owner.UID, identity, now)
			return identity
		}
	}

	var identity string
	if obj, err := r.getOwnerObject(ctx, namespace, top); err != nil {
		identity = workloadGroupName(podLabels, nil, top.Name, mode)
	} else {
		identity = workloadGroupName(obj.GetLabels(), obj.GetAnnotations(), obj.GetName(), mode)
	}
	r.cache.set(namespace, owner.UID, identity, now)
	return identity
}

// getOwnerObject fetches one owner reference's object through
// r.Client as an unstructured.Unstructured, addressed by the GVK the
// ownerReference itself names (apiVersion/kind) — no allowlist of
// recognized kinds: a Deployment, a Rollout, a CronJob, or any operator/CRD
// kind is fetched identically. Returns an error for a malformed apiVersion,
// an RBAC denial, or a deleted object; the caller treats every one of those
// the same way (see resolveOwnerIdentity's fallback).
func (r *OwnerIdentityResolver) getOwnerObject(ctx context.Context, namespace string, ref *metav1.OwnerReference) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gv.WithKind(ref.Kind))
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// identityModeFor resolves namespace's virtual-app identity mode via
// OwnerIdentityResolver.ModeFor (push.Pusher.IdentityModeFor). nil (no Sender
// wired, or a bare Collector literal predating this feature) returns "",
// which push.GroupNameFor resolves identically to push.IdentityModeWorkload
// — the default.
func (r *OwnerIdentityResolver) identityModeFor(namespace string) string {
	if r.ModeFor == nil {
		return ""
	}
	return r.ModeFor(namespace)
}

// workloadGroupName builds push.GroupNameFor's input from one owner's own
// labels, group-override annotation, and name, plus the namespace's own
// identity mode (mode) — the exact inputs virtual-app synthesis resolves
// identity from (push/virtualapps.go) — so an identity resolved here always
// agrees with the Application node virtual-app synthesis names for the same
// workload. A degraded call site passes the POD's labels and a nil
// annotations map instead of a fetched owner's own — see
// resolveOwnerIdentity's fallback path.
func workloadGroupName(labels, annotations map[string]string, name, mode string) string {
	return push.GroupNameFor(push.WorkloadIdentity{
		Labels:        labels,
		GroupOverride: annotations[annotationkeys.VirtualAppGroup],
		Name:          name,
		Mode:          mode,
	})
}

// ownerIdentityTTL bounds how long resolveOwnerIdentity trusts a cached
// result before re-walking and re-fetching it: long enough that one
// ReplicaSet/StatefulSet/DaemonSet/Job's many pods, read every
// Options.Interval, share a single walk instead of one per pod per window;
// short enough that a Rollout revision bump or an edited
// annotationkeys.VirtualAppGroup annotation is picked up within a few
// windows rather than for the process's whole lifetime.
const ownerIdentityTTL = 10 * time.Minute

// ownerIdentityCacheKey identifies one cached resolution by the pod's
// IMMEDIATE controller owner — not the top owner a ReplicaSet/Job walk may
// resolve to — since that immediate owner's UID is what every one of its
// pods carries without needing any Get to learn it.
type ownerIdentityCacheKey struct {
	namespace string
	uid       types.UID
}

type ownerIdentityCacheEntry struct {
	identity string
	expires  time.Time
}

// ownerIdentityCache caches resolveOwnerIdentity's result keyed by
// (namespace, immediate owner UID), so the many pods one ReplicaSet, Job,
// StatefulSet or DaemonSet owns resolve via a single walk per TTL window
// rather than one walk — and one or two API Gets — per pod per window.
//
// Not safe for concurrent use — same posture as Collector.prev and
// Collector.warnedUntrustedAddrs: touched only from the collector's own
// ticker-driven goroutine (Start -> runOnce).
type ownerIdentityCache struct {
	entries map[ownerIdentityCacheKey]ownerIdentityCacheEntry
}

// get returns the cached identity for (namespace, uid), false if absent or
// past its TTL — an expired entry is left in place for gc to reclaim rather
// than deleted here, since a miss is immediately followed by a set that
// overwrites it anyway.
func (c *ownerIdentityCache) get(namespace string, uid types.UID, now time.Time) (string, bool) {
	if c.entries == nil {
		return "", false
	}
	e, ok := c.entries[ownerIdentityCacheKey{namespace: namespace, uid: uid}]
	if !ok || now.After(e.expires) {
		return "", false
	}
	return e.identity, true
}

func (c *ownerIdentityCache) set(namespace string, uid types.UID, identity string, now time.Time) {
	if c.entries == nil {
		c.entries = make(map[ownerIdentityCacheKey]ownerIdentityCacheEntry)
	}
	c.entries[ownerIdentityCacheKey{namespace: namespace, uid: uid}] = ownerIdentityCacheEntry{
		identity: identity,
		expires:  now.Add(ownerIdentityTTL),
	}
}

// gc drops every entry past its TTL — called once per read tick
// (Collector.runOnce), mirroring the Collector.prev baseline GC, so a
// workload that stops being read (deleted, or rolled onto a new
// ReplicaSet) doesn't pin a stale entry for the life of the process.
func (c *ownerIdentityCache) gc(now time.Time) {
	for k, e := range c.entries {
		if now.After(e.expires) {
			delete(c.entries, k)
		}
	}
}

// resolveServiceIdentity resolves a pod through the collector's resolver;
// ok is false when none is wired.
func (c *Collector) resolveServiceIdentity(ctx context.Context, namespace, podName string) (string, bool) {
	if c.identity == nil {
		return "", false
	}
	return c.identity.ResolveIdentity(ctx, namespace, podName)
}

// Runtime per-namespace informers for the scoped-manager-cache gap
// (docs/todo/virtual-apps.md namespace-scope-by-annotation addendum,
// "scoped-cache limitation", 2026-08-25).
//
// namespace_scope.go lets a triageagent.dev/tenant Namespace annotation pull a
// namespace into DYNAMIC scope live, with no operator edit to the static
// --target-namespaces list and no restart. That works cleanly when the manager
// cache is cluster-wide (main.go's --namespace="" path — cacheOpts has no
// DefaultNamespaces at all, so every namespace is already resident). It does
// NOT work when --namespace scopes the cache to a fixed namespace set
// (main.go:~386-410): controller-runtime cannot add a namespace to an
// already namespace-scoped cache at runtime, so a namespace outside that set
// both (a) fails virtualAppsState.backfillNamespace's manager-cache list with
// "unknown namespace for the cache", and (b) never receives any configured
// workload resource's informer event at all — not "delayed until the next
// resync", NEVER, because the manager cache's own informers were never told
// to watch it.
//
// This file is the fallback for exactly that case: a small, independent
// dynamicinformer.DynamicSharedInformerFactory per (out-of-cache-scope
// namespace, configured workload resource), wired to the SAME onWorkload
// callback the manager-cache-backed informers use (virtualapps.go
// registerInformers), so a workload discovered this way feeds virtual-app
// synthesis identically to one discovered through the manager cache. It is
// deliberately NOT a second, general-purpose cache — just enough to keep
// push mode's own view of workloads live for namespaces the manager cache
// cannot be told about.
package push

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	toolscache "k8s.io/client-go/tools/cache"
)

// defaultDynamicInformerResync is the relist period for a per-namespace
// runtime informer factory when the caller does not specify one (Pusher.New
// passes 0). Independent of the manager cache's own --sync-period — this
// factory answers to nothing else in the process.
const defaultDynamicInformerResync = 10 * time.Minute

// errNoClientset is wrapped into the error dynamicInformerManager.ensure
// returns when no kubernetes.Interface was configured (Options.Clientset
// nil, or a bare struct literal in a test). Callers log it and move on —
// never a crash, and never a panic on a nil interface method call.
var errNoClientset = errors.New("no clientset configured for dynamic (out-of-cache) namespace informers")

// dynamicWorkloadHandlers bundles the exact callbacks
// virtualAppsState.registerInformers wires onto the manager-cache-backed
// workload informers, so a workload discovered through a runtime
// per-namespace factory is fed into applyWorkload through the identical
// inScope-gated path (onWorkload in virtualapps.go) — never a separate code
// path that could drift. Any nil handler simply skips that resource type's
// informer — pre-existing vapps-only tests wire only onWorkload.
type dynamicWorkloadHandlers struct {
	// onWorkload delivers an add/update/delete for ANY configured,
	// discovery-confirmed workload resource (Deployment, StatefulSet,
	// DaemonSet, Rollout, or any other namespaced kind named by
	// --workload-resources) — ONE path for every kind. kind is the
	// resource's discovered Kind (resolveWorkloadResources), passed by the
	// caller rather than read off obj — a List/informer delivery is not
	// guaranteed to carry TypeMeta.
	onWorkload func(obj any, kind string, deleted bool)
	// onEvent is the same idea for Kubernetes Warning events. It matters more
	// than it looks: unlike workloads, events have no other per-namespace
	// path — Pusher.registerHandlers takes them from the MANAGER cache
	// (GetInformer(&corev1.Event{})), which is cluster-wide. Scoping that
	// cache without this handler silently stops event push for every
	// dynamically-scoped namespace, and the only symptom is signals that
	// never arrive.
	onEvent func(obj any)
}

// dynamicNamespaceFactory is one namespace's runtime informer set. wlFactory
// (unstructured, workloads) may be nil when no dynamic client was
// configured; it shares the one stopCh with evtFactory.
type dynamicNamespaceFactory struct {
	wlFactory dynamicinformer.DynamicSharedInformerFactory
	// evtFactory is a typed factory, separate from wlFactory because the
	// Warning filter is a factory-wide ListOptions tweak: sharing one factory
	// would apply "type=Warning" to the workload watches too, which matches
	// nothing. Mirrors the field selector main.go puts on the manager cache's
	// own Event informer.
	evtFactory informers.SharedInformerFactory
	stopCh     chan struct{}
}

// dynamicInformerManager maintains at most one dynamicNamespaceFactory per
// dynamically-scoped namespace that the manager cache does not cover
// (virtualAppsState.cacheCovers decides which). Safe for concurrent use —
// ensure/stop/running/list* are all called from informer-handler goroutines.
//
// A dynamicInformerManager with a nil clientset is a deliberate, safe
// degrade: every method that would need to talk to the API server returns
// errNoClientset (wrapped) instead of panicking on a nil kubernetes.Interface
// — the same "may be nil in tests" posture Options.Clientset documents.
type dynamicInformerManager struct {
	clientset kubernetes.Interface
	dynClient dynamic.Interface
	resync    time.Duration
	log       logr.Logger
	handlers  dynamicWorkloadHandlers

	// workloadResources is the configured, discovery-confirmed list of
	// namespaced GVRs this manager's per-namespace workload factories watch
	// (docs/todo/virtual-apps.md "Workload resources") — set once by
	// Pusher.New (the same resolved list shared onto virtualAppsState), read
	// by ensure/listWorkloads. Empty means no workload informers are started
	// even when handlers.onWorkload is set — the pre-vapps-existing "nothing
	// configured yet" shape, and also what a bare struct literal in an older
	// test gets.
	workloadResources []workloadResource

	mu         sync.Mutex
	namespaces map[string]*dynamicNamespaceFactory
}

// newDynamicInformerManager always returns a non-nil manager, even with a
// nil clientset/dynClient — every call site can be a plain method call with
// no extra nil check, matching nsDefaults/dynamicScope's own "always
// constructed" convention on Pusher. dynClient backs the per-namespace
// workload informers (every configured resource is watched generically, as
// unstructured, never through the typed clientset); nil is the same
// documented degrade as a nil clientset.
func newDynamicInformerManager(clientset kubernetes.Interface, dynClient dynamic.Interface, resync time.Duration, log logr.Logger, handlers dynamicWorkloadHandlers) *dynamicInformerManager {
	if resync <= 0 {
		resync = defaultDynamicInformerResync
	}
	return &dynamicInformerManager{
		clientset:  clientset,
		dynClient:  dynClient,
		resync:     resync,
		log:        log,
		handlers:   handlers,
		namespaces: make(map[string]*dynamicNamespaceFactory),
	}
}

// ensure idempotently starts a per-namespace informer factory for namespace
// and waits for its initial sync. A namespace that already has a running
// factory is a no-op — this is what makes annotation flapping (add/remove/
// add) safe: it can never leak a goroutine or double-register a factory.
//
// Returns a wrapped errNoClientset when no clientset is configured; the
// caller (virtualAppsState.backfillNamespace) logs it and the namespace's
// workloads are simply never observed via this path until the process is
// reconfigured with a working clientset — degrade, not crash.
func (m *dynamicInformerManager) ensure(ctx context.Context, namespace string) error {
	if m.running(namespace) {
		return nil
	}
	wantsWorkloads := m.handlers.onWorkload != nil
	wantsEvents := m.handlers.onEvent != nil
	runWorkloads := wantsWorkloads && m.dynClient != nil
	// Events share the workload gate (wantsWorkloads) rather than a
	// standalone check: onEvent is wired unconditionally whenever a
	// clientset exists (Pusher.New), and wantsWorkloads is true exactly when
	// virtual-app synthesis is on — so Warning events for an out-of-cache
	// namespace stay tied to the same on/off switch as workload discovery.
	runEvents := wantsWorkloads && wantsEvents && m.clientset != nil
	if !runWorkloads && !runEvents {
		return fmt.Errorf("namespace %q entered dynamic scope outside the manager cache's namespace set: %w", namespace, errNoClientset)
	}
	// Partial degrade, not all-or-nothing: whichever half has its client run;
	// the missing half is logged once here and simply never observed for this
	// namespace — same "degrade, never crash" posture as the full-miss error.
	if wantsWorkloads && !runWorkloads {
		m.log.Info("no dynamic client configured; workloads in out-of-cache namespace will not be observed", "namespace", namespace)
	}
	if wantsWorkloads && wantsEvents && m.clientset == nil {
		m.log.Info("no clientset configured; events in out-of-cache namespace will not be observed", "namespace", namespace)
	}

	stopCh := make(chan struct{})

	var wlFactory dynamicinformer.DynamicSharedInformerFactory
	if runWorkloads {
		wlFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(m.dynClient, m.resync, namespace, nil)
		for _, res := range m.workloadResources {
			kind := res.Kind
			wlInf := wlFactory.ForResource(res.GVR).Informer()
			if _, err := wlInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
				AddFunc:    func(obj any) { m.handlers.onWorkload(obj, kind, false) },
				UpdateFunc: func(_, obj any) { m.handlers.onWorkload(obj, kind, false) },
				DeleteFunc: func(obj any) { m.handlers.onWorkload(obj, kind, true) },
			}); err != nil {
				close(stopCh)
				return fmt.Errorf("namespace %q: watch %s: %w", namespace, kind, err)
			}
		}
	}

	var evtFactory informers.SharedInformerFactory
	if runEvents {
		evtFactory = informers.NewSharedInformerFactoryWithOptions(m.clientset, m.resync,
			informers.WithNamespace(namespace),
			informers.WithTweakListOptions(func(o *metav1.ListOptions) {
				o.FieldSelector = fields.OneTermEqualSelector("type", corev1.EventTypeWarning).String()
			}),
		)
		evtInf := evtFactory.Core().V1().Events().Informer()
		// Add + Update only, matching registerHandlers: a deleted Event is an
		// expired record, not a new signal, and a resync (old == new) is not
		// an update.
		if _, err := evtInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) { m.handlers.onEvent(obj) },
			UpdateFunc: func(old, obj any) {
				if !eventUnchanged(old, obj) {
					m.handlers.onEvent(obj)
				}
			},
		}); err != nil {
			close(stopCh)
			return fmt.Errorf("namespace %q: watch Event: %w", namespace, err)
		}
	}

	if wlFactory != nil {
		wlFactory.Start(stopCh)
	}
	if evtFactory != nil {
		evtFactory.Start(stopCh)
	}
	if evtFactory != nil {
		for gvr, ok := range evtFactory.WaitForCacheSync(stopCh) {
			if !ok {
				close(stopCh)
				return fmt.Errorf("namespace %q: Event informer cache sync failed for %v", namespace, gvr)
			}
		}
	}
	if wlFactory != nil {
		for gvr, ok := range wlFactory.WaitForCacheSync(stopCh) {
			if !ok {
				close(stopCh)
				return fmt.Errorf("namespace %q: workload informer cache sync failed for %v", namespace, gvr)
			}
		}
	}

	m.mu.Lock()
	if _, ok := m.namespaces[namespace]; ok {
		// Lost a race against a concurrent ensure() for the same namespace
		// (shouldn't happen in practice — onNamespace/addDynamicScope run
		// off one informer's single-threaded event handler — but this keeps
		// "at most one factory per namespace" true regardless). Keep the
		// winner already recorded; stop the one just built here.
		m.mu.Unlock()
		close(stopCh)
		return nil
	}
	m.namespaces[namespace] = &dynamicNamespaceFactory{wlFactory: wlFactory, evtFactory: evtFactory, stopCh: stopCh}
	m.mu.Unlock()

	m.log.Info("started runtime informer factory for out-of-cache namespace", "namespace", namespace)
	return nil
}

// stop closes namespace's stop channel (terminating its informers) and
// forgets the factory. Returns whether a factory was actually running —
// stopping a namespace that never had one (always cache-covered, or never
// dynamically scoped) is a no-op, same idiom as dynamicScopeSet.remove.
func (m *dynamicInformerManager) stop(namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.namespaces[namespace]
	if !ok {
		return false
	}
	close(f.stopCh)
	delete(m.namespaces, namespace)
	return true
}

// running reports whether namespace currently has a runtime factory.
func (m *dynamicInformerManager) running(namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.namespaces[namespace]
	return ok
}

func (m *dynamicInformerManager) wlFactoryFor(namespace string) (dynamicinformer.DynamicSharedInformerFactory, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.namespaces[namespace]
	if !ok || f.wlFactory == nil {
		return nil, false
	}
	return f.wlFactory, true
}

// listWorkloads reads namespace's runtime workload factory listers, across
// every configured resource — the "backfill FROM THIS FACTORY'S LISTERS"
// step (virtualapps.go backfillNamespace), used in place of the manager-
// cache list that fails once a namespace has entered dynamic scope outside
// the cache's namespace set. A namespace with no running factory yet (ensure
// was never called, or failed) returns (nil, nil) — the same "nothing known
// yet" shape listCachedWorkloads returns for an unset cache, so
// backfillNamespace's existing error handling is unchanged.
func (m *dynamicInformerManager) listWorkloads(namespace string) ([]rawWorkload, error) {
	f, ok := m.wlFactoryFor(namespace)
	if !ok {
		return nil, nil
	}
	var out []rawWorkload
	for _, res := range m.workloadResources {
		objs, err := f.ForResource(res.GVR).Lister().ByNamespace(namespace).List(labels.Everything())
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", res.Kind, err)
		}
		for _, obj := range objs {
			if u, ok := obj.(*unstructured.Unstructured); ok {
				out = append(out, rawWorkload{Obj: u, Kind: res.Kind})
			}
		}
	}
	return out, nil
}

// unstructuredFromAny converts an object delivered by an unstructured
// informer (event handler or lister) to *unstructured.Unstructured,
// unwrapping a DeletedFinalStateUnknown tombstone first — a relist-missed
// delete can arrive wrapped this way on either the manager-cache-backed or
// the runtime per-namespace informer path. Anything else unexpected returns
// nil.
func unstructuredFromAny(obj any) *unstructured.Unstructured {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u
	}
	tomb, isTomb := obj.(toolscache.DeletedFinalStateUnknown)
	if !isTomb {
		return nil
	}
	u, _ := tomb.Obj.(*unstructured.Unstructured)
	return u
}

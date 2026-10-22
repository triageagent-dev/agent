// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/push"
	ctrl "sigs.k8s.io/controller-runtime"
)

// namespaceTenantWatch tracks which namespaces carry a non-empty
// annotationkeys.Tenant value, and not annotationkeys.Triage "false", live. It is the log-exporter mode's
// equivalent of the Pusher's dynamic scope set: that mode runs standalone in
// the DaemonSet with no Pusher to ask, and it reads application log CONTENT
// off the node, so "may I read this namespace" has to be answered from the
// same annotation everything else keys on rather than from a static list
// that goes stale.
type namespaceTenantWatch struct {
	mu   sync.RWMutex
	set  map[string]bool
	seen bool // the informer's initial list has been delivered to the handlers, not just fetched into its store

	// changed carries one edge per membership change, coalesced. Buffered-1
	// with a non-blocking send so no informer callback blocks.
	changed chan struct{}
}

// Changed fires when the annotated set changes, not on every namespace
// update.
func (w *namespaceTenantWatch) Changed() <-chan struct{} { return w.changed }

// names returns the annotated namespaces, sorted. ok is false only before the
// first list completes; an empty set afterwards means none are annotated,
// which is an answer, not an absence of one.
func (w *namespaceTenantWatch) names() (list []string, ok bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.seen {
		return nil, false
	}
	list = make([]string, 0, len(w.set))
	for ns := range w.set {
		list = append(list, ns)
	}
	sort.Strings(list)
	return list, true
}

func (w *namespaceTenantWatch) notifyChanged() {
	select {
	case w.changed <- struct{}{}:
	default:
	}
}

// allowed reports whether ns may be read. The annotation is authoritative:
// an unannotated namespace is never read. Everything is allowed only before
// the first list completes, when there is no answer yet.
func (w *namespaceTenantWatch) allowed(ns string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.seen {
		return true
	}
	return w.set[ns]
}

func (w *namespaceTenantWatch) apply(obj any, deleted bool) {
	n, ok := obj.(*corev1.Namespace)
	if !ok {
		return
	}
	_, hasTenant := push.NamespaceTenant(n)
	// annotationkeys.Triage "false" takes the whole namespace out of triage:
	// nothing in it is instrumented or has its logs read, even when it is
	// tenant-annotated.
	excluded := push.TriageExcluded(n.Annotations)
	w.mu.Lock()
	if w.set == nil {
		w.set = map[string]bool{}
	}
	before := w.set[n.Name]
	after := !deleted && hasTenant && !excluded
	if after {
		w.set[n.Name] = true
	} else {
		delete(w.set, n.Name)
	}
	w.mu.Unlock()
	// Only on a real membership edge: namespace objects churn for unrelated
	// reasons, and each false wake re-attaches every uprobe on the node.
	if before != after {
		w.notifyChanged()
	}
}

// startNamespaceTenantWatch starts the Namespace informer backing the watch.
// Namespaces are cluster-scoped, so this is one cheap watch on one low-churn
// resource — not a per-namespace cost.
func startNamespaceTenantWatch(ctx context.Context, cfg *rest.Config) (*namespaceTenantWatch, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newNamespaceTenantWatch(ctx, cs)
}

// newNamespaceTenantWatch builds the watch from an already-constructed
// clientset, so a fake one can stand in for tests. Kept separate from
// startNamespaceTenantWatch, which only adds the *rest.Config -> clientset
// step.
func newNamespaceTenantWatch(ctx context.Context, cs kubernetes.Interface) (*namespaceTenantWatch, error) {
	w := &namespaceTenantWatch{set: map[string]bool{}, changed: make(chan struct{}, 1)}
	factory := informers.NewSharedInformerFactory(cs, 10*time.Minute)
	inf := factory.Core().V1().Namespaces().Informer()
	reg, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.apply(obj, false) },
		UpdateFunc: func(_, obj any) { w.apply(obj, false) },
		DeleteFunc: func(obj any) { w.apply(obj, true) },
	})
	if err != nil {
		return nil, err
	}
	// BOUNDED. Without a deadline this blocks forever when the ServiceAccount
	// cannot list Namespaces — which is exactly what happened on the eBPF
	// DaemonSet, whose ClusterRole covers pods/services/nodes and workloads but
	// not namespaces: the log-exporter role never started at all, and the only
	// symptom was a pod that logged "starting" and then nothing.
	syncCtx, cancelSync := context.WithTimeout(ctx, 20*time.Second)
	defer cancelSync()
	factory.Start(ctx.Done())
	// factory.WaitForCacheSync only reports the informer STORE is filled, not
	// that the Add handlers above have been delivered for it. Without also
	// waiting on the registration's own HasSynced, seen can flip true while
	// w.set is still empty or partial, and names() would hand back an empty
	// set as if that were the answer.
	for _, ok := range factory.WaitForCacheSync(syncCtx.Done()) {
		if !ok {
			return nil, errors.New("timed out syncing the Namespace informer (does this ServiceAccount have get/list/watch on namespaces?)")
		}
	}
	if !toolscache.WaitForCacheSync(syncCtx.Done(), reg.HasSynced) {
		return nil, errors.New("timed out delivering the initial Namespace list to the event handler (does this ServiceAccount have get/list/watch on namespaces?)")
	}
	w.mu.Lock()
	w.seen = true
	w.mu.Unlock()
	// The initial list is itself an edge for anyone waiting on Changed().
	w.notifyChanged()
	return w, nil
}

// sharedNamespaceTenantWatch memoizes one watch per process. The eBPF and
// log-exporter roles run in the same binary and must not disagree about which
// namespaces are onboarded.
var (
	sharedNSWatchOnce sync.Once
	sharedNSWatch     *namespaceTenantWatch
	sharedNSWatchErr  error
)

func sharedNamespaceTenantWatch(ctx context.Context) (*namespaceTenantWatch, error) {
	sharedNSWatchOnce.Do(func() {
		sharedNSWatch, sharedNSWatchErr = startNamespaceTenantWatch(ctx, ctrl.GetConfigOrDie())
	})
	return sharedNSWatch, sharedNSWatchErr
}

// logExporterNamespaceGate resolves the log-exporter's "may I read this
// namespace" predicate. Shared by the standalone `--mode=log-exporter` branch
// and the composed-role branch (`--mode=ebpf,node-exporter,log-exporter`, what
// the DaemonSet actually runs) so the two cannot diverge — the composed path is
// the one that ships, and wiring only the standalone one leaves the shipped
// configuration reading every namespace.
//
// An explicit --log-exporter-include-namespaces still wins; nil means the
// static set decides.
func logExporterNamespaceGate(includeNamespaces []string) func(string) bool {
	if len(includeNamespaces) > 0 {
		return nil
	}
	w, err := sharedNamespaceTenantWatch(context.Background())
	if err != nil {
		// FAIL CLOSED. The gate exists so a namespace nobody onboarded is never
		// read; if we cannot tell which those are, reading everything is the one
		// outcome the gate was added to prevent. Loud, because the fix is an RBAC
		// grant and the alternative failure (silently exporting every namespace's
		// log content) is invisible.
		setupLog.Error(err, "log-exporter: cannot determine annotated namespaces — reading NONE until this is fixed; grant get/list/watch on namespaces", "key", annotationkeys.Tenant)
		return func(string) bool { return false }
	}
	setupLog.Info("log-exporter: reading only annotated namespaces", "key", annotationkeys.Tenant)
	return w.allowed
}

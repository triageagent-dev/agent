// Namespace-scope-by-annotation, "automatic enablement" (docs/todo/
// virtual-apps.md namespace-scope-by-annotation addendum, 2026-08-25) —
// generalized to MULTI-TENANT (docs/todo/virtual-apps.md multi-tenant-push
// addendum, 2026-08-25).
//
// Today an agent only watches the namespaces named in its static
// --target-namespaces list (Pusher.opts.Namespaces / virtualAppsState.namespaces);
// an empty list meant "watch everything" (no longer true, see the scope rule
// below). This addendum adds a DYNAMIC,
// live-updating source of additional in-scope namespaces: a Namespace object
// carrying the annotation `triageagent.dev/tenant: "<tenant>"` is in scope
// automatically — no operator edit to the static list, no restart.
//
// A single agent process now serves its PRIMARY tenant (Options.Tenant, the
// same value that always selected the triage service graph and signed the
// hello frame) PLUS every tenant named by an annotated namespace's
// triageagent.dev/tenant value, whatever that value is — not just a value matching
// Options.Tenant. All tenants share the ONE outbound WebSocket connection
// (connworker.go); the mapping recorded here (namespace -> tenant) is what
// stamps every frame produced for that namespace with its tenant's per-frame
// Tenant envelope value (tenant_routing.go). A namespace with no recorded
// mapping always resolves to the primary tenant (whose frames carry no
// stamp), unchanged from before this generalization.
//
// CLUSTER MODE (addendum 2026-08-27, docs/todo/virtual-apps.md
// "Cluster-mode push"): Options.Tenant may now be empty — the agent then has
// no primary at all and is identified only by Options.ClusterName. The
// namespace -> tenant mapping recorded here is unaffected (any non-empty
// annotation value still admits a namespace exactly the same way), but
// "a namespace with no recorded mapping always resolves to the primary
// tenant" above no longer holds: with no primary to fall back to, such a
// namespace's items are dropped instead (tenant_routing.go's
// resolveTenant/recordNoTenantDrop) — never defaulted onto an invented
// destination.
//
// Scope rule (read this before touching inScope anywhere):
//
//	effective scope = union(static list, dynamic set)   — when static is non-empty
//	effective scope = the dynamic set ALONE             — when static is empty,
//	                                                        in BOTH modes
//
// An empty static --target-namespaces list means the triageagent.dev/tenant-annotated
// set, in both modes. A primary tenant decides which tenant an in-scope
// namespace is filed under; it does not widen scope. With no list and no
// annotation the agent observes nothing.
//
// resolveTenant/recordNoTenantDrop drop every item from an unannotated
// namespace in cluster mode, so watching those namespaces produces nothing
// while costing a cluster-wide informer cache and cluster-wide list/watch
// RBAC.
//
// So: never let the dynamic set narrow a PRIMARY-TENANT agent, and never let a
// cluster-mode agent watch what it is structurally incapable of pushing.
package push

import (
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
)

// annotationkeys.Tenant is the Namespace-level annotation that pulls a
// namespace into DYNAMIC scope (see NamespaceTenant, below). Any non-empty
// value qualifies; it no longer has to match this agent's own
// Options.Tenant, since a single agent can now serve several tenants at
// once, one outbound connection per tenant.

// dynamicScopeSet is the thread-safe map of namespace name -> tenant,
// recording every namespace currently pulled into scope by a triageagent.dev/tenant
// Namespace annotation, and which tenant's value it carries. Maintained by
// Pusher.onNamespace off the same Namespace informer that already backs
// nsDefaultsStore (namespace_defaults.go) — a different concern (scope
// membership + tenant routing, not triageagent.dev/* metadata fallback), so it is its
// own type rather than folded into namespaceDefaults.
type dynamicScopeSet struct {
	mu   sync.Mutex
	data map[string]string // namespace -> tenant
}

func newDynamicScopeSet() *dynamicScopeSet {
	return &dynamicScopeSet{data: make(map[string]string)}
}

// add records ns as dynamically scoped under tenant. Returns the tenant it
// was PREVIOUSLY recorded under (empty if none) and whether this call is a
// genuine transition — either a brand-new namespace, or the same namespace
// re-annotated with a DIFFERENT tenant value. A redundant informer re-sync
// carrying the same (ns, tenant) pair already recorded is not a transition:
// changed is false and the caller does no extra work (no re-backfill, no
// worker churn).
func (s *dynamicScopeSet) add(ns, tenant string) (prevTenant string, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, existed := s.data[ns]
	if existed && prev == tenant {
		return prev, false
	}
	s.data[ns] = tenant
	return prev, true
}

// remove drops ns from dynamic scope. Returns the tenant it was scoped under
// and whether it was actually present — the caller uses this to decide
// whether to evict cached workloads / stop that tenant's connection worker,
// so removing a namespace that was never dynamically scoped (only ever
// covered by the static list, or never annotated at all) does no extra work.
func (s *dynamicScopeSet) remove(ns string) (tenant string, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.data[ns]
	if !ok {
		return "", false
	}
	delete(s.data, ns)
	return t, true
}

// has reports whether ns is currently dynamically scoped, under any tenant.
func (s *dynamicScopeSet) has(ns string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[ns]
	return ok
}

// tenantFor reports the tenant ns is currently dynamically scoped under, and
// whether it is scoped at all — the routing lookup tenant_routing.go's
// tenantForNamespace consults before falling back to the primary tenant.
func (s *dynamicScopeSet) tenantFor(ns string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.data[ns]
	return t, ok
}

// tenantCounts returns, for every tenant currently recorded, how many
// namespaces are presently scoped under it. Used by
// Pusher.activeSecondaryTenants (tenant_routing.go) to enumerate the
// tenants whose stamped snapshot sequences a (re)connect must send — a
// tenant absent from this map holds no namespace and has no state to sync.
func (s *dynamicScopeSet) tenantCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[string]int, len(s.data))
	for _, t := range s.data {
		counts[t]++
	}
	return counts
}

// effectiveInScopeMode implements the scope-union rule documented at the top
// of this file. static is the operator-configured --target-namespaces list
// (Pusher.opts.Namespaces / virtualAppsState.namespaces); dynamic is the live
// triageagent.dev/tenant-annotation-derived set, nil-safe: with none wired
// the answer is static-list-only, or watch-all when static is empty.
// clusterMode true means "no primary tenant", which decides what
// happens to an item from a namespace that IS in scope but carries no
// annotation: dropped in cluster mode, filed under the primary tenant
// otherwise. It no longer affects SCOPE — see the empty-static branch.
func effectiveInScopeMode(ns string, static []string, dynamic *dynamicScopeSet, clusterMode bool) bool {
	if len(static) == 0 {
		// Not configured is not "everything". With no --target-namespaces the
		// annotated set IS the scope, in BOTH modes: a primary tenant answers
		// "which tenant does this belong to", never "observe the whole
		// cluster". It used to return true here for single-tenant, so an
		// agent given --target-tenant and no list pushed every namespace on
		// the cluster under that tenant — observed on cluster-a, which shipped
		// train, perf, sandbox, argo and sqlproxy while three were intended.
		//
		// A nil dynamic set means the annotation watcher was never wired
		// (bare struct literals in tests, and the static-list-only probe on
		// the onNamespace path). That is not a deployment, so it keeps the
		// permissive answer rather than silently scoping those callers to
		// nothing.
		if dynamic == nil {
			return true
		}
		return dynamic.has(ns)
	}
	for _, want := range static {
		if want == ns {
			return true
		}
	}
	return dynamic != nil && dynamic.has(ns)
}

// staticInScope answers "is ns in scope from the static --target-namespaces
// list ALONE", with no dynamic fallback at all — not even the permissive
// nil-dynamic-set answer effectiveInScopeMode gives everywhere else. It
// exists for exactly one caller, dropDynamicScope's post-removal probe: that
// probe asks what ns's scope will be once its ONLY dynamic mapping is gone,
// so "no dynamic set was wired" cannot mean "assume still in scope" the way
// it means for onDeployment/onApp — dropDynamicScope only runs on a Pusher
// whose dynamicScope is provably non-nil (it already loaded ns's tenant out
// of it a few lines up). An empty static list here means nothing is
// statically in scope, so a namespace whose dynamic membership just ended
// left scope entirely.
func staticInScope(ns string, static []string) bool {
	for _, want := range static {
		if want == ns {
			return true
		}
	}
	return false
}

// cacheCovers reports whether the manager's informer cache covers namespace,
// given cacheNamespaces (Options.CacheNamespaces / virtualAppsState.
// cacheNamespaces — main.go's cacheOpts.DefaultNamespaces keys). Empty means
// a cluster-wide cache (main.go's --namespace="" path): every namespace is
// covered, unconditionally.
//
// This answers a DIFFERENT question from effectiveInScopeMode above:
// effectiveInScopeMode asks "should this agent push this namespace at all";
// cacheCovers asks "does the manager cache already know about it at the
// informer level" — which decides whether virtual-app workload tracking
// (virtualapps.go) can list/watch it through the manager cache directly, or
// must fall back to a runtime, per-namespace informer factory
// (dynamic_informers.go) because controller-runtime cannot add a namespace
// to an already namespace-scoped cache at runtime.
func cacheCovers(ns string, cacheNamespaces []string) bool {
	if len(cacheNamespaces) == 0 {
		return true
	}
	for _, want := range cacheNamespaces {
		if want == ns {
			return true
		}
	}
	return false
}

// NamespaceTenant returns a Namespace object's annotationkeys.Tenant value
// (trimmed) and whether it is present and non-empty. Any non-empty value
// pulls the namespace into dynamic scope now, not just a value equal to
// this agent's own primary tenant (Options.Tenant), so a single agent can
// serve several tenants over N connections, one per active tenant
// (connworker.go). The admission predicate this replaces (tenantScopeMatch)
// only ever admitted a namespace whose annotation equaled Options.Tenant;
// that equals-primary check is gone, but the fact that an absent/empty
// value never admits a namespace is unchanged.
//
// Exported for main.go and the standalone log-exporter/ebpf roles
// (cmd/triage-agent/namespace_tenant_watch.go), which must resolve the same
// annotation before the manager (and any Pusher) exists.
func NamespaceTenant(ns *corev1.Namespace) (string, bool) {
	v := strings.TrimSpace(ns.Annotations[annotationkeys.Tenant])
	return v, v != ""
}

// addDynamicScope records ns as scoped under tenant and, on a genuine
// transition, backfills its already-cached workloads into vapp synthesis —
// what makes the namespace's EXISTING content appear immediately instead of
// only ever working for that namespace's next informer event (see
// virtualAppsState.backfillNamespace's doc comment for why the backfill step
// is necessary at all) — and then, for a secondary tenant, sends that
// tenant's full stamped snapshot sequence on the live connection
// (sendTenantSnapshotOnLive, tenant_routing.go), so the receiver gets its
// complete state without waiting for the next reconnect.
//
// A namespace RE-annotated with a DIFFERENT tenant (prevTenant != tenant) is
// also a transition: its workloads no longer belong to the old tenant's
// view, so they are evicted from it BEFORE the mapping is updated — while
// tenantForNamespace still resolves ns to prevTenant — so the OpDelete
// deltas the eviction emits are stamped with the OLD tenant and clean up its
// hub-side view. (After the remap they would be stamped with the new tenant,
// and with one shared connection there is no per-tenant disconnect left to
// clean the old view up otherwise.) The lookup-then-mutate pair here is not
// atomic, but every caller runs on the single Namespace informer handler
// goroutine, which serializes scope transitions — the same assumption
// onNamespace's other side effects already make.
//
// nil-safe: p.dynamicScope is only ever nil for a bare *Pusher struct literal
// built directly by a test that predates this feature.
func (p *Pusher) addDynamicScope(ns, tenant string) {
	if p.dynamicScope == nil {
		return
	}
	prevTenant, hadPrev := p.dynamicScope.tenantFor(ns)
	if hadPrev && prevTenant == tenant {
		return // already scoped under this exact tenant — nothing changed
	}
	if hadPrev && prevTenant != tenant {
		if p.vapps != nil {
			p.vapps.evictNamespaceAndRecompute(ns)
		}
	}
	p.dynamicScope.add(ns, tenant)
	p.log.Info("namespace entered dynamic scope via annotation", "key", annotationkeys.Tenant, "namespace", ns, "tenant", tenant)
	if p.vapps != nil {
		// backfillNamespace starts (idempotently) the runtime informer factory
		// for an out-of-cache namespace itself, before listing its already-
		// cached workloads.
		p.vapps.backfillNamespace(ns)
	}
	p.sendTenantSnapshotOnLive(tenant)
}

// dropDynamicScope removes ns from tenant scope and, on a genuine transition
// that leaves ns out of scope ENTIRELY (it may still be covered by the
// static --target-namespaces list even though its dynamic membership just
// ended), evicts its cached workloads from vapp synthesis so the group they
// formed actually disappears (virtualAppsState.evictNamespaceAndRecompute).
// The eviction runs BEFORE the mapping is removed — while tenantForNamespace
// still resolves ns to its tenant — so the OpDelete deltas it emits go out
// stamped with the leaving tenant, via the normal split path, and clean up
// that tenant's hub-side view; a tenant's last namespace leaving needs
// nothing more than that (there is no per-tenant connection to tear down
// anymore). The static-list-only scope probe (staticInScope, which never
// falls back to the permissive nil-dynamic-set answer effectiveInScopeMode
// gives elsewhere) asks what ns's scope will be AFTER the removal — the
// dynamic mapping being removed is the ONLY thing staticInScope leaves out —
// without mutating first. Same informer-handler serialization assumption as
// addDynamicScope.
//
// nil-safe, same reasoning as addDynamicScope.
func (p *Pusher) dropDynamicScope(ns string) {
	if p.dynamicScope == nil {
		return
	}
	tenant, ok := p.dynamicScope.tenantFor(ns)
	if !ok {
		return // was never dynamically scoped — nothing changed
	}
	if !staticInScope(ns, p.opts.Namespaces) {
		if p.vapps != nil {
			p.vapps.evictNamespaceAndRecompute(ns)
		}
	}
	// Stop the namespace's runtime informer factory (workloads and any
	// Warning-event informer share one lifecycle) — a no-op when none was
	// ever started (cache-covered namespace, or already stopped by
	// evictNamespaceAndRecompute above).
	if p.dynInformers != nil {
		p.dynInformers.stop(ns)
	}
	p.dynamicScope.remove(ns)
	p.log.Info("namespace left dynamic scope", "namespace", ns, "tenant", tenant)
}

// declaredNamespaces is the namespace set the hello declares, and it must be
// the set this agent actually observes rather than the one it was configured
// with. The core renders an empty list as "all" (ClusterSession.Namespaces,
// "empty means cluster-wide"), so a cluster-mode agent — which takes no
// --target-namespaces at all — declared nothing and was displayed as watching
// every namespace while its cache covered only the triageagent.dev/tenant-annotated
// ones. Observed on cluster-a: three annotated namespaces, pill read "all".
//
// Namespaces (--target-namespaces) wins when set, since it is what bounds
// pushing. Otherwise the cache scope is the effective answer.
func (p *Pusher) declaredNamespaces() []string {
	if len(p.opts.Namespaces) > 0 {
		return p.opts.Namespaces
	}
	return p.opts.CacheNamespaces
}

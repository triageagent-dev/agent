package push

import (
	"encoding/json"
	"slices"
	"sync"
	"time"
)

// Workspace namespace scope. The core tells the agent, in the limits frame,
// which namespaces each tenant (workspace) takes from this agent:
// LimitsPayload.Namespaces maps a tenant ID to its allowed namespaces. A
// tenant that is absent, or has an empty list, takes every namespace. A
// limits frame replaces the whole scope: the core leaves the field out to
// say a binding no longer limits a connection it limited before, so a frame
// without it lifts the scope.
//
// The core sends a limits frame right after the hello whenever a binding
// names namespaces, and says nothing otherwise. So the scope is kept across
// process starts: a key join's bindings and every limits frame are saved in
// the join store (joinScopeFile) and restored at the next handshake, which
// covers the snapshots sent before the core's first frame. A restored scope
// stands until a limits frame replaces it, or until scopeLimitsGrace after a
// session opened has passed without one, which is the core saying nothing is
// scoped any more. An agent that never had a scope, or a core that never
// sends the field, changes nothing: everything is allowed.
//
// The scope is enforced at the earliest point each path has: the OTLP
// receiver asks WorkspaceNamespaceInScope before it stores a resource, and the push
// path drops any item outside its tenant's scope (resolveTenant,
// filterAppsForTenant) and counts it in Link.OutOfScopeDropped.

type workspaceScope struct {
	mu sync.RWMutex
	// ns maps a tenant to the set of namespaces it takes; a tenant not in the
	// map takes all.
	ns map[string]map[string]struct{}
	// received is set once a limits frame replaced the scope, so a later
	// seed from a join result never overwrites what the core sent.
	received bool
	// restored is set while ns comes from a join result or the join store
	// and no limits frame has confirmed it.
	restored bool
	// tried is set once the join store was read for a scope, so a core that
	// never sends one costs no read per reconnect.
	tried bool
	// saved is the last scope written to, or read from, the join store.
	saved string
}

// scopeLimitsGrace is how long a restored scope stands after a session opens
// without a limits frame.
var scopeLimitsGrace = 30 * time.Second

func scopeSets(m map[string][]string) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{}, len(m))
	for t, list := range m {
		if len(list) == 0 {
			continue
		}
		set := make(map[string]struct{}, len(list))
		for _, n := range list {
			set[n] = struct{}{}
		}
		out[t] = set
	}
	return out
}

// set replaces the scope with what a limits frame carries.
func (s *workspaceScope) set(m map[string][]string) {
	next := scopeSets(m)
	s.mu.Lock()
	s.ns, s.received, s.restored = next, true, false
	s.mu.Unlock()
}

// restore installs a scope read from the join store, unless a limits frame
// or an earlier restore already set one.
func (s *workspaceScope) restore(m map[string][]string) {
	next := scopeSets(m)
	s.mu.Lock()
	s.tried = true
	if !s.received && !s.restored {
		s.ns, s.restored = next, len(next) > 0
	}
	s.mu.Unlock()
}

// needsRestore reports whether the join store should be read for a scope.
func (s *workspaceScope) needsRestore() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.received && !s.restored && !s.tried
}

// dropRestored lifts a restored scope that no limits frame confirmed and
// reports whether it did.
func (s *workspaceScope) dropRestored() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.received || !s.restored {
		return false
	}
	s.ns, s.restored = nil, false
	return true
}

// armGrace starts the wait for a limits frame after a session opened; the
// returned stop cancels it. It does nothing unless a scope was restored.
func (s *workspaceScope) armGrace(onDrop func()) (stop func()) {
	s.mu.RLock()
	pending := s.restored && !s.received
	s.mu.RUnlock()
	if !pending {
		return func() {}
	}
	t := time.AfterFunc(scopeLimitsGrace, func() {
		if s.dropRestored() {
			onDrop()
		}
	})
	return func() { t.Stop() }
}

// encoded returns the scope in the form the join store keeps it, and whether
// it differs from what was last saved or read. No scope and nothing saved are
// the same, so an agent that never had a scope never writes one.
func (s *workspaceScope) encoded() (data []byte, changed bool) {
	data, _ = json.Marshal(s.snapshot())
	s.mu.RLock()
	defer s.mu.RUnlock()
	cur := string(data)
	return data, cur != s.saved && (cur != "{}" || s.saved != "")
}

// markSaved records data as what the join store now holds.
func (s *workspaceScope) markSaved(data []byte) {
	s.mu.Lock()
	s.saved = string(data)
	s.mu.Unlock()
}

// seed installs the scope of a join result's bindings, unless a limits frame
// already set one.
func (s *workspaceScope) seed(bindings []JoinBinding) {
	m := make(map[string][]string, len(bindings))
	for _, b := range bindings {
		m[b.Tenant] = b.Namespaces
	}
	next := scopeSets(m)
	s.mu.Lock()
	s.tried = true
	if !s.received {
		s.ns, s.restored = next, len(next) > 0
	}
	s.mu.Unlock()
}

func (s *workspaceScope) allowed(tenant, ns string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set, ok := s.ns[tenant]
	if !ok {
		return true
	}
	_, in := set[ns]
	return in
}

// snapshot returns the scope as sorted lists, for tests and logs.
func (s *workspaceScope) snapshot() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]string, len(s.ns))
	for t, set := range s.ns {
		list := make([]string, 0, len(set))
		for n := range set {
			list = append(list, n)
		}
		slices.Sort(list)
		out[t] = list
	}
	return out
}

// WorkspaceNamespaceInScope reports whether namespace ns (the plain Kubernetes name,
// never the "<tenant>~<ns>" form) is inside tenant's workspace scope. A
// tenant with no scope from the core takes every namespace, and so does an
// empty ns: a source with no namespace of its own is not a namespace to
// exclude. Safe on a nil Pusher (everything is in scope).
func (p *Pusher) WorkspaceNamespaceInScope(tenant, ns string) bool {
	if p == nil || ns == "" {
		return true
	}
	return p.scope.allowed(tenant, ns)
}

// recordOutOfScopeDrop counts one payload item dropped because its namespace
// is outside its tenant's workspace scope, and logs the first and then every
// thousandth.
func (p *Pusher) recordOutOfScopeDrop(tenant, ns string) {
	n := p.outOfScopeDropped.Add(1)
	if n == 1 || n%1000 == 0 {
		p.log.Info("dropping item: namespace is outside the workspace scope the core set",
			"tenant", tenant, "namespace", ns, "droppedTotal", n)
	}
}

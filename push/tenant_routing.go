// Multi-tenant frame routing (docs/todo/virtual-apps.md multi-tenant-push
// addendum, 2026-08-25): every frame this Pusher produces derives its
// tenant(s) from the namespace(s) it carries, is SPLIT per tenant where the
// payload spans more than one (splitFrameByTenant), and every split travels
// on the ONE connection (connworker.go) carrying a per-frame Tenant envelope
// stamp — protocol v3, replacing the short-lived one-socket-per-tenant model.
//
// Two operating modes, selected by whether Options.Tenant is set:
//
//   - Single-tenant / PRIMARY mode (Options.Tenant != ""): the primary
//     tenant's frames get Tenant left EMPTY, omitted on the wire —
//     byte-identical to a plain single-tenant deployment — and every other
//     tenant's frames carry that tenant's name, which the receiver validates
//     against the cluster's pinned tenant set and files accordingly. A
//     namespace with no dynamic-scope mapping (namespace_scope.go) always
//     resolves to the primary tenant — the "no mapping => primary" default.
//
//   - CLUSTER mode (Options.Tenant == "", addendum 2026-08-27, see
//     docs/todo/virtual-apps.md "Cluster-mode push"): there is no primary to
//     imply by omission. The agent is identified only by Options.ClusterName;
//     the hello frame declares an empty tenant and EVERY frame this Pusher sends must carry an explicit,
//     non-empty Tenant stamp derived purely from a triageagent.dev/tenant namespace
//     annotation. A namespace with no such mapping resolves to NO tenant at
//     all — its items are DROPPED agent-side (counted + rate-limited logged,
//     recordNoTenantDrop) rather than defaulted onto an invented destination,
//     and no unstamped sequence/frame is ever sent. The core accepts the
//     empty hello for the tenants the agent's certificate is bound to; see
//     the ROLLING-ORDER CONSTRAINT below, which applies to cluster-mode too.
//
// ROLLING-ORDER CONSTRAINT (deployment order: core first): an obi.12 agent
// REQUIRES a core with per-frame tenant support (v2.9.5+ — the commit that
// added frame_tenant_test.go on the triage side). An older core does not
// reject a stamped frame; it silently ignores the unknown envelope field and
// files the frame under the SESSION tenant, so a secondary tenant's data
// would be quietly misfiled into the primary tenant's graph. There is no
// capability negotiation for this yet — upgrade the core before rolling out
// an agent that stamps. Cluster-mode raises the bar further: an old core
// rejects the empty-tenant hello at handshake outright (fail-closed,
// connection refused) rather than misfiling anything, since single-tenant
// handshake verification has always required a non-empty tenant — see the
// doc addendum for the full rollout note.
package push

import (
	"context"
	"sort"
	"time"

	"github.com/gorilla/websocket"

	"github.com/triageagent-dev/agent/internal/annotationkeys"
	"github.com/triageagent-dev/agent/internal/remotens"
)

// tenantForNamespace resolves which tenant's connection namespace ns's
// frames belong on: its own triageagent.dev/tenant annotation when dynamically
// scoped under one, else the primary tenant (Options.Tenant) — "no mapping"
// always means primary, unchanged from before this feature existed. An
// empty ns (a frame with no namespace at all, e.g. a malformed payload)
// also resolves to primary, defensively.
//
// In cluster-mode (Options.Tenant == "") "primary" is itself empty, so an
// unmapped namespace resolves to "" here too — callers must not treat that
// as a valid destination the way single-tenant mode does. resolveTenant
// below is the wrapper every multi-item split path uses to tell the two
// cases apart; this function stays the raw lookup so filterAppsForTenant's
// direct-equality use (see its own doc comment) is unaffected.
func (p *Pusher) tenantForNamespace(ns string) string {
	// A namespace qualified "<tenant>~<ns>" came from a remote Collector
	// whose certificate named the tenant (remote_tenants.go). A local
	// namespace cannot contain the separator, so this cannot be spoofed by an
	// annotation, and the OTLP receiver and the sensor stream refuse any
	// namespace a sender qualified itself.
	if t, _, ok := remotens.Split(ns); ok {
		p.noteRemoteTenant(t)
		return t
	}
	if ns != "" && p.dynamicScope != nil {
		if t, ok := p.dynamicScope.tenantFor(ns); ok && t != "" {
			return t
		}
	}
	return p.opts.Tenant
}

// resolveTenant is tenantForNamespace with an explicit "no destination"
// outcome, for callers that must not silently default an item onto the
// primary connection. In single-tenant mode (Options.Tenant != "") it
// always succeeds — tenantForNamespace's own "no mapping ⇒ primary"
// fallback guarantees a non-empty result, so every existing deployment sees
// zero behavior change. In cluster-mode (Options.Tenant == "") there is no
// primary to fall back to: a namespace with no triageagent.dev/tenant mapping
// resolves to "" and this reports ok=false, counting + rate-limit-logging
// the drop (recordNoTenantDrop) instead of inventing a destination.
//
// Every per-item split path that decides where ONE item goes —
// splitFrameByTenant's single-object branches (app_delta, k8s_event) and
// splitByTenant's generic multi-item path (log/red/use metrics, red/use
// summaries, topology nodes/edges) — calls this instead of
// tenantForNamespace directly for exactly this reason.
func (p *Pusher) resolveTenant(ns string) (tenant string, ok bool) {
	if t, rest, remote := remotens.Split(ns); remote && rest == "" {
		// A remote source with no namespace of its own (a node-level series).
		// Its tenant is known, but an item with an empty namespace is not a
		// shape the core has been sent by a cluster agent, so it is dropped
		// and counted like any item with no destination.
		p.recordNoTenantDrop(t + remotens.Sep)
		return "", false
	}
	t := p.tenantForNamespace(ns)
	if t == "" {
		// Only reachable when Options.Tenant == "" (cluster-mode) and ns has
		// no triageagent.dev/tenant mapping — single-tenant mode's "no mapping"
		// fallback above always returns a non-empty Options.Tenant.
		p.recordNoTenantDrop(ns)
		return "", false
	}
	// The workspace scope the core set (workspace_scope.go): an item whose
	// plain namespace is outside its tenant's scope goes nowhere.
	if plain := remotens.Strip(t, ns); !p.WorkspaceNamespaceInScope(t, plain) {
		p.recordOutOfScopeDrop(t, plain)
		return "", false
	}
	return t, true
}

// recordNoTenantDrop counts one payload item dropped because its namespace
// resolved to no tenant at all (cluster-mode, see resolveTenant) — always
// zero in single-tenant mode. Logged at Info for the first occurrence and
// every 1000th after that (mirrors connWorker.enqueueOK's send-queue-full
// drop logging), so a persistently un-annotated namespace is visible
// without flooding the log on every event.
func (p *Pusher) recordNoTenantDrop(ns string) {
	n := p.noTenantDropped.Add(1)
	if n == 1 || n%1000 == 0 {
		p.log.Info("dropping item: namespace has no tenant mapping and this agent has no primary tenant (cluster-mode)",
			"key", annotationkeys.Tenant, "namespace", ns, "droppedTotal", n)
	}
}

// wireTenantFor returns the value a frame's Tenant envelope field must carry
// for frames belonging to tenant.
//
// Single-tenant mode (Options.Tenant != ""): empty for the primary tenant
// (omitted on the wire, so a plain single-tenant deployment's frames are
// byte-identical to before the field existed) and the tenant's own name for
// every other tenant — unchanged from before cluster-mode existed.
//
// Cluster-mode (Options.Tenant == ""): there is no primary tenant to imply
// by omission, so the wire stamp is always the tenant's own name, verbatim
// — every caller that reaches here has already resolved a real, non-empty
// tenant (resolveTenant/fallbackTenantFrame drop anything that didn't,
// before this is ever called), so tenant is never "" in practice on this
// branch; returning it verbatim rather than special-casing keeps that
// invariant visible instead of silently mapping an unresolved value onto
// the single-tenant "primary" convention that does not exist here.
func (p *Pusher) wireTenantFor(tenant string) string {
	if p.opts.Tenant == "" {
		return tenant
	}
	if tenant == "" || tenant == p.opts.Tenant {
		return ""
	}
	return tenant
}

// activeSecondaryTenants returns the sorted set of tenants that currently
// hold at least one dynamically-scoped namespace — the tenants whose
// stamped snapshot sequences a (re)connect must send (connWorker.runOnce).
//
// Single-tenant mode: this excludes the primary tenant itself — its own
// snapshot sequence is unconditional and sent separately, unstamped, before
// this list is consulted. Cluster-mode: there is no primary tenant to
// exclude (Options.Tenant == "", and dynamicScope only ever records
// non-empty tenant values, NamespaceTenant requires one), so every tenant
// holding a namespace is returned, connWorker.runOnce sends ONLY these
// stamped sequences, no unstamped one, in that mode.
func (p *Pusher) activeSecondaryTenants() []string {
	seen := map[string]bool{}
	var out []string
	add := func(tenant string) {
		if tenant != "" && tenant != p.opts.Tenant && !seen[tenant] {
			seen[tenant] = true
			out = append(out, tenant)
		}
	}
	if p.dynamicScope != nil {
		for tenant := range p.dynamicScope.tenantCounts() {
			add(tenant)
		}
	}
	for _, tenant := range p.activeRemoteTenants() {
		add(tenant)
	}
	sort.Strings(out)
	return out
}

// filterAppsForTenant keeps only the Applications whose own namespace
// resolves to tenant. Called once per active tenant while building that
// tenant's snapshot sequence (snapshotFramesForTenant) — deliberately
// compares directly against tenantForNamespace rather than routing through
// resolveTenant/recordNoTenantDrop: tenant here is always a real,
// already-resolved destination (Options.Tenant in single-tenant mode, or
// one of activeSecondaryTenants' names — connWorker.runOnce never calls
// this with tenant==""), and an app whose namespace has no tenant at all
// (cluster-mode, tenantForNamespace returns "") can never equal that
// non-empty tenant, so it is excluded for free, with no separate drop
// bookkeeping needed here — counting it too would multiply-count the same
// app once per active tenant on every reconnect.
func (p *Pusher) filterAppsForTenant(apps []*VirtualApp, tenant string) []*VirtualApp {
	out := make([]*VirtualApp, 0, len(apps))
	for _, a := range apps {
		if p.tenantForNamespace(a.Namespace) != tenant {
			continue
		}
		if plain := remotens.Strip(tenant, a.Namespace); !p.WorkspaceNamespaceInScope(tenant, plain) {
			p.recordOutOfScopeDrop(tenant, plain)
			continue
		}
		out = append(out, a)
	}
	return out
}

// snapshotFramesForTenant builds one tenant's full snapshot sequence —
// chunked snapshot frames terminated by snapshot_end, every frame carrying
// the tenant's wire stamp (wireTenantFor: empty for the primary tenant in
// single-tenant mode, including its snapshot_end; the tenant's name on
// every chunk AND the snapshot_end otherwise, so the receiver can bracket
// that tenant's sequence). It reads the synthesized virtual apps in mode
// "on", then keeps only the ones whose own namespace resolves to tenant.
//
// Before reading the virtual-app set it forces a synchronous recompute
// (virtualAppsState.recomputeNow): applyWorkload's 2s debounce is a churn-
// coalescing courtesy for the delta stream, not a barrier the very first
// snapshot after connect (or any reconnect) should ever be blocked by — a
// pre-existing steady workload that never fires another informer event would
// otherwise never make it into v.current before this reads it (docs/todo/
// virtual-apps.md). Safe to call once per tenant sequence: recompute is
// idempotent when nothing has changed between calls.
func (p *Pusher) snapshotFramesForTenant(ctx context.Context, tenant string) (frames []*Frame, appCount int) {
	var apps []*VirtualApp
	if p.vapps != nil && p.opts.VirtualApps == VirtualAppsOn {
		p.vapps.recomputeNow(ctx)
		apps = p.vapps.snapshotApps()
	}
	apps = p.filterAppsForTenant(apps, tenant)
	wire := p.wireTenantFor(tenant)
	for i := 0; i < len(apps); i += p.opts.SnapshotChunk {
		end := i + p.opts.SnapshotChunk
		if end > len(apps) {
			end = len(apps)
		}
		f := newSnapshot(p.opts.ClusterName, apps[i:end])
		f.Tenant = wire
		unqualifyFrame(f, tenant)
		frames = append(frames, f)
	}
	end := newSnapshotEnd(p.opts.ClusterName)
	end.Tenant = wire
	return append(frames, end), len(apps)
}

// sendSnapshotForTenant writes tenant's snapshot sequence directly to conn —
// the (re)connect path (connWorker.runOnce), which in single-tenant mode
// sends the primary tenant's sequence first and then one stamped sequence
// per active secondary tenant; in cluster-mode there is no primary sequence
// at all, only one stamped sequence per active tenant — all on the same
// socket.
func (p *Pusher) sendSnapshotForTenant(ctx context.Context, conn *websocket.Conn, tenant string) error {
	frames, appCount := p.snapshotFramesForTenant(ctx, tenant)
	for _, f := range frames {
		if err := p.writeFrame(conn, f); err != nil {
			return err
		}
	}
	p.log.Info("sent snapshot", "apps", appCount, "cluster", p.opts.ClusterName, "tenant", tenant)
	return nil
}

// sendTenantSnapshotOnLive enqueues a SECONDARY tenant's full stamped
// snapshot sequence onto the live connection — called from addDynamicScope
// when a namespace joins that tenant while connected, so the receiver gets
// the tenant's complete state immediately instead of waiting for the next
// reconnect. Runs after backfillNamespace so the freshly-backfilled virtual
// apps are already in the set the snapshot reads. A no-op for the primary
// tenant in single-tenant mode (its state is already live on the
// connection, and the backfill's own deltas cover a namespace joining it —
// unchanged from before multi-tenant push existed); in cluster-mode there
// is no primary tenant to no-op against, so every tenant reaches the send
// below. Disconnected → every frame drops in connWorker.enqueueOK (logged
// at V(1)); nothing is lost, since the next (re)connect's per-tenant
// snapshot pass covers every tenant still in dynamicScope.
func (p *Pusher) sendTenantSnapshotOnLive(tenant string) {
	if tenant == "" || (p.opts.Tenant != "" && tenant == p.opts.Tenant) {
		return
	}
	frames, appCount := p.snapshotFramesForTenant(context.Background(), tenant)
	// Routed through the mux like any other frame (tenant_worker.go) — this
	// tenant's queue is lazily created here if it doesn't exist yet
	// ("created lazily on first frame for a tenant"), and the sequence's
	// completion (KindSnapshotEnd) is recorded on that worker by mux.next as
	// it is actually drained onto the wire.
	mux := p.mux()
	for _, f := range frames {
		mux.enqueue(tenant, f)
	}
	p.log.Info("enqueued tenant snapshot on live connection", "apps", appCount, "tenant", tenant)
}

// handleResyncFrame answers a core-triggered resync (KindResync, see
// protocol.go): the core reloaded a tenant's service graph and wants that
// tenant's full snapshot resent, exactly as if this agent had just
// (re)connected — but without forcing an actual reconnect.
//
// tenant is the frame's own Tenant stamp; an empty stamp means the
// session's handshake tenant (Options.Tenant). Cluster-mode has no
// handshake tenant to fall back to, so an unstamped resync there resolves
// to "" and is dropped — a resync must always name its tenant when there is
// no primary, same as every other tenant-routing path in this file.
//
// Unlike sendTenantSnapshotOnLive (which only ever resends a SECONDARY
// tenant's state, since the primary's is already live on the connection),
// a resync is answered for whichever tenant the core named, primary
// included — the core would not have asked otherwise.
//
// Coalescing: tryStartResync (tenant_worker.go) refuses a second resync for
// the same tenant while one is still queued or being drained onto the wire,
// so a burst of resync frames for one tenant produces at most one snapshot
// sequence in flight at a time.
//
// Routed through the same per-tenant outbound queue every other frame uses
// (mux.enqueue) so the write is serialized with everything else
// connWorker.writeLoop writes — this function itself never touches conn.
func (p *Pusher) handleResyncFrame(f *Frame) {
	tenant := f.Tenant
	if tenant == "" {
		tenant = p.opts.Tenant
	}
	if tenant == "" {
		p.log.V(1).Info("ignoring resync frame with no resolvable tenant (cluster-mode requires an explicit stamp)")
		return
	}

	mux := p.mux()
	if !mux.tryStartResync(tenant) {
		p.log.V(1).Info("resync already queued or in flight for tenant; coalescing", "tenant", tenant)
		return
	}

	frames, appCount := p.snapshotFramesForTenant(context.Background(), tenant)
	for _, fr := range frames {
		mux.enqueue(tenant, fr)
	}
	p.log.Info("resync requested by core", "tenant", tenant, "apps", appCount)
}

// currentActiveTenants returns the primary tenant (if set) followed by every
// currently active secondary tenant, sorted — the exact set
// connWorker.runOnce replays a snapshot sequence for at connect, and the set
// connWorker.reconcileTenants compares the tenant-worker registry against on
// every heartbeat tick to decide what to retire.
func (p *Pusher) currentActiveTenants() []string {
	var out []string
	if p.opts.Tenant != "" {
		out = append(out, p.opts.Tenant)
	}
	out = append(out, p.activeSecondaryTenants()...)
	return out
}

// enqueue offers a frame to the single connection, split and stamped per
// tenant — see splitFrameByTenant. Never blocks: a not-yet-connected mux
// just drops it (tenantMux.enqueue).
func (p *Pusher) enqueue(f *Frame) {
	p.enqueueOK(f)
}

// enqueueOK is enqueue's boolean-reporting form, for the one caller
// (SendTopologySnapshot) that needs to distinguish "at least one split
// reached a live tenant queue" from "every split of this frame was dropped"
// rather than fire-and-forget. Every other caller keeps using plain
// enqueue, unchanged. This is the ONE place delta-path frames get their
// Tenant wire stamp: each split is stamped for the tenant it resolved to
// (empty for the primary in single-tenant mode — wireTenantFor) and handed
// to that tenant's own worker queue (tenant_worker.go) — a DIFFERENT
// tenant's queue backing up or flooding never affects this frame's chances
// of being accepted. splitFrameByTenant never produces an entry keyed by a
// tenant that failed to resolve (cluster-mode drops those before
// returning), so ranging over an empty/nil map here correctly sends
// nothing.
func (p *Pusher) enqueueOK(f *Frame) bool {
	// Entries about annotationkeys.Triage-excluded workloads never reach a
	// tenant queue (exclusion.go).
	if f = p.dropExcluded(f); f == nil {
		return false
	}
	// Entries the core would refuse the whole frame over are named and held
	// back here (wellformed.go).
	if f = p.dropMalformed(f); f == nil {
		return false
	}
	mux := p.mux()
	ok := false
	for tenant, split := range p.splitFrameByTenant(f) {
		// Kubernetes events are the one flood the agent holds back itself:
		// the core told it the cap (event_caps.go). Every other kind carries
		// state the core needs whole, and the core caps the events it
		// derives from them.
		if split.Kind == KindK8sEvent && !p.eventCaps.admit(tenant, time.Now()) {
			p.recordCapDrop(tenant)
			continue
		}
		split.Tenant = p.wireTenantFor(tenant)
		unqualifyFrame(split, tenant)
		p.observe(tenant, split)
		if mux.enqueue(tenant, split) {
			ok = true
		}
	}
	return ok
}

// fallbackTenantFrame is the {tenant: frame} map every splitFrameByTenant
// branch falls back to when it has no namespace to resolve a destination
// from at all — a malformed payload (nil AppDelta/K8sEvent/etc, defensive
// only, should never occur upstream) or, for topology, a genuinely
// tenant-less level-state frame (nil payload / an entirely empty snapshot —
// see splitTopologyByTenant). Single-tenant mode routes it to the primary
// connection, unchanged from before this feature existed. Cluster-mode has
// no primary to fall back to — "never invent a default" applies here just
// as much as it does to an unmapped namespace — so it is dropped and
// counted exactly like a resolved-but-unmapped namespace (recordNoTenantDrop
// with an empty namespace, since none is known).
func (p *Pusher) fallbackTenantFrame(f *Frame) map[string]*Frame {
	if p.opts.Tenant != "" {
		return map[string]*Frame{p.opts.Tenant: f}
	}
	p.recordNoTenantDrop("")
	return nil
}

// splitFrameByTenant is the ONE place that knows how each frame kind's
// namespace(s) map onto a tenant connection. A deployment with no dynamic
// scope configured always resolves every namespace to the single primary
// tenant (single-tenant mode) or drops it (cluster-mode, nothing to route
// it to), so every branch here comes back with exactly one entry in
// single-tenant mode — routing everything onto the one connection this
// Pusher had before this feature existed.
func (p *Pusher) splitFrameByTenant(f *Frame) map[string]*Frame {
	switch f.Kind {
	case KindAppDelta:
		if f.AppDelta == nil || f.AppDelta.App == nil {
			return p.fallbackTenantFrame(f)
		}
		tenant, ok := p.resolveTenant(f.AppDelta.App.Namespace)
		if !ok {
			return nil
		}
		return map[string]*Frame{tenant: f}

	case KindK8sEvent:
		if f.K8sEvent == nil || f.K8sEvent.Event == nil {
			return p.fallbackTenantFrame(f)
		}
		tenant, ok := p.resolveTenant(f.K8sEvent.Event.Namespace)
		if !ok {
			return nil
		}
		return map[string]*Frame{tenant: f}

	case KindLogMetrics:
		if f.LogMetrics == nil {
			return p.fallbackTenantFrame(f)
		}
		return splitPayloadByTenant(p, f, f.LogMetrics.Services,
			func(s ServiceLogMetrics) string { return s.Namespace },
			func(services []ServiceLogMetrics) *Frame {
				return &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, LogMetrics: &LogMetricsPayload{
					WindowStart: f.LogMetrics.WindowStart, WindowSec: f.LogMetrics.WindowSec, Services: services,
				}}
			})

	case KindRedMetrics:
		if f.RedMetrics == nil {
			return p.fallbackTenantFrame(f)
		}
		return splitPayloadByTenant(p, f, f.RedMetrics.Services,
			func(s ServiceRedMetrics) string { return s.Namespace },
			func(services []ServiceRedMetrics) *Frame {
				return &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, RedMetrics: &RedMetricsPayload{
					WindowStart: f.RedMetrics.WindowStart, WindowSec: f.RedMetrics.WindowSec, Services: services,
				}}
			})

	case KindRedSummary:
		if f.RedSummary == nil {
			return p.fallbackTenantFrame(f)
		}
		return splitPayloadByTenant(p, f, f.RedSummary.Services,
			func(s ServiceRedMetrics) string { return s.Namespace },
			func(services []ServiceRedMetrics) *Frame {
				return &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, RedSummary: &RedSummaryPayload{
					WindowStart: f.RedSummary.WindowStart, WindowSec: f.RedSummary.WindowSec, Services: services,
				}}
			})

	case KindUseMetrics:
		if f.UseMetrics == nil {
			return p.fallbackTenantFrame(f)
		}
		return splitPayloadByTenant(p, f, f.UseMetrics.Services,
			func(s ServiceUseMetrics) string { return s.Namespace },
			func(services []ServiceUseMetrics) *Frame {
				return &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, UseMetrics: &UseMetricsPayload{
					WindowStart: f.UseMetrics.WindowStart, WindowSec: f.UseMetrics.WindowSec, Services: services,
				}}
			})

	case KindUseSummary:
		if f.UseSummary == nil {
			return p.fallbackTenantFrame(f)
		}
		return splitPayloadByTenant(p, f, f.UseSummary.Services,
			func(s ServiceUseSummary) string { return s.Namespace },
			func(services []ServiceUseSummary) *Frame {
				return &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, UseSummary: &UseSummaryPayload{
					WindowStart: f.UseSummary.WindowStart, WindowSec: f.UseSummary.WindowSec, Services: services,
				}}
			})

	case KindTopologySnapshot:
		return p.splitTopologyByTenant(f)

	default:
		// hello/snapshot/snapshot_end/heartbeat/pong never reach enqueue — they
		// are written directly per-connection in connWorker.runOnce/writeLoop.
		// Any other/unforeseen kind falls back to the primary connection in
		// single-tenant mode, exactly where a single-connection Pusher would
		// have sent it; cluster-mode drops it (fallbackTenantFrame).
		return p.fallbackTenantFrame(f)
	}
}

// splitByTenant groups items by the tenant their own namespace resolves to
// (resolveTenant), preserving each tenant's relative item order. Shared by
// every collector-frame family (log/red/use metrics, red/use summaries) via
// splitPayloadByTenant, and by topology's nodes/edges — they all carry a
// namespace per item, just under different Go types, so this is generic
// over the item type rather than duplicated per type. An item whose
// namespace has no tenant at all (cluster-mode, no mapping) is dropped —
// never folded into an invented destination.
func splitByTenant[T any](p *Pusher, items []T, namespaceOf func(T) string) map[string][]T {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string][]T)
	for _, it := range items {
		t, ok := p.resolveTenant(namespaceOf(it))
		if !ok {
			continue
		}
		out[t] = append(out[t], it)
	}
	return out
}

// splitPayloadByTenant is splitFrameByTenant's shared engine for every
// collector-frame family (log/red/use metrics, red/use summaries): they all
// carry one flat []ServiceXxx with an identical Namespace field, just as
// distinct Go types, so this is generic over the service type rather than
// duplicated five times. namespaceOf extracts a service's namespace; build
// re-wraps one tenant's subset of services into a fresh Frame of the same
// kind, preserving every field except Services itself. An empty services
// list (should not happen — callers guard against it before building the
// frame — but defensive here too) falls back to fallbackTenantFrame (the
// primary connection in single-tenant mode; dropped in cluster-mode, since
// there is no namespace here to resolve a destination from).
func splitPayloadByTenant[T any](p *Pusher, f *Frame, services []T, namespaceOf func(T) string, build func([]T) *Frame) map[string]*Frame {
	if len(services) == 0 {
		return p.fallbackTenantFrame(f)
	}
	byTenant := splitByTenant(p, services, namespaceOf)
	out := make(map[string]*Frame, len(byTenant))
	for tenant, subset := range byTenant {
		out[tenant] = build(subset)
	}
	return out
}

// splitTopologyByTenant groups a topology_snapshot's Nodes by their own
// namespace's tenant and its Edges by their SOURCE namespace's tenant — the
// workload that made the observed call is what "owns" that edge on the
// wire, so a cross-tenant edge's DstNamespace/DstWorkload still ride along
// as plain informational fields on the source tenant's connection rather
// than being duplicated onto the destination tenant's. An entirely empty
// snapshot (no nodes, no edges) is itself meaningful level state (see
// SendTopologySnapshot's doc comment): single-tenant mode routes it to the
// primary tenant alone, matching this feature's "no mapping => primary"
// default rule; cluster-mode has no primary to attribute a global,
// namespace-less snapshot to, so it is dropped (fallbackTenantFrame) rather
// than invented onto one tenant arbitrarily — it heals on the next tick
// once that window actually observes namespaced traffic.
func (p *Pusher) splitTopologyByTenant(f *Frame) map[string]*Frame {
	payload := f.TopologySnapshot
	if payload == nil {
		return p.fallbackTenantFrame(f)
	}
	if len(payload.Nodes) == 0 && len(payload.Edges) == 0 {
		return p.fallbackTenantFrame(f)
	}
	nodesByTenant := splitByTenant(p, payload.Nodes, func(n TopologyNode) string { return n.Namespace })
	edgesByTenant := splitByTenant(p, payload.Edges, func(e TopologyEdge) string { return e.SrcNamespace })

	tenants := make(map[string]bool, len(nodesByTenant)+len(edgesByTenant))
	for t := range nodesByTenant {
		tenants[t] = true
	}
	for t := range edgesByTenant {
		tenants[t] = true
	}

	out := make(map[string]*Frame, len(tenants))
	for t := range tenants {
		out[t] = &Frame{V: f.V, Kind: f.Kind, Cluster: f.Cluster, TopologySnapshot: &TopologySnapshotPayload{
			CapturedAt: payload.CapturedAt, WindowSec: payload.WindowSec,
			Nodes: nodesByTenant[t], Edges: edgesByTenant[t],
		}}
	}
	return out
}

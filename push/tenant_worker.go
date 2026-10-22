// Per-tenant push workers: ONE worker per active tenant, all sharing the
// single outbound WebSocket connection (connworker.go) — "single agent per
// cluster, a thread per tenant" (docs/todo/virtual-apps.md multi-tenant-push
// / cluster-mode addenda). Before this file, every tenant's frames funneled
// through one shared send channel owned by connWorker (tenant_routing.go's
// enqueueOK called connWorker.enqueueOK directly) — a single FIFO, so an
// event storm on one tenant could delay or starve a sibling tenant's frames
// sitting behind it in the same queue, and there was no per-tenant
// visibility into how much a given tenant was sending or dropping.
//
// "Worker" here means an isolated per-tenant QUEUE + counters (tenantWorker),
// not a dedicated goroutine: gorilla/websocket forbids concurrent writes to
// one *websocket.Conn, so whichever design is chosen, the actual bytes-on-
// the-wire write has to be serialized through exactly one goroutine —
// connWorker's existing writeLoop. Giving every tenant its own writer
// goroutine would just mean N goroutines contending for that same
// serialization point, buying nothing over N passive queues drained by ONE
// fair scheduler (tenantMux), while adding a teardown handshake per tenant.
// This also makes "no goroutine leaks" trivial: there is no per-tenant
// goroutine to leak, and retiring a tenant is a synchronous map/slice
// mutation under tenantMux.mu, not a cancel-and-join.
//
// Lock ordering: tenantMux.mu is the ONLY lock in this file — it guards the
// worker registry (the map, the round-robin order/pos) AND every worker's
// queue/counters. tenantWorker itself holds no lock of its own, so there is
// no nested-lock ordering to get wrong here. Every tenantMux method takes mu
// at most once per call and never calls back into Pusher/dynamicScope/etc.
// while holding it (only plain map/slice mutation and, after releasing mu, a
// non-blocking channel send), so this file cannot deadlock against
// Pusher.workerMu or dynamicScopeSet.mu regardless of what order a caller
// takes those relative to mux()/WorkerStats().
//
// Two connection-lifecycle behaviors are carried over unchanged from the
// pre-refactor single-channel design, both load-bearing for existing tests:
//
//   - everConnected is a one-way latch, exactly like the old "sendC == nil"
//     check was: false only until this Pusher's very first successful dial,
//     then true forever after — even across a later disconnect. So
//     Pusher.SendTopologySnapshot's bool return ("did this reach a live
//     connection") really answers "has this Pusher EVER connected", which is
//     exactly what TestPusher_SendTopologySnapshot_DisconnectedIsNoop
//     exercises (a Pusher on which runOnce/Start was never even called).
//   - beginConnection resets (drains) every tenant about to receive a fresh
//     replay, exactly like the old "w.sendC = make(chan *Frame, 4096)"
//     discarded whatever was still sitting in the previous connection's
//     channel — a reconnect's full resnapshot makes any pre-reconnect queued
//     delta moot, so carrying it forward into the new connection would only
//     replay stale state ahead of (or interleaved with) the fresh sequence.
package push

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
)

// tenantWorkerQueueSize bounds one tenant's outbound queue. Matches the
// pre-refactor single shared send channel's capacity (connWorker.runOnce
// used to size sendC at 4096), so a deployment with exactly one active
// tenant — every single-tenant deployment, and a cluster-mode deployment
// with exactly one active secondary — sees identical backpressure/capacity
// characteristics to before this refactor.
const tenantWorkerQueueSize = 4096

// TenantWorkerStats is a point-in-time snapshot of one tenant's outbound
// state on the shared connection, returned by Pusher.WorkerStats — the
// per-tenant observability surface this refactor adds. There is no existing
// HTTP status or metrics surface exporting push internals in this repo (see
// pusher.go), so this is exposed as a plain method rather than a new HTTP
// route; a caller wanting it on a status page or metrics surface wires it in
// from here.
type TenantWorkerStats struct {
	Tenant string
	Queued int
	// SentByKind counts frames actually written to the wire from this
	// tenant's queue, by Frame.Kind. It does NOT include the connect-time
	// snapshot replay (hello + the initial/reconnect snapshot chunks), which
	// is written directly to the socket outside any queue (replayTenant) —
	// LastSnapshot below covers that path; only frames that flowed through
	// the mux (deltas, metrics, a live-join snapshot sequence) are counted
	// here.
	SentByKind map[string]uint64
	// Dropped is every frame this tenant's queue ever evicted to make room,
	// INCLUDING the Coalesced subset below.
	Dropped uint64
	// Coalesced is the subset of Dropped evicted because a fresh snapshot
	// sequence superseded them, rather than a plain drop-oldest eviction.
	Coalesced uint64
	// LastSnapshot is when this tenant last completed a full snapshot ->
	// snapshot_end sequence, whichever path it went out on (connect-time
	// direct replay or a live-join queued sequence). Zero if never.
	LastSnapshot time.Time
}

// tenantWorker owns one tenant's outbound queue and counters. It has no lock
// of its own — every field here is only ever touched while the owning
// tenantMux's mu is held (see this file's package doc, "Lock ordering").
type tenantWorker struct {
	tenant       string
	queue        []*Frame // bounded FIFO; oldest at index 0
	sentByKind   map[string]uint64
	dropped      uint64
	coalesced    uint64
	lastSnapshot time.Time
	// resyncPending is true from the moment a core-triggered resync
	// (Pusher.handleResyncFrame) starts building/enqueueing this tenant's
	// snapshot sequence until that sequence's KindSnapshotEnd is actually
	// popped onto the wire (see next()) — the coalescing gate that keeps a
	// second resync request arriving while one is still queued or being
	// drained from enqueueing a duplicate sequence.
	resyncPending bool
}

func newTenantWorker(tenant string) *tenantWorker {
	return &tenantWorker{tenant: tenant, sentByKind: make(map[string]uint64)}
}

// isSnapshotFrame reports whether f is part of a full-resync sequence (a
// KindSnapshot chunk or its terminating KindSnapshotEnd) — the frame kind
// the overflow policy below treats as superseding a currently-queued delta.
func isSnapshotFrame(f *Frame) bool {
	return f.Kind == KindSnapshot || f.Kind == KindSnapshotEnd
}

// enqueueLocked applies the queue-overflow policy and appends f. Called with
// the owning tenantMux's mu held.
//
// Overflow policy: a fresh snapshot-sequence frame arriving at a full queue
// coalesces by evicting every currently-queued NON-snapshot frame first — a
// full resync makes those deltas moot — before falling back to plain
// drop-oldest if that alone didn't free enough room (e.g. the queue is
// already full of not-yet-sent snapshot chunks, which nothing here
// supersedes but a NEWER snapshot would). Any other incoming frame kind
// drop-oldest straight away. The incoming frame is always ultimately
// queued — there is always room after eviction, since capacity is >= 1.
func (w *tenantWorker) enqueueLocked(f *Frame) {
	if len(w.queue) >= tenantWorkerQueueSize {
		if isSnapshotFrame(f) {
			kept := w.queue[:0]
			for _, q := range w.queue {
				if isSnapshotFrame(q) {
					kept = append(kept, q)
				} else {
					w.coalesced++
					w.dropped++
				}
			}
			w.queue = kept
		}
		for len(w.queue) >= tenantWorkerQueueSize {
			w.queue = w.queue[1:]
			w.dropped++
		}
	}
	w.queue = append(w.queue, f)
}

// tenantMux is the shared registry of active tenantWorkers for one Pusher's
// whole lifetime — spanning reconnects, unlike the old connWorker.sendC,
// which was recreated (and its contents discarded) on every dial. It also
// implements the fair scheduler: round-robin draining across every
// registered tenant's queue, so one tenant's event storm delays a sibling
// tenant's frame by at most one full pass over the active tenant set, never
// indefinitely.
type tenantMux struct {
	log logr.Logger
	now func() time.Time

	mu      sync.Mutex
	workers map[string]*tenantWorker
	order   []string // round-robin visiting order; getOrCreateLocked appends new tenants
	pos     int

	// everConnected latches true on this Pusher's first successful dial and
	// never resets — see this file's package doc.
	everConnected bool

	// wake is signalled (non-blocking, capacity 1) on every accepted
	// enqueue, so connWorker.writeLoop can block on it between drain passes
	// instead of busy-polling.
	wake chan struct{}
}

func newTenantMux(log logr.Logger, now func() time.Time) *tenantMux {
	if now == nil {
		now = time.Now
	}
	return &tenantMux{
		log:     log,
		now:     now,
		workers: make(map[string]*tenantWorker),
		wake:    make(chan struct{}, 1),
	}
}

// getOrCreateLocked returns tenant's worker, creating it (and registering it
// into the round-robin order) on first use — "created lazily on first frame
// for a tenant". Called with mu held.
func (m *tenantMux) getOrCreateLocked(tenant string) *tenantWorker {
	if w, ok := m.workers[tenant]; ok {
		return w
	}
	w := newTenantWorker(tenant)
	m.workers[tenant] = w
	m.order = append(m.order, tenant)
	return w
}

// tryStartResync marks tenant as having a core-triggered resync in flight,
// returning false without changing anything when one is already pending —
// the coalescing gate Pusher.handleResyncFrame calls before building a fresh
// snapshot sequence, so a resync arriving while the previous one is still
// queued or being drained onto the wire is dropped rather than duplicated.
// Creates tenant's worker lazily like any other first frame for a tenant.
func (m *tenantMux) tryStartResync(tenant string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.getOrCreateLocked(tenant)
	if w.resyncPending {
		return false
	}
	w.resyncPending = true
	return true
}

// enqueue offers f onto tenant's queue. Returns false without touching
// anything when this Pusher has never yet completed a connection — the
// latch mirrors the pre-refactor "sendC == nil" check exactly (see this
// file's package doc). Once true, enqueue always succeeds (the overflow
// policy guarantees room), matching the pre-refactor channel's behavior of
// continuing to accept into a buffered channel regardless of the CURRENT
// connection state once it had ever been non-nil.
func (m *tenantMux) enqueue(tenant string, f *Frame) bool {
	m.mu.Lock()
	if !m.everConnected {
		m.mu.Unlock()
		m.log.V(1).Info("push disconnected; dropping frame (state resyncs on next connect/tick)",
			"kind", f.Kind, "tenant", tenant)
		return false
	}
	w := m.getOrCreateLocked(tenant)
	before := w.dropped
	w.enqueueLocked(f)
	if w.dropped > before {
		n := w.dropped
		if n == 1 || n%1000 == 0 {
			m.log.Info("tenant send queue full; dropping frame",
				"tenant", tenant, "kind", f.Kind, "droppedOnTenant", n)
		}
	}
	m.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return true
}

// next pops the next frame to send, round-robin across every registered
// tenant with something queued — at most one frame per tenant per call, so a
// tenant with a deep backlog never delays a sibling tenant behind it in the
// rotation by more than one full pass over the active set. Updates the
// popped frame's tenant's sent-by-kind counter and, for a KindSnapshotEnd
// frame, its LastSnapshot timestamp — this is the ONE place a queued (as
// opposed to connect-time direct-replay) snapshot sequence's completion is
// recorded.
func (m *tenantMux) next() (*Frame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.order)
	for i := 0; i < n; i++ {
		idx := (m.pos + i) % n
		tenant := m.order[idx]
		w := m.workers[tenant]
		if w != nil && len(w.queue) > 0 {
			f := w.queue[0]
			w.queue = w.queue[1:]
			w.sentByKind[f.Kind]++
			if f.Kind == KindSnapshotEnd {
				w.lastSnapshot = m.now()
				w.resyncPending = false
			}
			m.pos = idx + 1
			return f, true
		}
	}
	return nil, false
}

// beginConnection is called once per successful dial (connWorker.runOnce),
// before any frame is written on the new connection. It latches
// everConnected, ensures a worker exists for every tenant this connection is
// about to replay a snapshot for, and DRAINS each of those workers' queues —
// a reconnect's fresh replay makes anything queued before it moot, mirroring
// the pre-refactor "fresh sendC channel per connection" discard. Tenants
// outside tenants (e.g. one mid-retirement, or one that will be lazily
// created later via a live namespace-scope join) are left untouched.
func (m *tenantMux) beginConnection(tenants []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.everConnected = true
	for _, t := range tenants {
		w := m.getOrCreateLocked(t)
		w.queue = nil
	}
	m.pos = 0
}

// replayTenant sends tenant's full snapshot sequence directly on conn (via
// Pusher.sendSnapshotForTenant, unchanged) and records the completion on
// tenant's worker. connWorker.runOnce's connect-time replay loop calls this
// once for the primary tenant (when set) and once per active secondary, in
// the exact order it always has — see connworker.go; this function only
// wraps that existing call with the per-worker bookkeeping, it does not
// change what gets built or the order it is sent in.
func (m *tenantMux) replayTenant(ctx context.Context, conn *websocket.Conn, p *Pusher, tenant string) error {
	if err := p.sendSnapshotForTenant(ctx, conn, tenant); err != nil {
		return err
	}
	m.mu.Lock()
	m.getOrCreateLocked(tenant).lastSnapshot = m.now()
	m.mu.Unlock()
	return nil
}

// staleTenants returns every currently-registered tenant NOT present in
// active — retirement candidates for reconcileTenants (connworker.go).
func (m *tenantMux) staleTenants(active []string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keep := make(map[string]bool, len(active))
	for _, t := range active {
		keep[t] = true
	}
	var stale []string
	for _, t := range m.order {
		if !keep[t] {
			stale = append(stale, t)
		}
	}
	return stale
}

// nextFor pops the next queued frame for exactly one tenant, ignoring round
// robin — used only to flush a retiring tenant's residual queue immediately
// before it is removed (retire below), so nothing already accepted onto the
// queue is silently discarded without at least being offered to the wire.
func (m *tenantMux) nextFor(tenant string) (*Frame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workers[tenant]
	if !ok || len(w.queue) == 0 {
		return nil, false
	}
	f := w.queue[0]
	w.queue = w.queue[1:]
	w.sentByKind[f.Kind]++
	return f, true
}

// retire removes tenant from the registry — "retired when its tenant loses
// all scoped namespaces" — logging a summary of its lifetime counters. The
// caller (connWorker.reconcileTenants) is expected to have already drained
// the worker's queue via nextFor.
func (m *tenantMux) retire(tenant string) {
	m.mu.Lock()
	w, ok := m.workers[tenant]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.workers, tenant)
	for i, t := range m.order {
		if t == tenant {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
	m.log.Info("push tenant worker retired (namespace scope emptied)",
		"tenant", tenant, "sentByKind", w.sentByKind, "dropped", w.dropped,
		"coalesced", w.coalesced, "lastSnapshot", w.lastSnapshot)
}

// snapshot returns a tenant-sorted copy of every registered tenant's current
// stats — Pusher.WorkerStats' implementation.
func (m *tenantMux) snapshot() []TenantWorkerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TenantWorkerStats, 0, len(m.workers))
	for tenant, w := range m.workers {
		sent := make(map[string]uint64, len(w.sentByKind))
		for k, v := range w.sentByKind {
			sent[k] = v
		}
		out = append(out, TenantWorkerStats{
			Tenant:       tenant,
			Queued:       len(w.queue),
			SentByKind:   sent,
			Dropped:      w.dropped,
			Coalesced:    w.coalesced,
			LastSnapshot: w.lastSnapshot,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tenant < out[j].Tenant })
	return out
}

// mux returns (creating on first call) the Pusher's tenant-worker registry —
// the per-tenant queues + fair scheduler multiplexed onto the single
// connection. Every enqueue path (informer handlers via Pusher.enqueueOK,
// collector SendXxx calls, sendTenantSnapshotOnLive) goes through this.
// Lazy and mutex-guarded like primaryWorker (connworker.go), so it works
// whether or not New() was used to construct the Pusher (bare struct
// literals in tests get a fresh mux on first access).
func (p *Pusher) mux() *tenantMux {
	p.muxMu.Lock()
	defer p.muxMu.Unlock()
	if p.tenantMux == nil {
		p.tenantMux = newTenantMux(p.log, p.opts.now)
	}
	return p.tenantMux
}

// WorkerStats returns a point-in-time snapshot of every currently-registered
// tenant worker's counters — the observability surface for the per-tenant
// push refactor (see this file's package doc). nil-safe: a nil *Pusher
// (push mode disabled) returns nil.
func (p *Pusher) WorkerStats() []TenantWorkerStats {
	if p == nil {
		return nil
	}
	return p.mux().snapshot()
}

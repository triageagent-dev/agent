// connWorker: the ONE outbound WebSocket connection to Endpoint,
// authenticated as this agent's primary tenant (Options.Tenant) when one is
// configured — or, in cluster-mode (Options.Tenant == "", addendum
// 2026-08-27), identified only by Options.ClusterName, with an empty tenant
// declared in the hello frame.
//
// Before this file, *Pusher* itself owned the connection's state (sendC,
// droppedFrames, rejected) and its reconnect/dial/stream loop (Start's
// backoff loop, runOnce, writeLoop, readLoop). connWorker is that
// per-connection state and loop, lifted out of Pusher essentially unchanged
// (same dial/hello/snapshot/stream/reconnect structure, same backoff
// schedule). Pusher.runOnce is kept as a thin delegate to the worker so
// every pre-existing single-connection call site and test is unaffected.
//
// Multi-tenant push rides this SAME single connection: an earlier iteration
// of the multi-tenant design ran one connWorker per active tenant (N
// sockets, each with its own signed hello), which this reverts — the
// dial/hello/reconnect loop below always authenticates as the primary
// tenant (or, cluster-mode, as no tenant at all), and every other tenant's
// frames travel on the same socket with a per-frame Tenant envelope stamp
// instead. Which tenant a given frame belongs to (and the deployment-order
// constraint the stamping model imposes) lives in tenant_routing.go, not
// here — this file only knows how to run the one connection.
//
// PER-TENANT WORKERS (tenant_worker.go, 2026-08-27): the single shared send
// channel this file used to own (sendC, droppedFrames) has moved into a
// *tenantMux — one isolated queue per tenant instead of one shared FIFO for
// all of them — but this is still ONE socket, ONE handshake, ONE writer
// goroutine (writeLoop). Nothing above changes: connWorker still owns
// exactly one WebSocket connection and its dial/backoff/reconnect loop; it
// now asks the Pusher-level tenantMux to replay each active tenant's
// snapshot sequence and to fairly drain their queues onto that one
// connection, instead of writing everything through a single sendC.
package push

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
)

// connWorker owns the single outbound WebSocket connection, created lazily
// by Pusher.primaryWorker (from Start, or directly by tests driving
// Pusher.runOnce) and living for the Pusher's whole lifetime — its run loop's
// lifetime is the Start context itself, so it needs no cancel/teardown
// machinery of its own.
type connWorker struct {
	p   *Pusher
	log logr.Logger

	// rejected is set when the hub closes with a policy-violation (auth)
	// code, so run's reconnect loop backs off to the maximum instead of
	// retrying with a certificate or CA the core will not accept.
	rejected atomic.Bool
}

func newConnWorker(p *Pusher) *connWorker {
	return &connWorker{
		p:   p,
		log: p.log,
	}
}

// run dials, streams, and reconnects with capped exponential backoff until
// ctx is cancelled — the same loop Pusher.Start ran inline before this file
// existed.
func (w *connWorker) run(ctx context.Context) {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := w.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		// A dropped or refused connection is a routine, self-healing condition
		// (the triage agent may be restarting or briefly unreachable). Log it at
		// a low level without a stack trace and keep retrying with capped
		// backoff; only a genuinely unexpected error is worth a louder line.
		if err != nil {
			w.log.V(1).Info("push connection ended; will reconnect",
				"reason", err.Error(), "backoff", backoff.String())
		}
		// A connection that actually established resets the backoff, so a
		// long-lived stream that finally drops reconnects promptly rather than
		// at max backoff.
		if connected {
			backoff = time.Second
		}
		// An auth rejection (policy-violation close) is a config problem that
		// will not fix itself by retrying quickly — go straight to max backoff
		// so a misconfigured agent probes gently rather than hammering the
		// receiver.
		if w.rejected.Swap(false) {
			backoff = maxBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// maxInboundReadBytes bounds a single inbound WebSocket message on the push
// connection. Everything this agent ever reads from the hub side is
// control-frame scale (an occasional pong, a close frame) — gorilla's
// default read limit is 0 (unlimited), so without this a misbehaving or
// malicious peer could otherwise make readLoop buffer an arbitrarily large
// message.
// AgentNonceHeader carries the agent's per-connection random nonce (32 bytes,
// base64.StdEncoding) on the WebSocket upgrade request. A core
// that supports challenge binding echoes it in the challenge and signs it.
const AgentNonceHeader = "X-Triage-Agent-Nonce"

const maxInboundReadBytes = 512 * 1024 // 512KB

// runOnce dials, sends hello + the snapshot sequences, then streams deltas
// until the connection or context ends. A fresh send channel per connection
// ensures a reconnect re-snapshots cleanly. It returns whether the
// connection was successfully established (used to reset reconnect backoff)
// alongside any error.
func (w *connWorker) runOnce(ctx context.Context) (connected bool, err error) {
	p := w.p
	tenant := p.opts.Tenant

	dialer := websocket.DefaultDialer
	if p.opts.InsecureTLS {
		dialer = &websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	}

	nowFn := p.opts.now
	if nowFn == nil {
		nowFn = time.Now
	}
	// The agent nonce rides the upgrade request so a core that supports
	// challenge binding echoes it in the challenge and signs it.
	agentNonce := make([]byte, 32)
	if _, err := rand.Read(agentNonce); err != nil {
		return false, fmt.Errorf("agent nonce: %w", err)
	}
	hdr := http.Header{}
	hdr.Set(AgentNonceHeader, base64.StdEncoding.EncodeToString(agentNonce))

	conn, _, err := dialer.DialContext(ctx, p.opts.Endpoint, hdr)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()
	conn.SetReadLimit(maxInboundReadBytes)

	// Agent join: the core speaks first; answer its challenge with a
	// certificate hello, or with a join or renewal that ends this connection.
	certHello, err := w.joinHandshake(conn, tenant, nowFn(), joinBinding{sentNonce: agentNonce, ekm: channelBindingMaterial(conn), insecureTLS: p.opts.InsecureTLS})
	if err != nil {
		return false, err
	}
	connected = true
	p.connected.Store(true)
	defer p.connected.Store(false)

	// A half-dead peer (network partition, silently killed process) is otherwise
	// only detected once TCP send buffers fill, which can take a long time on a
	// low-traffic stream. Prime a read deadline now and extend it on every pong /
	// app-level read so a genuinely unresponsive peer trips it well before that.
	readTimeout := 3 * p.opts.Heartbeat
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The certificate is renewed only on a connect, so a session that outlives
	// it is ended when the renewal comes due and the reconnect renews it; left
	// alone, the certificate would expire under a long session and the next
	// reconnect would have nothing to renew.
	if id, err := p.joinIdentity(); err == nil {
		if due := id.renewDue(nowFn(), p.opts.ClusterName, p.joinTenants()); !due.IsZero() {
			renewTimer := time.AfterFunc(due.Sub(nowFn()), func() {
				w.log.Info("agent certificate renewal is due; reconnecting to renew it", "cluster", p.opts.ClusterName)
				cancel()
			})
			defer renewTimer.Stop()
		}
	}

	// Any cancellation of connCtx must unblock readLoop's blocked
	// conn.ReadMessage() immediately, rather than leaving it to wait out the
	// ~3*Heartbeat read deadline primed above — see the identical reasoning
	// this carried on Pusher.runOnce before this file existed (BUG 2, AUDIT.md).
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	// mux is the Pusher-level tenant-worker registry (tenant_worker.go),
	// spanning reconnects — unlike the old sendC, which was thrown away and
	// recreated here on every dial. beginConnection is this connection's
	// equivalent of that discard: it latches the mux "ever connected" and
	// drains every tenant about to be replayed below, so nothing queued
	// before this reconnect leaks into the fresh sequence.
	mux := p.mux()
	mux.beginConnection(p.currentActiveTenants())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.readLoop(connCtx, cancel, conn, readTimeout) }()

	// Hello + snapshots before streaming deltas. The hello is the certificate
	// hello the join handshake built, so the receiver can verify + route. The
	// hello always declares the PRIMARY tenant (tenant, i.e. Options.Tenant):
	// this is the connection's session tenant, and secondary tenants exist
	// only as per-frame Tenant stamps on this same socket (tenant_routing.go).
	// In cluster-mode tenant is "" and there IS no session tenant: every frame below, including the
	// snapshot sequences, must carry its own explicit stamp (see the
	// tenant != "" guard below).
	var extraCaps []string
	if p.opts.LogMetrics {
		extraCaps = append(extraCaps, "log_metrics")
	}
	if p.opts.RedMetrics {
		extraCaps = append(extraCaps, "red_metrics")
	}
	if p.opts.UseMetrics {
		extraCaps = append(extraCaps, "use_metrics")
	}
	if p.opts.Topology {
		extraCaps = append(extraCaps, "topology")
	}
	if p.opts.SensorStream {
		extraCaps = append(extraCaps, "sensor_state")
	}
	if p.opts.Host {
		extraCaps = append(extraCaps, "host")
	}
	hello := certHello(extraCaps...)
	if err := p.writeFrame(conn, hello); err != nil {
		cancel()
		wg.Wait()
		return connected, err
	}
	// The core sends its limits frame right after the hello when a binding
	// names namespaces; none within the grace means a restored scope is stale.
	stopGrace := p.scope.armGrace(func() {
		w.log.Info("no limits frame from the core; the restored workspace scope is lifted")
		w.saveScope(false)
	})
	defer stopGrace()
	// Primary tenant's snapshot first, exactly as a single-tenant connection
	// always sent it (chunks + snapshot_end, no Tenant stamp) — only when
	// this agent actually HAS a primary (tenant != "", i.e. Options.Tenant
	// set). Cluster-mode (tenant == "") has no primary and therefore sends
	// no unstamped sequence at all: every tenant currently holding a
	// dynamically-scoped namespace gets its own stamped sequence below
	// (activeSecondaryTenants returns ALL of them in that mode, not just
	// non-primary ones — see its doc comment), and a namespace with no
	// triageagent.dev/tenant mapping is dropped rather than folded into an invented
	// "default" sequence (tenant_routing.go). Either way every (re)connect
	// re-syncs EVERY tenant's full state on this one socket.
	if tenant != "" {
		if err := mux.replayTenant(connCtx, conn, p, tenant); err != nil {
			cancel()
			wg.Wait()
			return connected, err
		}
	}
	for _, secondary := range p.activeSecondaryTenants() {
		if err := mux.replayTenant(connCtx, conn, p, secondary); err != nil {
			cancel()
			wg.Wait()
			return connected, err
		}
	}

	err = w.writeLoop(connCtx, conn, mux)
	cancel()
	wg.Wait()
	return connected, err
}

// writeLoop is the ONE goroutine that ever writes to conn (gorilla/websocket
// forbids concurrent writes) — it fairly drains every active tenant's queue
// (tenant_worker.go's round-robin mux.next) and emits heartbeats until the
// context ends. Frames enqueued while a drain pass is already in progress
// are picked up by that same pass (next() re-scans every tenant each call);
// mux.wake only matters for waking this loop back up once a drain pass finds
// nothing left.
func (w *connWorker) writeLoop(ctx context.Context, conn *websocket.Conn, mux *tenantMux) error {
	p := w.p
	ticker := time.NewTicker(p.opts.Heartbeat)
	defer ticker.Stop()
	for {
		for {
			f, ok := mux.next()
			if !ok {
				break
			}
			if err := p.writeFrame(conn, f); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-mux.wake:
			// A tenant queue has something new — loop back to the drain above.
		case <-ticker.C:
			// Retirement is eventually consistent on this same cadence: a
			// tenant that lost every scoped namespace since the last tick is
			// flushed (any residual queued frames still reach the wire) and
			// removed from the registry here, rather than synchronously from
			// the namespace-informer callback (namespace_scope.go) — keeping
			// tenant lifecycle management inside connworker.go/tenant_worker.go.
			if err := w.reconcileTenants(conn, mux); err != nil {
				return err
			}
			// WebSocket ping control frame first: LBs and proxies reset their idle
			// timer on ping/pong, but many ignore data (text) frames — a text-only
			// heartbeat gets the connection reaped mid-stream (1006) at the LB's
			// idle boundary. The text heartbeat still follows for the app-level pong.
			if err := p.writePing(conn); err != nil {
				return err
			}
			if err := p.writeFrame(conn, newHeartbeat(p.opts.ClusterName)); err != nil {
				return err
			}
			// sensor_state is session-level like heartbeat above: the latest
			// report (SendSensorState) goes out unstamped once per tick, never
			// split or duplicated per tenant (docs/adr/0015-sensor-stream-grpc.md).
			if f := p.sensorState.Swap(nil); f != nil {
				if err := p.writeFrame(conn, f); err != nil {
					return err
				}
			}
		}
	}
}

// reconcileTenants retires every tenant worker whose tenant no longer holds
// a scoped namespace (mux.staleTenants against the CURRENT active set),
// flushing each one's residual queue to the wire first so a namespace's last
// few deltas/deletes (emitAppDeletesForNamespace, namespace_scope.go) are not
// dropped just because the round-robin scheduler hadn't reached them yet.
func (w *connWorker) reconcileTenants(conn *websocket.Conn, mux *tenantMux) error {
	p := w.p
	for _, tenant := range mux.staleTenants(p.currentActiveTenants()) {
		for {
			f, ok := mux.nextFor(tenant)
			if !ok {
				break
			}
			if err := p.writeFrame(conn, f); err != nil {
				return err
			}
		}
		mux.retire(tenant)
	}
	return nil
}

// readLoop consumes hub->agent frames (pong, resync). Any read error ends
// the connection. If the hub closed with a policy-violation code (auth
// rejection), it records that so run's reconnect loop backs off hard
// instead of hammering with a certificate the core refuses.
func (w *connWorker) readLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, readTimeout time.Duration) {
	defer cancel()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				// conn was closed because ctx was cancelled, not because the
				// peer sent anything — nothing more to report.
				return
			}
			if ce, ok := err.(*websocket.CloseError); ok && ce.Code == websocket.ClosePolicyViolation {
				// Revoked, expired or no longer approved: the certificate
				// cannot open a session again, so ask to join afresh with
				// the same key on the next attempt.
				if id, ierr := w.p.joinIdentity(); ierr == nil {
					id.dropCert()
				}
				w.log.Error(nil, "the core refused the agent certificate; asking to join again", "reason", ce.Text)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		f, err := decode(data)
		if err != nil {
			w.log.V(2).Info("ignoring undecodable hub frame")
			continue
		}
		switch f.Kind {
		case KindResync:
			w.p.handleResyncFrame(f)
		case KindLimits:
			if f.Limits != nil {
				w.p.eventCaps.set(f.Limits.EventsPerDay, time.Now())
				w.p.scope.set(f.Limits.Namespaces)
				w.saveScope(false)
				w.log.V(1).Info("workspace namespace scope from the core", "namespaces", f.Limits.Namespaces)
				w.log.V(1).Info("event caps from the core", "eventsPerDay", f.Limits.EventsPerDay)
			}
		default:
			w.log.V(2).Info("ignoring hub frame", "kind", f.Kind)
		}
	}
}

// restoreScope installs the workspace scope the join store holds, so the
// snapshots sent before the core's first limits frame already obey it.
func (w *connWorker) restoreScope(id *joinIdentity) {
	if !w.p.scope.needsRestore() {
		return
	}
	m, raw, err := id.loadScope()
	if err != nil {
		w.log.Error(err, "could not read the workspace scope from the join store")
		return
	}
	w.p.scope.restore(m)
	w.p.scope.markSaved(raw)
	if len(m) > 0 {
		w.log.Info("workspace namespace scope restored from the join store, kept until the core confirms it", "namespaces", m)
	}
}

// saveScope keeps the current workspace scope in the join store when it
// changed, or always with force. A failure is logged: the scope still holds
// for this process.
func (w *connWorker) saveScope(force bool) {
	data, changed := w.p.scope.encoded()
	if !changed && !force {
		return
	}
	id, err := w.p.joinIdentity()
	if err != nil {
		return
	}
	if err := id.saveScope(data); err != nil {
		w.log.Error(err, "could not save the workspace scope in the join store")
		return
	}
	w.p.scope.markSaved(data)
}

// joinHandshake reads the core's challenge and checks it. With a usable
// certificate it returns the hello builder for this connection. Otherwise it
// sends a join (or a renewal when the certificate is due) and returns an
// error, since the core closes the connection after answering: errJoinIssued
// to reconnect with the new certificate, errJoinPending while an admin has
// not approved, or the refusal.
func (w *connWorker) joinHandshake(conn *websocket.Conn, tenant string, now time.Time, bind joinBinding) (func(caps ...string) *Frame, error) {
	p := w.p
	id, err := p.joinIdentity()
	if err != nil {
		return nil, err
	}
	cluster := p.opts.ClusterName
	_ = conn.SetReadDeadline(now.Add(30 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read core challenge: %w", err)
	}
	f, err := decode(data)
	if err != nil || f.Kind != KindChallenge || f.Challenge == nil {
		w.rejected.Store(true)
		return nil, errors.New("the core sent no challenge; enable agent join on the core (clusterAgents.join.caSecret)")
	}
	if err := id.verifyChallenge(f.Challenge, bind); err != nil {
		w.rejected.Store(true)
		return nil, fmt.Errorf("the core failed the CA check, not connecting: %w", err)
	}
	nonce := f.Challenge.Nonce
	tenants := p.joinTenants()

	cert, usable, renew := id.certState(now, cluster, tenants)
	if usable && !renew {
		w.restoreScope(id)
		proof := id.sign(joinProofMessage("hello", nonce, cluster, tenant))
		return func(caps ...string) *Frame {
			return newCertHello(cluster, tenant, p.opts.AgentVersion, cert, proof, p.declaredNamespaces(), caps...)
		}, nil
	}

	csr, err := newJoinCSR(id.key, cluster, tenants)
	if err != nil {
		return nil, err
	}
	req := &Frame{V: ProtocolVersion, Kind: KindJoin, Cluster: cluster, Join: &JoinPayload{
		CSR: csr, Proof: id.sign(joinProofMessage("join", nonce, joinCSRHash(csr))),
	}}
	// A certificate to renew is a usable one near its expiry, or one that
	// expired within the grace while the agent stayed connected on it: the
	// core renews either, and the one-time key, spent at the first join, is
	// offered only when there is no certificate to renew.
	renewal := len(cert) > 0
	if renewal {
		req.Kind = KindRenew
		req.Join.Cert = cert
		req.Join.Proof = id.sign(joinProofMessage("renew", nonce, joinCSRHash(csr)))
	} else {
		req.Join.EnrollKey = id.enrollKey()
	}
	if err := p.writeFrame(conn, req); err != nil {
		return nil, err
	}
	_, data, err = conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read join result: %w", err)
	}
	res, err := decode(data)
	if err != nil || res.Kind != KindJoinResult || res.JoinResult == nil {
		return nil, errors.New("the core sent no join result")
	}
	switch res.JoinResult.Status {
	case JoinIssued:
		if err := id.validateIssued(res.JoinResult.Cert, now, cluster, tenants); err != nil {
			w.rejected.Store(true)
			return nil, fmt.Errorf("the core issued an unusable certificate, not saving it: %w", err)
		}
		if err := id.setCert(res.JoinResult.Cert); err != nil {
			return nil, err
		}
		if req.Join.EnrollKey != "" {
			if err := id.dropEnrollKey(); err != nil {
				w.log.Error(err, "could not remove the spent agent key from the join store")
			}
			w.p.scope.seed(res.JoinResult.Bindings)
			w.saveScope(true) // a new identity: whatever the store holds is not its scope
			w.log.Info("agent certificate issued for its one-time key", "cluster", cluster, "tenants", tenants, "bindings", res.JoinResult.Bindings)
			return nil, errJoinIssued
		}
		w.log.Info("agent certificate issued", "cluster", cluster, "tenants", tenants, "renewal", renewal)
		return nil, errJoinIssued
	case JoinPending:
		w.log.Info("join pending: approve this fingerprint in the core (sources panel, or POST /v2/agents/approve)",
			"cluster", cluster, "fingerprint", id.fingerprint(), "tenants", tenants)
		return nil, errJoinPending
	default:
		if renewal {
			// A refused renewal (revoked, past the grace, or tenants changed):
			// join afresh.
			id.dropCert()
		} else {
			w.rejected.Store(true)
		}
		w.log.Error(nil, "join refused by the core", "cluster", cluster, "reason", res.JoinResult.Message, "withKey", req.Join.EnrollKey != "")
		return nil, fmt.Errorf("join refused: %s", res.JoinResult.Message)
	}
}

// primaryWorker returns (creating on first call) the Pusher's single
// connection worker. Both Start and every single-connection test call site
// (via Pusher.runOnce) go through this, so there is exactly one code path
// that creates it.
func (p *Pusher) primaryWorker() *connWorker {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()
	if p.worker == nil {
		p.worker = newConnWorker(p)
	}
	return p.worker
}

// channelBindingMaterial derives the join channel-binding exporter keying
// material from the agent's side of conn; nil when conn is not TLS or the
// export fails.
func channelBindingMaterial(conn *websocket.Conn) []byte {
	tc, ok := conn.NetConn().(*tls.Conn)
	if !ok {
		return nil
	}
	st := tc.ConnectionState()
	ekm, err := st.ExportKeyingMaterial(ChannelBindingLabel, nil, ChannelBindingLen)
	if err != nil {
		return nil
	}
	return ekm
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package mesh is the replica-to-replica gossip between controller replicas
// (docs/adr/0015-sensor-stream-grpc.md, phase 4). A sensor holds its stream
// to whichever replica the Service picked, so each replica sends the sensor
// messages it receives to every peer, and merges what its peers send into
// its own sensor state. Whichever replica is the leader then sees every
// node, and a newly elected one already holds them.
//
// The transport is a WebSocket per peer over mutual TLS. Every replica dials
// every peer it discovers through a headless Service and sends its own
// records on the connections it dialed; the connections it accepted only
// receive. A received record is applied and never forwarded again, so with
// every replica dialing every other one, one hop reaches the whole mesh.
//
// The peer transport follows the core repo's internal/gossip mesh; the frame
// carries one record kind, NodeSnapshot.
package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
)

// Path is the WebSocket path peers dial.
const Path = "/gossip/ws"

const (
	protocolVersion = 1
	kindHello       = "hello"
	kindNodes       = "nodes"
	kindUI          = "ui"

	// writeTimeout bounds one frame write to one peer.
	writeTimeout = 10 * time.Second
	// maxFrameBytes bounds one inbound frame: one sensor message (up to the
	// sensor stream's own 32 MiB limit) base64-encoded in JSON.
	maxFrameBytes = 48 << 20
	// peerSendBuffer is each connection's outbound frame queue. A full queue
	// drops the frame: the next tick of that sensor replaces it anyway.
	peerSendBuffer = 256

	defaultDialInterval = 15 * time.Second
	defaultReadDeadline = 90 * time.Second
	defaultPingInterval = 30 * time.Second
)

// NodeSnapshot is the one gossip record: one message a sensor sent on its
// stream, as received by the replica holding that stream.
type NodeSnapshot struct {
	// Node is the sensor's node.
	Node string `json:"node"`
	// Seq is the message's sequence number on the sensor's stream.
	Seq uint64 `json:"seq"`
	// At is when the replica holding the stream received the message.
	At time.Time `json:"at"`
	// Snapshot is the marshaled sensorpb.StreamRequest: a hello, one role's
	// snapshot or the sensor's state; with no body, the stream closed.
	Snapshot []byte `json:"snapshot"`
}

type frame struct {
	V      int            `json:"v"`
	Kind   string         `json:"kind"`
	Origin string         `json:"origin,omitempty"`
	Nodes  []NodeSnapshot `json:"nodes,omitempty"`
	// UI is an opaque payload for the local TUI (agentui): the leader's view,
	// which every other replica serves too.
	UI json.RawMessage `json:"ui,omitempty"`
}

func encode(f *frame) ([]byte, error) { return json.Marshal(f) }

// Config configures a Mesh.
type Config struct {
	// Self is this replica's name (its pod name), sent in the hello; a peer
	// naming the same is refused as a connection to itself.
	Self string
	// ListenAddr the mesh serves on, for example ":9471". Empty serves
	// nothing; Start still dials (tests mount Handler themselves).
	ListenAddr string
	// ServiceDNS is the headless Service name resolved for peer pod IPs.
	// Empty disables DNS discovery.
	ServiceDNS string
	// PeerPort is the port dialed on each discovered pod IP.
	PeerPort int
	// StaticPeers are peer URLs (wss://host:port/gossip/ws) dialed besides
	// the discovered ones.
	StaticPeers []string
	// TLS is required: every peer connection is mutual TLS.
	TLS *TLS
	// DialInterval is how often discovery runs and missing peers are
	// redialed. Default 15s.
	DialInterval time.Duration
	// LookupHost resolves ServiceDNS. Default net.LookupHost.
	LookupHost func(host string) ([]string, error)
	// Apply receives the records a peer sent. It must not publish them
	// again.
	Apply func([]NodeSnapshot)
	// Replay returns this replica's own current records, sent to each peer
	// when this replica connects to it.
	Replay func() []NodeSnapshot
	Log    logr.Logger
}

// Mesh is one replica's end of the gossip.
type Mesh struct {
	cfg Config
	log logr.Logger

	readDeadline time.Duration
	pingInterval time.Duration

	// peers holds the outbound connections by dialed address; a nil value
	// is a dial in flight.
	peersMu sync.Mutex
	peers   map[string]*peerConn

	inboundMu sync.Mutex
	inbound   map[*peerConn]struct{}

	uiMu    sync.RWMutex
	applyUI func([]byte)
}

// New returns a Mesh; Start runs it.
func New(cfg Config) *Mesh {
	if cfg.DialInterval <= 0 {
		cfg.DialInterval = defaultDialInterval
	}
	if cfg.LookupHost == nil {
		cfg.LookupHost = defaultLookupHost
	}
	return &Mesh{
		cfg:          cfg,
		log:          cfg.Log.WithName("mesh"),
		readDeadline: defaultReadDeadline,
		pingInterval: defaultPingInterval,
		peers:        map[string]*peerConn{},
		inbound:      map[*peerConn]struct{}{},
	}
}

// Publish sends rec to every peer this replica dialed. It never blocks: a
// peer whose queue is full misses rec.
func (m *Mesh) Publish(rec NodeSnapshot) {
	conns := m.outbound()
	if len(conns) == 0 {
		return
	}
	b, err := encode(&frame{V: protocolVersion, Kind: kindNodes, Origin: m.cfg.Self, Nodes: []NodeSnapshot{rec}})
	if err != nil {
		m.log.Error(err, "encode record", "node", rec.Node)
		return
	}
	for _, pc := range conns {
		if !pc.enqueue(b) {
			m.log.V(1).Info("peer queue full, record dropped", "peer", pc.getOrigin(), "node", rec.Node)
		}
	}
}

// SetApplyUI sets the function that receives the payloads a peer published
// with PublishUI. Safe to call after Start.
func (m *Mesh) SetApplyUI(fn func([]byte)) {
	m.uiMu.Lock()
	m.applyUI = fn
	m.uiMu.Unlock()
}

// PublishUI sends payload (JSON) to every peer this replica dialed. It never
// blocks: a peer whose queue is full misses it, and the next one replaces it.
func (m *Mesh) PublishUI(payload []byte) {
	conns := m.outbound()
	if len(conns) == 0 {
		return
	}
	b, err := encode(&frame{V: protocolVersion, Kind: kindUI, Origin: m.cfg.Self, UI: payload})
	if err != nil {
		m.log.Error(err, "encode ui payload")
		return
	}
	for _, pc := range conns {
		if !pc.enqueue(b) {
			m.log.V(1).Info("peer queue full, ui payload dropped", "peer", pc.getOrigin())
		}
	}
}

func (m *Mesh) outbound() []*peerConn {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	out := make([]*peerConn, 0, len(m.peers))
	for _, pc := range m.peers {
		if pc != nil {
			out = append(out, pc)
		}
	}
	return out
}

// Peers returns the names of the connected peers, both directions.
func (m *Mesh) Peers() []string {
	seen := map[string]struct{}{}
	for _, pc := range m.outbound() {
		seen[pc.getOrigin()] = struct{}{}
	}
	m.inboundMu.Lock()
	for pc := range m.inbound {
		seen[pc.getOrigin()] = struct{}{}
	}
	m.inboundMu.Unlock()
	out := make([]string, 0, len(seen))
	for name := range seen {
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

// Handler returns the accept handler, so tests can serve it themselves.
func (m *Mesh) Handler() http.Handler { return http.HandlerFunc(m.handleAccept) }

// NeedLeaderElection is false: every replica gossips.
func (m *Mesh) NeedLeaderElection() bool { return false }

// Start implements manager.Runnable: it runs discovery and, when ListenAddr
// is set, serves the mesh there until ctx ends.
func (m *Mesh) Start(ctx context.Context) error {
	if m.cfg.TLS == nil {
		return errors.New("mesh: TLS is required")
	}
	m.goSafe("mesh-discovery", func() { m.discoveryLoop(ctx) })

	if m.cfg.ListenAddr == "" {
		<-ctx.Done()
		m.closeAllPeers()
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle(Path, m.Handler())
	srv := &http.Server{
		Addr:              m.cfg.ListenAddr,
		Handler:           mux,
		TLSConfig:         m.cfg.TLS.Server,
		ReadHeaderTimeout: 10 * time.Second,
	}
	m.log.Info("mesh listening", "addr", m.cfg.ListenAddr, "service", m.cfg.ServiceDNS)
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(shutCtx)
		m.closeAllPeers()
		return err
	case err := <-errCh:
		m.closeAllPeers()
		return fmt.Errorf("mesh listen %s: %w", m.cfg.ListenAddr, err)
	}
}

func (m *Mesh) closeAllPeers() {
	m.peersMu.Lock()
	for addr, pc := range m.peers {
		if pc != nil {
			pc.close()
		}
		delete(m.peers, addr)
	}
	m.peersMu.Unlock()

	m.inboundMu.Lock()
	for pc := range m.inbound {
		pc.close()
	}
	m.inbound = map[*peerConn]struct{}{}
	m.inboundMu.Unlock()
}

// upgrader accepts any Origin header: only pods dial this endpoint, and TLS
// pinning is the check.
var upgrader = websocket.Upgrader{
	CheckOrigin:     func(*http.Request) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

// authorized reports whether r came over TLS with the pinned certificate.
// The listener's handshake already demands it; this also covers Handler
// mounted some other way.
func (m *Mesh) authorized(r *http.Request) bool {
	return m.cfg.TLS != nil && r.TLS != nil && len(r.TLS.PeerCertificates) > 0 &&
		m.cfg.TLS.pinned(r.TLS.PeerCertificates[0].Raw)
}

// handleAccept upgrades a peer's connection, requires a hello as its first
// frame, answers with its own hello and reads until the connection drops.
func (m *Mesh) handleAccept(w http.ResponseWriter, r *http.Request) {
	if !m.authorized(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		m.log.V(1).Info("upgrade failed", "error", err.Error())
		return
	}
	pc := newPeerConn(conn, false)
	conn.SetReadLimit(maxFrameBytes)
	_ = conn.SetReadDeadline(time.Now().Add(m.readDeadline))

	var hello frame
	if err := conn.ReadJSON(&hello); err != nil || hello.Kind != kindHello || hello.Origin == "" {
		m.log.V(1).Info("first frame was not a hello, closing")
		_ = conn.Close()
		return
	}
	if hello.Origin == m.cfg.Self {
		m.log.Info("refusing a connection from this replica itself", "origin", hello.Origin)
		_ = conn.Close()
		return
	}
	pc.setOrigin(hello.Origin)
	m.log.Info("peer accepted", "origin", hello.Origin, "direction", "inbound")

	m.inboundMu.Lock()
	m.inbound[pc] = struct{}{}
	m.inboundMu.Unlock()
	defer func() {
		m.inboundMu.Lock()
		delete(m.inbound, pc)
		m.inboundMu.Unlock()
	}()

	m.startWriteLoop(pc)
	if reply, err := encode(&frame{V: protocolVersion, Kind: kindHello, Origin: m.cfg.Self}); err == nil {
		pc.enqueue(reply)
	}
	m.runReadLoop(r.Context(), pc)
}

// runReadLoop reads one connection's frames, either direction, until it
// drops. The writer's ping and the peer's pong both refresh the read
// deadline, so an idle connection stays up and a dead peer times out.
func (m *Mesh) runReadLoop(ctx context.Context, pc *peerConn) {
	defer pc.close()
	pc.conn.SetReadLimit(maxFrameBytes)
	_ = pc.conn.SetReadDeadline(time.Now().Add(m.readDeadline))
	pc.conn.SetPongHandler(func(string) error {
		return pc.conn.SetReadDeadline(time.Now().Add(m.readDeadline))
	})
	pc.conn.SetPingHandler(func(appData string) error {
		_ = pc.conn.SetReadDeadline(time.Now().Add(m.readDeadline))
		_ = pc.conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
		return nil
	})

	direction := "inbound"
	if pc.outbound {
		direction = "outbound"
	}
	var disconnectErr error
	defer func() {
		kv := []any{"direction", direction, "origin", pc.getOrigin()}
		if disconnectErr != nil {
			kv = append(kv, "reason", disconnectErr.Error())
		}
		m.log.Info("peer disconnected", kv...)
	}()

	for {
		if err := ctx.Err(); err != nil {
			disconnectErr = err
			return
		}
		var f frame
		if err := pc.conn.ReadJSON(&f); err != nil {
			disconnectErr = err
			return
		}
		_ = pc.conn.SetReadDeadline(time.Now().Add(m.readDeadline))
		if f.V != protocolVersion {
			continue
		}
		switch f.Kind {
		case kindNodes:
			if m.cfg.Apply != nil && len(f.Nodes) > 0 {
				m.cfg.Apply(f.Nodes)
			}
		case kindUI:
			m.uiMu.RLock()
			fn := m.applyUI
			m.uiMu.RUnlock()
			if fn != nil && len(f.UI) > 0 {
				fn(f.UI)
			}
		case kindHello:
			// The acceptor's reply to this replica's dial names the peer.
			if f.Origin != "" {
				pc.setOrigin(f.Origin)
			}
		}
	}
}

// goSafe runs fn on its own goroutine and logs a panic instead of crashing
// the process.
func (m *Mesh) goSafe(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.log.Error(fmt.Errorf("panic: %v", r), "recovered in "+name, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

// peerConn is one peer connection, either direction, with its own send
// queue and writer goroutine, so a slow peer never blocks another.
type peerConn struct {
	conn     *websocket.Conn
	send     chan []byte
	outbound bool

	mu     sync.Mutex
	origin string

	closeOnce sync.Once
	closed    chan struct{}
}

func newPeerConn(conn *websocket.Conn, outbound bool) *peerConn {
	return &peerConn{
		conn:     conn,
		send:     make(chan []byte, peerSendBuffer),
		outbound: outbound,
		closed:   make(chan struct{}),
	}
}

func (pc *peerConn) setOrigin(origin string) {
	pc.mu.Lock()
	pc.origin = origin
	pc.mu.Unlock()
}

func (pc *peerConn) getOrigin() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.origin
}

// close is idempotent; it unblocks the read loop and stops the writer.
func (pc *peerConn) close() {
	pc.closeOnce.Do(func() {
		close(pc.closed)
		_ = pc.conn.Close()
	})
}

// enqueue queues b without blocking; false when the queue is full or the
// connection is closed.
func (pc *peerConn) enqueue(b []byte) bool {
	select {
	case pc.send <- b:
		return true
	case <-pc.closed:
		return false
	default:
		return false
	}
}

// enqueueWait queues b, waiting for room; false when the connection closes
// or ctx ends first.
func (pc *peerConn) enqueueWait(ctx context.Context, b []byte) bool {
	select {
	case pc.send <- b:
		return true
	case <-pc.closed:
		return false
	case <-ctx.Done():
		return false
	}
}

// startWriteLoop runs pc's writer. A panic in it closes pc, so the peer is
// dropped and redialed instead of left without a writer.
func (m *Mesh) startWriteLoop(pc *peerConn) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.log.Error(fmt.Errorf("panic: %v", r), "recovered in the mesh write loop, closing the peer connection")
				pc.close()
			}
		}()
		pc.writeLoop(m.log, m.pingInterval)
	}()
}

// writeLoop is the connection's only writer (a websocket.Conn allows one
// concurrent writer) and sends its keepalive ping.
func (pc *peerConn) writeLoop(log logr.Logger, pingEvery time.Duration) {
	pinger := time.NewTicker(pingEvery)
	defer pinger.Stop()
	for {
		select {
		case <-pc.closed:
			return
		case <-pinger.C:
			if err := pc.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				log.V(1).Info("keepalive ping failed, closing the peer connection", "error", err.Error())
				pc.close()
				return
			}
		case b := <-pc.send:
			_ = pc.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := pc.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				log.V(1).Info("write failed, closing the peer connection", "error", err.Error())
				pc.close()
				return
			}
		}
	}
}

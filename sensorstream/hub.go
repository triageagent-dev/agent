// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

// Package sensorstream carries sensor data from the eBPF DaemonSet to the
// controller over one gRPC stream per sensor pod
// (docs/adr/0015-sensor-stream-grpc.md). Hub is the controller side, Client
// the sensor side.
package sensorstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/internal/remotens"
	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// TargetPrefix marks a collector target as a sensor node, keeping the
// collectors' baseline keys distinct from any other target kind.
const TargetPrefix = "sensor/"

// maxMessageBytes bounds one message. An OBI snapshot of a busy node is a
// few megabytes.
const maxMessageBytes = 32 << 20

// Identity is who a stream's bearer token belongs to.
type Identity struct {
	User string
	// Node is the node the token was issued on, when the API server says
	// (a projected ServiceAccount token on Kubernetes 1.30+). Empty when not
	// known.
	Node string
	// OTLP is set for the otlp role (ADR-0022): the one identity that may send
	// OTLP sources, and the only one that may not send a node's own role
	// metrics. It is the identity of a peer that presents the pinned
	// certificate.
	OTLP bool
}

// Authenticator checks a stream's bearer token.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Identity, error)
}

// HubOptions configures a Hub.
type HubOptions struct {
	// Addr the gRPC server listens on, for example ":9470".
	Addr string
	// TLS is the server certificate source.
	TLS  credentials.TransportCredentials
	Auth Authenticator
	// Stale is how old a node's snapshot may be and still be a target.
	Stale time.Duration
	// Gossip, when set, receives every message a locally connected sensor
	// sends, for the other replicas (mesh.Mesh.Publish). Nil with one
	// replica.
	Gossip func(mesh.NodeSnapshot)
	// SourceFilter, when set, sees the resources an otlp-role sensor sends for
	// one source and returns those the controller accepts; the controller's
	// own tenant and workspace-scope checks sit here, since the sensor pod
	// holds no copy of the scope the core sets. Nil accepts them all.
	SourceFilter func(rms []*metricspb.ResourceMetrics) []*metricspb.ResourceMetrics
	// PinnedPeer, when set, is the DER certificate of the otlp role: a peer
	// that presents it as its client certificate is the role and needs no
	// token. It is the controller's own serving certificate, which the role's
	// pods already hold to serve OTLP, as the replica mesh pins it
	// (ServerTLS). Nil accepts no otlp role.
	PinnedPeer []byte
	Log        logr.Logger
	now        func() time.Time
}

// NodeStatus is one sensor's latest state as the hub knows it.
type NodeStatus struct {
	Node    string
	Pod     string
	Version string
	Roles   []string
	// Settings are the sensor's flags as it sent them in its Hello; empty
	// from an older sensor.
	Settings []*sensorpb.Setting
	// Connected is true while the sensor holds a stream to this replica,
	// or, as the peers last reported, to another one.
	Connected bool
	LastSeen  time.Time
	// Gaps counts sequence gaps seen by this replica; a peer's are not
	// gossiped.
	Gaps  uint64
	State *sensorpb.SensorState
}

// stamp orders the records the hub keeps per node: the receiving replica's
// receive time, then the sensor's sequence number.
type stamp struct {
	at  time.Time
	seq uint64
}

func (s stamp) newerThan(o stamp) bool {
	return s.at.After(o.at) || (s.at.Equal(o.at) && s.seq > o.seq)
}

type snapshot struct {
	set metricset.Set
	at  time.Time
	seq uint64
	// raw is the message as received, kept for replay to a peer only when
	// gossip is on.
	raw []byte
}

type nodeEntry struct {
	hello     *sensorpb.Hello
	helloAt   stamp
	helloRaw  []byte
	stream    uint64
	connected bool
	lastSeen  time.Time
	gaps      uint64
	state     *sensorpb.SensorState
	stateAt   stamp
	stateRaw  []byte
	snapshots map[string]snapshot
	// peerConnected is the peers' last word on whether the sensor holds a
	// stream to one of them, as of peerAt.
	peerConnected bool
	peerAt        stamp
	// ingestedAt is when this replica last ingested the entry's metrics
	// from an OTLP source (Ingest); zero for a sensor node.
	ingestedAt time.Time
}

// local reports whether the sensor holds its stream to this replica. Peer
// records never overwrite a local node: its own stream is newer than any
// peer's report of it.
func (e *nodeEntry) local() bool { return e.connected && e.stream != 0 }

// Hub accepts sensor streams and keeps each node's latest snapshot per role
// and its latest state.
type Hub struct {
	sensorpb.UnimplementedSensorServiceServer

	opts HubOptions
	log  logr.Logger

	mu        sync.RWMutex
	nodes     map[string]*nodeEntry
	streamID  uint64
	ingestSeq uint64
	lastPrune time.Time
}

// NewHub returns a Hub; Start serves it.
func NewHub(opts HubOptions) *Hub {
	if opts.Stale <= 0 {
		opts.Stale = 2 * time.Minute
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	return &Hub{opts: opts, log: opts.Log.WithName("sensorstream"), nodes: map[string]*nodeEntry{}}
}

// NeedLeaderElection is false: every replica accepts sensor streams.
func (h *Hub) NeedLeaderElection() bool { return false }

// Start serves the gRPC listener until ctx ends.
func (h *Hub) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", h.opts.Addr)
	if err != nil {
		return fmt.Errorf("sensor stream listen %s: %w", h.opts.Addr, err)
	}
	return h.Serve(ctx, lis)
}

// Serve serves the stream on lis until ctx ends.
func (h *Hub) Serve(ctx context.Context, lis net.Listener) error {
	srv := grpc.NewServer(
		grpc.Creds(h.opts.TLS),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	sensorpb.RegisterSensorServiceServer(srv, h)
	h.log.Info("sensor stream listening", "addr", lis.Addr().String())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// Stream implements sensorpb.SensorServiceServer.
func (h *Hub) Stream(stream grpc.BidiStreamingServer[sensorpb.StreamRequest, sensorpb.StreamResponse]) error {
	ctx := stream.Context()
	id, err := h.authenticate(ctx)
	if err != nil {
		h.log.Info("sensor stream refused", "reason", err.Error())
		return status.Error(codes.Unauthenticated, err.Error())
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil || hello.GetNode() == "" {
		return status.Error(codes.InvalidArgument, "the first message must be a hello naming the node")
	}
	otlpRole := slices.Contains(hello.GetRoles(), RoleOTLP)
	if otlpRole != id.OTLP || (otlpRole && len(hello.GetRoles()) != 1) {
		h.log.Info("sensor stream refused: the otlp role and the otlp account go together, and alone", "user", id.User, "roles", hello.GetRoles())
		return status.Error(codes.PermissionDenied, "the otlp role is for the otlp account only, and that account runs no other role")
	}
	if id.Node != "" && id.Node != hello.GetNode() {
		h.log.Info("sensor stream refused: hello names another node", "tokenNode", id.Node, "helloNode", hello.GetNode())
		return status.Error(codes.PermissionDenied, "the hello names a node other than the token's")
	}
	node := hello.GetNode()
	helloRaw := h.encode(first)
	sid, at := h.open(hello, first.GetSeq(), helloRaw)
	h.gossip(node, first.GetSeq(), at, helloRaw)
	h.log.Info("sensor connected", "node", node, "pod", hello.GetPod(), "version", hello.GetVersion(), "roles", hello.GetRoles())

	last := first.GetSeq()
	defer func() {
		if at, ok := h.close(node, sid); ok {
			h.gossip(node, last, at, h.encode(&sensorpb.StreamRequest{Seq: last}))
		}
		h.log.Info("sensor disconnected", "node", node, "pod", hello.GetPod())
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		if snap := msg.GetSnapshot(); snap != nil && snap.GetSource() != "" && !otlpRole {
			// Only the otlp role's certificate may name a source. A token
			// sensor that did would file data under a name of its choosing,
			// past the tenant checks that guard the other roles' snapshots.
			h.log.Info("sensor stream refused: a snapshot names a source on a stream that is not the otlp role's", "user", id.User, "node", node)
			return status.Error(codes.PermissionDenied, "only the otlp role may name a source")
		}
		if snap := msg.GetSnapshot(); snap != nil && otlpRole {
			// The otlp role's snapshots are OTLP sources: Ingest files each as
			// the source it names and gossips it, so nothing is stored under
			// this sensor's node but its liveness.
			h.ingestSource(node, sid, snap)
			last = msg.GetSeq()
			continue
		}
		raw := h.encode(msg)
		if at, ok := h.receive(node, sid, msg, last, raw); ok {
			h.gossip(node, msg.GetSeq(), at, raw)
		}
		last = msg.GetSeq()
		if msg.GetState() != nil {
			if err := stream.Send(&sensorpb.StreamResponse{Body: &sensorpb.StreamResponse_Ack{Ack: &sensorpb.Ack{Seq: msg.GetSeq()}}}); err != nil {
				return err
			}
		}
	}
}

func (h *Hub) authenticate(ctx context.Context) (Identity, error) {
	if p, ok := peer.FromContext(ctx); ok && h.opts.PinnedPeer != nil {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(ti.State.PeerCertificates) > 0 {
			if !bytes.Equal(ti.State.PeerCertificates[0].Raw, h.opts.PinnedPeer) {
				return Identity{}, errors.New("client certificate is not the pinned one")
			}
			return Identity{User: "otlp-role", OTLP: true}, nil
		}
	}
	md, _ := metadata.FromIncomingContext(ctx)
	var token string
	for _, v := range md.Get("authorization") {
		if t, ok := strings.CutPrefix(v, "Bearer "); ok {
			token = t
		}
	}
	if token == "" {
		return Identity{}, errors.New("no bearer token")
	}
	if h.opts.Auth == nil {
		return Identity{}, errors.New("no authenticator configured")
	}
	return h.opts.Auth.Authenticate(ctx, token)
}

func (h *Hub) open(hello *sensorpb.Hello, seq uint64, raw []byte) (uint64, time.Time) {
	now := h.opts.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.streamID++
	e := h.nodes[hello.GetNode()]
	if e == nil {
		e = &nodeEntry{snapshots: map[string]snapshot{}}
		h.nodes[hello.GetNode()] = e
	}
	e.hello, e.stream, e.connected, e.lastSeen = hello, h.streamID, true, now
	e.helloAt, e.helloRaw = stamp{at: now, seq: seq}, raw
	return h.streamID, now
}

// close marks the node's stream closed, unless a newer stream replaced it;
// ok is false then.
func (h *Hub) close(node string, sid uint64) (time.Time, bool) {
	now := h.opts.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.nodes[node]; e != nil && e.stream == sid {
		e.connected = false
		return now, true
	}
	return time.Time{}, false
}

// receive stores one message of the node's stream sid and returns when it
// was received; ok is false when sid is no longer the node's stream or the
// message was refused (the node stays live and is not gossiped).
func (h *Hub) receive(node string, sid uint64, msg *sensorpb.StreamRequest, last uint64, raw []byte) (time.Time, bool) {
	now := h.opts.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.nodes[node]
	if e == nil || e.stream != sid {
		return time.Time{}, false
	}
	e.lastSeen = now
	if msg.GetSeq() > last+1 {
		e.gaps += msg.GetSeq() - last - 1
	}
	switch b := msg.GetBody().(type) {
	case *sensorpb.StreamRequest_Snapshot:
		set := metricset.FromOTLP(b.Snapshot.GetResourceMetrics())
		// A sensor reports namespaces of this cluster, none of which has the
		// "<tenant>~<namespace>" form that only the OTLP receiver gives a
		// remote tenant's data. The collectors would route such a namespace
		// to the tenant it names, so the whole snapshot is refused.
		if ns := qualifiedNamespace(set); ns != "" {
			h.log.Info("sensor snapshot refused: namespace has the tenant separator", "node", node, "role", b.Snapshot.GetRole(), "namespace", ns)
			return now, false
		}
		e.snapshots[b.Snapshot.GetRole()] = snapshot{set: set, at: now, seq: msg.GetSeq(), raw: raw}
	case *sensorpb.StreamRequest_State:
		e.state, e.stateAt, e.stateRaw = b.State, stamp{at: now, seq: msg.GetSeq()}, raw
	}
	return now, true
}

// ingestSource files one otlp-role snapshot as the OTLP source it names. A
// snapshot with no source, for a role that is not a data role, or refused
// by the filter is dropped; the sensor stays live either way.
func (h *Hub) ingestSource(node string, sid uint64, snap *sensorpb.Snapshot) {
	now := h.opts.now()
	h.mu.Lock()
	e := h.nodes[node]
	if e == nil || e.stream != sid {
		h.mu.Unlock()
		return
	}
	e.lastSeen = now
	h.mu.Unlock()
	switch snap.GetRole() {
	case metricset.RoleEBPF, metricset.RoleNodeExporter, metricset.RoleLogExporter:
	default:
		h.log.Info("otlp snapshot refused: not a data role", "node", node, "role", snap.GetRole())
		return
	}
	if snap.GetSource() == "" {
		h.log.Info("otlp snapshot refused: no source", "node", node, "role", snap.GetRole())
		return
	}
	rms := snap.GetResourceMetrics()
	if h.opts.SourceFilter != nil {
		rms = h.opts.SourceFilter(rms)
	}
	if len(rms) == 0 {
		return
	}
	h.Ingest(snap.GetSource(), snap.GetRole(), rms)
}

// qualifiedNamespace returns a namespace of set that has the
// "<tenant>~<namespace>" form, or "" when there is none.
func qualifiedNamespace(set metricset.Set) string {
	for _, m := range set {
		for _, p := range m.Points {
			for k, v := range p.Attrs {
				if remotens.NamespaceKey(k) && strings.Contains(v, remotens.Sep) {
					return v
				}
			}
		}
	}
	return ""
}

// encode marshals msg for the peers; nil when gossip is off.
func (h *Hub) encode(msg *sensorpb.StreamRequest) []byte {
	if h.opts.Gossip == nil {
		return nil
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		h.log.Error(err, "encode a sensor message for the peers")
		return nil
	}
	return b
}

func (h *Hub) gossip(node string, seq uint64, at time.Time, raw []byte) {
	if h.opts.Gossip == nil || raw == nil {
		return
	}
	h.opts.Gossip(mesh.NodeSnapshot{Node: node, Seq: seq, At: at, Snapshot: raw})
}

// Merge applies records the peers sent: per node and per kind (hello, each
// role's snapshot, state) the newest record wins, by receive time and then
// sequence number. A node whose sensor holds its stream to this replica is
// left alone. Merged records are not gossiped again.
func (h *Hub) Merge(recs []mesh.NodeSnapshot) {
	type decoded struct {
		rec mesh.NodeSnapshot
		msg *sensorpb.StreamRequest
		set metricset.Set
	}
	in := make([]decoded, 0, len(recs))
	for _, rec := range recs {
		if rec.Node == "" {
			continue
		}
		msg := &sensorpb.StreamRequest{}
		if err := proto.Unmarshal(rec.Snapshot, msg); err != nil {
			h.log.Info("dropping a peer record that does not decode", "node", rec.Node, "error", err.Error())
			continue
		}
		d := decoded{rec: rec, msg: msg}
		if s := msg.GetSnapshot(); s != nil {
			d.set = metricset.FromOTLP(s.GetResourceMetrics())
		}
		in = append(in, d)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked(h.opts.now())
	for _, d := range in {
		node, at := d.rec.Node, stamp{at: d.rec.At, seq: d.rec.Seq}
		e := h.nodes[node]
		if e != nil && e.local() {
			continue
		}
		if e == nil {
			if d.msg.GetBody() == nil {
				continue
			}
			e = &nodeEntry{snapshots: map[string]snapshot{}}
			h.nodes[node] = e
		}
		switch b := d.msg.GetBody().(type) {
		case *sensorpb.StreamRequest_Hello:
			if at.newerThan(e.helloAt) {
				e.hello, e.helloAt, e.helloRaw = b.Hello, at, d.rec.Snapshot
			}
		case *sensorpb.StreamRequest_Snapshot:
			role := b.Snapshot.GetRole()
			cur, ok := e.snapshots[role]
			if !ok || at.newerThan(stamp{at: cur.at, seq: cur.seq}) {
				e.snapshots[role] = snapshot{set: d.set, at: d.rec.At, seq: d.rec.Seq, raw: d.rec.Snapshot}
			}
		case *sensorpb.StreamRequest_State:
			if at.newerThan(e.stateAt) {
				e.state, e.stateAt, e.stateRaw = b.State, at, d.rec.Snapshot
			}
		}
		// A record older than what this replica last saw of the node, its
		// own stream included, says nothing about where the sensor is now.
		if at.newerThan(e.peerAt) && !d.rec.At.Before(e.lastSeen) {
			e.peerConnected, e.peerAt = d.msg.GetBody() != nil, at
		}
		if d.rec.At.After(e.lastSeen) {
			e.lastSeen = d.rec.At
		}
	}
}

// pruneLocked forgets the nodes nobody will hear from again: a sensor that is
// not connected, here or on a peer (a Kubernetes node with no sensor is a
// node that left the cluster: a replaced or scaled-down node never returns
// under its name, and a live node always runs the DaemonSet), and an OTLP
// source not refreshed for otlpRetainFactor times Stale (it has no stream, so
// silence is the only sign it went away). h.mu must be held for writing.
func (h *Hub) pruneLocked(now time.Time) {
	h.lastPrune = now
	for node, e := range h.nodes {
		if strings.HasPrefix(node, OTLPNodePrefix) {
			if now.Sub(e.lastSeen) > otlpRetainFactor*h.opts.Stale {
				delete(h.nodes, node)
			}
			continue
		}
		peer := e.peerConnected && now.Sub(e.peerAt.at) <= h.opts.Stale
		if !e.local() && !peer {
			delete(h.nodes, node)
		}
	}
}

// Local returns the latest records of every sensor that holds its stream to
// this replica: its hello, each role's snapshot and its state. A peer gets
// them when this replica connects to it.
func (h *Hub) Local() []mesh.NodeSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []mesh.NodeSnapshot
	add := func(node string, at stamp, raw []byte) {
		if raw != nil {
			out = append(out, mesh.NodeSnapshot{Node: node, Seq: at.seq, At: at.at, Snapshot: raw})
		}
	}
	for node, e := range h.nodes {
		if !e.local() {
			// An OTLP source this replica ingested: its snapshots only.
			if !e.ingestedAt.IsZero() {
				for _, s := range e.snapshots {
					add(node, stamp{at: s.at, seq: s.seq}, s.raw)
				}
			}
			continue
		}
		add(node, e.helloAt, e.helloRaw)
		for _, s := range e.snapshots {
			add(node, stamp{at: s.at, seq: s.seq}, s.raw)
		}
		add(node, e.stateAt, e.stateRaw)
	}
	return out
}

// Targets implements metricset.Source: the nodes with a snapshot for role
// no older than Stale, as TargetPrefix+node.
func (h *Hub) Targets(role string) []string {
	now := h.opts.now()
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []string
	for node, e := range h.nodes {
		if s, ok := e.snapshots[role]; ok && now.Sub(s.at) <= h.opts.Stale {
			out = append(out, TargetPrefix+node)
		}
	}
	sort.Strings(out)
	return out
}

var _ metricset.Source = (*Hub)(nil)

var _ metricset.TimedSource = (*Hub)(nil)

// Set implements metricset.Source.
func (h *Hub) Set(role, target string) (metricset.Set, error) {
	set, _, err := h.SetAt(role, target)
	return set, err
}

// SetAt implements metricset.TimedSource: the set and when it was received.
func (h *Hub) SetAt(role, target string) (metricset.Set, time.Time, error) {
	node := strings.TrimPrefix(target, TargetPrefix)
	h.mu.RLock()
	defer h.mu.RUnlock()
	e := h.nodes[node]
	if e == nil {
		return nil, time.Time{}, fmt.Errorf("no sensor for node %s", node)
	}
	s, ok := e.snapshots[role]
	if !ok {
		return nil, time.Time{}, fmt.Errorf("sensor on node %s has no %s snapshot", node, role)
	}
	return s.set, s.at, nil
}

// Nodes returns every known sensor's status, sorted by node.
func (h *Hub) Nodes() []NodeStatus {
	now := h.opts.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked(now)
	out := make([]NodeStatus, 0, len(h.nodes))
	for node, e := range h.nodes {
		if strings.HasPrefix(node, OTLPNodePrefix) {
			continue // an OTLP source, not a sensor
		}
		// A peer that stopped reporting (it died with the stream) stops
		// counting as connected once its last record is stale.
		peer := e.peerConnected && now.Sub(e.peerAt.at) <= h.opts.Stale
		out = append(out, NodeStatus{
			Node: node, Pod: e.hello.GetPod(), Version: e.hello.GetVersion(), Roles: e.hello.GetRoles(), Settings: e.hello.GetSettings(),
			Connected: e.local() || peer, LastSeen: e.lastSeen, Gaps: e.gaps, State: e.state,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

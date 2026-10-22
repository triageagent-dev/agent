// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package mesh

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

// defaultLookupHost resolves the headless Service name through the pod's
// resolver.
var defaultLookupHost = net.LookupHost

// discoveryLoop dials every discovered or static peer not already
// connected, at once and then every DialInterval. A dropped peer is redialed
// on the next tick; there is no tighter retry loop.
func (m *Mesh) discoveryLoop(ctx context.Context) {
	m.dialAll(ctx)
	ticker := time.NewTicker(m.cfg.DialInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.dialAll(ctx)
		}
	}
}

func (m *Mesh) dialAll(ctx context.Context) {
	for _, addr := range m.discoverPeers() {
		if !m.claimDialSlot(addr) {
			continue
		}
		m.goSafe("mesh-dial", func() { m.dialPeer(ctx, addr) })
	}
}

// discoverPeers returns every candidate peer URL: StaticPeers verbatim, plus
// one wss://<ip>:<PeerPort>/gossip/ws per IP the headless ServiceDNS name
// resolves to, minus this pod's own addresses. The headless Service resolves
// to the pod IPs behind it, the same set an Endpoints watch would give,
// without any API access.
func (m *Mesh) discoverPeers() []string {
	targets := make([]string, 0, len(m.cfg.StaticPeers)+2)
	targets = append(targets, m.cfg.StaticPeers...)
	if m.cfg.ServiceDNS == "" {
		return targets
	}
	ips, err := m.cfg.LookupHost(m.cfg.ServiceDNS)
	if err != nil {
		m.log.V(1).Info("peer discovery lookup failed", "service", m.cfg.ServiceDNS, "error", err.Error())
		return targets
	}
	own := ownAddresses()
	for _, ip := range ips {
		if _, self := own[ip]; self {
			continue
		}
		targets = append(targets, "wss://"+net.JoinHostPort(ip, strconv.Itoa(m.cfg.PeerPort))+Path)
	}
	return targets
}

// ownAddresses returns this pod's interface IPs, so discoverPeers can drop
// its own address from the Service's answer. The hello check in handleAccept
// (a peer naming Self) is the backstop when this filter misses.
func ownAddresses() map[string]struct{} {
	set := make(map[string]struct{})
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return set
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil {
			set[ip.String()] = struct{}{}
		}
	}
	return set
}

// claimDialSlot reserves addr with a nil placeholder, so a slow dial that
// spans a tick never races a second dial to the same peer. False when a
// connection or a dial already holds the slot.
func (m *Mesh) claimDialSlot(addr string) bool {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if _, exists := m.peers[addr]; exists {
		return false
	}
	m.peers[addr] = nil
	return true
}

// releaseDialSlot frees addr so the next tick redials it. Called when a dial
// fails and when an established connection's read loop returns.
func (m *Mesh) releaseDialSlot(addr string) {
	m.peersMu.Lock()
	delete(m.peers, addr)
	m.peersMu.Unlock()
}

// dialPeer dials addr, sends hello, replays this replica's own records so
// the peer holds them without waiting for the next sensor tick, then reads
// until the connection drops. The slot is released on every exit path.
func (m *Mesh) dialPeer(ctx context.Context, addr string) {
	defer m.releaseDialSlot(addr)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second, TLSClientConfig: m.cfg.TLS.Client}
	conn, resp, err := dialer.DialContext(ctx, addr, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		m.log.V(1).Info("peer dial failed", "addr", addr, "error", err.Error())
		return
	}

	pc := newPeerConn(conn, true)
	m.peersMu.Lock()
	m.peers[addr] = pc
	m.peersMu.Unlock()
	m.log.Info("peer connected", "addr", addr, "direction", "outbound")

	m.startWriteLoop(pc)
	hello, err := encode(&frame{V: protocolVersion, Kind: kindHello, Origin: m.cfg.Self})
	if err != nil || !pc.enqueue(hello) {
		pc.close()
		return
	}
	m.goSafe("mesh-replay", func() { m.replay(ctx, pc) })
	m.runReadLoop(ctx, pc)
}

// replay sends every record of cfg.Replay to pc, one per frame. It waits for
// room in pc's buffer rather than dropping, since a replay is what brings a
// new peer level with this replica.
func (m *Mesh) replay(ctx context.Context, pc *peerConn) {
	if m.cfg.Replay == nil {
		return
	}
	for _, rec := range m.cfg.Replay() {
		b, err := encode(&frame{V: protocolVersion, Kind: kindNodes, Origin: m.cfg.Self, Nodes: []NodeSnapshot{rec}})
		if err != nil {
			m.log.Error(err, "encode replay record", "node", rec.Node)
			continue
		}
		if !pc.enqueueWait(ctx, b) {
			return
		}
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package mesh

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

// testCert is a self-signed server certificate for localhost, shaped like
// the controller's sensor-stream certificate (server auth only).
func testCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "mesh test"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func testTLS(t *testing.T, cert tls.Certificate) *TLS {
	t.Helper()
	mt, err := NewTLS(cert, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	return mt
}

type sink struct {
	mu   sync.Mutex
	recs []NodeSnapshot
}

func (s *sink) apply(recs []NodeSnapshot) {
	s.mu.Lock()
	s.recs = append(s.recs, recs...)
	s.mu.Unlock()
}

func (s *sink) nodes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.recs {
		out = append(out, r.Node)
	}
	return out
}

// serve runs m's accept handler on a loopback TLS listener and returns the
// URL a peer dials.
func serve(t *testing.T, m *Mesh) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(m.Handler())
	srv.TLS = m.cfg.TLS.Server.Clone()
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return "wss://" + strings.TrimPrefix(srv.URL, "https://") + Path
}

func start(t *testing.T, m *Mesh) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = m.Start(ctx) }()
	return cancel
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestMesh_ReplayThenPublishReachThePeer(t *testing.T) {
	mt := testTLS(t, testCert(t))
	var got sink
	b := New(Config{Self: "replica-b", TLS: mt, Apply: got.apply, Log: logr.Discard()})
	urlB := serve(t, b)
	start(t, b)

	at := time.Unix(1_800_000_000, 0).UTC()
	a := New(Config{
		Self: "replica-a", TLS: mt, StaticPeers: []string{urlB}, Log: logr.Discard(),
		Replay: func() []NodeSnapshot {
			return []NodeSnapshot{{Node: "node-a", Seq: 1, At: at, Snapshot: []byte("hello")}}
		},
	})
	start(t, a)

	waitFor(t, "the replayed record", func() bool { return len(got.nodes()) == 1 })
	waitFor(t, "A's outbound connection", func() bool { return len(a.outbound()) == 1 })
	a.Publish(NodeSnapshot{Node: "node-b", Seq: 2, At: at, Snapshot: []byte("snap")})
	waitFor(t, "the published record", func() bool { return len(got.nodes()) == 2 })

	got.mu.Lock()
	first, second := got.recs[0], got.recs[1]
	got.mu.Unlock()
	if first.Node != "node-a" || first.Seq != 1 || !first.At.Equal(at) || string(first.Snapshot) != "hello" {
		t.Errorf("replayed record = %+v", first)
	}
	if second.Node != "node-b" || string(second.Snapshot) != "snap" {
		t.Errorf("published record = %+v", second)
	}
	waitFor(t, "both peers named", func() bool {
		pa, pb := a.Peers(), b.Peers()
		return len(pa) == 1 && pa[0] == "replica-b" && len(pb) == 1 && pb[0] == "replica-a"
	})
}

func TestMesh_RedialsAfterThePeerRestarts(t *testing.T) {
	mt := testTLS(t, testCert(t))
	var got sink
	b := New(Config{Self: "replica-b", TLS: mt, Apply: got.apply, Log: logr.Discard()})
	urlB := serve(t, b)
	stopB := start(t, b)

	a := New(Config{Self: "replica-a", TLS: mt, StaticPeers: []string{urlB}, DialInterval: 50 * time.Millisecond, Log: logr.Discard()})
	start(t, a)
	waitFor(t, "A connected", func() bool { return len(a.outbound()) == 1 })
	first := a.outbound()[0]

	// B drops every connection; A's slot frees and the next tick redials.
	stopB()
	start(t, b)
	waitFor(t, "A reconnected", func() bool {
		conns := a.outbound()
		return len(conns) == 1 && conns[0] != first
	})
	a.Publish(NodeSnapshot{Node: "node-a", Snapshot: []byte("x")})
	waitFor(t, "a record after the redial", func() bool { return len(got.nodes()) == 1 })
}

func TestMesh_RefusesAnotherCertificate(t *testing.T) {
	var got sink
	b := New(Config{Self: "replica-b", TLS: testTLS(t, testCert(t)), Apply: got.apply, Log: logr.Discard()})
	urlB := serve(t, b)
	start(t, b)

	// Same host name, another key: a pod that does not hold the Secret.
	a := New(Config{Self: "replica-a", TLS: testTLS(t, testCert(t)), StaticPeers: []string{urlB}, DialInterval: 50 * time.Millisecond, Log: logr.Discard()})
	start(t, a)
	time.Sleep(300 * time.Millisecond)
	a.Publish(NodeSnapshot{Node: "node-a", Snapshot: []byte("x")})
	time.Sleep(100 * time.Millisecond)
	if n := got.nodes(); len(n) != 0 || len(a.Peers()) != 0 || len(b.Peers()) != 0 {
		t.Fatalf("an unpinned peer connected: records %v, A peers %v, B peers %v", n, a.Peers(), b.Peers())
	}
}

func TestMesh_RefusesItself(t *testing.T) {
	mt := testTLS(t, testCert(t))
	var got sink
	m := New(Config{Self: "replica-a", TLS: mt, Apply: got.apply, DialInterval: 50 * time.Millisecond, Log: logr.Discard()})
	m.cfg.StaticPeers = []string{serve(t, m)}
	start(t, m)
	time.Sleep(300 * time.Millisecond)
	if p := m.Peers(); len(p) != 0 {
		t.Fatalf("a replica connected to itself: %v", p)
	}
}

func TestDiscoverPeers_ResolvesTheServiceAndSkipsItself(t *testing.T) {
	m := New(Config{
		ServiceDNS: "peers.ns-a.svc", PeerPort: 9471, StaticPeers: []string{"wss://static:1" + Path},
		LookupHost: func(host string) ([]string, error) {
			if host != "peers.ns-a.svc" {
				t.Errorf("looked up %q", host)
			}
			return []string{"127.0.0.1", "10.0.0.7", "fd00::7"}, nil
		},
		Log: logr.Discard(),
	})
	got := m.discoverPeers()
	want := []string{"wss://static:1" + Path, "wss://10.0.0.7:9471" + Path, "wss://[fd00::7]:9471" + Path}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("peers = %v, want %v", got, want)
	}
}

func TestStart_RequiresTLS(t *testing.T) {
	if err := New(Config{Log: logr.Discard()}).Start(context.Background()); err == nil {
		t.Fatal("Start without TLS must fail")
	}
}

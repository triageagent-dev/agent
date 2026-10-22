package push

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// stubCore plays the core's side of agent join: challenge, join result,
// certificate hello check.
type stubCore struct {
	caCert   *x509.Certificate
	caKey    ed25519.PrivateKey
	approved atomic.Bool
	revoked  atomic.Bool

	mu        sync.Mutex
	helloOK   bool
	helloFail string

	badTenants atomic.Bool  // issue a certificate naming an extra tenant
	legacy     atomic.Bool  // sign the unbound challenge
	bindTLS    atomic.Bool  // bind the challenge to the TLS channel (needs serveTLS)
	wrongEKM   atomic.Bool  // channel-bound over other material: a relay's own session
	lastHeader atomic.Value // string: the agent nonce header of the latest dial
	enrollKey  string       // a join carrying this one-time key is issued at once
}

func newStubCore(t *testing.T) *stubCore {
	t.Helper()
	return mustStubCore()
}

// sharedTestCore is the one core CA every frame-pipeline test's fake hub
// and pre-joined Pusher share, so newTestPusher needs no per-test setup.
var sharedTestCore = sync.OnceValue(mustStubCore)

func mustStubCore() *stubCore {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		panic(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &stubCore{caCert: c, caKey: key}
}

// issue signs a certificate for the CSR the way an admin approval does.
func (s *stubCore) issue(csr *x509.CertificateRequest, uris []*url.URL) ([]byte, error) {
	return x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: csr.Subject, URIs: uris,
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, s.caCert, csr.PublicKey, s.caKey)
}

// memJoinStore keeps the join identity in memory, for a Pusher that needs no
// state directory.
type memJoinStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *memJoinStore) read(name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[name], nil
}

func (s *memJoinStore) write(name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string][]byte{}
	}
	if data == nil {
		delete(s.m, name)
	} else {
		s.m[name] = data
	}
	return nil
}

// joinedTo gives p an identity whose certificate core already issued for p's
// cluster and tenants, so its first connection opens with a certificate hello
// instead of ending in a join. It panics on a setup failure: it runs from
// constructors that have no *testing.T.
func (s *stubCore) joinedTo(p *Pusher) *Pusher {
	p.opts.JoinCAFingerprint = s.fingerprint()
	id, err := loadJoinIdentity(&memJoinStore{}, p.opts.JoinCAFingerprint)
	if err != nil {
		panic(err)
	}
	csrDER, err := newJoinCSR(id.key, p.opts.ClusterName, p.joinTenants())
	if err != nil {
		panic(err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		panic(err)
	}
	der, err := s.issue(csr, csr.URIs)
	if err != nil {
		panic(err)
	}
	if err := id.setCert(der); err != nil {
		panic(err)
	}
	p.joinOnce.Do(func() { p.joinID = id })
	return p
}

func (s *stubCore) fingerprint() string { return joinFingerprint(s.caKey.Public().(ed25519.PublicKey)) }

func (s *stubCore) serve(t *testing.T) *httptest.Server {
	return s.serveWith(t, httptest.NewServer, nil)
}

func (s *stubCore) serveTLS(t *testing.T) *httptest.Server {
	return s.serveWith(t, httptest.NewTLSServer, nil)
}

// serveSession serves like serve, and hands a connection whose certificate
// hello verified to session, with the hello already read: the frame-recording
// hub the pusher tests stream into.
func (s *stubCore) serveSession(t *testing.T, session func(conn *websocket.Conn, hello *Frame)) *httptest.Server {
	return s.serveWith(t, httptest.NewServer, session)
}

func (s *stubCore) serveWith(t *testing.T, start func(http.Handler) *httptest.Server, session func(conn *websocket.Conn, hello *Frame)) *httptest.Server {
	up := websocket.Upgrader{}
	srv := start(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hv := r.Header.Get(AgentNonceHeader)
		s.lastHeader.Store(hv)
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		nonce := make([]byte, 32)
		_, _ = rand.Read(nonce)
		chal := &ChallengePayload{Nonce: nonce, CACert: s.caCert.Raw}
		if s.legacy.Load() {
			chal.Sig = ed25519.Sign(s.caKey, joinProofMessage("challenge", nonce))
		} else {
			an, _ := base64.StdEncoding.DecodeString(hv)
			chal.AgentNonce = an
			parts := []string{hv}
			if s.bindTLS.Load() && r.TLS != nil {
				ekm, err := r.TLS.ExportKeyingMaterial(ChannelBindingLabel, nil, ChannelBindingLen)
				if err != nil {
					t.Error(err)
					return
				}
				if s.wrongEKM.Load() {
					ekm[0] ^= 0xff
				}
				chal.ChannelBound = true
				parts = append(parts, base64.StdEncoding.EncodeToString(ekm))
			}
			chal.Sig = ed25519.Sign(s.caKey, joinProofMessage("challenge", nonce, parts...))
		}
		ch, _ := encode(&Frame{V: ProtocolVersion, Kind: KindChallenge, Challenge: chal})
		_ = conn.WriteMessage(websocket.TextMessage, ch)
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		f, err := decode(data)
		if err != nil {
			return
		}
		switch f.Kind {
		case KindJoin:
			csr, err := x509.ParseCertificateRequest(f.Join.CSR)
			if err != nil || csr.CheckSignature() != nil {
				t.Error("bad CSR")
				return
			}
			pub := csr.PublicKey.(ed25519.PublicKey)
			if !ed25519.Verify(pub, joinProofMessage("join", nonce, joinCSRHash(f.Join.CSR)), f.Join.Proof) {
				t.Error("join proof does not verify")
			}
			res := &JoinResultPayload{Status: JoinPending}
			keyed := s.enrollKey != "" && f.Join.EnrollKey == s.enrollKey
			if f.Join.EnrollKey != "" && !keyed {
				res = &JoinResultPayload{Status: JoinRefused, Message: "agent key is invalid"}
			}
			if s.approved.Load() || keyed {
				uris := csr.URIs
				if s.badTenants.Load() {
					uris = append(slices.Clone(uris), &url.URL{Scheme: "triage", Opaque: "tenant/example-extra"})
				}
				der, err := s.issue(csr, uris)
				if err != nil {
					t.Error(err)
					return
				}
				res = &JoinResultPayload{Status: JoinIssued, Cert: der, CACert: s.caCert.Raw}
				if keyed {
					res.Bindings = []JoinBinding{{Tenant: "tenant-a", Namespaces: []string{"shop"}}}
				}
			}
			out, _ := encode(&Frame{V: ProtocolVersion, Kind: KindJoinResult, JoinResult: res})
			_ = conn.WriteMessage(websocket.TextMessage, out)
		case KindHello:
			if s.revoked.Load() {
				_ = conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "certificate is revoked"))
				return
			}
			if !s.verifyHello(f.Hello, nonce) {
				return
			}
			if session != nil {
				session(conn, f)
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// verifyHello checks a certificate hello and records the outcome.
func (s *stubCore) verifyHello(h *HelloPayload, nonce []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, err := x509.ParseCertificate(h.Cert)
	if err != nil || cert.CheckSignatureFrom(s.caCert) != nil {
		s.helloFail = "certificate not from the CA"
		return false
	}
	if !ed25519.Verify(cert.PublicKey.(ed25519.PublicKey), joinProofMessage("hello", nonce, h.ClusterName, h.Tenant), h.Proof) {
		s.helloFail = "hello proof does not verify"
		return false
	}
	s.helloOK = true
	return true
}

func newJoinTestPusher(t *testing.T, endpoint, caFP, dir string) *Pusher {
	p := newUnjoinedTestPusher(endpoint, nil)
	p.opts.JoinCAFingerprint = caFP
	p.opts.JoinStateDir = dir
	p.opts.now = time.Now
	return p
}

func TestJoin_PendingIssuedThenCertHello(t *testing.T) {
	core := newStubCore(t)
	srv := core.serve(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dir := t.TempDir()
	p := newJoinTestPusher(t, url, core.fingerprint(), dir)
	w := p.primaryWorker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := w.runOnce(ctx); !errors.Is(err, errJoinPending) {
		t.Fatalf("first attempt: %v, want pending", err)
	}
	core.approved.Store(true)
	if _, err := w.runOnce(ctx); !errors.Is(err, errJoinIssued) {
		t.Fatalf("after approval: %v, want issued", err)
	}

	// A restart keeps the key and certificate.
	p2 := newJoinTestPusher(t, url, core.fingerprint(), dir)
	id, err := p2.joinIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id.fingerprint() != func() string { i, _ := p.joinIdentity(); return i.fingerprint() }() {
		t.Fatal("the key must survive a restart")
	}
	connCtx, connCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer connCancel()
	_, _ = p2.primaryWorker().runOnce(connCtx)
	core.mu.Lock()
	defer core.mu.Unlock()
	if !core.helloOK {
		t.Fatalf("certificate hello not accepted: %s", core.helloFail)
	}
}

func TestJoin_RevokedCertificateDroppedAndJoinsAgain(t *testing.T) {
	core := newStubCore(t)
	srv := core.serve(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	p := newJoinTestPusher(t, url, core.fingerprint(), t.TempDir())
	w := p.primaryWorker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	core.approved.Store(true)
	if _, err := w.runOnce(ctx); !errors.Is(err, errJoinIssued) {
		t.Fatalf("first attempt: %v, want issued", err)
	}
	core.approved.Store(false)
	core.revoked.Store(true)
	connCtx, connCancel := context.WithTimeout(ctx, time.Second)
	_, _ = w.runOnce(connCtx)
	connCancel()

	id, err := p.joinIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if _, usable, _ := id.certState(time.Now(), p.opts.ClusterName, p.joinTenants()); usable {
		t.Fatal("a refused certificate must be dropped")
	}
	if w.rejected.Load() {
		t.Error("a refused certificate is not a config error; the agent should retry soon")
	}
	if _, err := w.runOnce(ctx); !errors.Is(err, errJoinPending) {
		t.Fatalf("next attempt: %v, want a new pending join", err)
	}
}

func TestJoin_UntrustedCoreRefused(t *testing.T) {
	core := newStubCore(t)
	srv := core.serve(t)
	p := newJoinTestPusher(t, "ws"+strings.TrimPrefix(srv.URL, "http"), "SHA256:not-this-core", t.TempDir())
	w := p.primaryWorker()
	_, err := w.runOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "CA check") {
		t.Fatalf("err = %v, want a CA check failure", err)
	}
	if !w.rejected.Load() {
		t.Error("a failed CA check must back off to the maximum")
	}
}

func TestJoinCertState(t *testing.T) {
	core := newStubCore(t)
	ds, err := newDirJoinStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id, err := loadJoinIdentity(ds, core.fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := newJoinCSR(id.key, "cluster-a", []string{"tenant-b", "tenant-a"})
	parsed, _ := x509.ParseCertificateRequest(csr)
	issue := func(notBefore, notAfter time.Time) []byte {
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber: big.NewInt(3), Subject: parsed.Subject, URIs: parsed.URIs,
			NotBefore: notBefore, NotAfter: notAfter,
		}, core.caCert, id.key.Public(), core.caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	now := time.Now()
	if err := id.setCert(issue(now.Add(-time.Hour), now.Add(10*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, usable, renew := id.certState(now, "cluster-a", []string{"tenant-a", "tenant-b"}); !usable || renew {
		t.Errorf("fresh cert: usable=%v renew=%v", usable, renew)
	}
	if _, usable, _ := id.certState(now, "cluster-a", []string{"tenant-a"}); usable {
		t.Error("a different tenant list needs a new join")
	}
	_ = id.setCert(issue(now.Add(-9*time.Hour), now.Add(time.Hour)))
	if _, usable, renew := id.certState(now, "cluster-a", []string{"tenant-a", "tenant-b"}); !usable || !renew {
		t.Errorf("late cert: usable=%v renew=%v, want renewal", usable, renew)
	}
}

func TestJoinSecretStore_IdentitySurvivesNewPod(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "agent-join", Namespace: "triage"}})
	store := &secretJoinStore{cs: cs, namespace: "triage", name: "agent-join"}

	first, err := loadJoinIdentity(store, "SHA256:ca")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.setCert([]byte("cert")); err != nil {
		t.Fatal(err)
	}
	second, err := loadJoinIdentity(store, "SHA256:ca")
	if err != nil {
		t.Fatal(err)
	}
	if first.fingerprint() != second.fingerprint() {
		t.Fatal("a new pod must load the same key from the Secret")
	}
	if string(second.cert) != "cert" {
		t.Fatalf("certificate not loaded from the Secret: %q", second.cert)
	}
	second.dropCert()
	sec, _ := cs.CoreV1().Secrets("triage").Get(context.Background(), "agent-join", metav1.GetOptions{})
	if _, ok := sec.Data[joinCertFile]; ok {
		t.Error("dropCert must remove the certificate from the Secret")
	}
	if _, ok := sec.Data[joinKeyFile]; !ok {
		t.Error("the key stays")
	}
}

func TestJoinSecretStore_MissingSecretFails(t *testing.T) {
	store := &secretJoinStore{cs: fake.NewSimpleClientset(), namespace: "triage", name: "agent-join"}
	if _, err := loadJoinIdentity(store, "SHA256:ca"); err == nil {
		t.Fatal("a missing Secret must fail loudly, not fall back to a throwaway key")
	}
}

func TestJoin_DialCarriesAgentNonce(t *testing.T) {
	core := newStubCore(t)
	srv := core.serve(t)
	p := newJoinTestPusher(t, "ws"+strings.TrimPrefix(srv.URL, "http"), core.fingerprint(), t.TempDir())
	if _, err := p.primaryWorker().runOnce(context.Background()); !errors.Is(err, errJoinPending) {
		t.Fatalf("runOnce: %v, want pending", err)
	}
	first, _ := core.lastHeader.Load().(string)
	raw, err := base64.StdEncoding.DecodeString(first)
	if err != nil || len(raw) != 32 {
		t.Fatalf("%s = %q, want base64 of 32 bytes", AgentNonceHeader, first)
	}
	_, _ = p.primaryWorker().runOnce(context.Background())
	if second, _ := core.lastHeader.Load().(string); second == first {
		t.Error("the agent nonce must be fresh per connection")
	}
}

func tlsJoinPusher(t *testing.T, core *stubCore, srv *httptest.Server, insecure bool) *Pusher {
	p := newJoinTestPusher(t, "wss"+strings.TrimPrefix(srv.URL, "https"), core.fingerprint(), t.TempDir())
	p.opts.InsecureTLS = insecure
	return p
}

func TestJoin_LegacyChallengeRefusedOnEveryScheme(t *testing.T) {
	for name, tc := range map[string]struct {
		tls, insecure bool
	}{"ws": {false, false}, "wss insecure": {true, true}} {
		t.Run(name, func(t *testing.T) {
			core := newStubCore(t)
			core.legacy.Store(true)
			var p *Pusher
			if tc.tls {
				p = tlsJoinPusher(t, core, core.serveTLS(t), tc.insecure)
			} else {
				srv := core.serve(t)
				p = newJoinTestPusher(t, "ws"+strings.TrimPrefix(srv.URL, "http"), core.fingerprint(), t.TempDir())
			}
			_, err := p.primaryWorker().runOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), "upgrade the core") {
				t.Fatalf("err = %v, want the unbound-challenge refusal", err)
			}
		})
	}
}

func TestJoin_ChannelBoundOverTLS(t *testing.T) {
	core := newStubCore(t)
	core.bindTLS.Store(true)
	p := tlsJoinPusher(t, core, core.serveTLS(t), true)
	if _, err := p.primaryWorker().runOnce(context.Background()); !errors.Is(err, errJoinPending) {
		t.Fatalf("runOnce: %v, want pending", err)
	}
}

func TestJoin_ChannelBoundByAnotherSessionRefused(t *testing.T) {
	core := newStubCore(t)
	core.bindTLS.Store(true)
	core.wrongEKM.Store(true)
	p := tlsJoinPusher(t, core, core.serveTLS(t), true)
	_, err := p.primaryWorker().runOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "different TLS channel") {
		t.Fatalf("err = %v, want the channel refusal", err)
	}
}

func TestJoin_UnboundChannelWithInsecureTLSRefused(t *testing.T) {
	core := newStubCore(t)
	p := tlsJoinPusher(t, core, core.serveTLS(t), true)
	_, err := p.primaryWorker().runOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "must terminate TLS itself") {
		t.Fatalf("err = %v, want the insecure-TLS refusal", err)
	}
}

func TestJoin_UnboundChannelOverPlaintextAccepted(t *testing.T) {
	core := newStubCore(t)
	srv := core.serve(t)
	p := newJoinTestPusher(t, "ws"+strings.TrimPrefix(srv.URL, "http"), core.fingerprint(), t.TempDir())
	if _, err := p.primaryWorker().runOnce(context.Background()); !errors.Is(err, errJoinPending) {
		t.Fatalf("runOnce: %v, want pending", err)
	}
}

func TestVerifyJoinChallenge_Binding(t *testing.T) {
	core := newStubCore(t)
	fp := core.fingerprint()
	nonce := bytes.Repeat([]byte{1}, 32)
	mine := bytes.Repeat([]byte{2}, 32)
	ekm := bytes.Repeat([]byte{4}, 32)
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	sign := func(parts ...string) []byte {
		return ed25519.Sign(core.caKey, joinProofMessage("challenge", nonce, parts...))
	}
	bound := func(echo []byte, signed string) *ChallengePayload {
		return &ChallengePayload{Nonce: nonce, AgentNonce: echo, CACert: core.caCert.Raw, Sig: sign(signed)}
	}
	channel := func(material []byte) *ChallengePayload {
		return &ChallengePayload{Nonce: nonce, AgentNonce: mine, ChannelBound: true, CACert: core.caCert.Raw,
			Sig: sign(b64(mine), b64(material))}
	}
	legacy := &ChallengePayload{Nonce: nonce, CACert: core.caCert.Raw,
		Sig: ed25519.Sign(core.caKey, joinProofMessage("challenge", nonce))}
	verify := func(c *ChallengePayload, b joinBinding) error {
		_, err := verifyJoinChallenge(fp, nil, c, b)
		return err
	}
	plain := joinBinding{sentNonce: mine}
	verified := joinBinding{sentNonce: mine, ekm: ekm}
	insecure := joinBinding{sentNonce: mine, ekm: ekm, insecureTLS: true}

	// ChannelBound false: plaintext and verified TLS accept, insecure TLS refuses.
	if err := verify(bound(mine, b64(mine)), plain); err != nil {
		t.Errorf("unbound channel over plaintext: %v", err)
	}
	if err := verify(bound(mine, b64(mine)), verified); err != nil {
		t.Errorf("unbound channel over verified TLS: %v", err)
	}
	if err := verify(bound(mine, b64(mine)), insecure); err == nil ||
		!strings.Contains(err.Error(), "must terminate TLS itself") {
		t.Errorf("unbound channel with insecure TLS: %v", err)
	}
	// ChannelBound true.
	if err := verify(channel(ekm), insecure); err != nil {
		t.Errorf("channel-bound: %v", err)
	}
	if err := verify(channel(bytes.Repeat([]byte{5}, 32)), insecure); err == nil ||
		!strings.Contains(err.Error(), "different TLS channel") {
		t.Errorf("channel-bound by another session: %v", err)
	}
	if err := verify(channel(ekm), plain); err == nil ||
		!strings.Contains(err.Error(), "different TLS channel") {
		t.Errorf("channel-bound without local material: %v", err)
	}
	// ChannelBound claimed but only the nonce-bound message was signed.
	claimed := bound(mine, b64(mine))
	claimed.ChannelBound = true
	if err := verify(claimed, verified); err == nil {
		t.Error("channelBound flag without the EKM part accepted")
	}
	// Flag stripped from a channel-bound signature: not a valid nonce-bound one.
	stripped := channel(ekm)
	stripped.ChannelBound = false
	if err := verify(stripped, verified); err == nil {
		t.Error("stripped channelBound flag accepted")
	}
	// Echo problems.
	other := bytes.Repeat([]byte{3}, 32)
	if err := verify(bound(other, b64(other)), verified); err == nil ||
		!strings.Contains(err.Error(), "different agent nonce") {
		t.Errorf("wrong echo: %v", err)
	}
	if err := verify(bound(mine, b64(other)), verified); err == nil {
		t.Error("bad bound signature accepted")
	}
	// Legacy challenges are refused on every scheme.
	for name, b := range map[string]joinBinding{"plain": plain, "verified": verified, "insecure": insecure} {
		if err := verify(legacy, b); err == nil || !strings.Contains(err.Error(), "upgrade the core") {
			t.Errorf("legacy challenge, %s: %v", name, err)
		}
		if err := verify(bound(nil, b64(mine)), b); err == nil || !strings.Contains(err.Error(), "upgrade the core") {
			t.Errorf("stripped echo, %s: %v", name, err)
		}
	}
}

func TestValidateIssuedCert(t *testing.T) {
	core := newStubCore(t)
	otherCore := newStubCore(t)
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	issue := func(ca *x509.Certificate, caKey ed25519.PrivateKey, subj ed25519.PublicKey, cluster string, tenants []string, nb, na time.Time) []byte {
		csr, err := newJoinCSR(key, cluster, tenants)
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ := x509.ParseCertificateRequest(csr)
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber: big.NewInt(9), Subject: parsed.Subject, URIs: parsed.URIs, NotBefore: nb, NotAfter: na,
		}, ca, subj, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	good := func() []byte {
		return issue(core.caCert, core.caKey, pub, "demo", []string{"example-a", "example-b"}, now.Add(-time.Minute), now.Add(time.Hour))
	}
	check := func(der []byte) error {
		return validateIssuedCert(der, core.caCert.Raw, pub, now, "demo", []string{"example-b", "example-a"})
	}
	if err := check(good()); err != nil {
		t.Fatalf("good certificate: %v", err)
	}
	cases := map[string]struct {
		der  []byte
		want string
	}{
		"garbage": {[]byte("nope"), "does not parse"},
		"other CA": {issue(otherCore.caCert, otherCore.caKey, pub, "demo", []string{"example-a", "example-b"}, now.Add(-time.Minute), now.Add(time.Hour)),
			"does not chain"},
		"expired": {issue(core.caCert, core.caKey, pub, "demo", []string{"example-a", "example-b"}, now.Add(-2*time.Hour), now.Add(-time.Hour)),
			"not currently valid"},
		"wrong cluster": {issue(core.caCert, core.caKey, pub, "example", []string{"example-a", "example-b"}, now.Add(-time.Minute), now.Add(time.Hour)),
			"names cluster"},
		"missing tenant": {issue(core.caCert, core.caKey, pub, "demo", []string{"example-a"}, now.Add(-time.Minute), now.Add(time.Hour)),
			"names tenants"},
		"extra tenant": {issue(core.caCert, core.caKey, pub, "demo", []string{"example-a", "example-b", "example-c"}, now.Add(-time.Minute), now.Add(time.Hour)),
			"names tenants"},
		"foreign key": {issue(core.caCert, core.caKey, otherKey.Public().(ed25519.PublicKey), "demo", []string{"example-a", "example-b"}, now.Add(-time.Minute), now.Add(time.Hour)),
			"this agent's key"},
	}
	for name, tc := range cases {
		if err := check(tc.der); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
	if err := validateIssuedCert(good(), nil, pub, now, "demo", []string{"example-a", "example-b"}); err == nil {
		t.Error("no trusted CA: certificate accepted")
	}
}

// A refused issued certificate is not saved.
func TestJoin_IssuedCertificateForWrongTenantsNotSaved(t *testing.T) {
	core := newStubCore(t)
	core.badTenants.Store(true)
	core.approved.Store(true)
	srv := core.serve(t)
	p := newJoinTestPusher(t, "ws"+strings.TrimPrefix(srv.URL, "http"), core.fingerprint(), t.TempDir())
	_, err := p.primaryWorker().runOnce(context.Background())
	if err == nil || errors.Is(err, errJoinIssued) || !strings.Contains(err.Error(), "names tenants") {
		t.Fatalf("err = %v, want a tenant refusal", err)
	}
	id, _ := p.joinIdentity()
	if len(id.cert) != 0 {
		t.Error("the refused certificate was saved")
	}
}

func TestVerifyJoinChallenge_CrossCertificate(t *testing.T) {
	old := newStubCore(t)
	next := newStubCore(t)
	nextPub := next.caKey.Public().(ed25519.PublicKey)
	now := time.Now()
	nonce := bytes.Repeat([]byte{1}, 32)
	mine := bytes.Repeat([]byte{2}, 32)
	cross := func(mut func(*x509.Certificate)) []byte {
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(5), Subject: pkix.Name{CommonName: "cross"},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		}
		mut(tmpl)
		der, err := x509.CreateCertificate(rand.Reader, tmpl, old.caCert, nextPub, old.caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	verify := func(crossDER []byte) error {
		c := &ChallengePayload{Nonce: nonce, AgentNonce: mine, CACert: next.caCert.Raw, CrossCert: crossDER,
			Sig: ed25519.Sign(next.caKey, joinProofMessage("challenge", nonce, base64.StdEncoding.EncodeToString(mine)))}
		_, err := verifyJoinChallenge("", old.caCert.Raw, c, joinBinding{sentNonce: mine})
		return err
	}
	if err := verify(cross(func(*x509.Certificate) {})); err != nil {
		t.Fatalf("valid rotation: %v", err)
	}
	cases := map[string]struct {
		mut  func(*x509.Certificate)
		want string
	}{
		"not a CA":      {func(c *x509.Certificate) { c.IsCA = false }, "not a CA certificate"},
		"no constraint": {func(c *x509.Certificate) { c.BasicConstraintsValid = false; c.IsCA = false }, "not a CA certificate"},
		"no certsign":   {func(c *x509.Certificate) { c.KeyUsage = x509.KeyUsageDigitalSignature }, "certificate-signing"},
		"expired":       {func(c *x509.Certificate) { c.NotBefore = now.Add(-2 * time.Hour); c.NotAfter = now.Add(-time.Hour) }, "not currently valid"},
		"not yet valid": {func(c *x509.Certificate) { c.NotBefore = now.Add(time.Hour); c.NotAfter = now.Add(2 * time.Hour) }, "not currently valid"},
	}
	for name, tc := range cases {
		if err := verify(cross(tc.mut)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestJoin_EnrollKeyIssuedAtOnceAndRemoved(t *testing.T) {
	core := newStubCore(t)
	core.enrollKey = "tak_abc_secret"
	srv := core.serve(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, joinEnrollKeyFile), []byte(core.enrollKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newJoinTestPusher(t, url, core.fingerprint(), dir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.primaryWorker().runOnce(ctx); !errors.Is(err, errJoinIssued) {
		t.Fatalf("join with a key: %v, want issued without an admin", err)
	}
	if _, err := os.Stat(filepath.Join(dir, joinEnrollKeyFile)); !os.IsNotExist(err) {
		t.Fatalf("the spent key must be removed from the join store, stat err %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, joinCertFile)); len(b) == 0 {
		t.Fatal("certificate not saved")
	}
}

func TestJoin_BadEnrollKeyRefusedAndKept(t *testing.T) {
	core := newStubCore(t)
	core.enrollKey = "tak_abc_secret"
	srv := core.serve(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, joinEnrollKeyFile), []byte("tak_abc_wrong"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newJoinTestPusher(t, url, core.fingerprint(), dir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.primaryWorker().runOnce(ctx); err == nil || errors.Is(err, errJoinIssued) {
		t.Fatalf("join with a wrong key: %v, want refused", err)
	}
	if _, err := os.Stat(filepath.Join(dir, joinEnrollKeyFile)); err != nil {
		t.Fatalf("a refused key stays for the operator to replace: %v", err)
	}
}

// The scope a key join returns, and every later limits frame, is kept in the
// join store, so a restart sends its first snapshots already limited.
func TestJoin_ScopeSurvivesRestart(t *testing.T) {
	core := newStubCore(t)
	core.enrollKey = "tak_abc_secret"
	atHello := make(chan map[string][]string, 4)
	var p2 *Pusher
	srv := core.serveSession(t, func(conn *websocket.Conn, _ *Frame) {
		atHello <- p2.scope.snapshot()
		time.Sleep(300 * time.Millisecond)
	})
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, joinEnrollKeyFile), []byte(core.enrollKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newJoinTestPusher(t, url, core.fingerprint(), dir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.primaryWorker().runOnce(ctx); !errors.Is(err, errJoinIssued) {
		t.Fatalf("join with a key: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, joinScopeFile)); string(b) != `{"tenant-a":["shop"]}` {
		t.Fatalf("saved scope = %q", b)
	}

	// A new process: no limits frame yet, but the scope is back before the
	// hello goes out.
	old := scopeLimitsGrace
	scopeLimitsGrace = 50 * time.Millisecond
	defer func() { scopeLimitsGrace = old }()
	p2 = newJoinTestPusher(t, url, core.fingerprint(), dir)
	_, _ = p2.primaryWorker().runOnce(ctx)
	select {
	case got := <-atHello:
		if !reflect.DeepEqual(got, map[string][]string{"tenant-a": {"shop"}}) {
			t.Fatalf("scope at the hello = %v", got)
		}
	default:
		t.Fatal("no certificate hello reached the core")
	}
	// The core said nothing about a scope within the grace, so the stored one
	// is lifted and saved as lifted.
	if p2.WorkspaceNamespaceInScope("tenant-a", "other") == false {
		t.Fatal("the restored scope stayed after the grace without a limits frame")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, joinScopeFile)); string(b) != `{}` {
		t.Fatalf("saved scope after the grace = %q", b)
	}
}

// An agent whose core never limits it writes nothing to the join store.
func TestJoin_NoScopeWritesNothing(t *testing.T) {
	store := &memJoinStore{}
	p := newTestPusher("", nil)
	id, err := loadJoinIdentity(store, "")
	if err != nil {
		t.Fatal(err)
	}
	p.joinID = id
	w := newConnWorker(p)
	p.scope.set(nil)
	w.saveScope(false)
	if b, _ := store.read(joinScopeFile); b != nil {
		t.Fatalf("scope saved for an unscoped agent: %q", b)
	}
	p.scope.set(map[string][]string{"t1": {"a"}})
	w.saveScope(false)
	if b, _ := store.read(joinScopeFile); string(b) != `{"t1":["a"]}` {
		t.Fatalf("saved scope = %q", b)
	}
}

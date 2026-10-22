package push

// Certificate join (core repo docs/todo/agent-join.md). The agent keeps an
// Ed25519 key on local disk, asks the core for a certificate with a CSR
// naming its cluster and tenants, waits for an admin to approve it, and then
// opens every session with that certificate and a signature over the core's
// challenge nonce. The core proves itself first by signing the same nonce
// with its agent CA key, which the agent checks against the CA fingerprint it
// was configured with.
//
// The CSR layout, proof messages and challenge check mirror the core's
// internal/agentjoin byte for byte; the two repos are separate Go modules.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	joinKeyFile  = "key.pem"
	joinCertFile = "cert.der"
	joinCAFile   = "ca.der"
	// joinEnrollKeyFile holds a one-time agent key (core ADR-090), written
	// by the chart (core.agentKey) or install.sh. A join that carries
	// it is approved at once; the agent removes it once the certificate is
	// issued.
	joinEnrollKeyFile = "enroll.key"
	// joinScopeFile holds the workspace namespace scope last known, as JSON
	// (workspace_scope.go), so a restart or a new leader starts with it.
	joinScopeFile = "scope.json"
)

var (
	errJoinPending = errors.New("join request pending approval")
	errJoinIssued  = errors.New("certificate issued; reconnecting with it")
)

// joinIdentity is the agent's key, its current certificate and the core CA it
// trusts, persisted in store.
type joinIdentity struct {
	mu            sync.Mutex
	store         joinStore
	caFingerprint string // configured trust anchor
	key           ed25519.PrivateKey
	cert          []byte
	caCert        []byte // saved from the first verified challenge
}

// loadJoinIdentity reads the identity from store, generating and saving a
// key on first use.
func loadJoinIdentity(store joinStore, caFingerprint string) (*joinIdentity, error) {
	j := &joinIdentity{store: store, caFingerprint: caFingerprint}
	b, err := store.read(joinKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read join key: %w", err)
	}
	if len(b) > 0 {
		blk, _ := pem.Decode(b)
		if blk == nil {
			return nil, errors.New("join key: no PEM block")
		}
		k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
		if err != nil {
			return nil, fmt.Errorf("join key: %w", err)
		}
		key, ok := k.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("join key: not an Ed25519 key")
		}
		j.key = key
	} else {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, err
		}
		if err := store.write(joinKeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); err != nil {
			return nil, fmt.Errorf("save join key: %w", err)
		}
		j.key = key
	}
	if j.cert, err = store.read(joinCertFile); err != nil {
		return nil, err
	}
	if j.caCert, err = store.read(joinCAFile); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *joinIdentity) fingerprint() string {
	return joinFingerprint(j.key.Public().(ed25519.PublicKey))
}

// verifyChallenge checks the core's challenge and saves the CA certificate
// it trusts from then on.
func (j *joinIdentity) verifyChallenge(c *ChallengePayload, b joinBinding) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	trusted, err := verifyJoinChallenge(j.caFingerprint, j.caCert, c, b)
	if err != nil {
		return err
	}
	if !bytes.Equal(trusted, j.caCert) {
		if err := j.store.write(joinCAFile, trusted); err != nil {
			return fmt.Errorf("save core CA: %w", err)
		}
		j.caCert = trusted
	}
	return nil
}

func (j *joinIdentity) setCert(der []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.store.write(joinCertFile, der); err != nil {
		return fmt.Errorf("save certificate: %w", err)
	}
	j.cert = der
	return nil
}

// validateIssued checks a certificate the core returned on JoinIssued before
// it is saved: it must chain to the trusted CA (the one this connection's
// challenge was verified against), be valid now, carry this agent's key, and
// name exactly the requested cluster and tenants.
func (j *joinIdentity) validateIssued(der []byte, now time.Time, cluster string, tenants []string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return validateIssuedCert(der, j.caCert, j.key.Public().(ed25519.PublicKey), now, cluster, tenants)
}

func validateIssuedCert(der, caDER []byte, pub ed25519.PublicKey, now time.Time, cluster string, tenants []string) error {
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return fmt.Errorf("issued certificate does not parse: %w", err)
	}
	if len(caDER) == 0 {
		return errors.New("issued certificate cannot be checked: no trusted CA certificate")
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse trusted CA: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}); err != nil {
		return fmt.Errorf("issued certificate does not chain to the trusted CA or is not currently valid: %w", err)
	}
	if k, ok := cert.PublicKey.(ed25519.PublicKey); !ok || !k.Equal(pub) {
		return errors.New("issued certificate does not carry this agent's key")
	}
	gotCluster, gotTenants := certNames(cert.URIs)
	if gotCluster != cluster {
		return fmt.Errorf("issued certificate names cluster %q, want triage:cluster/%s", gotCluster, cluster)
	}
	if want := sortedUnique(tenants); !slices.Equal(gotTenants, want) {
		return fmt.Errorf("issued certificate names tenants %v, want exactly %v (triage:tenant/<t>)", gotTenants, want)
	}
	return nil
}

// enrollKey returns the one-time agent key in the store, or "".
func (j *joinIdentity) enrollKey() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := j.store.read(joinEnrollKeyFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// dropEnrollKey removes the spent key from the store.
func (j *joinIdentity) dropEnrollKey() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.store.write(joinEnrollKeyFile, nil)
}

// loadScope returns the workspace scope saved in the store, and its stored
// form; nil when none was saved.
func (j *joinIdentity) loadScope() (map[string][]string, []byte, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := j.store.read(joinScopeFile)
	if err != nil || len(b) == 0 {
		return nil, nil, err
	}
	var m map[string][]string
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, nil, fmt.Errorf("join store %s: %w", joinScopeFile, err)
	}
	return m, b, nil
}

// saveScope writes the workspace scope to the store.
func (j *joinIdentity) saveScope(data []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.store.write(joinScopeFile, data)
}

func (j *joinIdentity) dropCert() {
	j.mu.Lock()
	defer j.mu.Unlock()
	_ = j.store.write(joinCertFile, nil)
	j.cert = nil
}

// certState reports whether the saved certificate can open a session for
// cluster and tenants, and whether it is due for renewal (less than a third
// of its lifetime left).
func (j *joinIdentity) certState(now time.Time, cluster string, tenants []string) (cert []byte, usable, renew bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.cert) == 0 {
		return nil, false, false
	}
	c, err := x509.ParseCertificate(j.cert)
	if err != nil || !now.Before(c.NotAfter) {
		return nil, false, false
	}
	pub, ok := c.PublicKey.(ed25519.PublicKey)
	if !ok || !pub.Equal(j.key.Public()) {
		return nil, false, false
	}
	gotCluster, gotTenants := certNames(c.URIs)
	if gotCluster != cluster || !slices.Equal(gotTenants, sortedUnique(tenants)) {
		return nil, false, false
	}
	life := c.NotAfter.Sub(c.NotBefore)
	return j.cert, true, c.NotAfter.Sub(now) < life/3
}

func (j *joinIdentity) sign(msg []byte) []byte { return ed25519.Sign(j.key, msg) }

func certNames(uris []*url.URL) (cluster string, tenants []string) {
	for _, u := range uris {
		if u.Scheme != "triage" {
			continue
		}
		if c, ok := strings.CutPrefix(u.Opaque, "cluster/"); ok {
			cluster = c
		} else if t, ok := strings.CutPrefix(u.Opaque, "tenant/"); ok {
			tenants = append(tenants, t)
		}
	}
	sort.Strings(tenants)
	return cluster, tenants
}

func sortedUnique(in []string) []string {
	out := slices.Clone(in)
	sort.Strings(out)
	return slices.Compact(out)
}

// joinFingerprint mirrors agentjoin.Fingerprint: "SHA256:<hex>" of the key's
// PKIX encoding.
func joinFingerprint(pub ed25519.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(der)
	return "SHA256:" + hex.EncodeToString(sum[:])
}

// joinProofMessage mirrors agentjoin.ProofMessage.
func joinProofMessage(kind string, nonce []byte, parts ...string) []byte {
	var b bytes.Buffer
	b.WriteString("triage-agent/" + kind + "\n" + base64.StdEncoding.EncodeToString(nonce))
	for _, p := range parts {
		b.WriteString("\n" + p)
	}
	return b.Bytes()
}

func joinCSRHash(csr []byte) string {
	sum := sha256.Sum256(csr)
	return hex.EncodeToString(sum[:])
}

// newJoinCSR mirrors agentjoin.NewCSR.
func newJoinCSR(key ed25519.PrivateKey, cluster string, tenants []string) ([]byte, error) {
	uris := []*url.URL{{Scheme: "triage", Opaque: "cluster/" + cluster}}
	for _, t := range sortedUnique(tenants) {
		uris = append(uris, &url.URL{Scheme: "triage", Opaque: "tenant/" + t})
	}
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "agent:" + cluster},
		URIs:    uris,
	}, key)
}

// joinBinding is what the agent knows about its own connection.
type joinBinding struct {
	sentNonce   []byte // the X-Triage-Agent-Nonce sent on this dial
	ekm         []byte // TLS exporter keying material, nil when not TLS
	insecureTLS bool   // --core-insecure-skip-verify
}

var errBoundToOtherChannel = errors.New("challenge is bound to a different TLS channel")

// verifyJoinChallenge mirrors agentjoin.VerifyChallenge: the core CA must be
// the trusted one (the saved certificate, else the configured fingerprint),
// or be certified by the saved certificate through the cross certificate
// during a rotation; the signature must be its signature over the challenge
// message.
//
// The core must bind the challenge to this connection: it echoes b.sentNonce
// (the X-Triage-Agent-Nonce the agent sent) as c.AgentNonce and signs
// joinProofMessage("challenge", nonce, base64(agentNonce)). A challenge
// without the echo is replayable and always refused.
//
// A core that also binds the TLS channel sets c.ChannelBound and appends
// base64(ekm), the exporter keying material of the TLS session, as one more
// part line; a relay terminating TLS itself cannot produce that. Without
// ChannelBound the nonce-bound signature is accepted only when the agent
// verified the core's TLS identity or there is no TLS at all; with
// --core-insecure-skip-verify nothing would stop a relay, so it is refused.
func verifyJoinChallenge(trustedFP string, trustedDER []byte, c *ChallengePayload, b joinBinding) ([]byte, error) {
	caDER, crossDER, nonce, sig := c.CACert, c.CrossCert, c.Nonce, c.Sig
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse core CA: %w", err)
	}
	pub, ok := caCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("core CA key must be Ed25519")
	}
	var trusted *x509.Certificate
	if len(trustedDER) > 0 {
		if trusted, err = x509.ParseCertificate(trustedDER); err != nil {
			return nil, fmt.Errorf("parse trusted CA: %w", err)
		}
		tp, ok := trusted.PublicKey.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("trusted CA key must be Ed25519")
		}
		trustedFP = joinFingerprint(tp)
	}
	if joinFingerprint(pub) != trustedFP {
		if trusted == nil || len(crossDER) == 0 {
			return nil, fmt.Errorf("core CA %s is not the trusted CA %s", joinFingerprint(pub), trustedFP)
		}
		cross, err := x509.ParseCertificate(crossDER)
		if err != nil {
			return nil, fmt.Errorf("parse cross certificate: %w", err)
		}
		crossPub, ok := cross.PublicKey.(ed25519.PublicKey)
		if !ok || !crossPub.Equal(pub) {
			return nil, errors.New("cross certificate does not certify the core CA")
		}
		if err := cross.CheckSignatureFrom(trusted); err != nil {
			return nil, fmt.Errorf("cross certificate not signed by the trusted CA: %w", err)
		}
		if !cross.IsCA || !cross.BasicConstraintsValid {
			return nil, errors.New("cross certificate is not a CA certificate")
		}
		if cross.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errors.New("cross certificate lacks the certificate-signing key usage")
		}
		if now := time.Now(); now.Before(cross.NotBefore) || !now.Before(cross.NotAfter) {
			return nil, errors.New("cross certificate is not currently valid")
		}
	}
	if len(c.AgentNonce) == 0 {
		return nil, errors.New("core does not bind the challenge to this connection; upgrade the core")
	}
	if !bytes.Equal(c.AgentNonce, b.sentNonce) {
		return nil, errors.New("challenge echoes a different agent nonce")
	}
	parts := []string{base64.StdEncoding.EncodeToString(c.AgentNonce)}
	switch {
	case c.ChannelBound:
		if len(b.ekm) == 0 {
			return nil, errBoundToOtherChannel
		}
		parts = append(parts, base64.StdEncoding.EncodeToString(b.ekm))
	case b.insecureTLS:
		return nil, errors.New("core did not bind the challenge to the TLS channel; with --core-insecure-skip-verify the core must terminate TLS itself")
	}
	if !ed25519.Verify(pub, joinProofMessage("challenge", nonce, parts...), sig) {
		if c.ChannelBound {
			return nil, errBoundToOtherChannel
		}
		return nil, errors.New("challenge signature does not verify")
	}
	return caDER, nil
}

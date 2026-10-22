// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

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
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/triageagent-dev/agent/push"
)

// joinCore is the core's side of the join handshake, just enough for a test
// agent that already holds a certificate: it proves itself with a challenge
// signed by its CA key and checks the certificate hello. The challenge
// layout mirrors push/join.go (itself a mirror of the core's agentjoin).
type joinCore struct {
	caCert      *x509.Certificate
	caKey       ed25519.PrivateKey
	fingerprint string
}

func newJoinCore(t *testing.T) *joinCore {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(spki)
	return &joinCore{caCert: c, caKey: key, fingerprint: "SHA256:" + hex.EncodeToString(sum[:])}
}

func proofMessage(kind string, nonce []byte, parts ...string) []byte {
	var b bytes.Buffer
	b.WriteString("triage-agent/" + kind + "\n" + base64.StdEncoding.EncodeToString(nonce))
	for _, p := range parts {
		b.WriteString("\n" + p)
	}
	return b.Bytes()
}

// joinedState writes a state directory holding an agent key and a
// certificate this core issued for cluster and tenant, so the agent opens its
// first session with a certificate hello and never has to join.
func (c *joinCore) joinedState(t *testing.T, cluster, tenant string) string {
	t.Helper()
	dir := t.TempDir()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "agent:" + cluster},
		URIs: []*url.URL{
			{Scheme: "triage", Opaque: "cluster/" + cluster},
			{Scheme: "triage", Opaque: "tenant/" + tenant},
		},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, c.caCert, pub, c.caKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cert.der"), der, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// accept runs the handshake on an upgraded connection: it sends the challenge
// bound to the agent nonce of the dial, then checks the certificate hello. It
// returns the hello, or false when the connection should end.
func (c *joinCore) accept(t *testing.T, r *http.Request, conn *websocket.Conn) (*push.Frame, bool) {
	t.Helper()
	header := r.Header.Get(push.AgentNonceHeader)
	agentNonce, _ := base64.StdEncoding.DecodeString(header)
	nonce := make([]byte, 32)
	_, _ = rand.Read(nonce)
	challenge, _ := json.Marshal(&push.Frame{V: push.ProtocolVersion, Kind: push.KindChallenge, Challenge: &push.ChallengePayload{
		Nonce: nonce, AgentNonce: agentNonce, CACert: c.caCert.Raw,
		Sig: ed25519.Sign(c.caKey, proofMessage("challenge", nonce, header)),
	}})
	if err := conn.WriteMessage(websocket.TextMessage, challenge); err != nil {
		return nil, false
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, false
	}
	f := &push.Frame{}
	if json.Unmarshal(data, f) != nil || f.Kind != push.KindHello || f.Hello == nil {
		t.Errorf("first frame after the challenge = %q, want a certificate hello", f.Kind)
		return nil, false
	}
	cert, err := x509.ParseCertificate(f.Hello.Cert)
	if err != nil || cert.CheckSignatureFrom(c.caCert) != nil {
		t.Errorf("hello certificate is not from the core CA: %v", err)
		return nil, false
	}
	if !ed25519.Verify(cert.PublicKey.(ed25519.PublicKey), proofMessage("hello", nonce, f.Hello.ClusterName, f.Hello.Tenant), f.Hello.Proof) {
		t.Error("hello proof does not verify")
		return nil, false
	}
	return f, true
}

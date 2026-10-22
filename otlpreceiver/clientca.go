// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/triageagent-dev/agent/internal/remotens"
)

// The agent's own client CA for remote senders (docs/adr/0017-otlp-receiver.md).
// The controller keeps it in a Secret it manages, like the sensor stream's
// serving certificate, and trusts it for mutual TLS on the OTLP port. An
// operator issues a sender's certificate from it with IssueClientCert, so a
// remote Collector needs no CA of its own.
const (
	// ClientCACertKey and ClientCAKeyKey are the Secret keys. ClientCAPrevKey
	// holds the CA a renewal replaced; it stays trusted until it expires, so
	// certificates issued from it keep working through the overlap.
	ClientCACertKey = "ca.crt"
	ClientCAKeyKey  = "ca.key"
	ClientCAPrevKey = "ca.prev.crt"

	clientCAValidity = 5 * 365 * 24 * time.Hour
	clientCARenew    = 90 * 24 * time.Hour
	// DefaultCertValidity is how long an issued client certificate lives.
	DefaultCertValidity = 365 * 24 * time.Hour
	// DefaultDomain is the SPIFFE trust domain of an issued certificate; the
	// receiver reads only the tenant path.
	DefaultDomain = "triage-agent"
)

// What EnsureClientCA did.
const (
	CAExisting = "existing"
	CACreated  = "created"
	CARenewed  = "renewed"
)

// EnsureClientCA returns the PEM bundle a receiver trusts for client
// certificates: the CA from Secret name in namespace ns, plus the previous one
// while it has not expired. It creates the CA on first use and renews it
// inside clientCARenew of expiry, keeping the old one under ClientCAPrevKey so
// certificates issued from it stay valid. The chart creates the Secret empty;
// the controller may get and update it. Replicas that start together race on
// the write: the loser reads the winner's CA. A running replica keeps the pool
// it loaded until it restarts, so issue certificates only after a renewal has
// rolled through every replica.
func EnsureClientCA(ctx context.Context, client kubernetes.Interface, ns, name string) (trustPEM []byte, what string, err error) {
	secrets := client.CoreV1().Secrets(ns)
	for range 5 {
		sec, err := secrets.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, "", fmt.Errorf("get secret %s/%s (the chart creates it): %w", ns, name, err)
		}
		certPEM, keyPEM := sec.Data[ClientCACertKey], sec.Data[ClientCAKeyKey]
		if usableClientCA(certPEM, keyPEM) {
			return trustBundle(certPEM, sec.Data[ClientCAPrevKey]), CAExisting, nil
		}
		what = CACreated
		if len(certPEM) > 0 {
			what = CARenewed
		}
		newCert, newKey, err := newClientCA()
		if err != nil {
			return nil, "", err
		}
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		if what == CARenewed && stillValid(certPEM) {
			sec.Data[ClientCAPrevKey] = certPEM
		}
		sec.Data[ClientCACertKey], sec.Data[ClientCAKeyKey] = newCert, newKey
		if _, err = secrets.Update(ctx, sec, metav1.UpdateOptions{}); err == nil {
			return trustBundle(newCert, sec.Data[ClientCAPrevKey]), what, nil
		}
		if !apierrors.IsConflict(err) {
			return nil, "", fmt.Errorf("store the client CA in %s/%s: %w", ns, name, err)
		}
	}
	return nil, "", fmt.Errorf("store the client CA in %s/%s: too many conflicting writers", ns, name)
}

// LoadClientCA reads the client CA without changing the Secret, for issuing
// certificates: it never creates or renews. It fails when the controller has
// not made a CA yet or the one stored has expired.
func LoadClientCA(ctx context.Context, client kubernetes.Interface, ns, name string) (certPEM, keyPEM []byte, err error) {
	sec, err := client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("get secret %s/%s: %w", ns, name, err)
	}
	certPEM, keyPEM = sec.Data[ClientCACertKey], sec.Data[ClientCAKeyKey]
	if !stillValid(certPEM) || len(keyPEM) == 0 {
		return nil, nil, fmt.Errorf("secret %s/%s holds no usable client CA: start the controller with otlp.ownCA so it creates one", ns, name)
	}
	return certPEM, keyPEM, nil
}

// trustBundle joins the current CA and the previous one, when it is still valid.
func trustBundle(current, prev []byte) []byte {
	out := append([]byte(nil), current...)
	if stillValid(prev) {
		out = append(out, prev...)
	}
	return out
}

// stillValid reports whether certPEM parses and has not expired.
func stillValid(certPEM []byte) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	return err == nil && time.Now().Before(cert.NotAfter)
}

func usableClientCA(certPEM, keyPEM []byte) bool {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return false
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	return err == nil && cert.IsCA && time.Until(cert.NotAfter) > clientCARenew
}

func newClientCA() (certPEM, keyPEM []byte, err error) { return newClientCAValid(clientCAValidity) }

func newClientCAValid(validity time.Duration) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "triage-agent otlp client CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// IssueClientCert signs a client certificate for the CA in caCertPEM/caKeyPEM.
// It carries one URI SAN per tenant, spiffe://<domain>/tenant/<tenant>, and
// the common name cn. The receiver still limits each tenant to the agent's
// join tenants, so issuing one the agent does not serve gives a certificate
// that is refused.
func IssueClientCert(caCertPEM, caKeyPEM []byte, domain, cn string, tenants []string, validity time.Duration) (certPEM, keyPEM []byte, err error) {
	if cn == "" {
		return nil, nil, errors.New("a common name is required")
	}
	if len(tenants) == 0 {
		return nil, nil, errors.New("at least one tenant is required")
	}
	if domain == "" {
		domain = DefaultDomain
	}
	for _, t := range tenants {
		if !remotens.ValidTenant(t) {
			return nil, nil, fmt.Errorf("invalid tenant name %q", t)
		}
	}
	pair, err := tls.X509KeyPair(caCertPEM, caKeyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("client CA: %w", err)
	}
	ca, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	var uris []*url.URL
	for _, t := range slices.Compact(slices.Sorted(slices.Values(tenants))) {
		uris = append(uris, &url.URL{Scheme: "spiffe", Host: domain, Path: "/tenant/" + t})
	}
	now := time.Now()
	notAfter := now.Add(validity)
	if notAfter.After(ca.NotAfter) {
		notAfter = ca.NotAfter
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		URIs:         uris,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, pair.PrivateKey)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

func randSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
}

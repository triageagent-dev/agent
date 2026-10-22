// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// writeCert writes a self-signed certificate and key into dir and returns
// their paths.
func writeCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestNewOTLPSensorReceiver_Validates(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir)
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, mustRead(t, cert), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.crt")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	sink := func(string, string, []*metricspb.ResourceMetrics) {}

	cases := []struct {
		name string
		f    otlpSensorFlags
		want string // substring of the error; empty means it must build
	}{
		{"no listen address", otlpSensorFlags{tlsCert: cert, tlsKey: key, clientCA: ca}, "--otlp-listen"},
		{"no serving certificate", otlpSensorFlags{listen: ":4317", clientCA: ca}, "--otlp-tls-cert-file"},
		{"nobody could send", otlpSensorFlags{listen: ":4317", tlsCert: cert, tlsKey: key}, "nobody could send"},
		{"unreadable certificate", otlpSensorFlags{listen: ":4317", tlsCert: empty, tlsKey: key, clientCA: ca}, "serving certificate"},
		{"client CA without a certificate", otlpSensorFlags{listen: ":4317", tlsCert: cert, tlsKey: key, clientCA: empty}, "no certificate in the client CA"},
		{"insecure needs no certificate", otlpSensorFlags{listen: ":4317", insecure: true}, ""},
		{"insecure excludes the certificate", otlpSensorFlags{listen: ":4317", insecure: true, tlsCert: cert, tlsKey: key}, "excludes"},
		{"insecure excludes a client CA", otlpSensorFlags{listen: ":4317", insecure: true, clientCA: ca}, "excludes"},
		{"insecure excludes tenants", otlpSensorFlags{listen: ":4317", insecure: true, tenants: []string{"acme"}}, "excludes"},
		{"mutual TLS only", otlpSensorFlags{listen: ":4317", tlsCert: cert, tlsKey: key, clientCA: ca}, ""},
	}
	for _, c := range cases {
		r, err := newOTLPSensorReceiver(c.f, sink)
		switch {
		case c.want == "" && (err != nil || r == nil):
			t.Errorf("%s: err = %v, want a receiver", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: err = %v, want it to mention %q", c.name, err, c.want)
		}
	}
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

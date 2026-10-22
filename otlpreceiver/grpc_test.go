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
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// issue returns a certificate for name signed by parent (self-signed when
// parent is nil) and its key.
func issue(t *testing.T, name string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey, tls.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:         isCA, BasicConstraintsValid: true,
	}
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}
}

// An OTel Collector's otlp exporter with a client certificate (mutual TLS)
// is accepted without a token; one without a certificate or token is not.
func TestGRPC_MutualTLS(t *testing.T) {
	srvCert, _, srvTLS := issue(t, "triage-agent-sensors", false, nil, nil)
	ca, caKey, _ := issue(t, "client-ca", true, nil, nil)
	_, _, clientTLS := issue(t, "otel-collector", false, ca, caKey)

	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca)
	var mu sync.Mutex
	var roles []string
	r := New(Options{
		TLS: credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{srvTLS}, MinVersion: tls.VersionTLS13,
			ClientCAs: clientCAs, ClientAuth: tls.VerifyClientCertIfGiven,
		}),
		Auth:  okAuth{},
		Flush: 20 * time.Millisecond,
		Sink: func(_, role string, _ []*metricspb.ResourceMetrics) {
			mu.Lock()
			roles = append(roles, role)
			mu.Unlock()
		},
		Log: logr.Discard(),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx, lis) }()
	t.Cleanup(func() { cancel(); <-done })

	roots := x509.NewCertPool()
	roots.AddCert(srvCert)
	dial := func(certs []tls.Certificate) collectorpb.MetricsServiceClient {
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs: roots, ServerName: "triage-agent-sensors", Certificates: certs,
		})))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return collectorpb.NewMetricsServiceClient(conn)
	}
	req := &collectorpb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource:     resource("service.name", "checkout"),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{counter("log.errors", 1, 1)}}},
	}}}
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()

	if _, err := dial(nil).Export(cctx, req); err == nil {
		t.Fatal("an export with neither a client certificate nor a token was accepted")
	}
	if _, err := dial([]tls.Certificate{clientTLS}).Export(cctx, req); err != nil {
		t.Fatalf("mutual TLS export: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(roles)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the export never reached the sink")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

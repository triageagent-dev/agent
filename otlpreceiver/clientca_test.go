// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func caClient() *fake.Clientset {
	return fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "triage"}})
}

func TestEnsureClientCA_CreatesThenReuses(t *testing.T) {
	client := caClient()
	ctx := context.Background()
	b1, what, err := EnsureClientCA(ctx, client, "triage", "ca")
	if err != nil || what != CACreated {
		t.Fatalf("first start: what=%q err=%v", what, err)
	}
	b2, what, err := EnsureClientCA(ctx, client, "triage", "ca")
	if err != nil || what != CAExisting {
		t.Fatalf("restart: what=%q err=%v", what, err)
	}
	if string(b1) != string(b2) {
		t.Error("a restart must keep the stored CA")
	}
	block, _ := pem.Decode(b1)
	if block == nil {
		t.Fatal("no PEM certificate")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !ca.IsCA {
		t.Fatalf("not a CA certificate: %v", err)
	}
}

func TestEnsureClientCA_NeedsTheChartSecret(t *testing.T) {
	if _, _, err := EnsureClientCA(context.Background(), fake.NewSimpleClientset(), "triage", "ca"); err == nil {
		t.Fatal("expected an error when the Secret does not exist")
	}
}

func TestEnsureClientCA_RenewalKeepsTheOldCATrusted(t *testing.T) {
	client := caClient()
	ctx := context.Background()
	oldCert, oldKey, err := newClientCAValid(10 * 24 * time.Hour) // inside the renewal window
	if err != nil {
		t.Fatal(err)
	}
	sec, _ := client.CoreV1().Secrets("triage").Get(ctx, "ca", metav1.GetOptions{})
	sec.Data = map[string][]byte{ClientCACertKey: oldCert, ClientCAKeyKey: oldKey}
	if _, err := client.CoreV1().Secrets("triage").Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// A certificate issued before the renewal.
	oldLeaf, _, err := IssueClientCert(oldCert, oldKey, "", "sender", []string{"a"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	bundle, what, err := EnsureClientCA(ctx, client, "triage", "ca")
	if err != nil || what != CARenewed {
		t.Fatalf("what=%q err=%v", what, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		t.Fatal("no certificate in the trust bundle")
	}
	block, _ := pem.Decode(oldLeaf)
	leaf, _ := x509.ParseCertificate(block.Bytes)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("a certificate issued before the renewal must stay trusted: %v", err)
	}
	newCert, newKey, err := LoadClientCA(ctx, client, "triage", "ca")
	if err != nil {
		t.Fatal(err)
	}
	if string(newCert) == string(oldCert) {
		t.Error("the CA must have been replaced")
	}
	newLeaf, _, err := IssueClientCert(newCert, newKey, "", "sender", []string{"a"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	block, _ = pem.Decode(newLeaf)
	nl, _ := x509.ParseCertificate(block.Bytes)
	if _, err := nl.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("a certificate from the new CA must be trusted: %v", err)
	}
}

func TestLoadClientCA_IsReadOnly(t *testing.T) {
	client := caClient()
	ctx := context.Background()
	if _, _, err := LoadClientCA(ctx, client, "triage", "ca"); err == nil {
		t.Fatal("an empty Secret must be an error, not a new CA")
	}
	sec, _ := client.CoreV1().Secrets("triage").Get(ctx, "ca", metav1.GetOptions{})
	if len(sec.Data) != 0 {
		t.Error("issuing must not write the Secret")
	}
}

func TestIssueClientCert_ChainsAndNamesTenants(t *testing.T) {
	caCert, caKey, err := newClientCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := IssueClientCert(caCert, caKey, "", "collector-1", []string{"zeta", "alpha", "alpha"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("the certificate must chain to the CA as a client certificate: %v", err)
	}
	p, err := principalFromCert(leaf)
	if err != nil || p.Name != "collector-1" || !p.Forwarder || !reflect.DeepEqual(p.Tenants, []string{"alpha", "zeta"}) {
		t.Errorf("principal = %+v", p)
	}

	other, otherKey, _ := newClientCA()
	otherPool := x509.NewCertPool()
	otherPool.AppendCertsFromPEM(other)
	_ = otherKey
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: otherPool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
		t.Error("a certificate must not chain to another CA")
	}
}

func TestIssueClientCert_Refusals(t *testing.T) {
	caCert, caKey, _ := newClientCA()
	for name, tc := range map[string]struct {
		cn      string
		tenants []string
	}{
		"no tenant":      {"c", nil},
		"no common name": {"", []string{"a"}},
		"bad tenant":     {"c", []string{"Not_Valid!"}},
	} {
		if _, _, err := IssueClientCert(caCert, caKey, "", tc.cn, tc.tenants, time.Hour); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

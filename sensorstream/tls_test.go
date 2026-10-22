// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func certOpts(client *fake.Clientset, names ...string) ServingCertOptions {
	return ServingCertOptions{Client: client, Namespace: "triage", Secret: "sensor-tls", ConfigMap: "sensor-ca", DNSNames: names}
}

func emptyObjects() *fake.Clientset {
	return fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sensor-tls", Namespace: "triage"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sensor-ca", Namespace: "triage"}},
	)
}

func TestEnsureServingCert_GeneratesStoresAndPublishes(t *testing.T) {
	client := emptyObjects()
	ctx := context.Background()
	cert, err := EnsureServingCert(ctx, certOpts(client, "svc.triage.svc"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "svc.triage.svc" {
		t.Errorf("DNS names = %v", leaf.DNSNames)
	}

	sec, _ := client.CoreV1().Secrets("triage").Get(ctx, "sensor-tls", metav1.GetOptions{})
	cm, _ := client.CoreV1().ConfigMaps("triage").Get(ctx, "sensor-ca", metav1.GetOptions{})
	if len(sec.Data[secretKeyKey]) == 0 || string(sec.Data[secretCertKey]) != cm.Data[CAKey] {
		t.Fatal("the Secret must hold the key and the ConfigMap the same certificate")
	}
	block, _ := pem.Decode([]byte(cm.Data[CAKey]))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("ca.crt is not a PEM certificate")
	}
}

func TestEnsureServingCert_ReusesAndRenewsOnNewName(t *testing.T) {
	client := emptyObjects()
	ctx := context.Background()
	first, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc"))
	if err != nil {
		t.Fatal(err)
	}
	again, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Certificate[0]) != string(again.Certificate[0]) {
		t.Error("a restart must keep the stored certificate")
	}
	renamed, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc", "b.triage.svc"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Certificate[0]) == string(renamed.Certificate[0]) {
		t.Error("a certificate missing a DNS name must be replaced")
	}
}

func TestEnsureServingCert_CoversIPAddresses(t *testing.T) {
	client := emptyObjects()
	ctx := context.Background()
	first, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc", "otlp.example.com"))
	if err != nil {
		t.Fatal(err)
	}
	withIP, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc", "otlp.example.com", "203.0.113.7"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Certificate[0]) == string(withIP.Certificate[0]) {
		t.Fatal("a certificate missing an IP address must be replaced")
	}
	leaf, err := x509.ParseCertificate(withIP.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.VerifyHostname("203.0.113.7"); err != nil {
		t.Errorf("the IP must be a SAN: %v", err)
	}
	if err := leaf.VerifyHostname("otlp.example.com"); err != nil {
		t.Errorf("the host name must stay covered: %v", err)
	}
	again, err := EnsureServingCert(ctx, certOpts(client, "a.triage.svc", "otlp.example.com", "203.0.113.7"))
	if err != nil {
		t.Fatal(err)
	}
	if string(withIP.Certificate[0]) != string(again.Certificate[0]) {
		t.Error("a restart must keep a certificate that already covers the IP")
	}
}

func TestEnsureServingCert_NeedsTheChartObjects(t *testing.T) {
	if _, err := EnsureServingCert(context.Background(), certOpts(fake.NewSimpleClientset(), "a")); err == nil {
		t.Fatal("expected an error when the Secret does not exist")
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"bytes"
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
	"net"
	"os"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Keys of the controller's self-managed TLS material.
const (
	secretCertKey = "tls.crt"
	secretKeyKey  = "tls.key"
	// CAKey is the ConfigMap key the sensors pin the controller certificate from.
	CAKey = "ca.crt"
)

const (
	certValidity = 10 * 365 * 24 * time.Hour
	// renewBefore replaces a certificate this close to expiry.
	renewBefore = 30 * 24 * time.Hour
)

// ServingCertOptions names where the controller keeps its self-signed
// certificate and where it publishes it for the sensors.
type ServingCertOptions struct {
	Client    kubernetes.Interface
	Namespace string
	// Secret holds tls.crt and tls.key. The chart creates it empty; the
	// controller may get and update it.
	Secret string
	// ConfigMap receives the certificate under ca.crt. The chart creates it
	// empty; the controller may get and update it.
	ConfigMap string
	// DNSNames the certificate must cover: the sensor Service's names, and
	// any further host names or IP addresses a sender outside the cluster
	// reaches the controller by. An IP literal becomes an IP SAN.
	DNSNames []string
}

// EnsureServingCert loads the controller's certificate from its Secret, or
// generates a new self-signed one when there is none, it is near expiry or
// it misses a name, and publishes the certificate to the ConfigMap.
func EnsureServingCert(ctx context.Context, o ServingCertOptions) (tls.Certificate, error) {
	secrets := o.Client.CoreV1().Secrets(o.Namespace)
	sec, err := secrets.Get(ctx, o.Secret, metav1.GetOptions{})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("get secret %s/%s (the chart creates it): %w", o.Namespace, o.Secret, err)
	}
	certPEM, keyPEM := sec.Data[secretCertKey], sec.Data[secretKeyKey]
	if !usableCert(certPEM, keyPEM, o.DNSNames) {
		certPEM, keyPEM, err = newSelfSigned(o.DNSNames)
		if err != nil {
			return tls.Certificate{}, err
		}
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		sec.Data[secretCertKey], sec.Data[secretKeyKey] = certPEM, keyPEM
		if _, err := secrets.Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
			return tls.Certificate{}, fmt.Errorf("store the serving certificate in %s/%s: %w", o.Namespace, o.Secret, err)
		}
	}

	cms := o.Client.CoreV1().ConfigMaps(o.Namespace)
	cm, err := cms.Get(ctx, o.ConfigMap, metav1.GetOptions{})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("get configmap %s/%s (the chart creates it): %w", o.Namespace, o.ConfigMap, err)
	}
	if cm.Data[CAKey] != string(certPEM) {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[CAKey] = string(certPEM)
		if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			return tls.Certificate{}, fmt.Errorf("publish the serving certificate to %s/%s: %w", o.Namespace, o.ConfigMap, err)
		}
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

func usableCert(certPEM, keyPEM []byte, dnsNames []string) bool {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return false
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || time.Until(cert.NotAfter) < renewBefore {
		return false
	}
	dns, ips := splitSANs(dnsNames)
	for _, n := range dns {
		if !slices.Contains(cert.DNSNames, n) {
			return false
		}
	}
	for _, ip := range ips {
		if !slices.ContainsFunc(cert.IPAddresses, ip.Equal) {
			return false
		}
	}
	return true
}

// splitSANs separates IP literals from host names.
func splitSANs(names []string) (dns []string, ips []net.IP) {
	for _, n := range names {
		if ip := net.ParseIP(n); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, n)
		}
	}
	return dns, ips
}

func newSelfSigned(names []string) (certPEM, keyPEM []byte, err error) {
	dnsNames, ips := splitSANs(names)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "triage-agent sensor stream"},
		DNSNames:              dnsNames,
		IPAddresses:           ips,
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
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

// loadPinnedCA reads the controller certificate the sensor pins. Read at
// every dial, so a certificate the kubelet has just updated in the mounted
// ConfigMap is picked up without a restart.
func loadPinnedCA(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("%s is empty: the controller has not published its certificate yet", path)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%s holds no PEM certificate", path)
	}
	return pool, nil
}

// ServerTLS is the stream listener's TLS config for cert. A sensor that
// presents no client certificate authenticates with its token as before. One
// that presents a certificate must present cert itself: the otlp role's pods
// hold it to serve OTLP, so it is their identity, pinned as the replica mesh
// pins it (it has no client-auth usage, so it cannot be verified as a chain).
func ServerTLS(cert tls.Certificate) *tls.Config {
	pinned := cert.Certificate[0]
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequestClientCert,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) > 0 && !bytes.Equal(raw[0], pinned) {
				return errors.New("sensor stream: the client certificate is not the pinned one")
			}
			return nil
		},
	}
}

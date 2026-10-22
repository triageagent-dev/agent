// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package mesh

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// TLS is the mesh's mutual-TLS setup. Every replica holds the same
// certificate (the controller's sensor-stream certificate, kept in one
// Secret), so both sides present it and accept only a peer presenting exactly
// that certificate: the same pin the sensors use, applied both ways.
type TLS struct {
	Server *tls.Config
	Client *tls.Config
	leaf   []byte
}

// NewTLS builds the server and client configs from this replica's
// certificate. serverName is a DNS name the certificate carries; peers are
// dialed by pod IP, so the dial verifies that name instead of the address.
func NewTLS(cert tls.Certificate, serverName string) (*TLS, error) {
	if len(cert.Certificate) == 0 {
		return nil, errors.New("mesh tls: no certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("mesh tls: parse certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	t := &TLS{leaf: cert.Certificate[0]}
	// The certificate is a server certificate (no client-auth usage), so the
	// server cannot verify it as a client chain; it pins it instead.
	t.Server = &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{cert},
		ClientAuth:            tls.RequireAnyClientCert,
		VerifyPeerCertificate: t.verifyPinned,
	}
	t.Client = &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{cert},
		RootCAs:               pool,
		ServerName:            serverName,
		VerifyPeerCertificate: t.verifyPinned,
	}
	return t, nil
}

// verifyPinned accepts only a peer whose leaf certificate is this replica's
// own.
func (t *TLS) verifyPinned(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 || !t.pinned(rawCerts[0]) {
		return errors.New("mesh tls: peer certificate is not the pinned one")
	}
	return nil
}

func (t *TLS) pinned(der []byte) bool { return bytes.Equal(der, t.leaf) }

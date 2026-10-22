// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"crypto/tls"
	"crypto/x509"
)

// ServerTLS returns the listener's TLS configuration: TLS 1.3 only. With
// clientCAs set, a client certificate must chain to it; it is mandatory
// (RequireAndVerifyClientCert) unless tokens is true, when a sender without
// one may still present an allowed ServiceAccount token
// (VerifyClientCertIfGiven).
func ServerTLS(cert tls.Certificate, clientCAs *x509.CertPool, tokens bool) *tls.Config {
	tc := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	if clientCAs != nil {
		tc.ClientCAs = clientCAs
		tc.ClientAuth = tls.RequireAndVerifyClientCert
		if tokens {
			tc.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return tc
}

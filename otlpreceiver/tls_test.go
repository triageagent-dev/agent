// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func TestServerTLS(t *testing.T) {
	pool := x509.NewCertPool()
	for name, c := range map[string]struct {
		pool   *x509.CertPool
		tokens bool
		want   tls.ClientAuthType
	}{
		"tokens only":             {nil, true, tls.NoClientCert},
		"client CA only":          {pool, false, tls.RequireAndVerifyClientCert},
		"client CA and allowlist": {pool, true, tls.VerifyClientCertIfGiven},
	} {
		tc := ServerTLS(tls.Certificate{}, c.pool, c.tokens)
		if tc.MinVersion != tls.VersionTLS13 {
			t.Errorf("%s: MinVersion = %x, want TLS 1.3", name, tc.MinVersion)
		}
		if tc.ClientAuth != c.want {
			t.Errorf("%s: ClientAuth = %v, want %v", name, tc.ClientAuth, c.want)
		}
	}
}

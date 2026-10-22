// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"strings"
	"testing"
)

// TestCheckPushCredentials pins the credential policy: the join certificate
// is the only way to authenticate to the core, so a core endpoint without the
// core CA fingerprint is refused, in a pod and on a host alike. Without an
// endpoint (standalone) nothing is sent, so nothing is needed.
func TestCheckPushCredentials(t *testing.T) {
	const fp = "SHA256:0000000000000000000000000000000000000000000000000000000000000000"
	const endpoint = "wss://core.example.internal/events/ws"

	for _, tc := range []struct {
		name         string
		endpoint, fp string
		wantErr      bool
	}{
		{"endpoint, no fingerprint: refused", endpoint, "", true},
		{"endpoint, fingerprint: accepted", endpoint, fp, false},
		{"standalone, no fingerprint: accepted", "", "", false},
		{"standalone, fingerprint: accepted", "", fp, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPushCredentials(tc.endpoint, tc.fp)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, errNeedsJoin) {
				t.Errorf("err = %v, want errNeedsJoin", err)
			}
		})
	}
	if !strings.Contains(errNeedsJoin.Error(), "--join-ca-fingerprint") {
		t.Errorf("refusal message %q does not name --join-ca-fingerprint", errNeedsJoin)
	}
	if strings.Contains(errNeedsJoin.Error(), "token") {
		t.Errorf("refusal message %q still points at a token", errNeedsJoin)
	}
}

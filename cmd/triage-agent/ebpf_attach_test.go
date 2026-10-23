//go:build obi

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"go.opentelemetry.io/obi/pkg/export/imetrics"
)

func TestAttachReporter(t *testing.T) {
	r := newAttachReporter()
	if got := r.status(); got != "" {
		t.Fatalf("status with no failures = %q, want empty", got)
	}

	// Other error types are not attach failures.
	r.InstrumentationError("payment", imetrics.InstrumentationErrorSymbolNotFound)
	if got := r.status(); got != "" {
		t.Fatalf("status after a non-attach error = %q, want empty", got)
	}

	// Non-Go failures get no SYS_ADMIN hint.
	r.InstrumentationError("valkey-server", imetrics.InstrumentationErrorAttachingUprobe)
	r.InstrumentationError("ruby", imetrics.InstrumentationErrorAttachingUprobe)
	got := r.status()
	if !strings.Contains(got, "2 non-Go executables (last: ruby)") || strings.Contains(got, "SYS_ADMIN") {
		t.Fatalf("non-Go status = %q", got)
	}

	// A Go binary is reported twice by OBI; it counts once, under Go.
	r.InstrumentationError("checkout", imetrics.InstrumentationErrorAttachingUprobe)
	r.InstrumentationError("checkout", imetrics.InstrumentationErrorAttachingGoUprobe)
	got = r.status()
	for _, want := range []string{
		"Go uprobes failed to attach for 1 executables (last: checkout)",
		"SYS_ADMIN",
		"2 non-Go executables (last: ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status = %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "last: checkout)") && strings.Count(got, "checkout") != 1 {
		t.Errorf("a Go binary was also named as non-Go: %q", got)
	}
}

func TestAttachReporter_Bounded(t *testing.T) {
	r := newAttachReporter()
	for i := 0; i < maxAttachFailures+10; i++ {
		r.InstrumentationError(strings.Repeat("x", i+1), imetrics.InstrumentationErrorAttachingUprobe)
	}
	if n := len(r.all.execs); n != maxAttachFailures {
		t.Fatalf("remembered %d executables, want %d", n, maxAttachFailures)
	}
}

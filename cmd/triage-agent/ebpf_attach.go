//go:build obi

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/obi/pkg/export/imetrics"
)

// maxAttachFailures bounds each set of executables attachReporter remembers.
const maxAttachFailures = 256

// attachReporter is OBI's internal-metrics reporter in the ebpf role
// (imetrics.InternalMetricsConfig.Reporter). OBI logs a failed uprobe attach
// only at debug level, so a service whose probes did not attach silently
// reports no RED. attachReporter keeps those failures so the role can report
// them in sensor_state. Go binaries are kept apart: on a kernel that refuses
// Go uprobes without SYS_ADMIN (ADR-0016) they all fail the same way and
// have a known fix, which the other failures do not.
type attachReporter struct {
	imetrics.NoopReporter

	mu  sync.Mutex
	all failureSet
	goE failureSet
}

// failureSet is a bounded set of executable names and the latest one added.
type failureSet struct {
	execs map[string]struct{}
	last  string
}

func (f *failureSet) add(name string) {
	if f.execs == nil {
		f.execs = map[string]struct{}{}
	}
	if _, ok := f.execs[name]; !ok && len(f.execs) < maxAttachFailures {
		f.execs[name] = struct{}{}
	}
	f.last = name
}

func newAttachReporter() *attachReporter { return &attachReporter{} }

// obiAttach is the process's reporter: runEBPF hands it to OBI, and the ebpf
// role's metrics source reads it.
var obiAttach = newAttachReporter()

// InstrumentationError records an executable whose uprobes did not attach.
// OBI reports a Go binary's failure twice, as attaching_uprobe and as
// attaching_go_uprobe.
func (r *attachReporter) InstrumentationError(processName, errorType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch errorType {
	case imetrics.InstrumentationErrorAttachingUprobe:
		r.all.add(processName)
	case imetrics.InstrumentationErrorAttachingGoUprobe:
		r.goE.add(processName)
	}
}

// status is the role's lastError while any executable failed to attach,
// empty otherwise. Only Go failures carry the SYS_ADMIN hint.
func (r *attachReporter) status() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var parts []string
	if n := len(r.goE.execs); n > 0 {
		parts = append(parts, fmt.Sprintf("Go uprobes failed to attach for %d executables (last: %s); kernels that refuse Go uprobes need SYS_ADMIN in ebpf.extraCapabilities (ADR-0016)", n, r.goE.last))
	}
	other, last := 0, ""
	for name := range r.all.execs {
		if _, isGo := r.goE.execs[name]; !isGo {
			other++
		}
	}
	if _, isGo := r.goE.execs[r.all.last]; !isGo {
		last = r.all.last
	} else {
		for name := range r.all.execs {
			if _, isGo := r.goE.execs[name]; !isGo && (last == "" || name < last) {
				last = name
			}
		}
	}
	if other > 0 {
		parts = append(parts, fmt.Sprintf("uprobes failed to attach for %d non-Go executables (last: %s)", other, last))
	}
	return strings.Join(parts, "; ")
}

// StatusError implements sensorstream.RoleStatus for the ebpf role.
func (s *obiMetricsStore) StatusError() string { return obiAttach.status() }

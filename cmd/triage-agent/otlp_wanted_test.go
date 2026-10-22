// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/triageagent-dev/agent/otlpreceiver"
)

// The receiver drops a metric no collector reads at intake, so every name the
// apps source reads must be on its list.
func TestWorkloadMetricsAreReadByCollectors(t *testing.T) {
	for _, w := range workloadMetrics {
		for _, name := range []string{w.desired, w.ready} {
			if !otlpreceiver.ReadByCollectors(name) {
				t.Errorf("%s (%s) is read by the apps source but dropped at intake", name, w.kind)
			}
		}
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
)

func TestReadByCollectors(t *testing.T) {
	for name, want := range map[string]bool{
		"http.server.request.duration":      true,
		"http.server.duration":              true, // renamed to the one above
		"rpc.client.call.duration":          true,
		"db.client.operation.duration":      true,
		"traces.span.metrics.duration":      true,
		"log.errors":                        true,
		"container_cpu_usage_seconds_total": true,
		"machine_memory_bytes":              true,
		"use.mem.psi.avg60":                 true,
		"k8s.deployment.desired":            true,
		"k8s.pod.phase":                     false,
		"k8s.container.restarts":            false,
		"go.memory.used":                    false,
		"otelcol_process_uptime":            false,
		"otel.sdk.log.created":              false,
		"http.server.request.body.size":     false,
	} {
		if got := ReadByCollectors(name); got != want {
			t.Errorf("ReadByCollectors(%q) = %v, want %v", name, got, want)
		}
	}
}

// A sender that sends everything it has must not spend its resource quota on
// metrics nobody reads: the resource that carries a RED histogram still fits.
func TestWanted_UnreadMetricsTakeNoQuota(t *testing.T) {
	var got []sunk
	now := time.Unix(1_800_000_000, 0)
	r := New(Options{
		Auth: okAuth{}, Log: logr.Discard(), now: func() time.Time { return now },
		Sink: func(key, role string, rms []*metricspb.ResourceMetrics) {
			got = append(got, sunk{key: key, role: role, set: metricset.FromOTLP(rms)})
		},
		MaxResourcesPerSender: 3,
		Wanted:                ReadByCollectors,
	})
	for i := 0; i < 20; i++ {
		res := resource("service.name", fmt.Sprintf("pod-%d", i), "k8s.namespace.name", "shop")
		resp := export(t, r, authed(), res, counter("k8s.pod.phase", 100, 1), counter("go.memory.used", 100, 5))
		if resp.GetPartialSuccess().GetRejectedDataPoints() != 0 {
			t.Fatalf("an unread metric was refused instead of dropped: %v", resp.GetPartialSuccess())
		}
	}
	res := resource("service.name", "cart", "k8s.namespace.name", "shop")
	resp := export(t, r, authed(), res, counter("k8s.pod.phase", 100, 1), counter("log.errors", 100, 3))
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 0 {
		t.Fatalf("the resource with a read metric was refused: %v", resp.GetPartialSuccess())
	}
	r.flush()
	if len(got) != 1 {
		t.Fatalf("flushed %d sources, want only the one that carries log.errors", len(got))
	}
	if got[0].set.Get("log.errors") == nil || got[0].set.Get("k8s.pod.phase") != nil {
		t.Errorf("stored metrics = %v, want log.errors without k8s.pod.phase", got[0].set)
	}
	if r.unread != 41 {
		t.Errorf("unread = %d, want 41 (20 resources x 2 points, plus 1)", r.unread)
	}
}

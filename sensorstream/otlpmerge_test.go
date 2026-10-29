// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
)

func mergedPoints(t *testing.T, h *Hub, key, role, name string) map[string]float64 {
	t.Helper()
	h.mu.RLock()
	defer h.mu.RUnlock()
	e := h.nodes[OTLPNodePrefix+key]
	if e == nil {
		t.Fatalf("source %s not in the hub", key)
	}
	out := map[string]float64{}
	if m := e.snapshots[role].set.Get(name); m != nil {
		for _, p := range m.Points {
			out[p.Attr("k")] = p.Value
		}
	}
	return out
}

// A source reaches the hub one export at a time, and an export may carry a
// part of its series: the hub merges them per series, so the set the
// collectors read is the union, with the newest value of each.
func TestIngest_MergesASourcesExportsPerSeries(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := NewHub(HubOptions{Log: logr.Discard(), Stale: time.Minute, now: func() time.Time { return now }})
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, map[string]string{"k": "a"})})
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 5, map[string]string{"k": "b"})})
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 3, map[string]string{"k": "a"})})
	if got := mergedPoints(t, h, "src", metricset.RoleEBPF, "requests"); got["a"] != 3 || got["b"] != 5 || len(got) != 2 {
		t.Fatalf("merged = %v, want a:3 (the newer) and b:5", got)
	}
	if n := h.HeldPoints(); n != 2 {
		t.Fatalf("held points = %d, want one per series", n)
	}
}

// A series no export refreshed for the retention is dropped from the
// merged set; the rest stays.
func TestIngest_DropsSeriesNoExportRefreshed(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := NewHub(HubOptions{Log: logr.Discard(), Stale: time.Minute, now: func() time.Time { return now }})
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, map[string]string{"k": "gone"})})
	now = now.Add(otlpPointRetainFactor*time.Minute + time.Second)
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 2, map[string]string{"k": "live"})})
	if got := mergedPoints(t, h, "src", metricset.RoleEBPF, "requests"); len(got) != 1 || got["live"] != 2 {
		t.Fatalf("merged = %v, want live alone", got)
	}
	// A metric whose every series went is gone with them.
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("other", 1, map[string]string{"k": "x"})})
	now = now.Add(otlpPointRetainFactor*time.Minute + time.Second)
	h.Ingest("src", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("other", 2, map[string]string{"k": "x"})})
	h.mu.RLock()
	set := h.nodes[OTLPNodePrefix+"src"].snapshots[metricset.RoleEBPF].set
	h.mu.RUnlock()
	if set.Get("requests") != nil || set.Get("other") == nil {
		t.Fatalf("set = %v, want other alone", set)
	}
}

// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/internal/mesh"
	"github.com/triageagent-dev/agent/metricset"
)

// An OTLP source is a collector target like a sensor node, but not a
// sensor: it stays out of Nodes() and so out of sensor_state.
func TestIngest_TargetNotSensor(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := newTestHub(now)
	h.Ingest("abc", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 5, nil)})

	if got := h.Targets(metricset.RoleEBPF); len(got) != 1 || got[0] != TargetPrefix+OTLPNodePrefix+"abc" {
		t.Fatalf("targets = %v", got)
	}
	if got, _ := requests(t, h, OTLPNodePrefix+"abc"); got != 5 {
		t.Fatalf("requests = %v", got)
	}
	if n := h.Nodes(); len(n) != 0 {
		t.Fatalf("an OTLP source is listed as a sensor: %+v", n)
	}
}

// What one replica ingests reaches the others through gossip, and a newer
// record from a peer replaces it: the source moved to that replica.
func TestIngest_GossipAndMove(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	var sent []mesh.NodeSnapshot
	a := NewHub(HubOptions{Log: logr.Discard(), now: func() time.Time { return t0 }, Gossip: func(r mesh.NodeSnapshot) { sent = append(sent, r) }})
	a.Ingest("abc", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 5, nil)})
	if len(sent) != 1 {
		t.Fatalf("gossiped %d records", len(sent))
	}

	b := newTestHub(t0.Add(time.Second))
	b.Merge(sent)
	if got, _ := requests(t, b, OTLPNodePrefix+"abc"); got != 5 {
		t.Fatalf("peer requests = %v", got)
	}
	if len(a.Local()) != 1 {
		t.Fatalf("the ingesting replica does not replay its source to a new peer: %d", len(a.Local()))
	}

	a.Merge([]mesh.NodeSnapshot{record(t, OTLPNodePrefix+"abc", 1, t0.Add(time.Minute), 9)})
	if got, _ := requests(t, a, OTLPNodePrefix+"abc"); got != 9 {
		t.Fatalf("a newer peer record did not replace the ingested one: %v", got)
	}
}

func TestIngest_PrunesGoneSources(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := NewHub(HubOptions{Log: logr.Discard(), Stale: time.Minute, now: func() time.Time { return now }})
	h.Ingest("old", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	now = now.Add(otlpRetainFactor*time.Minute + 2*time.Minute)
	h.Ingest("new", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 1, nil)})
	h.mu.RLock()
	_, kept := h.nodes[OTLPNodePrefix+"old"]
	h.mu.RUnlock()
	if kept {
		t.Fatal("a source silent past the retention was kept")
	}
}

func TestOTLPSources_Summary(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := newTestHub(now)
	h.Ingest("abc", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("requests", 5, map[string]string{"service.name": "cart", "k8s.namespace.name": "shop"})})
	h.Ingest("abc", metricset.RoleNodeExporter, []*metricspb.ResourceMetrics{counter("use.cpu.usage.usec", 5, map[string]string{"k8s.pod.name": "cart-1", "k8s.namespace.name": "shop"})})
	got := h.OTLPSources()
	if len(got) != 1 || got[0].Service != "cart" || got[0].Namespace != "shop" || len(got[0].Families) != 2 {
		t.Fatalf("sources = %+v", got)
	}
	for _, f := range got[0].Families {
		if f.Role == metricset.RoleNodeExporter && f.Pods != 1 {
			t.Fatalf("use family pods = %d, want 1", f.Pods)
		}
	}
}

// A cAdvisor source names no namespace: its pods span many.
func TestOTLPSources_CAdvisorLabel(t *testing.T) {
	h := newTestHub(time.Unix(1_800_000_000, 0))
	h.Ingest("node", metricset.RoleNodeExporter, []*metricspb.ResourceMetrics{counter("use.cpu.usage.usec", 5, map[string]string{"k8s.pod.name": "a-1", "k8s.namespace.name": "triage"})})
	got := h.OTLPSources()
	if len(got) != 1 || got[0].Service != "kubelet-cadvisor" || got[0].Namespace != "" || got[0].Families[0].Pods != 1 {
		t.Fatalf("sources = %+v", got)
	}
}

// A k8s_cluster workload resource carries no service.name: it is named by its
// workload ("deployment/cart"), so it reads apart from the service's own source.
func TestOTLPSources_WorkloadResourceNamedByWorkload(t *testing.T) {
	h := newTestHub(time.Unix(1_800_000_000, 0))
	h.Ingest("w1", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("k8s.deployment.desired", 2, map[string]string{"k8s.deployment.name": "cart", "k8s.namespace.name": "shop"})})
	h.Ingest("w2", metricset.RoleEBPF, []*metricspb.ResourceMetrics{counter("k8s.statefulset.desired_pods", 1, map[string]string{"k8s.statefulset.name": "db", "k8s.namespace.name": "shop"})})
	got := map[string]string{}
	for _, s := range h.OTLPSources() {
		got[s.Key] = s.Workload + "@" + s.Namespace + ":" + s.Service
	}
	if got["w1"] != "deployment/cart@shop:" || got["w2"] != "statefulset/db@shop:" {
		t.Fatalf("sources = %v, want the workload names", got)
	}
}

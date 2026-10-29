// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"sort"
	"strings"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/sensorstream/sensorpb"
)

// OTLPNodePrefix marks a hub entry that holds one OTLP source's metrics
// (one resource pushed to the OTLP receiver) rather than a sensor node.
const OTLPNodePrefix = metricset.OTLPSourcePrefix

// otlpRetainFactor times Stale is how long an OTLP source's entry is kept
// after its last metrics; a restarted process is a new source, so entries
// of gone ones must be dropped.
const otlpRetainFactor = 5

// Ingest stores rms as role's latest snapshot of the OTLP source node
// (OTLPNodePrefix + a stable key) and gossips it to the peers. Unlike a
// sensor stream, an OTLP source can move between replicas with every
// reconnect, so its entry is never local: the newest snapshot wins on
// every replica, this one's included.
func (h *Hub) Ingest(node, role string, rms []*metricspb.ResourceMetrics) {
	if !strings.HasPrefix(node, OTLPNodePrefix) {
		node = OTLPNodePrefix + node
	}
	set := metricset.FromOTLP(rms)
	h.mu.Lock()
	h.ingestSeq++
	seq := h.ingestSeq
	h.mu.Unlock()
	raw := h.encode(&sensorpb.StreamRequest{Seq: seq, Body: &sensorpb.StreamRequest_Snapshot{Snapshot: &sensorpb.Snapshot{
		Role: role, ResourceMetrics: rms,
	}}})

	now := h.opts.now()
	h.mu.Lock()
	e := h.nodes[node]
	if e == nil {
		e = &nodeEntry{snapshots: map[string]snapshot{}}
		h.nodes[node] = e
	}
	merged := mergeOTLP(e.snapshots[role], set, now, otlpPointRetainFactor*h.opts.Stale)
	merged.at, merged.seq, merged.raw = now, seq, raw
	e.snapshots[role] = merged
	e.lastSeen, e.ingestedAt = now, now
	if now.Sub(h.lastPrune) >= time.Minute { // ingest is hot; prune at most once a minute here
		h.pruneLocked(now)
	}
	h.mu.Unlock()
	h.gossip(node, seq, now, raw)
}

// OTLPFamily is what one OTLP source feeds one sensor role.
type OTLPFamily struct {
	Role            string
	Metrics, Points int
	Pods            int
}

// OTLPSource is one OTLP source as the hub holds it: every role's latest
// snapshot no older than Stale, from this replica or a peer.
type OTLPSource struct {
	Key, Service, Namespace, Instance string
	// Workload names the Deployment, StatefulSet or DaemonSet a k8s_cluster
	// resource describes ("deployment/ad"); empty for a service's own metrics.
	Workload string
	LastSeen time.Time
	Families []OTLPFamily
}

// otlpWorkloadName names the workload a Collector's k8s_cluster receiver
// reports a point for, as "<kind>/<name>" ("deployment/ad"); those resources
// carry no service.name. Empty for any other point.
func otlpWorkloadName(p metricset.Point) string {
	for _, k := range []string{"deployment", "statefulset", "daemonset"} {
		if v := p.Attr("k8s." + k + ".name"); v != "" {
			return k + "/" + v
		}
	}
	return ""
}

// OTLPSources lists the fresh OTLP sources, sorted by namespace, service
// and key.
func (h *Hub) OTLPSources() []OTLPSource {
	now := h.opts.now()
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []OTLPSource
	for node, e := range h.nodes {
		if !strings.HasPrefix(node, OTLPNodePrefix) {
			continue
		}
		src := OTLPSource{Key: strings.TrimPrefix(node, OTLPNodePrefix), LastSeen: e.lastSeen}
		roles := make([]string, 0, len(e.snapshots))
		for r := range e.snapshots {
			roles = append(roles, r)
		}
		sort.Strings(roles)
		for _, role := range roles {
			s := e.snapshots[role]
			if now.Sub(s.at) > h.opts.Stale {
				continue
			}
			f := OTLPFamily{Role: role, Metrics: len(s.set)}
			pods := map[string]bool{}
			for _, m := range s.set {
				f.Points += len(m.Points)
				for _, p := range m.Points {
					// A use set from kubelet cAdvisor spans every pod on a
					// node: its points name pods, not the source.
					if role == metricset.RoleNodeExporter {
						if pod := p.Attr("k8s.pod.name"); pod != "" {
							pods[pod] = true
						}
						continue
					}
					if src.Service == "" {
						src.Service = p.Attr("service.name")
					}
					if src.Workload == "" {
						src.Workload = otlpWorkloadName(p)
					}
					if src.Namespace == "" {
						src.Namespace = p.Attr("k8s.namespace.name")
					}
					if src.Instance == "" {
						src.Instance = p.Attr("service.instance.id")
					}
				}
			}
			f.Pods = len(pods)
			src.Families = append(src.Families, f)
		}
		if len(src.Families) == 0 {
			continue
		}
		if src.Service == "" && len(src.Families) == 1 && src.Families[0].Role == metricset.RoleNodeExporter {
			src.Service = "kubelet-cadvisor"
		}
		out = append(out, src)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		return a.Key < b.Key
	})
	return out
}

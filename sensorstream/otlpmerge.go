// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package sensorstream

import (
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/triageagent-dev/agent/metricset"
)

// An OTLP source reaches the hub one export at a time: the otlp role and
// the controller's own receiver translate and forward each export as it
// arrives and hold nothing (package otlpreceiver). A source whose series
// come in several exports, or whose Collector splits a batch, is whole
// only here, so the hub merges a source's exports per series: the newest
// point of each attribute set is the one held, and a series no export
// refreshed for otlpPointRetainFactor times Stale is dropped. The merged
// set is what the collectors read, and what a replica holds of the
// cluster: one Point per live series.

// otlpPointRetainFactor times Stale is how long a merged series lives
// without an export refreshing it.
const otlpPointRetainFactor = 2

// metricIndex locates a metric's points by the hash of their attributes,
// with when each was last refreshed.
type metricIndex struct {
	at   map[uint64]int
	seen []time.Time
}

// mergeOTLP returns cur with the points of set merged in, as of now, and
// cur's series not refreshed for retain dropped. cur may be the zero
// snapshot.
func mergeOTLP(cur snapshot, set metricset.Set, now time.Time, retain time.Duration) snapshot {
	if cur.set == nil {
		cur.set = metricset.Set{}
	}
	if cur.index == nil {
		cur.index = map[string]*metricIndex{}
	}
	for name, m := range set {
		held := cur.set[name]
		if held == nil || held.Kind != m.Kind {
			held = &metricset.Metric{Name: name, Kind: m.Kind}
			cur.set[name] = held
			cur.index[name] = &metricIndex{at: map[uint64]int{}}
		}
		idx := cur.index[name]
		for _, p := range m.Points {
			h := attrsHash(p.Attrs)
			if i, ok := idx.at[h]; ok {
				held.Points[i], idx.seen[i] = p, now
				continue
			}
			idx.at[h] = len(held.Points)
			held.Points = append(held.Points, p)
			idx.seen = append(idx.seen, now)
		}
	}
	for name, held := range cur.set {
		idx := cur.index[name]
		kept, seen := held.Points[:0], idx.seen[:0]
		for i, p := range held.Points {
			if now.Sub(idx.seen[i]) > retain {
				continue
			}
			kept, seen = append(kept, p), append(seen, idx.seen[i])
		}
		if len(kept) == 0 {
			delete(cur.set, name)
			delete(cur.index, name)
			continue
		}
		if len(kept) != len(held.Points) {
			clear(held.Points[len(kept):])
			held.Points, idx.seen = kept, seen
			idx.at = make(map[uint64]int, len(kept))
			for i, p := range kept {
				idx.at[attrsHash(p.Attrs)] = i
			}
		}
	}
	return cur
}

// attrsHash is a point's identity: its attributes, sorted, hashed.
func attrsHash(attrs map[string]string) uint64 {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(attrs[k]))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// HeldPoints is the number of points the hub holds for OTLP sources over
// every role: what the merge keeps for the cluster.
func (h *Hub) HeldPoints() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for node, e := range h.nodes {
		if !strings.HasPrefix(node, OTLPNodePrefix) {
			continue
		}
		for _, s := range e.snapshots {
			for _, m := range s.set {
				n += len(m.Points)
			}
		}
	}
	return n
}

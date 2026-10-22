// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"fmt"

	"github.com/triageagent-dev/agent/internal/ledger"
)

// The core refuses a whole frame over one entry it cannot place: a service
// with no namespace or name, a topology node or edge with a missing end. Such
// an entry is real data about a source that carries no Kubernetes identity (an
// OTLP sender whose Collector did not enrich it), so it is dropped here, by
// name, instead of taking the rest of the frame down with it.

// What the drop records say.
const (
	reasonNoNamespace = "no k8s.namespace.name on the source"
	reasonNoName      = "no service name on the source"
	reasonBadEdge     = "an end of the edge has no namespace or workload"
)

// dropMalformed removes the entries the core would refuse the frame over. A
// frame left with no entries is dropped (nil), except a topology snapshot,
// which is level state and is kept. It is the choke point every collector
// frame passes through (enqueueOK), next to dropExcluded.
func (p *Pusher) dropMalformed(f *Frame) *Frame {
	if f == nil {
		return nil
	}
	switch f.Kind {
	case KindLogMetrics:
		if f.LogMetrics != nil {
			kept, n := keepWellFormed(p, f.Kind, f.LogMetrics.Services, func(s ServiceLogMetrics) (string, string) { return s.Namespace, s.Service })
			if n > 0 {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.LogMetrics
				pl.Services, nf.LogMetrics = kept, &pl
				return &nf
			}
		}
	case KindRedMetrics:
		if f.RedMetrics != nil {
			kept, n := keepWellFormed(p, f.Kind, f.RedMetrics.Services, redKey)
			if n > 0 {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.RedMetrics
				pl.Services, nf.RedMetrics = kept, &pl
				return &nf
			}
		}
	case KindRedSummary:
		if f.RedSummary != nil {
			kept, n := keepWellFormed(p, f.Kind, f.RedSummary.Services, redKey)
			if n > 0 {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.RedSummary
				pl.Services, nf.RedSummary = kept, &pl
				return &nf
			}
		}
	case KindUseMetrics:
		if f.UseMetrics != nil {
			kept, n := keepWellFormed(p, f.Kind, f.UseMetrics.Services, func(s ServiceUseMetrics) (string, string) { return s.Namespace, s.Service })
			if n > 0 {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.UseMetrics
				pl.Services, nf.UseMetrics = kept, &pl
				return &nf
			}
		}
	case KindUseSummary:
		if f.UseSummary != nil {
			kept, n := keepWellFormed(p, f.Kind, f.UseSummary.Services, func(s ServiceUseSummary) (string, string) { return s.Namespace, s.Service })
			if n > 0 {
				if len(kept) == 0 {
					return nil
				}
				nf, pl := *f, *f.UseSummary
				pl.Services, nf.UseSummary = kept, &pl
				return &nf
			}
		}
	case KindTopologySnapshot:
		if f.TopologySnapshot != nil {
			nodes, dn := keepWellFormed(p, f.Kind, f.TopologySnapshot.Nodes, func(n TopologyNode) (string, string) { return n.Namespace, n.Workload })
			edges, de := keepWellFormedEdges(p, f.Kind, f.TopologySnapshot.Edges)
			if dn > 0 || de > 0 {
				nf, pl := *f, *f.TopologySnapshot
				pl.Nodes, pl.Edges, nf.TopologySnapshot = nodes, edges, &pl
				return &nf
			}
		}
	}
	return f
}

// keepWellFormed returns items without the ones that name no namespace or no
// service, and how many that was. The input slice is never modified: a
// collector may still hold it.
func keepWellFormed[T any](p *Pusher, kind string, items []T, key func(T) (namespace, name string)) ([]T, int) {
	dropped := 0
	var out []T
	for i, it := range items {
		ns, name := key(it)
		reason := ""
		switch {
		case ns == "":
			reason = reasonNoNamespace
		case name == "":
			reason = reasonNoName
		}
		if reason == "" {
			if out != nil {
				out = append(out, it)
			}
			continue
		}
		if out == nil {
			out = append(make([]T, 0, len(items)-1), items[:i]...)
		}
		dropped++
		p.recordMalformed(kind, ns+"/"+name, reason)
	}
	if dropped == 0 {
		return items, 0
	}
	return out, dropped
}

// keepWellFormedEdges is keepWellFormed for topology edges: a source with both
// names, and either a destination with both names or an external host.
func keepWellFormedEdges(p *Pusher, kind string, edges []TopologyEdge) ([]TopologyEdge, int) {
	dropped := 0
	var out []TopologyEdge
	for i, e := range edges {
		src, dst := e.SrcNamespace+"/"+e.SrcWorkload, e.External
		ok := e.SrcNamespace != "" && e.SrcWorkload != ""
		if e.External == "" {
			dst = e.DstNamespace + "/" + e.DstWorkload
			ok = ok && e.DstNamespace != "" && e.DstWorkload != ""
		}
		if ok {
			if out != nil {
				out = append(out, e)
			}
			continue
		}
		if out == nil {
			out = append(make([]TopologyEdge, 0, len(edges)-1), edges[:i]...)
		}
		dropped++
		p.recordMalformed(kind, fmt.Sprintf("%s -> %s", src, dst), reasonBadEdge)
	}
	if dropped == 0 {
		return edges, 0
	}
	return out, dropped
}

// recordMalformed counts one entry held back and names it in the ledger, which
// the local UI lists.
func (p *Pusher) recordMalformed(kind, subject, reason string) {
	n := p.malformedDropped.Add(1)
	ledger.Default.Drop(kind, subject, reason)
	if n == 1 || n%1000 == 0 {
		p.log.Info("dropping an entry the core would refuse the frame over", "frame", kind, "entry", subject, "reason", reason, "droppedTotal", n)
	}
}

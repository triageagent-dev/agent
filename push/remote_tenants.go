// Remote tenants: tenants that reach this agent as OTLP metrics from a remote
// Collector, not as annotated namespaces of this cluster.
//
// The OTLP receiver qualifies every namespace of such a sender as
// "<tenant>~<namespace>" (internal/remotens), so the tenant travels with the
// data through the collectors. This file is the push side of that convention:
// routing a qualified namespace to its tenant, keeping that tenant's
// connection alive while data keeps arriving, and removing the qualification
// from the frames that leave for the core, which knows the tenant from the
// frame's own stamp and the namespace as the sender named it.
package push

import (
	"time"

	"github.com/triageagent-dev/agent/internal/remotens"
)

// remoteTenantTTL is how long a remote tenant stays active after its last
// data. It is longer than the OTLP receiver's expiry of a silent source, so a
// tenant's worker is not retired between two slow exports.
const remoteTenantTTL = 15 * time.Minute

// noteRemoteTenant records that data for tenant is flowing. The first data of
// a tenant, or the first after it went quiet, also sends its snapshot
// sequence on the live connection, as a namespace joining a tenant does, so
// the core brackets the tenant's state before its metrics. It runs inside the
// split that is about to enqueue that data, so the sequence is queued first.
// A tenant that already holds an annotated namespace has its sequence live
// and is not sent a second one.
func (p *Pusher) noteRemoteTenant(tenant string) {
	now := time.Now()
	prev, loaded := p.remoteSeen.Swap(tenant, now)
	if loaded && now.Sub(prev.(time.Time)) < remoteTenantTTL {
		return
	}
	if p.dynamicScope != nil && p.dynamicScope.tenantCounts()[tenant] > 0 {
		return
	}
	p.sendTenantSnapshotOnLive(tenant)
}

// activeRemoteTenants lists the remote tenants with data inside the TTL.
func (p *Pusher) activeRemoteTenants() []string {
	var out []string
	cut := time.Now().Add(-remoteTenantTTL)
	p.remoteSeen.Range(func(k, v any) bool {
		if v.(time.Time).After(cut) {
			out = append(out, k.(string))
		}
		return true
	})
	return out
}

// unqualifyFrame removes the "<tenant>~" prefix from every namespace in f,
// which is the frame split off for tenant. Namespaces without that prefix are
// left alone.
func unqualifyFrame(f *Frame, tenant string) {
	strip := func(ns *string) { *ns = remotens.Strip(tenant, *ns) }
	// Apps are the agent's own state, shared with later snapshots: the frame
	// gets a copy with the plain namespace, never the original.
	stripApp := func(a *VirtualApp) *VirtualApp {
		if a == nil || remotens.Strip(tenant, a.Namespace) == a.Namespace {
			return a
		}
		c := *a
		c.Namespace = remotens.Strip(tenant, a.Namespace)
		return &c
	}
	switch f.Kind {
	case KindSnapshot:
		if f.Snapshot != nil {
			apps := make([]*VirtualApp, len(f.Snapshot.Apps))
			for i, a := range f.Snapshot.Apps {
				apps[i] = stripApp(a)
			}
			f.Snapshot = &SnapshotPayload{Apps: apps}
		}
	case KindAppDelta:
		if f.AppDelta != nil {
			f.AppDelta = &AppDeltaPayload{Op: f.AppDelta.Op, App: stripApp(f.AppDelta.App)}
		}
	case KindLogMetrics:
		if f.LogMetrics != nil {
			for i := range f.LogMetrics.Services {
				strip(&f.LogMetrics.Services[i].Namespace)
			}
		}
	case KindRedMetrics:
		if f.RedMetrics != nil {
			for i := range f.RedMetrics.Services {
				strip(&f.RedMetrics.Services[i].Namespace)
			}
		}
	case KindRedSummary:
		if f.RedSummary != nil {
			for i := range f.RedSummary.Services {
				strip(&f.RedSummary.Services[i].Namespace)
			}
		}
	case KindUseMetrics:
		if f.UseMetrics != nil {
			for i := range f.UseMetrics.Services {
				strip(&f.UseMetrics.Services[i].Namespace)
			}
		}
	case KindUseSummary:
		if f.UseSummary != nil {
			for i := range f.UseSummary.Services {
				strip(&f.UseSummary.Services[i].Namespace)
			}
		}
	case KindTopologySnapshot:
		if f.TopologySnapshot != nil {
			for i := range f.TopologySnapshot.Nodes {
				strip(&f.TopologySnapshot.Nodes[i].Namespace)
			}
			for i := range f.TopologySnapshot.Edges {
				strip(&f.TopologySnapshot.Edges[i].SrcNamespace)
				strip(&f.TopologySnapshot.Edges[i].DstNamespace)
			}
		}
	}
}

// StripRemoteNamespace is unqualifyFrame's single-namespace form, for status
// reports that carry a namespace outside a frame payload. It returns the
// tenant the qualified name carries and the plain namespace; ok is false for a
// local namespace.
func StripRemoteNamespace(qualified string) (tenant, ns string, ok bool) {
	return remotens.Split(qualified)
}

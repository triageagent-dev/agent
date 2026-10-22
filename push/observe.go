package push

import "net/url"

// Observer receives a read-only view of every frame the Pusher queues for
// the core, after exclusion and tenant routing: the local TUI (agentui)
// builds its picture of the cluster from it. Observe runs on the collector
// and informer goroutines, so it must return quickly and must not keep or
// modify f or anything it points to beyond the call.
type Observer interface {
	// Observe is called once per frame per destination tenant. tenant is
	// the resolved tenant name, empty for a session-level frame.
	Observe(tenant string, f *Frame)
}

type observerBox struct{ o Observer }

// SetObserver attaches o to the Pusher; a nil o detaches it. Safe to call at
// any time, and a no-op on a nil Pusher.
func (p *Pusher) SetObserver(o Observer) {
	if p == nil {
		return
	}
	if o == nil {
		p.observer.Store(nil)
		return
	}
	p.observer.Store(&observerBox{o: o})
}

func (p *Pusher) observe(tenant string, f *Frame) {
	if b := p.observer.Load(); b != nil && f != nil {
		b.o.Observe(tenant, f)
	}
}

// Apps returns the virtual apps as last synthesized, or nil when virtual
// apps are off. The apps are shared with the sender: treat them as read-only.
func (p *Pusher) Apps() []*VirtualApp {
	if p == nil || p.vapps == nil {
		return nil
	}
	return p.vapps.snapshotApps()
}

// Link is the push connection's state, for the local TUI.
type Link struct {
	Connected     bool
	EverConnected bool
	// Host is the core's host[:port], never the full URL.
	Host            string
	Cluster         string
	Tenant          string
	Version         string
	Sent            map[string]uint64
	Dropped         uint64
	NoTenantDropped uint64
	// OutOfScopeDropped counts items dropped because their namespace is
	// outside the workspace scope the core set for their tenant.
	OutOfScopeDropped uint64
	// MalformedDropped counts entries dropped because the core would have
	// refused their whole frame, such as a service with no namespace.
	MalformedDropped uint64
	// Standalone: no core URL is set, so nothing is ever sent.
	Standalone bool
}

// Link reports the push connection's state.
func (p *Pusher) Link() Link {
	if p == nil {
		return Link{}
	}
	l := Link{
		Connected:  p.connected.Load(),
		Cluster:    p.opts.ClusterName,
		Tenant:     p.opts.Tenant,
		Version:    p.opts.AgentVersion,
		Standalone: p.opts.Endpoint == "",
		Sent:       map[string]uint64{},
	}
	if u, err := url.Parse(p.opts.Endpoint); err == nil {
		l.Host = u.Host
	}
	l.NoTenantDropped = p.noTenantDropped.Load()
	l.OutOfScopeDropped = p.outOfScopeDropped.Load()
	l.MalformedDropped = p.malformedDropped.Load()
	m := p.mux()
	m.mu.Lock()
	defer m.mu.Unlock()
	l.EverConnected = m.everConnected
	for _, w := range m.workers {
		l.Dropped += w.dropped
		for k, n := range w.sentByKind {
			l.Sent[k] += n
		}
	}
	return l
}

// Host reports whether this Pusher runs in host mode (NewHost).
func (p *Pusher) Host() bool { return p != nil && p.host }

// NoKubernetes reports whether this Pusher watches no Kubernetes objects
// (Options.NoKubernetes): its applications come from an app source, not from
// the cluster.
func (p *Pusher) NoKubernetes() bool { return p != nil && p.opts.NoKubernetes }

// AppTenants returns the tenant of each namespace the virtual apps live in,
// resolved the way a frame's tenant is. A namespace with no tenant is absent.
func (p *Pusher) AppTenants() map[string]string {
	out := map[string]string{}
	for _, a := range p.Apps() {
		if _, done := out[a.Namespace]; done {
			continue
		}
		if t := p.tenantForNamespace(a.Namespace); t != "" {
			out[a.Namespace] = t
		}
	}
	return out
}

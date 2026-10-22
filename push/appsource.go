// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// AppEvent is one workload upsert or delete delivered by an AppSource: the
// host-mode counterpart of a Deployment informer event (docs/adr/0018-linux-
// host-mode.md). One AppEvent names one workload, which the Pusher turns into
// one virtual app in its synthetic namespace.
type AppEvent struct {
	// Name is the workload name, and so the virtual app name.
	Name string
	// Deleted removes the workload; the other fields are then ignored.
	Deleted bool
	// Ready is the workload's readiness.
	Ready bool
	// Restarts is the workload's restart count. A change in it changes the
	// virtual app, so a restart that leaves Ready true still reaches the core
	// as an app_delta.
	Restarts uint64
	// Kind labels the workload in the app's object list (for example
	// "Service"); empty means "Service".
	Kind string
	// Replicas and ReadyReplicas are the workload's desired and ready pod
	// counts; both 0 when the source has none. They travel on the app's
	// component so the core can tell one replica of several down from all.
	Replicas, ReadyReplicas int64
	// Namespace places the workload. Empty means the host's one synthetic
	// namespace; an OTLP source (NoKubernetes) names the namespace its
	// service reports.
	Namespace string
}

// AppSource feeds a Pusher that has no Kubernetes informers with workloads: a
// host Pusher (NewHost) or a cluster Pusher with NoKubernetes.
type AppSource interface {
	// Watch delivers the current set and every later change through emit
	// until ctx ends. emit is safe to call from any goroutine.
	Watch(ctx context.Context, emit func(AppEvent)) error
}

// appSourceRetry is the pause before a failed source is started again.
const appSourceRetry = 5 * time.Second

// defaultAppKind is the object kind of a source workload without a Kind.
const defaultAppKind workloadKind = "Service"

// SetAppSource attaches src to a host Pusher (NewHost) or to a NoKubernetes
// cluster Pusher and switches virtual apps on, so the snapshot and the
// app_delta stream carry the source's workloads through the same state,
// debounce and diffing as the cluster path. Call it before Start. It is an
// error on a Pusher whose workloads come from informers.

func (p *Pusher) SetAppSource(src AppSource) error {
	if p == nil {
		return errors.New("push: SetAppSource on a nil Pusher")
	}
	if !p.host && !p.opts.NoKubernetes {
		return errors.New("push: SetAppSource needs a host Pusher (NewHost) or Options.NoKubernetes")
	}
	if src == nil {
		return errors.New("push: SetAppSource with a nil source")
	}
	p.opts.VirtualApps = VirtualAppsOn
	p.vapps = newVirtualAppsState(VirtualAppsOn, p.opts.ClusterName, p.opts.Namespaces,
		p.log.WithName("virtualapps"), p.enqueue)
	p.vapps.nsDefaultsFn = p.nsDefaults.get
	p.vapps.dynamicScope = p.dynamicScope
	// With no annotation watcher and no static list, the source's namespaces
	// are the scope, as for the receiver (Pusher.inScope).
	p.vapps.allNamespaces = p.opts.NoKubernetes && len(p.opts.Namespaces) == 0
	p.appSource = src
	return nil
}

// hostAppNamespace returns the namespace host workloads live in: the first (and only)
// static namespace.
func (p *Pusher) hostAppNamespace() string {
	if len(p.opts.Namespaces) > 0 {
		return p.opts.Namespaces[0]
	}
	return ""
}

// applyAppEvent turns one source event into a workload and feeds applyWorkload.
func (p *Pusher) applyAppEvent(ev AppEvent) {
	if p.vapps == nil || ev.Name == "" {
		return
	}
	kind := workloadKind(ev.Kind)
	if kind == "" {
		kind = defaultAppKind
	}
	ns := ev.Namespace
	if ns == "" {
		ns = p.hostAppNamespace()
	}
	key := workloadKey(kind, ns, ev.Name)
	if ev.Deleted {
		p.vapps.applyWorkload(key, nil, true)
		return
	}
	info := workloadInfo{Kind: kind, Namespace: ns, Name: ev.Name, Ready: ev.Ready,
		Replicas: ev.Replicas, ReadyReplicas: ev.ReadyReplicas}
	if ev.Restarts > 0 {
		info.Description = fmt.Sprintf("restarts: %d", ev.Restarts)
	}
	p.vapps.applyWorkload(key, &info, false)
}

// runAppSource runs the attached source until ctx ends, restarting it after an
// error so a transient failure of the source cannot end the transport.
func (p *Pusher) runAppSource(ctx context.Context) {
	for {
		err := p.appSource.Watch(ctx, p.applyAppEvent)
		if ctx.Err() != nil {
			return
		}
		p.log.Error(err, "app source ended; restarting")
		select {
		case <-ctx.Done():
			return
		case <-time.After(appSourceRetry):
		}
	}
}

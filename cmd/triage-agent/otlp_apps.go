// Copyright 2026 Denys Vasyliev.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"time"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
)

// otlpAppInterval is how often the OTLP receiver's metrics are read for apps.
const otlpAppInterval = 15 * time.Second

// otlpAppHub is the part of *sensorstream.Hub the app source reads: the
// metrics every OTLP source holds, and the list of the sources themselves.
type otlpAppHub interface {
	metricset.Source
	OTLPSources() []sensorstream.OTLPSource
}

// otlpApps is the push.AppSource of an agent that watches no Kubernetes objects
// (--kubernetes=false, docs/adr/0021-otlp-only-no-kubernetes-watch.md).
//
// Applications come from the workload metrics an OpenTelemetry Collector's
// k8s_cluster receiver sends over OTLP: a Deployment, StatefulSet or DaemonSet
// is one application, ready when its ready pods reach its desired pods, with
// both counts on the component. A workload scaled to zero makes no app.
//
// A Collector that sends no workload metrics leaves nothing to read readiness
// from, so each service an OTLP sender reports in a namespace is one
// application instead, ready while its metrics are fresh. The two are never
// mixed: workload metrics, once present, define the applications.
type otlpApps struct {
	hub   otlpAppHub
	every time.Duration
}

type otlpAppKey struct {
	kind      string
	namespace string
	name      string
}

// workloadMetrics names, per workload kind, the k8s_cluster receiver's desired
// and ready counts and the resource attribute that carries the workload's name.
var workloadMetrics = []struct {
	kind, nameAttr, desired, ready string
}{
	{"Deployment", "k8s.deployment.name", "k8s.deployment.desired", "k8s.deployment.available"},
	{"StatefulSet", "k8s.statefulset.name", "k8s.statefulset.desired_pods", "k8s.statefulset.ready_pods"},
	{"DaemonSet", "k8s.daemonset.name", "k8s.daemonset.desired_scheduled_nodes", "k8s.daemonset.ready_nodes"},
}

// workloadApps reads the workloads out of one source's metrics.
func workloadApps(set metricset.Set, into map[otlpAppKey]push.AppEvent) {
	for _, w := range workloadMetrics {
		desired := set.Get(w.desired)
		if desired == nil {
			continue
		}
		ready := map[otlpAppKey]float64{}
		if m := set.Get(w.ready); m != nil {
			for _, p := range m.Points {
				ready[otlpAppKey{w.kind, p.Attr("k8s.namespace.name"), p.Attr(w.nameAttr)}] = p.Value
			}
		}
		for _, p := range desired.Points {
			k := otlpAppKey{w.kind, p.Attr("k8s.namespace.name"), p.Attr(w.nameAttr)}
			if k.namespace == "" || k.name == "" || p.Value <= 0 {
				continue // nothing to place, or scaled to zero
			}
			want, have := int64(p.Value), int64(ready[k])
			into[k] = push.AppEvent{
				Name: k.name, Namespace: k.namespace, Kind: w.kind,
				Ready: have >= want, Replicas: want, ReadyReplicas: have,
			}
		}
	}
}

// serviceApps is the fallback: one app per (namespace, service) an OTLP sender
// reports.
func serviceApps(srcs []sensorstream.OTLPSource, into map[otlpAppKey]push.AppEvent) {
	for _, s := range srcs {
		if s.Service == "" || s.Namespace == "" {
			continue
		}
		into[otlpAppKey{"Service", s.Namespace, s.Service}] = push.AppEvent{
			Name: s.Service, Namespace: s.Namespace, Kind: "Service", Ready: true,
		}
	}
}

// current is the applications the hub's metrics define right now.
func (o otlpApps) current() map[otlpAppKey]push.AppEvent {
	out := map[otlpAppKey]push.AppEvent{}
	for _, target := range o.hub.Targets(metricset.RoleEBPF) {
		if set, err := o.hub.Set(metricset.RoleEBPF, target); err == nil {
			workloadApps(set, out)
		}
	}
	if len(out) == 0 {
		serviceApps(o.hub.OTLPSources(), out)
	}
	return out
}

// Watch implements push.AppSource.
func (o otlpApps) Watch(ctx context.Context, emit func(push.AppEvent)) error {
	every := o.every
	if every <= 0 {
		every = otlpAppInterval
	}
	known := map[otlpAppKey]push.AppEvent{}
	tick := func() {
		current := o.current()
		for k, ev := range current {
			if prev, ok := known[k]; !ok || prev != ev {
				emit(ev)
			}
		}
		for k, ev := range known {
			if _, ok := current[k]; !ok {
				emit(push.AppEvent{Name: ev.Name, Namespace: ev.Namespace, Kind: ev.Kind, Deleted: true})
			}
		}
		known = current
	}
	tick()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			tick()
		}
	}
}

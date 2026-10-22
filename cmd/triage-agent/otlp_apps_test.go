package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/triageagent-dev/agent/metricset"
	"github.com/triageagent-dev/agent/push"
	"github.com/triageagent-dev/agent/sensorstream"
)

type fakeAppHub struct {
	mu      sync.Mutex
	sets    map[string]metricset.Set
	sources []sensorstream.OTLPSource
}

func (f *fakeAppHub) Targets(role string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	if role == metricset.RoleEBPF {
		for t := range f.sets {
			out = append(out, t)
		}
	}
	return out
}

func (f *fakeAppHub) Set(role, target string) (metricset.Set, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sets[target], nil
}

func (f *fakeAppHub) OTLPSources() []sensorstream.OTLPSource {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sensorstream.OTLPSource(nil), f.sources...)
}

func (f *fakeAppHub) update(sets map[string]metricset.Set, srcs ...sensorstream.OTLPSource) {
	f.mu.Lock()
	f.sets, f.sources = sets, srcs
	f.mu.Unlock()
}

func gauge(name string, points ...metricset.Point) *metricset.Metric {
	return &metricset.Metric{Name: name, Kind: metricset.Gauge, Points: points}
}

func wl(ns, nameAttr, name string, v float64) metricset.Point {
	return metricset.Point{Attrs: map[string]string{"k8s.namespace.name": ns, nameAttr: name}, Value: v}
}

// TestWorkloadApps_ReadinessFromDesiredAndReady checks the k8s_cluster
// receiver's counts become apps: ready when ready pods reach desired, a
// partial rollout not ready with both counts, scaled-to-zero and unplaced
// workloads skipped, and all three kinds read.
func TestWorkloadApps_ReadinessFromDesiredAndReady(t *testing.T) {
	set := metricset.Set{
		"k8s.deployment.desired": gauge("k8s.deployment.desired",
			wl("shop", "k8s.deployment.name", "cart", 2),
			wl("shop", "k8s.deployment.name", "ad", 3),
			wl("shop", "k8s.deployment.name", "off", 0),
			wl("", "k8s.deployment.name", "noNamespace", 1)),
		"k8s.deployment.available": gauge("k8s.deployment.available",
			wl("shop", "k8s.deployment.name", "cart", 2),
			wl("shop", "k8s.deployment.name", "ad", 1)),
		"k8s.statefulset.desired_pods": gauge("k8s.statefulset.desired_pods", wl("shop", "k8s.statefulset.name", "db", 1)),
		"k8s.statefulset.ready_pods":   gauge("k8s.statefulset.ready_pods", wl("shop", "k8s.statefulset.name", "db", 1)),
		"k8s.daemonset.desired_scheduled_nodes": gauge("k8s.daemonset.desired_scheduled_nodes",
			wl("shop", "k8s.daemonset.name", "agent", 4)),
	}
	got := map[otlpAppKey]push.AppEvent{}
	workloadApps(set, got)

	want := map[otlpAppKey]push.AppEvent{
		{"Deployment", "shop", "cart"}: {Name: "cart", Namespace: "shop", Kind: "Deployment", Ready: true, Replicas: 2, ReadyReplicas: 2},
		{"Deployment", "shop", "ad"}:   {Name: "ad", Namespace: "shop", Kind: "Deployment", Ready: false, Replicas: 3, ReadyReplicas: 1},
		{"StatefulSet", "shop", "db"}:  {Name: "db", Namespace: "shop", Kind: "StatefulSet", Ready: true, Replicas: 1, ReadyReplicas: 1},
		{"DaemonSet", "shop", "agent"}: {Name: "agent", Namespace: "shop", Kind: "DaemonSet", Ready: false, Replicas: 4, ReadyReplicas: 0},
	}
	if len(got) != len(want) {
		t.Fatalf("apps = %v, want %v", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%v = %+v, want %+v", k, got[k], w)
		}
	}
}

// TestOTLPApps_WorkloadsDefineAppsElseServices pins the two modes: with
// workload metrics the services add nothing, without them each (namespace,
// service) is an app.
func TestOTLPApps_WorkloadsDefineAppsElseServices(t *testing.T) {
	srcs := []sensorstream.OTLPSource{
		{Key: "a", Service: "cart", Namespace: "shop"},
		{Key: "b", Service: "cart", Namespace: "shop"}, // another instance of the same service
		{Key: "c", Service: "otelcol", Namespace: "shop"},
		{Key: "d", Service: "x"}, // no namespace: nothing to place
	}
	hub := &fakeAppHub{}
	hub.update(nil, srcs...)
	got := otlpApps{hub: hub}.current()
	if len(got) != 2 || !got[otlpAppKey{"Service", "shop", "cart"}].Ready || !got[otlpAppKey{"Service", "shop", "otelcol"}].Ready {
		t.Fatalf("service fallback apps = %v, want cart and otelcol in shop", got)
	}

	hub.update(map[string]metricset.Set{"k8s": {
		"k8s.deployment.desired":   gauge("k8s.deployment.desired", wl("shop", "k8s.deployment.name", "cart", 1)),
		"k8s.deployment.available": gauge("k8s.deployment.available", wl("shop", "k8s.deployment.name", "cart", 1)),
	}}, srcs...)
	got = otlpApps{hub: hub}.current()
	if len(got) != 1 || got[otlpAppKey{"Deployment", "shop", "cart"}].Kind != "Deployment" {
		t.Fatalf("with workload metrics apps = %v, want only the cart Deployment", got)
	}
}

// TestOTLPApps_WatchEmitsChangesOnly checks a new app is emitted once, a
// readiness change is emitted again, and a vanished app is deleted.
func TestOTLPApps_WatchEmitsChangesOnly(t *testing.T) {
	deploy := func(desired, ready float64) map[string]metricset.Set {
		return map[string]metricset.Set{"k8s": {
			"k8s.deployment.desired":   gauge("k8s.deployment.desired", wl("shop", "k8s.deployment.name", "cart", desired)),
			"k8s.deployment.available": gauge("k8s.deployment.available", wl("shop", "k8s.deployment.name", "cart", ready)),
		}}
	}
	hub := &fakeAppHub{}
	hub.update(deploy(2, 2))
	var mu sync.Mutex
	var events []push.AppEvent
	emit := func(e push.AppEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = otlpApps{hub: hub, every: 10 * time.Millisecond}.Watch(ctx, emit)
		close(done)
	}()
	waitFor := func(n int) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := len(events)
			mu.Unlock()
			if got >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("waited for %d events, have %d", n, len(events))
	}
	waitFor(1)
	time.Sleep(60 * time.Millisecond) // more ticks: nothing new may be emitted
	mu.Lock()
	if len(events) != 1 || !events[0].Ready {
		t.Fatalf("events = %+v, want one ready cart", events)
	}
	mu.Unlock()

	hub.update(deploy(2, 1))
	waitFor(2)
	mu.Lock()
	if events[1].Ready || events[1].ReadyReplicas != 1 {
		t.Errorf("after a pod went down: %+v, want not ready with 1 of 2", events[1])
	}
	mu.Unlock()

	hub.update(nil)
	waitFor(3)
	mu.Lock()
	if !events[2].Deleted || events[2].Name != "cart" {
		t.Errorf("after the workload left: %+v, want a delete of cart", events[2])
	}
	mu.Unlock()
	cancel()
	<-done
}
